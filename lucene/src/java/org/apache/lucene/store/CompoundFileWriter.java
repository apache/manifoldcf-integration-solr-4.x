package org.apache.lucene.store;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergePolicy.MergeAbortedException;
import org.apache.lucene.util.IOUtils;

/**
 * Combines multiple files into a single compound file.
 * 
 * The file format data file:<br>
 * <ul>
 * <li>VInt Version</li>
 * <li>{File Data} fileCount entries with the raw data of the corresponding file
 * </li>
 * <ul>
 * File format entry table:<br>
 * <ul>
 * <li>int Version</li>
 * <li>VInt fileCount - number of entries with the following structure:</li>
 * <ul>
 * <li>String fileName</li>
 * <li>long dataOffset</li>
 * <li>long dataLength</li>
 * </ul>
 * </li> </ul> The fileCount integer indicates how many files are contained in
 * this compound file. The entry table that follows has that many entries. Each
 * directory entry contains a long pointer to the start of this file's data
 * section, the files length, and a String with that file's name.
 * 
 * @lucene.internal
 */
final class CompoundFileWriter {

  private static final class FileEntry {
    /** source file */
    String file;
    long length;
    /** temporary holder for the start of this file's data section */
    long offset;
    /** the directory which contains the file. */
    Directory dir;
  }

  // Before versioning started.
  static final int FORMAT_PRE_VERSION = 0;

  // Segment name is not written in the file names.
  static final int FORMAT_NO_SEGMENT_PREFIX = -1;
  static final int FORMAT_APPEND_FILES = -2;

  static final int ENTRY_FORMAT_CURRENT = -1;

  // NOTE: if you introduce a new format, make it 1 lower
  // than the current one, and always change this if you
  // switch to a new format!
  /** @lucene.internal */
  static final int FORMAT_CURRENT = FORMAT_APPEND_FILES;

  private final Directory directory;
  private final Map<String, FileEntry> entries = new HashMap<String, FileEntry>();
  // all entries that are written to a sep. file but not yet moved into CFS
  private final Queue<FileEntry> pendingEntries = new LinkedList<FileEntry>();
  private boolean closed = false;
  private volatile IndexOutput dataOut;
  private final AtomicBoolean outputTaken = new AtomicBoolean(false);
  private final String entryTableName;
  private final String dataFileName;

  /**
   * Create the compound stream in the specified file. The file name is the
   * entire name (no extensions are added).
   * 
   * @throws NullPointerException
   *           if <code>dir</code> or <code>name</code> is null
   */
  CompoundFileWriter(Directory dir, String name) {
    if (dir == null)
      throw new NullPointerException("directory cannot be null");
    if (name == null)
      throw new NullPointerException("name cannot be null");
    directory = dir;
    entryTableName = IndexFileNames.segmentFileName(
        IndexFileNames.stripExtension(name), "",
        IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION);
    dataFileName = name;
  }

  /** Returns the directory of the compound file. */
  Directory getDirectory() {
    return directory;
  }

  /** Returns the name of the compound file. */
  String getName() {
    return dataFileName;
  }

  /**
   * Closes all resources and writes the entry table
   * 
   * @throws IllegalStateException
   *           if close() had been called before or if no file has been added to
   *           this object
   */
  void close() throws IOException {
    if (closed) {
      throw new IllegalStateException("already closed");
    }
    IOException priorException = null;
    IndexOutput entryTableOut = null;
    try {
      if (entries.isEmpty()) {
        throw new IllegalStateException("CFS has no entries");
      }
      
      if (!pendingEntries.isEmpty() || outputTaken.get()) {
        throw new IllegalStateException("CFS has pending open files");
      }
      closed = true;
      // open the compound stream
      assert dataOut != null;
      long finalLength = dataOut.getFilePointer();
      assert assertFileLength(finalLength, dataOut);
      entryTableOut = directory.createOutput(entryTableName);
      writeEntryTable(entries.values(), entryTableOut);
    } catch (IOException e) {
      priorException = e;
    } finally {
      IOUtils.closeSafely(priorException, dataOut, entryTableOut);
    }
  }

  private static boolean assertFileLength(long expected, IndexOutput out)
      throws IOException {
    out.flush();
    assert expected == out.length() : "expected: " + expected + " was "
        + out.length();
    return true;
  }

  private final void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("CFS Directory is already closed");
    }
  }

  /**
   * Copy the contents of the file with specified extension into the provided
   * output stream.
   */
  private final long copyFileEntry(IndexOutput dataOut, FileEntry fileEntry)
      throws IOException, MergeAbortedException {
    final IndexInput is = fileEntry.dir.openInput(fileEntry.file);
    try {
      final long startPtr = dataOut.getFilePointer();
      final long length = fileEntry.length;
      dataOut.copyBytes(is, length);
      // Verify that the output length diff is equal to original file
      long endPtr = dataOut.getFilePointer();
      long diff = endPtr - startPtr;
      if (diff != length)
        throw new IOException("Difference in the output file offsets " + diff
            + " does not match the original file length " + length);
      fileEntry.offset = startPtr;
      // copy successful - delete file
      fileEntry.dir.deleteFile(fileEntry.file);
      return length;
    } finally {
      is.close();
    }

  }

  protected void writeEntryTable(Collection<FileEntry> entries,
      IndexOutput entryOut) throws IOException {
    entryOut.writeInt(ENTRY_FORMAT_CURRENT);
    entryOut.writeVInt(entries.size());
    for (FileEntry fe : entries) {
      entryOut.writeString(IndexFileNames.stripSegmentName(fe.file));
      entryOut.writeLong(fe.offset);
      entryOut.writeLong(fe.length);
    }
  }

  IndexOutput createOutput(String name) throws IOException {
    ensureOpen();
    boolean success = false;
    try {
      assert name != null : "name must not be null";
      if (entries.containsKey(name)) {
        throw new IllegalArgumentException("File " + name + " already exists");
      }
      final FileEntry entry = new FileEntry();
      entry.file = name;
      entries.put(name, entry);
      final DirectCFSIndexOutput out;
      if (outputTaken.compareAndSet(false, true)) {
        initDataOut();
        success = true;
        out = new DirectCFSIndexOutput(dataOut, entry, false);
      } else {
        entry.dir = this.directory;
        if (directory.fileExists(name)) {
          throw new IOException("File already exists");
        }
        out = new DirectCFSIndexOutput(directory.createOutput(name), entry,
            true);
      }
      success = true;
      return out;
    } finally {
      if (!success) {
        entries.remove(name);
      }
    }
  }

  final void releaseOutputLock() {
    outputTaken.compareAndSet(true, false);
  }

  private synchronized final void initDataOut() throws IOException {
    if (dataOut == null) {
      boolean success = false;
      try {
        dataOut = directory.createOutput(dataFileName);
        dataOut.writeVInt(FORMAT_CURRENT);
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeSafely(true, dataOut);
        }
      }
    }
  }

  private final void prunePendingEntries() throws IOException {
    // claim the output and copy all pending files in
    if (outputTaken.compareAndSet(false, true)) {
      try {
        while (!pendingEntries.isEmpty()) {
          FileEntry entry = pendingEntries.poll();
          copyFileEntry(dataOut, entry);
          entries.put(entry.file, entry);
        }
      } finally {
        final boolean compareAndSet = outputTaken.compareAndSet(true, false);
        assert compareAndSet;
      }
    }
  }

  long fileLenght(String name) throws IOException {
    FileEntry fileEntry = entries.get(name);
    if (fileEntry == null) {
      throw new FileNotFoundException(name + " does not exist");
    }
    return fileEntry.length;
  }

  boolean fileExists(String name) {
    return entries.containsKey(name);
  }

  String[] listAll() {
    return entries.keySet().toArray(new String[0]);
  }

  private final class DirectCFSIndexOutput extends IndexOutput {
    private final IndexOutput delegate;
    private final long offset;
    private boolean closed;
    private FileEntry entry;
    private long writtenBytes;
    private final boolean isSeparate;

    DirectCFSIndexOutput(IndexOutput delegate, FileEntry entry,
        boolean isSeparate) {
      super();
      this.delegate = delegate;
      this.entry = entry;
      entry.offset = offset = delegate.getFilePointer();
      this.isSeparate = isSeparate;

    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
        entry.length = writtenBytes;
        if (isSeparate) {
          // we are a separate file - push into the pending entries
          pendingEntries.add(entry);
        } else {
          // we have been written into the CFS directly - release the lock
          releaseOutputLock();
        }
        // now prune all pending entries and push them into the CFS
        prunePendingEntries();
      }
    }

    @Override
    public long getFilePointer() {
      return delegate.getFilePointer() - offset;
    }

    @Override
    public void seek(long pos) throws IOException {
      assert !closed;
      delegate.seek(offset + pos);
    }

    @Override
    public long length() throws IOException {
      assert !closed;
      return delegate.length() - offset;
    }

    @Override
    public void writeByte(byte b) throws IOException {
      assert !closed;
      writtenBytes++;
      delegate.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      assert !closed;
      writtenBytes += length;
      delegate.writeBytes(b, offset, length);
    }
  }

}
