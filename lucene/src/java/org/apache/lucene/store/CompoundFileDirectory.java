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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.util.IOUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Class for accessing a compound stream.
 * This class implements a directory, but is limited to only read operations.
 * Directory methods that would normally modify data throw an exception.
 * @lucene.experimental
 */
public final class CompoundFileDirectory extends Directory {
  
  /** Offset/Length for a slice inside of a compound file */
  public static final class FileEntry {
    long offset;
    long length;
  }
  
  private final Directory directory;
  private final String fileName;
  protected final int readBufferSize;  
  private final Map<String,FileEntry> entries;
  private final boolean openForWrite;
  private static final Map<String,FileEntry> SENTINEL = Collections.emptyMap();
  private final CompoundFileWriter writer;
  private final IndexInputSlicer handle;
  
  /**
   * Create a new CompoundFileDirectory.
   */
  public CompoundFileDirectory(Directory directory, String fileName, IOContext context, boolean openForWrite) throws IOException {
    this.directory = directory;
    this.fileName = fileName;
    this.readBufferSize = BufferedIndexInput.bufferSize(context);
    this.isOpen = false;
    this.openForWrite = openForWrite;
    if (!openForWrite) {
      boolean success = false;
      handle = directory.createSlicer(fileName, context);
      try {
        this.entries = readEntries(handle, directory, fileName);
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeSafely(true, handle);
        }
      }
      this.isOpen = true;
      writer = null;
    } else {
      assert !(directory instanceof CompoundFileDirectory) : "compound file inside of compound file: " + fileName;
      this.entries = SENTINEL;
      this.isOpen = true;
      writer = new CompoundFileWriter(directory, fileName);
      handle = null;
    }
  }

  /** Helper method that reads CFS entries from an input stream */
  private static final Map<String, FileEntry> readEntries(
      IndexInputSlicer handle, Directory dir, String name) throws IOException {
    // read the first VInt. If it is negative, it's the version number
    // otherwise it's the count (pre-3.1 indexes)
    final IndexInput stream = handle.openFullSlice();
    final Map<String, FileEntry> mapping;
    boolean success = false;
    try {
      final int firstInt = stream.readVInt();
      if (firstInt == CompoundFileWriter.FORMAT_CURRENT) {
        IndexInput input = null;
        try {
          input = dir.openInput(IndexFileNames.segmentFileName(
              IndexFileNames.stripExtension(name), "",
              IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION),
              IOContext.READONCE);
          final int readInt = input.readInt(); // unused right now
          assert readInt == CompoundFileWriter.ENTRY_FORMAT_CURRENT;
          final int numEntries = input.readVInt();
          mapping = new HashMap<String, CompoundFileDirectory.FileEntry>(
              numEntries);
          for (int i = 0; i < numEntries; i++) {
            final FileEntry fileEntry = new FileEntry();
            mapping.put(input.readString(), fileEntry);
            fileEntry.offset = input.readLong();
            fileEntry.length = input.readLong();
          }
          return mapping;
        } finally {
          IOUtils.closeSafely(true, input);
        }
      } else {
        // TODO remove once 3.x is not supported anymore
        mapping = readLegacyEntries(stream, firstInt);
      }
      success = true;
      return mapping;
    } finally {
      IOUtils.closeSafely(!success, stream);
    }
  }

  private static Map<String, FileEntry> readLegacyEntries(IndexInput stream,
      int firstInt) throws CorruptIndexException, IOException {
    final Map<String,FileEntry> entries = new HashMap<String,FileEntry>();
    final int count;
    final boolean stripSegmentName;
    if (firstInt < CompoundFileWriter.FORMAT_PRE_VERSION) {
      if (firstInt < CompoundFileWriter.FORMAT_CURRENT) {
        throw new CorruptIndexException("Incompatible format version: "
            + firstInt + " expected " + CompoundFileWriter.FORMAT_CURRENT);
      }
      // It's a post-3.1 index, read the count.
      count = stream.readVInt();
      stripSegmentName = false;
    } else {
      count = firstInt;
      stripSegmentName = true;
    }
    
    // read the directory and init files
    long streamLength = stream.length();
    FileEntry entry = null;
    for (int i=0; i<count; i++) {
      long offset = stream.readLong();
      if (offset < 0 || offset > streamLength) {
        throw new CorruptIndexException("Invalid CFS entry offset: " + offset);
      }
      String id = stream.readString();
      
      if (stripSegmentName) {
        // Fix the id to not include the segment names. This is relevant for
        // pre-3.1 indexes.
        id = IndexFileNames.stripSegmentName(id);
      }
      
      if (entry != null) {
        // set length of the previous entry
        entry.length = offset - entry.offset;
      }
      
      entry = new FileEntry();
      entry.offset = offset;
      entries.put(id, entry);
    }
    
    // set the length of the final entry
    if (entry != null) {
      entry.length = streamLength - entry.offset;
    }
    
    return entries;
  }
  
  public Directory getDirectory() {
    return directory;
  }
  
  public String getName() {
    return fileName;
  }
  
  @Override
  public synchronized void close() throws IOException {
    if (!isOpen) {
      // allow double close - usually to be consistent with other closeables
      return; // already closed
     }
    isOpen = false;
    if (writer != null) {
      assert openForWrite;
      writer.close();
    } else {
      IOUtils.closeSafely(false, handle);
    }
  }
  
  @Override
  public synchronized IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    assert !openForWrite;
    final String id = IndexFileNames.stripSegmentName(name);
    final FileEntry entry = entries.get(id);
    if (entry == null) {
      throw new IOException("No sub-file with id " + id + " found (fileName=" + name + " files: " + entries.keySet() + ")");
    }
    return handle.openSlice(entry.offset, entry.length);
  }
  
  /** Returns an array of strings, one for each file in the directory. */
  @Override
  public String[] listAll() {
    ensureOpen();
    String[] res;
    if (writer != null) {
      res = writer.listAll(); 
    } else {
      res = entries.keySet().toArray(new String[entries.size()]);
      // Add the segment name
      String seg = fileName.substring(0, fileName.indexOf('.'));
      for (int i = 0; i < res.length; i++) {
        res[i] = seg + res[i];
      }
    }
    return res;
  }
  
  /** Returns true iff a file with the given name exists. */
  @Override
  public boolean fileExists(String name) {
    ensureOpen();
    if (this.writer != null) {
      return writer.fileExists(name);
    }
    return entries.containsKey(IndexFileNames.stripSegmentName(name));
  }
  
  
  /** Returns the time the compound file was last modified. */
  @Override
  public long fileModified(String name) throws IOException {
    ensureOpen();
    return directory.fileModified(fileName);
  }
  
  /** Not implemented
   * @throws UnsupportedOperationException */
  @Override
  public void deleteFile(String name) {
    throw new UnsupportedOperationException();
  }
  
  /** Not implemented
   * @throws UnsupportedOperationException */
  public void renameFile(String from, String to) {
    throw new UnsupportedOperationException();
  }
  
  /** Returns the length of a file in the directory.
   * @throws IOException if the file does not exist */
  @Override
  public long fileLength(String name) throws IOException {
    ensureOpen();
    if (this.writer != null) {
      return writer.fileLenght(name);
    }
    FileEntry e = entries.get(IndexFileNames.stripSegmentName(name));
    if (e == null)
      throw new FileNotFoundException(name);
    return e.length;
  }
  
  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();
    return writer.createOutput(name, context);
  }
  
  @Override
  public void sync(Collection<String> names) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  /** Not implemented
   * @throws UnsupportedOperationException */
  @Override
  public Lock makeLock(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexInputSlicer createSlicer(final String name, IOContext context)
      throws IOException {
    ensureOpen();
    assert !openForWrite;
    final String id = IndexFileNames.stripSegmentName(name);
    final FileEntry entry = entries.get(id);
    if (entry == null) {
      throw new IOException("No sub-file with id " + id + " found (fileName=" + name + " files: " + entries.keySet() + ")");
    }
    return new IndexInputSlicer() {
      @Override
      public void close() throws IOException {
      }
      
      @Override
      public IndexInput openSlice(long offset, long length) throws IOException {
        return handle.openSlice(entry.offset + offset, length);
      }

      @Override
      public IndexInput openFullSlice() throws IOException {
        return openSlice(0, entry.length);
      }
    };
  }
}
