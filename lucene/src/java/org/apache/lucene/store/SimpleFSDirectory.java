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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.lucene.util.IOUtils;


/** A straightforward implementation of {@link FSDirectory}
 *  using java.io.RandomAccessFile.  However, this class has
 *  poor concurrent performance (multiple threads will
 *  bottleneck) as it synchronizes when multiple threads
 *  read from the same file.  It's usually better to use
 *  {@link NIOFSDirectory} or {@link MMapDirectory} instead. */
public class SimpleFSDirectory extends FSDirectory {
    
  /** Create a new SimpleFSDirectory for the named location.
   *
   * @param path the path of the directory
   * @param lockFactory the lock factory to use, or null for the default
   * ({@link NativeFSLockFactory});
   * @throws IOException
   */
  public SimpleFSDirectory(File path, LockFactory lockFactory) throws IOException {
    super(path, lockFactory);
  }
  
  /** Create a new SimpleFSDirectory for the named location and {@link NativeFSLockFactory}.
   *
   * @param path the path of the directory
   * @throws IOException
   */
  public SimpleFSDirectory(File path) throws IOException {
    super(path, null);
  }

  /** Creates an IndexInput for the file with the given name. */
  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    return new SimpleFSIndexInput(new File(directory, name), context, getReadChunkSize());
  }
  
  @Override
  public CompoundFileDirectory openCompoundInput(String name, IOContext context) throws IOException {
    return new SimpleFSCompoundFileDirectory(name, context);
  }

  private final class SimpleFSCompoundFileDirectory extends CompoundFileDirectory {
    private SimpleFSIndexInput.Descriptor fd;

    public SimpleFSCompoundFileDirectory(String fileName, IOContext context) throws IOException {
      super(SimpleFSDirectory.this, fileName, context);
      IndexInput stream = null;
      try {
        final File f = new File(SimpleFSDirectory.this.getDirectory(), fileName);
        fd = new SimpleFSIndexInput.Descriptor(f, "r");
        stream = new SimpleFSIndexInput(fd, 0, fd.length, readBufferSize,
            getReadChunkSize());
        initForRead(CompoundFileDirectory.readEntries(stream, SimpleFSDirectory.this, fileName));
        stream.close();
      } catch (IOException e) {
        // throw our original exception
        IOUtils.closeSafely(e, fd, stream);
      }
    }

    @Override
    public IndexInput openInputSlice(String id, long offset, long length, int readBufferSize) throws IOException {
      return new SimpleFSIndexInput(fd, offset, length, readBufferSize, getReadChunkSize());
    }

    @Override
    public synchronized void close() throws IOException {
      try {
        fd.close();
      } finally {
        super.close();
      }
    }
  }

  protected static class SimpleFSIndexInput extends BufferedIndexInput {
  
    protected static class Descriptor extends RandomAccessFile {
      // remember if the file is open, so that we don't try to close it
      // more than once
      protected volatile boolean isOpen;
      long position;
      final long length;
      
      public Descriptor(File file, String mode) throws IOException {
        super(file, mode);
        isOpen=true;
        length=length();
      }
  
      @Override
      public void close() throws IOException {
        if (isOpen) {
          isOpen=false;
          super.close();
        }
      }
    }
  
    protected final Descriptor file;
    boolean isClone;
    //  LUCENE-1566 - maximum read length on a 32bit JVM to prevent incorrect OOM 
    protected final int chunkSize;
    protected final long off;
    protected final long end;
    
    public SimpleFSIndexInput(File path, IOContext context, int chunkSize) throws IOException {
      super(context);
      this.file = new Descriptor(path, "r"); 
      this.chunkSize = chunkSize;
      this.off = 0L;
      this.end = file.length;
    }
    
    public SimpleFSIndexInput(Descriptor file, long off, long length, int bufferSize, int chunkSize) throws IOException {
      super(bufferSize);
      this.file = file;
      this.chunkSize = chunkSize;
      this.off = off;
      this.end = off + length;
      this.isClone = true; // well, we are sorta?
    }
  
    /** IndexInput methods */
    @Override
    protected void readInternal(byte[] b, int offset, int len)
         throws IOException {
      synchronized (file) {
        long position = off + getFilePointer();
        if (position != file.position) {
          file.seek(position);
          file.position = position;
        }
        int total = 0;

        if (position + len > end) {
          throw new IOException("read past EOF");
        }

        try {
          do {
            final int readLength;
            if (total + chunkSize > len) {
              readLength = len - total;
            } else {
              // LUCENE-1566 - work around JVM Bug by breaking very large reads into chunks
              readLength = chunkSize;
            }
            final int i = file.read(b, offset + total, readLength);
            file.position += i;
            total += i;
          } while (total < len);
        } catch (OutOfMemoryError e) {
          // propagate OOM up and add a hint for 32bit VM Users hitting the bug
          // with a large chunk size in the fast path.
          final OutOfMemoryError outOfMemoryError = new OutOfMemoryError(
              "OutOfMemoryError likely caused by the Sun VM Bug described in "
              + "https://issues.apache.org/jira/browse/LUCENE-1566; try calling FSDirectory.setReadChunkSize "
              + "with a value smaller than the current chunk size (" + chunkSize + ")");
          outOfMemoryError.initCause(e);
          throw outOfMemoryError;
        }
      }
    }
  
    @Override
    public void close() throws IOException {
      // only close the file if this is not a clone
      if (!isClone) file.close();
    }
  
    @Override
    protected void seekInternal(long position) {
    }
  
    @Override
    public long length() {
      return end - off;
    }
  
    @Override
    public Object clone() {
      SimpleFSIndexInput clone = (SimpleFSIndexInput)super.clone();
      clone.isClone = true;
      return clone;
    }
  
    /** Method used for testing. Returns true if the underlying
     *  file descriptor is valid.
     */
    boolean isFDValid() throws IOException {
      return file.getFD().valid();
    }
    
    @Override
    public void copyBytes(IndexOutput out, long numBytes) throws IOException {
      numBytes -= flushBuffer(out, numBytes);
      // If out is FSIndexOutput, the copy will be optimized
      out.copyBytes(this, numBytes);
    }
  }
}
