package org.apache.lucene.store;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException; // javadoc @link
import java.nio.channels.FileChannel;
import java.util.concurrent.Future; // javadoc

import org.apache.lucene.store.SimpleFSDirectory.SimpleFSIndexInput;
import org.apache.lucene.util.IOUtils;

/**
 * An {@link FSDirectory} implementation that uses java.nio's FileChannel's
 * positional read, which allows multiple threads to read from the same file
 * without synchronizing.
 * <p>
 * This class only uses FileChannel when reading; writing is achieved with
 * {@link FSDirectory.FSIndexOutput}.
 * <p>
 * <b>NOTE</b>: NIOFSDirectory is not recommended on Windows because of a bug in
 * how FileChannel.read is implemented in Sun's JRE. Inside of the
 * implementation the position is apparently synchronized. See <a
 * href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6265734">here</a>
 * for details.
 * </p>
 * <p>
 * <font color="red"><b>NOTE:</b> Accessing this class either directly or
 * indirectly from a thread while it's interrupted can close the
 * underlying file descriptor immediately if at the same time the thread is
 * blocked on IO. The file descriptor will remain closed and subsequent access
 * to {@link NIOFSDirectory} will throw a {@link ClosedChannelException}. If
 * your application uses either {@link Thread#interrupt()} or
 * {@link Future#cancel(boolean)} you should use {@link SimpleFSDirectory} in
 * favor of {@link NIOFSDirectory}.</font>
 * </p>
 */
public class NIOFSDirectory extends FSDirectory {

  /** Create a new NIOFSDirectory for the named location.
   * 
   * @param path the path of the directory
   * @param lockFactory the lock factory to use, or null for the default
   * ({@link NativeFSLockFactory});
   * @throws IOException
   */
  public NIOFSDirectory(File path, LockFactory lockFactory) throws IOException {
    super(path, lockFactory);
  }

  /** Create a new NIOFSDirectory for the named location and {@link NativeFSLockFactory}.
   *
   * @param path the path of the directory
   * @throws IOException
   */
  public NIOFSDirectory(File path) throws IOException {
    super(path, null);
  }

  /** Creates an IndexInput for the file with the given name. */
  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    return new NIOFSIndexInput(new File(getDirectory(), name), context, getReadChunkSize());
  }
  
  @Override
  public CompoundFileDirectory openCompoundInput(String name, IOContext context) throws IOException {
    return new NIOFSCompoundFileDirectory(name, context);
  }

  private final class NIOFSCompoundFileDirectory extends CompoundFileDirectory {
    private SimpleFSIndexInput.Descriptor fd;
    private FileChannel fc;

    public NIOFSCompoundFileDirectory(String fileName, IOContext context) throws IOException {
      super(NIOFSDirectory.this, fileName, context);
      IndexInput stream = null;
      try {
        File f = new File(NIOFSDirectory.this.getDirectory(), fileName);
        fd = new SimpleFSIndexInput.Descriptor(f, "r");
        fc = fd.getChannel();
        stream = new NIOFSIndexInput(fd, fc, 0, fd.length, readBufferSize,
            getReadChunkSize());
        initForRead(CompoundFileDirectory.readEntries(stream, NIOFSDirectory.this, fileName));
        stream.close();
      } catch (IOException e) {
        // throw our original exception
        IOUtils.closeSafely(e, fc, fd, stream);
      }
    }
    
    @Override
    public IndexInput openInputSlice(String id, long offset, long length, int readBufferSize) throws IOException {
      return new NIOFSIndexInput(fd, fc, offset, length, readBufferSize, getReadChunkSize());
    }

    @Override
    public synchronized void close() throws IOException {
      try {
        IOUtils.closeSafely(false, fc, fd);
      } finally {
        super.close();
      }
    }
  }

  protected static class NIOFSIndexInput extends SimpleFSDirectory.SimpleFSIndexInput {

    private ByteBuffer byteBuf; // wraps the buffer for NIO

    private byte[] otherBuffer;
    private ByteBuffer otherByteBuf;

    final FileChannel channel;

    public NIOFSIndexInput(File path, IOContext context, int chunkSize) throws IOException {
      super(path, context, chunkSize);
      channel = file.getChannel();
    }
    
    public NIOFSIndexInput(Descriptor file, FileChannel fc, long off, long length, int bufferSize, int chunkSize) throws IOException {
      super(file, off, length, bufferSize, chunkSize);
      channel = fc;
      isClone = true;
    }

    @Override
    protected void newBuffer(byte[] newBuffer) {
      super.newBuffer(newBuffer);
      byteBuf = ByteBuffer.wrap(newBuffer);
    }

    @Override
    public void close() throws IOException {
      if (!isClone && file.isOpen) {
        // Close the channel & file
        try {
          channel.close();
        } finally {
          file.close();
        }
      }
    }

    @Override
    protected void readInternal(byte[] b, int offset, int len) throws IOException {

      final ByteBuffer bb;

      // Determine the ByteBuffer we should use
      if (b == buffer && 0 == offset) {
        // Use our own pre-wrapped byteBuf:
        assert byteBuf != null;
        byteBuf.clear();
        byteBuf.limit(len);
        bb = byteBuf;
      } else {
        if (offset == 0) {
          if (otherBuffer != b) {
            // Now wrap this other buffer; with compound
            // file, we are repeatedly called with its
            // buffer, so we wrap it once and then re-use it
            // on subsequent calls
            otherBuffer = b;
            otherByteBuf = ByteBuffer.wrap(b);
          } else
            otherByteBuf.clear();
          otherByteBuf.limit(len);
          bb = otherByteBuf;
        } else {
          // Always wrap when offset != 0
          bb = ByteBuffer.wrap(b, offset, len);
        }
      }

      int readOffset = bb.position();
      int readLength = bb.limit() - readOffset;
      assert readLength == len;

      long pos = getFilePointer() + off;
      
      if (pos + len > end) {
        throw new IOException("read past EOF");
      }

      try {
        while (readLength > 0) {
          final int limit;
          if (readLength > chunkSize) {
            // LUCENE-1566 - work around JVM Bug by breaking
            // very large reads into chunks
            limit = readOffset + chunkSize;
          } else {
            limit = readOffset + readLength;
          }
          bb.limit(limit);
          int i = channel.read(bb, pos);
          pos += i;
          readOffset += i;
          readLength -= i;
        }
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

}
