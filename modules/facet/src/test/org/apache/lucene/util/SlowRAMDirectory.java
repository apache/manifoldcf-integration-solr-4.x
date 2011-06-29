package org.apache.lucene.util;

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.ThreadInterruptedException;

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

/**
 * Test utility - slow directory
 */
public class SlowRAMDirectory extends RAMDirectory {

  private static final int IO_SLEEP_THRESHOLD = 50;
  
  private Random random;
  private int sleepMillis;

  public void setSleepMillis(int sleepMillis) {
    this.sleepMillis = sleepMillis;
  }
  
  public SlowRAMDirectory(int sleepMillis, Random random) {
    this.sleepMillis = sleepMillis;
    this.random = random;
  }

  @Override
  public IndexOutput createOutput(String name) throws IOException {
    if (sleepMillis != -1) {
      return new SlowIndexOutput(super.createOutput(name));
    } 

    return super.createOutput(name);
  }

  @Override
  public IndexInput openInput(String name) throws IOException {
    if (sleepMillis != -1) {
      return new SlowIndexInput(super.openInput(name));
    } 
    return super.openInput(name);
  }

  @Override
  public IndexInput openInput(String name, int bufferSize) throws IOException {
    if (sleepMillis != -1) {
      return new SlowIndexInput(super.openInput(name, bufferSize));
    } 
    return super.openInput(name, bufferSize);
  }

  void doSleep(int length) {
    int sTime = length<10 ? sleepMillis : (int) (sleepMillis * Math.log(length));
    if (random!=null) {
      sTime = random.nextInt(sTime);
    }
    try {
      Thread.sleep(sTime);
    } catch (InterruptedException e) {
      throw new ThreadInterruptedException(e);
    }
  }

  /**
   * Delegate class to wrap an IndexInput and delay reading bytes by some
   * specified time.
   */
  private class SlowIndexInput extends IndexInput {
    private IndexInput ii;
    private int numRead = 0;
    
    public SlowIndexInput(IndexInput ii) {
      this.ii = ii;
    }
    
    @Override
    public byte readByte() throws IOException {
      if (numRead >= IO_SLEEP_THRESHOLD) {
        doSleep(0);
        numRead = 0;
      }
      ++numRead;
      return ii.readByte();
    }
    
    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      if (numRead >= IO_SLEEP_THRESHOLD) {
        doSleep(len);
        numRead = 0;
      }
      numRead += len;
      ii.readBytes(b, offset, len);
    }
    
    @Override public Object clone() { return ii.clone(); }
    @Override public void close() throws IOException { ii.close(); }
    @Override public boolean equals(Object o) { return ii.equals(o); }
    @Override public long getFilePointer() { return ii.getFilePointer(); }
    @Override public int hashCode() { return ii.hashCode(); }
    @Override public long length() { return ii.length(); }
    @Override public void seek(long pos) throws IOException { ii.seek(pos); }
    
  }
  
  /**
   * Delegate class to wrap an IndexOutput and delay writing bytes by some
   * specified time.
   */
  private class SlowIndexOutput extends IndexOutput {
    
    private IndexOutput io;
    private int numWrote;
    
    public SlowIndexOutput(IndexOutput io) {
      this.io = io;
    }
    
    @Override
    public void writeByte(byte b) throws IOException {
      if (numWrote >= IO_SLEEP_THRESHOLD) {
        doSleep(0);
        numWrote = 0;
      }
      ++numWrote;
      io.writeByte(b);
    }
    
    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      if (numWrote >= IO_SLEEP_THRESHOLD) {
        doSleep(length);
        numWrote = 0;
      }
      numWrote += length;
      io.writeBytes(b, offset, length);
    }
    
    @Override public void close() throws IOException { io.close(); }
    @Override public void flush() throws IOException { io.flush(); }
    @Override public long getFilePointer() { return io.getFilePointer(); }
    @Override public long length() throws IOException { return io.length(); }
    @Override public void seek(long pos) throws IOException { io.seek(pos); }
  }
  
}
