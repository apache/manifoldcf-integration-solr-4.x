package org.apache.lucene.facet.taxonomy.writercache.cl2o;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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
 * Similar to {@link StringBuilder}, but with a more efficient growing strategy.
 * This class uses char array blocks to grow.
 * 
 * @lucene.experimental
 */
class CharBlockArray implements Appendable, Serializable, CharSequence {

  private static final long serialVersionUID = 1L;

  private final static int DefaultBlockSize = 32 * 1024;  // 32 KB default size

  final static class Block implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    char[] chars;
    int length;

    Block(int size) {
      this.chars = new char[size];
      this.length = 0;
    }
  }

  List<Block> blocks;
  Block current;
  int blockSize;
  int length;

  CharBlockArray() {
    this(DefaultBlockSize);
  }

  CharBlockArray(int blockSize) {
    this.blocks = new ArrayList<Block>();
    this.blockSize = blockSize;
    addBlock();
  }

  private void addBlock() {
    this.current = new Block(this.blockSize);
    this.blocks.add(this.current);
  }

  int blockIndex(int index) {
    return index / blockSize;
  }

  int indexInBlock(int index) {
    return index % blockSize;
  }

  public CharBlockArray append(CharSequence chars) {
    return append(chars, 0, chars.length());
  }

  public CharBlockArray append(char c) {
    if (this.current.length == this.blockSize) {
      addBlock();
    }
    this.current.chars[this.current.length++] = c;
    this.length++;

    return this;
  }

  public CharBlockArray append(CharSequence chars, int start, int length) {
    int end = start + length;
    for (int i = start; i < end; i++) {
      append(chars.charAt(i));
    }
    return this;
  }

  public CharBlockArray append(char[] chars, int start, int length) {
    int offset = start;
    int remain = length;
    while (remain > 0) {
      if (this.current.length == this.blockSize) {
        addBlock();
      }
      int toCopy = remain;
      int remainingInBlock = this.blockSize - this.current.length;
      if (remainingInBlock < toCopy) {
        toCopy = remainingInBlock;
      }
      System.arraycopy(chars, offset, this.current.chars, this.current.length, toCopy);
      offset += toCopy;
      remain -= toCopy;
      this.current.length += toCopy;
    }

    this.length += length;
    return this;
  }

  public CharBlockArray append(String s) {
    int remain = s.length();
    int offset = 0;
    while (remain > 0) {
      if (this.current.length == this.blockSize) {
        addBlock();
      }
      int toCopy = remain;
      int remainingInBlock = this.blockSize - this.current.length;
      if (remainingInBlock < toCopy) {
        toCopy = remainingInBlock;
      }
      s.getChars(offset, offset + toCopy, this.current.chars, this.current.length);
      offset += toCopy;
      remain -= toCopy;
      this.current.length += toCopy;
    }

    this.length += s.length();
    return this;
  }

  public char charAt(int index) {
    Block b = this.blocks.get(blockIndex(index));
    return b.chars[indexInBlock(index)];
  }

  public int length() {
    return this.length;
  }

  public CharSequence subSequence(int start, int end) {
    throw new UnsupportedOperationException("subsequence not implemented yet");
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder(blockSize * this.blocks.size());
    for (int i = 0; i < this.blocks.size(); i++) {
      b.append(this.blocks.get(i).chars);
    }
    return b.toString();
  }

  void flush(OutputStream out) throws IOException {
    ObjectOutputStream oos = null;
    try {
      oos = new ObjectOutputStream(out);
      oos.writeObject(this);
      oos.flush();
    } finally {
      if (oos != null) {
        oos.close();
      }
    }
  }

  public static CharBlockArray open(InputStream in) throws IOException, ClassNotFoundException {
    ObjectInputStream ois = null;
    try {
      ois = new ObjectInputStream(in);
      CharBlockArray a = (CharBlockArray) ois.readObject();
      return a;
    } finally {
      if (ois != null) {
        ois.close();
      }
    }
  }

}
