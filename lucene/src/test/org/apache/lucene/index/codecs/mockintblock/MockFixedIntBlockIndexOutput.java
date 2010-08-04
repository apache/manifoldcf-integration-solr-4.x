package org.apache.lucene.index.codecs.mockintblock;

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

/** Naive int block API that writes vInts.  This is
 *  expected to give poor performance; it's really only for
 *  testing the pluggability.  One should typically use pfor instead. */

import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.index.codecs.intblock.FixedIntBlockIndexOutput;

import java.io.IOException;

/** Don't use this class!!  It naively encodes ints one vInt
 * at a time.  Use it only for testing. */
public class MockFixedIntBlockIndexOutput extends FixedIntBlockIndexOutput {

  public final static String CODEC = "SIMPLE_INT_BLOCKS";
  public final static int VERSION_START = 0;
  public final static int VERSION_CURRENT = VERSION_START;

  public MockFixedIntBlockIndexOutput(Directory dir, String fileName, int blockSize) throws IOException {
    IndexOutput out = dir.createOutput(fileName);
    CodecUtil.writeHeader(out, CODEC, VERSION_CURRENT);
    init(out, blockSize);
  }

  @Override
  protected void flushBlock(int[] buffer, IndexOutput out) throws IOException {
    // silly impl
    for(int i=0;i<buffer.length;i++) {
      out.writeVInt(buffer[i]);
    }
  }
}

