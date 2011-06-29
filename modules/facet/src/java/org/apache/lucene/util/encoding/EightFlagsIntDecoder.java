package org.apache.lucene.util.encoding;

import java.io.IOException;
import java.io.InputStream;

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
 * Decodes data which was encoded by {@link EightFlagsIntEncoder}. Scans
 * the <code>indicator</code>, one flag (1-bits) at a time, and decodes extra
 * data using {@link VInt8IntDecoder}.
 * 
 * @see EightFlagsIntEncoder
 * @lucene.experimental
 */
public class EightFlagsIntDecoder extends IntDecoder {

  /**
   * Holds all combinations of <i>indicator</i> for fast decoding (saves time
   * on real-time bit manipulation)
   */
  private static final byte[][] decodeTable = new byte[256][8];

  /** Generating all combinations of <i>indicator</i> into separate flags. */
  static {
    for (int i = 256; i != 0;) {
      --i;
      for (int j = 8; j != 0;) {
        --j;
        decodeTable[i][j] = (byte) ((i >>> j) & 0x1);
      }
    }
  }

  private final IntDecoder decoder = new VInt8IntDecoder();

  /** The indicator for decoding a chunk of 8 integers. */
  private int indicator;

  /** Used as an ordinal of 0 - 7, as the decoder decodes chunks of 8 integers. */
  private int ordinal = 0;

  @Override
  public long decode() throws IOException { 
    // If we've decoded 8 integers, read the next indicator.
    if ((ordinal & 0x7) == 0) {
      indicator = in.read();
      if (indicator < 0) {
        return EOS;
      }
      ordinal = 0;
    }

    if (decodeTable[indicator][ordinal++] == 0) {
      // decode the value from the stream.
      long decode = decoder.decode();
      return decode == EOS ? EOS : decode + 2;
    }

    return 1;
  }

  @Override
  public void reInit(InputStream in) {
    super.reInit(in);
    decoder.reInit(in);
    ordinal = 0;
    indicator = 0;
  }

  @Override
  public String toString() {
    return "EightFlags (VInt8)";
  }

}
