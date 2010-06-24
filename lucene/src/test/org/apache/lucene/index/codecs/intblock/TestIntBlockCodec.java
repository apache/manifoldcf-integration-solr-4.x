package org.apache.lucene.index.codecs.intblock;

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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.store.*;
import org.apache.lucene.index.codecs.sep.*;

public class TestIntBlockCodec extends LuceneTestCase {

  public void testSimpleIntBlocks() throws Exception {
    Directory dir = new MockRAMDirectory();

    IntIndexOutput out = new SimpleIntBlockIndexOutput(dir, "test", 128);
    for(int i=0;i<11777;i++) {
      out.write(i);
    }
    out.close();

    IntIndexInput in = new SimpleIntBlockIndexInput(dir, "test", 128);
    IntIndexInput.Reader r = in.reader();

    for(int i=0;i<11777;i++) {
      assertEquals(i, r.next());
    }
    in.close();
    
    dir.close();
  }

  public void testEmptySimpleIntBlocks() throws Exception {
    Directory dir = new MockRAMDirectory();

    IntIndexOutput out = new SimpleIntBlockIndexOutput(dir, "test", 128);
    // write no ints
    out.close();

    IntIndexInput in = new SimpleIntBlockIndexInput(dir, "test", 128);
    in.reader();
    // read no ints
    in.close();
    dir.close();
  }
}
