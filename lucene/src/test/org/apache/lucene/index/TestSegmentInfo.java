package org.apache.lucene.index;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

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

public class TestSegmentInfo extends LuceneTestCase {

  public void testSizeInBytesCache() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer());
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new Field("a", "value", Store.YES, Index.ANALYZED));
    writer.addDocument(doc);
    writer.close();
    
    SegmentInfos sis = new SegmentInfos();
    sis.read(dir);
    SegmentInfo si = sis.info(0);
    long sizeInBytesNoStore = si.sizeInBytes(false);
    long sizeInBytesWithStore = si.sizeInBytes(true);
    assertTrue("sizeInBytesNoStore=" + sizeInBytesNoStore + " sizeInBytesWithStore=" + sizeInBytesWithStore, sizeInBytesWithStore > sizeInBytesNoStore);
    dir.close();
  }
  
}
