package org.apache.lucene.index;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestTieredMergePolicy extends LuceneTestCase {

  public void testExpungeDeletes() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer());
    TieredMergePolicy tmp = newTieredMergePolicy();
    conf.setMergePolicy(tmp);
    conf.setMaxBufferedDocs(4);
    tmp.setMaxMergeAtOnce(100);
    tmp.setSegmentsPerTier(100);
    tmp.setExpungeDeletesPctAllowed(30.0);
    IndexWriter w = new IndexWriter(dir, conf);
    w.setInfoStream(VERBOSE ? System.out : null);
    for(int i=0;i<80;i++) {
      Document doc = new Document();
      doc.add(newField("content", "aaa " + (i%4), Field.Store.NO, Field.Index.ANALYZED));
      w.addDocument(doc);
    }
    assertEquals(80, w.maxDoc());
    assertEquals(80, w.numDocs());

    if (VERBOSE) {
      System.out.println("\nTEST: delete docs");
    }
    w.deleteDocuments(new Term("content", "0"));
    w.expungeDeletes();

    assertEquals(80, w.maxDoc());
    assertEquals(60, w.numDocs());

    if (VERBOSE) {
      System.out.println("\nTEST: expunge2");
    }
    tmp.setExpungeDeletesPctAllowed(10.0);
    w.expungeDeletes();
    assertEquals(60, w.maxDoc());
    assertEquals(60, w.numDocs());
    w.close();
    dir.close();
  }

  public void testPartialOptimize() throws Exception {
    for(int iter=0;iter<10*RANDOM_MULTIPLIER;iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      Directory dir = newDirectory();
      IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer());
      conf.setMergeScheduler(new SerialMergeScheduler());
      TieredMergePolicy tmp = newTieredMergePolicy();
      conf.setMergePolicy(tmp);
      conf.setMaxBufferedDocs(2);
      tmp.setMaxMergeAtOnce(3);
      tmp.setSegmentsPerTier(6);

      IndexWriter w = new IndexWriter(dir, conf);
      w.setInfoStream(VERBOSE ? System.out : null);
      int maxCount = 0;
      final int numDocs = _TestUtil.nextInt(random, 20, 100);
      for(int i=0;i<numDocs;i++) {
        Document doc = new Document();
        doc.add(newField("content", "aaa " + (i%4), Field.Store.NO, Field.Index.ANALYZED));
        w.addDocument(doc);
        int count = w.getSegmentCount();
        maxCount = Math.max(count, maxCount);
        assertTrue("count=" + count + " maxCount=" + maxCount, count >= maxCount-3);
      }

      w.flush(true, true);

      int segmentCount = w.getSegmentCount();
      int targetCount = _TestUtil.nextInt(random, 1, segmentCount);
      if (VERBOSE) {
        System.out.println("TEST: optimize to " + targetCount + " segs (current count=" + segmentCount + ")");
      }
      w.optimize(targetCount);
      assertEquals(targetCount, w.getSegmentCount());

      w.close();
      dir.close();
    }
  }
}
