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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;
import org.apache.lucene.util._TestUtil;

public class TestNRTCachingDirectory extends LuceneTestCase {

  public void testNRTAndCommit() throws Exception {
    Directory dir = newDirectory();
    NRTCachingDirectory cachedDir = new NRTCachingDirectory(dir, 2.0, 25.0);
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));
    conf.setMergeScheduler(cachedDir.getMergeScheduler());
    RandomIndexWriter w = new RandomIndexWriter(random, cachedDir, conf);
    w.w.setInfoStream(VERBOSE ? System.out : null);
    final LineFileDocs docs = new LineFileDocs(random);    
    final int numDocs = _TestUtil.nextInt(random, 100, 400);

    if (VERBOSE) {
      System.out.println("TEST: numDocs=" + numDocs);
    }

    final List<BytesRef> ids = new ArrayList<BytesRef>();
    IndexReader r = null;
    for(int docCount=0;docCount<numDocs;docCount++) {
      final Document doc = docs.nextDoc();
      ids.add(new BytesRef(doc.get("docid")));
      w.addDocument(doc);
      if (random.nextInt(20) == 17) {
        if (r == null) {
          r = IndexReader.open(w.w, false);
        } else {
          final IndexReader r2 = r.reopen();
          if (r2 != r) {
            r.close();
            r = r2;
          }
        }
        assertEquals(1+docCount, r.numDocs());
        final IndexSearcher s = new IndexSearcher(r);
        // Just make sure search can run; we can't assert
        // totHits since it could be 0
        TopDocs hits = s.search(new TermQuery(new Term("body", "the")), 10);
        // System.out.println("tot hits " + hits.totalHits);
      }
    }

    if (r != null) {
      r.close();
    }

    // Close should force cache to clear since all files are sync'd
    w.close();

    final String[] cachedFiles = cachedDir.listCachedFiles();
    for(String file : cachedFiles) {
      System.out.println("FAIL: cached file " + file + " remains after sync");
    }
    assertEquals(0, cachedFiles.length);
    
    r = IndexReader.open(dir);
    for(BytesRef id : ids) {
      assertEquals(1, r.docFreq("docid", id));
    }
    r.close();
    cachedDir.close();
  }

  // NOTE: not a test; just here to make sure the code frag
  // in the javadocs is correct!
  public void verifyCompiles() throws Exception {
    Analyzer analyzer = null;

    Directory fsDir = FSDirectory.open(new File("/path/to/index"));
    NRTCachingDirectory cachedFSDir = new NRTCachingDirectory(fsDir, 2.0, 25.0);
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_32, analyzer);
    conf.setMergeScheduler(cachedFSDir.getMergeScheduler());
    IndexWriter writer = new IndexWriter(cachedFSDir, conf);
  }

  public void testDeleteFile() throws Exception {
    Directory dir = new NRTCachingDirectory(newDirectory(), 2.0, 25.0);
    dir.createOutput("foo.txt").close();
    dir.deleteFile("foo.txt");
    assertEquals(0, dir.listAll().length);
    dir.close();
  }
}
