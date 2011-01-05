package org.apache.lucene.search.regex;

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

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.LuceneTestCase;

public class TestSpanRegexQuery extends LuceneTestCase {
  
  Directory indexStoreA;
  Directory indexStoreB;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    indexStoreA = newDirectory();
    indexStoreB = newDirectory();
  }
  
  @Override
  public void tearDown() throws Exception {
    indexStoreA.close();
    indexStoreB.close();
    super.tearDown();
  }
  
  public void testSpanRegex() throws Exception {
    Directory directory = newDirectory();
    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document doc = new Document();
    // doc.add(newField("field", "the quick brown fox jumps over the lazy dog",
    // Field.Store.NO, Field.Index.ANALYZED));
    // writer.addDocument(doc);
    // doc = new Document();
    doc.add(newField("field", "auto update", Field.Store.NO,
        Field.Index.ANALYZED));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newField("field", "first auto update", Field.Store.NO,
        Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.optimize();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(directory, true);
    SpanQuery srq = new SpanMultiTermQueryWrapper<RegexQuery>(new RegexQuery(new Term("field", "aut.*")));
    SpanFirstQuery sfq = new SpanFirstQuery(srq, 1);
    // SpanNearQuery query = new SpanNearQuery(new SpanQuery[] {srq, stq}, 6,
    // true);
    int numHits = searcher.search(sfq, null, 1000).totalHits;
    assertEquals(1, numHits);
    searcher.close();
    directory.close();
  }
  
  private void createRAMDirectories() throws CorruptIndexException,
      LockObtainFailedException, IOException {
    // creating a document to store
    Document lDoc = new Document();
    lDoc.add(newField("field", "a1 b1", Field.Store.NO,
        Field.Index.ANALYZED_NO_NORMS));

    // creating a document to store
    Document lDoc2 = new Document();
    lDoc2.add(newField("field", "a2 b2", Field.Store.NO,
        Field.Index.ANALYZED_NO_NORMS));

    // creating first index writer
    IndexWriter writerA = new IndexWriter(indexStoreA, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer()).setOpenMode(OpenMode.CREATE));
    writerA.addDocument(lDoc);
    writerA.optimize();
    writerA.close();

    // creating second index writer
    IndexWriter writerB = new IndexWriter(indexStoreB, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer()).setOpenMode(OpenMode.CREATE));
    writerB.addDocument(lDoc2);
    writerB.optimize();
    writerB.close();
  }
}
