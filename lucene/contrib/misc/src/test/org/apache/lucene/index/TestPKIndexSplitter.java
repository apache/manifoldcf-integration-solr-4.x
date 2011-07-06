package org.apache.lucene.index;

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

import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;

public class TestPKIndexSplitter extends LuceneTestCase {

  public void testSplit() throws Exception {    
    NumberFormat format = new DecimalFormat("000000000");
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false))
        .setOpenMode(OpenMode.CREATE));
    for (int x = 0; x < 11; x++) {
      Document doc = createDocument(x, "1", 3, format);
      w.addDocument(doc);
    }
    for (int x = 11; x < 20; x++) {
      Document doc = createDocument(x, "2", 3, format);
      w.addDocument(doc);
    }
    w.close();
    
    final Term midTerm = new Term("id", format.format(11));
    
    checkSplitting(dir, midTerm, 11, 9);
    
    // delete some documents
    w = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false))
        .setOpenMode(OpenMode.APPEND));
    w.deleteDocuments(midTerm);
    w.deleteDocuments(new Term("id", format.format(2)));
    w.close();
    
    checkSplitting(dir, midTerm, 10, 8);
    
    dir.close();
  }
  
  private void checkSplitting(Directory dir, Term splitTerm, int leftCount, int rightCount) throws Exception {
    Directory dir1 = newDirectory();
    Directory dir2 = newDirectory();
    PKIndexSplitter splitter = new PKIndexSplitter(dir, dir1, dir2, splitTerm);
    splitter.split();
    
    IndexReader ir1 = IndexReader.open(dir1);
    IndexReader ir2 = IndexReader.open(dir2);
    assertEquals(leftCount, ir1.numDocs());
    assertEquals(rightCount, ir2.numDocs());
    
    checkContents(ir1, "1");
    checkContents(ir2, "2");
    
    ir1.close();
    ir2.close();
    
    dir1.close();
    dir2.close();
  }
  
  private void checkContents(IndexReader ir, String indexname) throws Exception {
    final Bits liveDocs = MultiFields.getLiveDocs(ir);
    for (int i = 0; i < ir.maxDoc(); i++) {
      if (liveDocs == null || liveDocs.get(i)) {
        assertEquals(indexname, ir.document(i).get("indexname"));
      }
    }
  }
  
  private Document createDocument(int n, String indexName, 
      int numFields, NumberFormat format) {
    StringBuilder sb = new StringBuilder();
    Document doc = new Document();
    String id = format.format(n);
    doc.add(newField("id", id, Store.YES, Index.NOT_ANALYZED));
    doc.add(newField("indexname", indexName, Store.YES, Index.NOT_ANALYZED));
    sb.append("a");
    sb.append(n);
    doc.add(newField("field1", sb.toString(), Store.YES, Index.ANALYZED));
    sb.append(" b");
    sb.append(n);
    for (int i = 1; i < numFields; i++) {
      doc.add(newField("field" + (i + 1), sb.toString(), Store.YES, Index.ANALYZED));
    }
    return doc;
  }
}
