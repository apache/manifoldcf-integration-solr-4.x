package org.apache.lucene.search;

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
import java.util.Random;

import org.apache.lucene.util.LuceneTestCaseJ4;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class BaseTestRangeFilter extends LuceneTestCaseJ4 {
  
  public static final boolean F = false;
  public static final boolean T = true;
  
  /**
   * Collation interacts badly with hyphens -- collation produces different
   * ordering than Unicode code-point ordering -- so two indexes are created:
   * one which can't have negative random integers, for testing collated ranges,
   * and the other which can have negative random integers, for all other tests.
   */
  static class TestIndex {
    int maxR;
    int minR;
    boolean allowNegativeRandomInts;
    Directory index;
    
    TestIndex(Random random, int minR, int maxR, boolean allowNegativeRandomInts) {
      this.minR = minR;
      this.maxR = maxR;
      this.allowNegativeRandomInts = allowNegativeRandomInts;
      try {
        index = newDirectory(random);
      } catch (IOException e) { throw new RuntimeException(e); }
    }
  }
  
  static IndexReader signedIndexReader;
  static IndexReader unsignedIndexReader;
  
  static TestIndex signedIndexDir;
  static TestIndex unsignedIndexDir;
  
  static int minId = 0;
  static int maxId = 10000;
  
  static final int intLength = Integer.toString(Integer.MAX_VALUE).length();
  
  /**
   * a simple padding function that should work with any int
   */
  public static String pad(int n) {
    StringBuilder b = new StringBuilder(40);
    String p = "0";
    if (n < 0) {
      p = "-";
      n = Integer.MAX_VALUE + n + 1;
    }
    b.append(p);
    String s = Integer.toString(n);
    for (int i = s.length(); i <= intLength; i++) {
      b.append("0");
    }
    b.append(s);
    
    return b.toString();
  }
  
  @BeforeClass
  public static void beforeClassBaseTestRangeFilter() throws Exception {
    Random random = newStaticRandom(BaseTestRangeFilter.class);
    signedIndexDir = new TestIndex(random, Integer.MAX_VALUE, Integer.MIN_VALUE, true);
    unsignedIndexDir = new TestIndex(random, Integer.MAX_VALUE, 0, false);
    signedIndexReader = build(random, signedIndexDir);
    unsignedIndexReader = build(random, unsignedIndexDir);
  }
  
  @AfterClass
  public static void afterClassBaseTestRangeFilter() throws Exception {
    signedIndexReader.close();
    unsignedIndexReader.close();
    signedIndexDir.index.close();
    unsignedIndexDir.index.close();
    signedIndexReader = null;
    unsignedIndexReader = null;
    signedIndexDir = null;
    unsignedIndexDir = null;
  }
  
  private static IndexReader build(Random random, TestIndex index) throws IOException {
    /* build an index */
    RandomIndexWriter writer = new RandomIndexWriter(random, index.index, 
        newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer())
    .setOpenMode(OpenMode.CREATE).setMaxBufferedDocs(_TestUtil.nextInt(random, 50, 1000)));
    
    Document doc = new Document();
    Field idField = newField(random, "id", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
    Field randField = newField(random, "rand", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
    Field bodyField = newField(random, "body", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
    doc.add(idField);
    doc.add(randField);
    doc.add(bodyField);
    
    for (int d = minId; d <= maxId; d++) {
      idField.setValue(pad(d));
      int r = index.allowNegativeRandomInts ? random.nextInt() : random
          .nextInt(Integer.MAX_VALUE);
      if (index.maxR < r) {
        index.maxR = r;
      }
      if (r < index.minR) {
        index.minR = r;
      }
      randField.setValue(pad(r));
      bodyField.setValue("body");
      writer.addDocument(doc);
    }
    
    IndexReader ir = writer.getReader();
    writer.close();
    return ir;
  }
  
  @Test
  public void testPad() {
    
    int[] tests = new int[] {-9999999, -99560, -100, -3, -1, 0, 3, 9, 10, 1000,
        999999999};
    for (int i = 0; i < tests.length - 1; i++) {
      int a = tests[i];
      int b = tests[i + 1];
      String aa = pad(a);
      String bb = pad(b);
      String label = a + ":" + aa + " vs " + b + ":" + bb;
      assertEquals("length of " + label, aa.length(), bb.length());
      assertTrue("compare less than " + label, aa.compareTo(bb) < 0);
    }
    
  }
  
}
