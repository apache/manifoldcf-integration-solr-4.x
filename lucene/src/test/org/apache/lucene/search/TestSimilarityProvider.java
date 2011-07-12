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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiNorms;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestSimilarityProvider extends LuceneTestCase {
  private Directory directory;
  private IndexReader reader;
  private IndexSearcher searcher;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    SimilarityProvider sim = new ExampleSimilarityProvider();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, 
        new MockAnalyzer(random)).setSimilarityProvider(sim);
    RandomIndexWriter iw = new RandomIndexWriter(random, directory, iwc);
    Document doc = new Document();
    Field field = newField("foo", "", Field.Store.NO, Field.Index.ANALYZED);
    doc.add(field);
    Field field2 = newField("bar", "", Field.Store.NO, Field.Index.ANALYZED);
    doc.add(field2);
    
    field.setValue("quick brown fox");
    field2.setValue("quick brown fox");
    iw.addDocument(doc);
    field.setValue("jumps over lazy brown dog");
    field2.setValue("jumps over lazy brown dog");
    iw.addDocument(doc);
    reader = iw.getReader();
    iw.close();
    searcher = newSearcher(reader);
    searcher.setSimilarityProvider(sim);
  }
  
  @Override
  public void tearDown() throws Exception {
    searcher.close();
    reader.close();
    directory.close();
    super.tearDown();
  }
  
  public void testBasics() throws Exception {
    // sanity check of norms writer
    byte fooNorms[] = MultiNorms.norms(reader, "foo");
    byte barNorms[] = MultiNorms.norms(reader, "bar");
    for (int i = 0; i < fooNorms.length; i++) {
      assertFalse(fooNorms[i] == barNorms[i]);
    }
    
    // sanity check of searching
    TopDocs foodocs = searcher.search(new TermQuery(new Term("foo", "brown")), 10);
    assertTrue(foodocs.totalHits > 0);
    TopDocs bardocs = searcher.search(new TermQuery(new Term("bar", "brown")), 10);
    assertTrue(bardocs.totalHits > 0);
    assertTrue(foodocs.scoreDocs[0].score < bardocs.scoreDocs[0].score);
  }
  
  private class ExampleSimilarityProvider implements SimilarityProvider {
    private Similarity sim1 = new Sim1();
    private Similarity sim2 = new Sim2();
    
    public float coord(int overlap, int maxOverlap) {
      return 1f;
    }

    public float queryNorm(float sumOfSquaredWeights) {
      return 1f;
    }

    public Similarity get(String field) {
      if (field.equals("foo")) {
        return sim1;
      } else {
        return sim2;
      }
    }
  }
  
  private class Sim1 extends TFIDFSimilarity {
    @Override
    public byte computeNorm(FieldInvertState state) {
      return encodeNormValue(1f);
    }

    @Override
    public float sloppyFreq(int distance) {
      return 1f;
    }

    @Override
    public float tf(float freq) {
      return 1f;
    }

    @Override
    public float idf(int docFreq, int numDocs) {
      return 1f;
    }

    @Override
    public float scorePayload(int doc, int start, int end, BytesRef payload) {
      return 1f;
    }
  }
  
  private class Sim2 extends TFIDFSimilarity {
    @Override
    public byte computeNorm(FieldInvertState state) {
      return encodeNormValue(10f);
    }

    @Override
    public float sloppyFreq(int distance) {
      return 10f;
    }

    @Override
    public float tf(float freq) {
      return 10f;
    }

    @Override
    public float idf(int docFreq, int numDocs) {
      return 10f;
    }

    @Override
    public float scorePayload(int doc, int start, int end, BytesRef payload) {
      return 1f;
    }
  }
}
