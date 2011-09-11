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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TermContext;

import java.io.IOException;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.SimilarityProvider;
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;

/** Similarity unit test.
 *
 *
 */
public class TestSimilarity extends LuceneTestCase {
  
  public static class SimpleSimilarityProvider implements SimilarityProvider {
    public float queryNorm(float sumOfSquaredWeights) { return 1.0f; }
    public float coord(int overlap, int maxOverlap) { return 1.0f; }
    public Similarity get(String field) {
      return new DefaultSimilarity() {
        @Override public byte computeNorm(FieldInvertState state) { return encodeNormValue(state.getBoost()); }
        @Override public float tf(float freq) { return freq; }
        @Override public float sloppyFreq(int distance) { return 2.0f; }
        @Override public float idf(int docFreq, int numDocs) { return 1.0f; }
        @Override public Explanation idfExplain(TermContext[] stats, IndexSearcher searcher) throws IOException {
          return new Explanation(1.0f, "Inexplicable"); 
        }
      };
    }
  }

  public void testSimilarity() throws Exception {
    Directory store = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, store, 
        newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random))
        .setSimilarityProvider(new SimpleSimilarityProvider()));
    
    Document d1 = new Document();
    d1.add(newField("field", "a c", TextField.TYPE_STORED));

    Document d2 = new Document();
    d2.add(newField("field", "a b c", TextField.TYPE_STORED));
    
    writer.addDocument(d1);
    writer.addDocument(d2);
    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(reader);
    searcher.setSimilarityProvider(new SimpleSimilarityProvider());

    Term a = new Term("field", "a");
    Term b = new Term("field", "b");
    Term c = new Term("field", "c");

    searcher.search(new TermQuery(b), new Collector() {
         private Scorer scorer;
         @Override
        public void setScorer(Scorer scorer) throws IOException {
           this.scorer = scorer; 
         }
         @Override
        public final void collect(int doc) throws IOException {
           assertEquals(1.0f, scorer.score(), 0);
         }
         @Override
        public void setNextReader(AtomicReaderContext context) {}
         @Override
        public boolean acceptsDocsOutOfOrder() {
           return true;
         }
       });

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(a), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(b), BooleanClause.Occur.SHOULD);
    //System.out.println(bq.toString("field"));
    searcher.search(bq, new Collector() {
         private int base = 0;
         private Scorer scorer;
         @Override
        public void setScorer(Scorer scorer) throws IOException {
           this.scorer = scorer; 
         }
         @Override
        public final void collect(int doc) throws IOException {
           //System.out.println("Doc=" + doc + " score=" + score);
           assertEquals((float)doc+base+1, scorer.score(), 0);
         }
         @Override
        public void setNextReader(AtomicReaderContext context) {
           base = context.docBase;
         }
         @Override
        public boolean acceptsDocsOutOfOrder() {
           return true;
         }
       });

    PhraseQuery pq = new PhraseQuery();
    pq.add(a);
    pq.add(c);
    //System.out.println(pq.toString("field"));
    searcher.search(pq,
       new Collector() {
         private Scorer scorer;
         @Override
         public void setScorer(Scorer scorer) throws IOException {
          this.scorer = scorer; 
         }
         @Override
         public final void collect(int doc) throws IOException {
           //System.out.println("Doc=" + doc + " score=" + score);
           assertEquals(1.0f, scorer.score(), 0);
         }
         @Override
         public void setNextReader(AtomicReaderContext context) {}
         @Override
         public boolean acceptsDocsOutOfOrder() {
           return true;
         }
       });

    pq.setSlop(2);
    //System.out.println(pq.toString("field"));
    searcher.search(pq, new Collector() {
      private Scorer scorer;
      @Override
      public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer; 
      }
      @Override
      public final void collect(int doc) throws IOException {
        //System.out.println("Doc=" + doc + " score=" + score);
        assertEquals(2.0f, scorer.score(), 0);
      }
      @Override
      public void setNextReader(AtomicReaderContext context) {}
      @Override
      public boolean acceptsDocsOutOfOrder() {
        return true;
      }
    });

    searcher.close();
    reader.close();
    store.close();
  }
}
