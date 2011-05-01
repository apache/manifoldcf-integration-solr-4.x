package org.apache.lucene.search;

import java.io.IOException;
import java.util.Random;
import java.lang.reflect.Method;

import junit.framework.Assert;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.Weight.ScorerContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util._TestUtil;

import static org.apache.lucene.util.LuceneTestCase.TEST_VERSION_CURRENT;

/**
 * Copyright 2005 Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



public class QueryUtils {

  /** Check the types of things query objects should be able to do. */
  public static void check(Query q) {
    checkHashEquals(q);
  }

  /** check very basic hashCode and equals */
  public static void checkHashEquals(Query q) {
    Query q2 = (Query)q.clone();
    checkEqual(q,q2);

    Query q3 = (Query)q.clone();
    q3.setBoost(7.21792348f);
    checkUnequal(q,q3);

    // test that a class check is done so that no exception is thrown
    // in the implementation of equals()
    Query whacky = new Query() {
      @Override
      public String toString(String field) {
        return "My Whacky Query";
      }
    };
    whacky.setBoost(q.getBoost());
    checkUnequal(q, whacky);
    
    // null test
    Assert.assertFalse(q.equals(null));
  }

  public static void checkEqual(Query q1, Query q2) {
    Assert.assertEquals(q1, q2);
    Assert.assertEquals(q1.hashCode(), q2.hashCode());
  }

  public static void checkUnequal(Query q1, Query q2) {
    Assert.assertTrue(!q1.equals(q2));
    Assert.assertTrue(!q2.equals(q1));

    // possible this test can fail on a hash collision... if that
    // happens, please change test to use a different example.
    Assert.assertTrue(q1.hashCode() != q2.hashCode());
  }
  
  /** deep check that explanations of a query 'score' correctly */
  public static void checkExplanations (final Query q, final IndexSearcher s) throws IOException {
    CheckHits.checkExplanations(q, null, s, true);
  }
  
  /** 
   * Various query sanity checks on a searcher, some checks are only done for
   * instanceof IndexSearcher.
   *
   * @see #check(Query)
   * @see #checkFirstSkipTo
   * @see #checkSkipTo
   * @see #checkExplanations
   * @see #checkEqual
   */
  public static void check(Random random, Query q1, IndexSearcher s) {
    check(random, q1, s, true);
  }
  private static void check(Random random, Query q1, IndexSearcher s, boolean wrap) {
    try {
      check(q1);
      if (s!=null) {
        checkFirstSkipTo(q1,s);
        checkSkipTo(q1,s);
        if (wrap) {
          IndexSearcher wrapped;
          check(random, q1, wrapped = wrapUnderlyingReader(random, s, -1), false);
          wrapped.close();
          check(random, q1, wrapped = wrapUnderlyingReader(random, s,  0), false);
          wrapped.close();
          check(random, q1, wrapped = wrapUnderlyingReader(random, s, +1), false);
          wrapped.close();
        }
        checkExplanations(q1,s);
        
        Query q2 = (Query)q1.clone();
        checkEqual(s.rewrite(q1),
                   s.rewrite(q2));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Given an IndexSearcher, returns a new IndexSearcher whose IndexReader 
   * is a MultiReader containing the Reader of the original IndexSearcher, 
   * as well as several "empty" IndexReaders -- some of which will have 
   * deleted documents in them.  This new IndexSearcher should 
   * behave exactly the same as the original IndexSearcher.
   * @param s the searcher to wrap
   * @param edge if negative, s will be the first sub; if 0, s will be in the middle, if positive s will be the last sub
   */
  public static IndexSearcher wrapUnderlyingReader(Random random, final IndexSearcher s, final int edge) 
    throws IOException {

    IndexReader r = s.getIndexReader();

    // we can't put deleted docs before the nested reader, because
    // it will throw off the docIds
    IndexReader[] readers = new IndexReader[] {
      edge < 0 ? r : IndexReader.open(makeEmptyIndex(random, 0), true),
      IndexReader.open(makeEmptyIndex(random, 0), true),
      new MultiReader(IndexReader.open(makeEmptyIndex(random, edge < 0 ? 4 : 0), true),
          IndexReader.open(makeEmptyIndex(random, 0), true),
          0 == edge ? r : IndexReader.open(makeEmptyIndex(random, 0), true)),
      IndexReader.open(makeEmptyIndex(random, 0 < edge ? 0 : 7), true),
      IndexReader.open(makeEmptyIndex(random, 0), true),
      new MultiReader(IndexReader.open(makeEmptyIndex(random, 0 < edge ? 0 : 5), true),
          IndexReader.open(makeEmptyIndex(random, 0), true),
          0 < edge ? r : IndexReader.open(makeEmptyIndex(random, 0), true))
    };
    IndexSearcher out = LuceneTestCase.newSearcher(new MultiReader(readers));
    out.setSimilarityProvider(s.getSimilarityProvider());
    return out;
  }

  private static Directory makeEmptyIndex(Random random, final int numDeletedDocs) 
    throws IOException {
    Directory d = new MockDirectoryWrapper(random, new RAMDirectory());
      IndexWriter w = new IndexWriter(d, new IndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      for (int i = 0; i < numDeletedDocs; i++) {
        w.addDocument(new Document());
      }
      w.commit();
      w.deleteDocuments( new MatchAllDocsQuery() );
      _TestUtil.keepFullyDeletedSegments(w);
      w.commit();

      if (0 < numDeletedDocs)
        Assert.assertTrue("writer has no deletions", w.hasDeletions());

      Assert.assertEquals("writer is missing some deleted docs", 
                          numDeletedDocs, w.maxDoc());
      Assert.assertEquals("writer has non-deleted docs", 
                          0, w.numDocs());
      w.close();
      IndexReader r = IndexReader.open(d, true);
      Assert.assertEquals("reader has wrong number of deleted docs", 
                          numDeletedDocs, r.numDeletedDocs());
      r.close();
      return d;
  }

  /** alternate scorer skipTo(),skipTo(),next(),next(),skipTo(),skipTo(), etc
   * and ensure a hitcollector receives same docs and scores
   */
  public static void checkSkipTo(final Query q, final IndexSearcher s) throws IOException {
    //System.out.println("Checking "+q);
    final AtomicReaderContext[] readerContextArray = ReaderUtil.leaves(s.getTopReaderContext());
    if (q.weight(s).scoresDocsOutOfOrder()) return;  // in this case order of skipTo() might differ from that of next().

    final int skip_op = 0;
    final int next_op = 1;
    final int orders [][] = {
        {next_op},
        {skip_op},
        {skip_op, next_op},
        {next_op, skip_op},
        {skip_op, skip_op, next_op, next_op},
        {next_op, next_op, skip_op, skip_op},
        {skip_op, skip_op, skip_op, next_op, next_op},
    };
    for (int k = 0; k < orders.length; k++) {

        final int order[] = orders[k];
        // System.out.print("Order:");for (int i = 0; i < order.length; i++)
        // System.out.print(order[i]==skip_op ? " skip()":" next()");
        // System.out.println();
        final int opidx[] = { 0 };
        final int lastDoc[] = {-1};

        // FUTURE: ensure scorer.doc()==-1

        final float maxDiff = 1e-5f;
        final IndexReader lastReader[] = {null};

        s.search(q, new Collector() {
          private Scorer sc;
          private Scorer scorer;
          private int leafPtr;

          @Override
          public void setScorer(Scorer scorer) throws IOException {
            this.sc = scorer;
          }

          @Override
          public void collect(int doc) throws IOException {
            float score = sc.score();
            lastDoc[0] = doc;
            try {
              if (scorer == null) {
                Weight w = q.weight(s);
                scorer = w.scorer(readerContextArray[leafPtr], ScorerContext.def());
              }
              
              int op = order[(opidx[0]++) % order.length];
              // System.out.println(op==skip_op ?
              // "skip("+(sdoc[0]+1)+")":"next()");
              boolean more = op == skip_op ? scorer.advance(scorer.docID() + 1) != DocIdSetIterator.NO_MORE_DOCS
                  : scorer.nextDoc() != DocIdSetIterator.NO_MORE_DOCS;
              int scorerDoc = scorer.docID();
              float scorerScore = scorer.score();
              float scorerScore2 = scorer.score();
              float scoreDiff = Math.abs(score - scorerScore);
              float scorerDiff = Math.abs(scorerScore2 - scorerScore);
              if (!more || doc != scorerDoc || scoreDiff > maxDiff
                  || scorerDiff > maxDiff) {
                StringBuilder sbord = new StringBuilder();
                for (int i = 0; i < order.length; i++)
                  sbord.append(order[i] == skip_op ? " skip()" : " next()");
                throw new RuntimeException("ERROR matching docs:" + "\n\t"
                    + (doc != scorerDoc ? "--> " : "") + "doc=" + doc + ", scorerDoc=" + scorerDoc
                    + "\n\t" + (!more ? "--> " : "") + "tscorer.more=" + more
                    + "\n\t" + (scoreDiff > maxDiff ? "--> " : "")
                    + "scorerScore=" + scorerScore + " scoreDiff=" + scoreDiff
                    + " maxDiff=" + maxDiff + "\n\t"
                    + (scorerDiff > maxDiff ? "--> " : "") + "scorerScore2="
                    + scorerScore2 + " scorerDiff=" + scorerDiff
                    + "\n\thitCollector.doc=" + doc + " score=" + score
                    + "\n\t Scorer=" + scorer + "\n\t Query=" + q + "  "
                    + q.getClass().getName() + "\n\t Searcher=" + s
                    + "\n\t Order=" + sbord + "\n\t Op="
                    + (op == skip_op ? " skip()" : " next()"));
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void setNextReader(AtomicReaderContext context) throws IOException {
            // confirm that skipping beyond the last doc, on the
            // previous reader, hits NO_MORE_DOCS
            if (lastReader[0] != null) {
              final IndexReader previousReader = lastReader[0];
              IndexSearcher indexSearcher = LuceneTestCase.newSearcher(previousReader);
              Weight w = q.weight(indexSearcher);
              Scorer scorer = w.scorer((AtomicReaderContext)indexSearcher.getTopReaderContext(), ScorerContext.def());
              if (scorer != null) {
                boolean more = scorer.advance(lastDoc[0] + 1) != DocIdSetIterator.NO_MORE_DOCS;
                Assert.assertFalse("query's last doc was "+ lastDoc[0] +" but skipTo("+(lastDoc[0]+1)+") got to "+scorer.docID(),more);
              }
              leafPtr++;
              indexSearcher.close();
            }
            lastReader[0] = context.reader;
            assert readerContextArray[leafPtr].reader == context.reader;
            this.scorer = null;
            lastDoc[0] = -1;
          }

          @Override
          public boolean acceptsDocsOutOfOrder() {
            return true;
          }
        });

        if (lastReader[0] != null) {
          // confirm that skipping beyond the last doc, on the
          // previous reader, hits NO_MORE_DOCS
          final IndexReader previousReader = lastReader[0];
          IndexSearcher indexSearcher = LuceneTestCase.newSearcher(previousReader, false);
          Weight w = q.weight(indexSearcher);
          Scorer scorer = w.scorer((AtomicReaderContext)previousReader.getTopReaderContext(), ScorerContext.def());
          if (scorer != null) {
            boolean more = scorer.advance(lastDoc[0] + 1) != DocIdSetIterator.NO_MORE_DOCS;
            Assert.assertFalse("query's last doc was "+ lastDoc[0] +" but skipTo("+(lastDoc[0]+1)+") got to "+scorer.docID(),more);
          }
          indexSearcher.close();
        }
      }
  }
    
  // check that first skip on just created scorers always goes to the right doc
  private static void checkFirstSkipTo(final Query q, final IndexSearcher s) throws IOException {
    //System.out.println("checkFirstSkipTo: "+q);
    final float maxDiff = 1e-3f;
    final int lastDoc[] = {-1};
    final IndexReader lastReader[] = {null};
    final AtomicReaderContext[] context = ReaderUtil.leaves(s.getTopReaderContext());
    s.search(q,new Collector() {
      private Scorer scorer;
      private int leafPtr;
      @Override
      public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer;
      }
      @Override
      public void collect(int doc) throws IOException {
        float score = scorer.score();
        try {
          long startMS = System.currentTimeMillis();
          for (int i=lastDoc[0]+1; i<=doc; i++) {
            Weight w = q.weight(s);
            Scorer scorer = w.scorer(context[leafPtr], ScorerContext.def());
            Assert.assertTrue("query collected "+doc+" but skipTo("+i+") says no more docs!",scorer.advance(i) != DocIdSetIterator.NO_MORE_DOCS);
            Assert.assertEquals("query collected "+doc+" but skipTo("+i+") got to "+scorer.docID(),doc,scorer.docID());
            float skipToScore = scorer.score();
            Assert.assertEquals("unstable skipTo("+i+") score!",skipToScore,scorer.score(),maxDiff); 
            Assert.assertEquals("query assigned doc "+doc+" a score of <"+score+"> but skipTo("+i+") has <"+skipToScore+">!",score,skipToScore,maxDiff);
            
            // Hurry things along if they are going slow (eg
            // if you got SimpleText codec this will kick in):
            if (i < doc && System.currentTimeMillis() - startMS > 5) {
              i = doc-1;
            }
          }
          lastDoc[0] = doc;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void setNextReader(AtomicReaderContext context) throws IOException {
        // confirm that skipping beyond the last doc, on the
        // previous reader, hits NO_MORE_DOCS
        if (lastReader[0] != null) {
          final IndexReader previousReader = lastReader[0];
          IndexSearcher indexSearcher = LuceneTestCase.newSearcher(previousReader);
          Weight w = q.weight(indexSearcher);
          Scorer scorer = w.scorer((AtomicReaderContext)indexSearcher.getTopReaderContext(), ScorerContext.def());
          if (scorer != null) {
            boolean more = scorer.advance(lastDoc[0] + 1) != DocIdSetIterator.NO_MORE_DOCS;
            Assert.assertFalse("query's last doc was "+ lastDoc[0] +" but skipTo("+(lastDoc[0]+1)+") got to "+scorer.docID(),more);
          }
          indexSearcher.close();
          leafPtr++;
        }

        lastReader[0] = context.reader;
        lastDoc[0] = -1;
      }
      @Override
      public boolean acceptsDocsOutOfOrder() {
        return false;
      }
    });

    if (lastReader[0] != null) {
      // confirm that skipping beyond the last doc, on the
      // previous reader, hits NO_MORE_DOCS
      final IndexReader previousReader = lastReader[0];
      IndexSearcher indexSearcher = LuceneTestCase.newSearcher(previousReader);
      Weight w = q.weight(indexSearcher);
      Scorer scorer = w.scorer((AtomicReaderContext)indexSearcher.getTopReaderContext(), ScorerContext.def());
      if (scorer != null) {
        boolean more = scorer.advance(lastDoc[0] + 1) != DocIdSetIterator.NO_MORE_DOCS;
        Assert.assertFalse("query's last doc was "+ lastDoc[0] +" but skipTo("+(lastDoc[0]+1)+") got to "+scorer.docID(),more);
      }
      indexSearcher.close();
    }
  }
}
