package org.apache.lucene.search.spans;

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

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexReader.ReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SloppyDocScorer;
import org.apache.lucene.util.TermContext;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/**
 * Expert-only.  Public for use by other weight implementations
 */
public class SpanWeight extends Weight {
  protected Similarity similarity;
  protected Set<Term> terms;
  protected SpanQuery query;
  protected Similarity.Stats stats;

  public SpanWeight(SpanQuery query, IndexSearcher searcher)
    throws IOException {
    this.similarity = searcher.getSimilarityProvider().get(query.getField());
    this.query = query;
    
    terms=new TreeSet<Term>();
    query.extractTerms(terms);
    final ReaderContext context = searcher.getTopReaderContext();
    final TermContext states[] = new TermContext[terms.size()];
    int i = 0;
    for (Term term : terms)
      states[i++] = TermContext.build(context, term, true);
    stats = similarity.computeStats(searcher, query.getField(), query.getBoost(), states);
  }

  @Override
  public Query getQuery() { return query; }

  @Override
  public float getValueForNormalization() throws IOException {
    return stats.getValueForNormalization();
  }

  @Override
  public void normalize(float queryNorm, float topLevelBoost) {
    stats.normalize(queryNorm, topLevelBoost);
  }

  @Override
  public Scorer scorer(AtomicReaderContext context, ScorerContext scorerContext) throws IOException {
    return new SpanScorer(query.getSpans(context), this, similarity.sloppyDocScorer(stats, query.getField(), context));
  }

  @Override
  public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
    Scorer scorer = scorer(context, ScorerContext.def());
    if (scorer != null) {
      int newDoc = scorer.advance(doc);
      if (newDoc == doc) {
        float freq = scorer.freq();
        SloppyDocScorer docScorer = similarity.sloppyDocScorer(stats, query.getField(), context);
        ComplexExplanation result = new ComplexExplanation();
        result.setDescription("weight("+getQuery()+" in "+doc+") [" + similarity.getClass().getSimpleName() + "], result of:");
        Explanation scoreExplanation = docScorer.explain(doc, new Explanation(freq, "phraseFreq=" + freq));
        result.addDetail(scoreExplanation);
        result.setValue(scoreExplanation.getValue());
        result.setMatch(true);          
        return result;
      }
    }
    
    return new ComplexExplanation(false, 0.0f, "no matching term");
  }
}
