package org.apache.lucene.search.similarities;

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

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TermContext;

/**
 * Implements the CombSUM method for combining evidence from multiple
 * similarity values described in: Joseph A. Shaw, Edward A. Fox. 
 * In Text REtrieval Conference (1993), pp. 243-252
 * @lucene.experimental
 */
public class MultiSimilarity extends Similarity {
  protected final Similarity sims[];
  
  public MultiSimilarity(Similarity sims[]) {
    this.sims = sims;
  }
  
  @Override
  public byte computeNorm(FieldInvertState state) {
    return sims[0].computeNorm(state);
  }

  @Override
  public Stats computeStats(IndexSearcher searcher, String fieldName, float queryBoost, TermContext... termContexts) throws IOException {
    Stats subStats[] = new Stats[sims.length];
    for (int i = 0; i < subStats.length; i++) {
      subStats[i] = sims[i].computeStats(searcher, fieldName, queryBoost, termContexts);
    }
    return new MultiStats(subStats);
  }

  @Override
  public ExactDocScorer exactDocScorer(Stats stats, String fieldName, AtomicReaderContext context) throws IOException {
    ExactDocScorer subScorers[] = new ExactDocScorer[sims.length];
    for (int i = 0; i < subScorers.length; i++) {
      subScorers[i] = sims[i].exactDocScorer(((MultiStats)stats).subStats[i], fieldName, context);
    }
    return new MultiExactDocScorer(subScorers);
  }

  @Override
  public SloppyDocScorer sloppyDocScorer(Stats stats, String fieldName, AtomicReaderContext context) throws IOException {
    SloppyDocScorer subScorers[] = new SloppyDocScorer[sims.length];
    for (int i = 0; i < subScorers.length; i++) {
      subScorers[i] = sims[i].sloppyDocScorer(((MultiStats)stats).subStats[i], fieldName, context);
    }
    return new MultiSloppyDocScorer(subScorers);
  }
  
  public static class MultiExactDocScorer extends ExactDocScorer {
    private final ExactDocScorer subScorers[];
    
    MultiExactDocScorer(ExactDocScorer subScorers[]) {
      this.subScorers = subScorers;
    }
    
    @Override
    public float score(int doc, int freq) {
      float sum = 0.0f;
      for (ExactDocScorer subScorer : subScorers) {
        sum += subScorer.score(doc, freq);
      }
      return sum;
    }

    @Override
    public Explanation explain(int doc, Explanation freq) {
      Explanation expl = new Explanation(score(doc, (int)freq.getValue()), "sum of:");
      for (ExactDocScorer subScorer : subScorers) {
        expl.addDetail(subScorer.explain(doc, freq));
      }
      return expl;
    }
  }
  
  public static class MultiSloppyDocScorer extends SloppyDocScorer {
    private final SloppyDocScorer subScorers[];
    
    MultiSloppyDocScorer(SloppyDocScorer subScorers[]) {
      this.subScorers = subScorers;
    }
    
    @Override
    public float score(int doc, float freq) {
      float sum = 0.0f;
      for (SloppyDocScorer subScorer : subScorers) {
        sum += subScorer.score(doc, freq);
      }
      return sum;
    }

    @Override
    public Explanation explain(int doc, Explanation freq) {
      Explanation expl = new Explanation(score(doc, freq.getValue()), "sum of:");
      for (SloppyDocScorer subScorer : subScorers) {
        expl.addDetail(subScorer.explain(doc, freq));
      }
      return expl;
    }

    @Override
    public float computeSlopFactor(int distance) {
      return subScorers[0].computeSlopFactor(distance);
    }

    @Override
    public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
      return subScorers[0].computePayloadFactor(doc, start, end, payload);
    }
  }

  public static class MultiStats extends Stats {
    final Stats subStats[];
    
    MultiStats(Stats subStats[]) {
      this.subStats = subStats;
    }
    
    @Override
    public float getValueForNormalization() {
      float sum = 0.0f;
      for (Stats stat : subStats) {
        sum += stat.getValueForNormalization();
      }
      return sum / subStats.length;
    }

    @Override
    public void normalize(float queryNorm, float topLevelBoost) {
      for (Stats stat : subStats) {
        stat.normalize(queryNorm, topLevelBoost);
      }
    }
  }
}
