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

package org.apache.lucene.queries.function.valuesource;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.queries.function.DocValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.LongDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Map;

/**
 * <code>TotalTermFreqValueSource</code> returns the total term freq (sum of term freqs across all docuyments).
 * @lucene.internal
 */
public class SumTotalTermFreqValueSource extends ValueSource {
  protected String indexedField;

  public SumTotalTermFreqValueSource(String indexedField) {
    this.indexedField = indexedField;
  }

  public String name() {
    return "sumtotaltermfreq";
  }

  @Override
  public String description() {
    return name() + '(' + indexedField + ')';
  }

  @Override
  public DocValues getValues(Map context, IndexReader.AtomicReaderContext readerContext) throws IOException {
    return (DocValues)context.get(this);
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    long sumTotalTermFreq = 0;
    for (IndexReader.AtomicReaderContext readerContext : searcher.getTopReaderContext().leaves()) {
      Fields fields = readerContext.reader.fields();
      if (fields == null) continue;
      Terms terms = fields.terms(indexedField);
      if (terms == null) continue;
      sumTotalTermFreq += terms.getSumTotalTermFreq();
    }
    final long ttf = Math.max(-1, sumTotalTermFreq);  // we may have added up -1s if not supported
    context.put(this, new LongDocValues(this) {
      @Override
      public long longVal(int doc) {
        return ttf;
      }
    });
  }

  @Override
  public int hashCode() {
    return getClass().hashCode() + indexedField.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) return false;
    SumTotalTermFreqValueSource other = (SumTotalTermFreqValueSource)o;
    return this.indexedField.equals(other.indexedField);
  }
}
