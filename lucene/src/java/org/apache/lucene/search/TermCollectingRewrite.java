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
import java.util.Comparator;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexReader.ReaderContext;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PerReaderTermState;
import org.apache.lucene.util.ReaderUtil;

abstract class TermCollectingRewrite<Q extends Query> extends MultiTermQuery.RewriteMethod {
  
  
  /** Return a suitable top-level Query for holding all expanded terms. */
  protected abstract Q getTopLevelQuery() throws IOException;
  
  /** Add a MultiTermQuery term to the top-level query */
  protected final void addClause(Q topLevel, Term term, int docCount, float boost) throws IOException {
    addClause(topLevel, term, docCount, boost, null);
  }
  
  protected abstract void addClause(Q topLevel, Term term, int docCount, float boost, PerReaderTermState states) throws IOException;

  
  protected final void collectTerms(IndexReader reader, MultiTermQuery query, TermCollector collector) throws IOException {
    ReaderContext topReaderContext = reader.getTopReaderContext();
    Comparator<BytesRef> lastTermComp = null;
    final AtomicReaderContext[] leaves = ReaderUtil.leaves(topReaderContext);
    for (AtomicReaderContext context : leaves) {
      final Fields fields = context.reader.fields();
      if (fields == null) {
        // reader has no fields
        continue;
      }

      final Terms terms = fields.terms(query.field);
      if (terms == null) {
        // field does not exist
        continue;
      }

      final TermsEnum termsEnum = query.getTermsEnum(terms, collector.attributes);
      assert termsEnum != null;

      if (termsEnum == TermsEnum.EMPTY)
        continue;
      
      // Check comparator compatibility:
      final Comparator<BytesRef> newTermComp = termsEnum.getComparator();
      if (lastTermComp != null && newTermComp != null && newTermComp != lastTermComp)
        throw new RuntimeException("term comparator should not change between segments: "+lastTermComp+" != "+newTermComp);
      lastTermComp = newTermComp;
      collector.setReaderContext(topReaderContext, context);
      collector.setNextEnum(termsEnum);
      BytesRef bytes;
      while ((bytes = termsEnum.next()) != null) {
        if (!collector.collect(bytes))
          return; // interrupt whole term collection, so also don't iterate other subReaders
      }
    }
  }
  
  protected static abstract class TermCollector {
    
    protected AtomicReaderContext readerContext;
    protected ReaderContext topReaderContext;

    public void setReaderContext(ReaderContext topReaderContext, AtomicReaderContext readerContext) {
      this.readerContext = readerContext;
      this.topReaderContext = topReaderContext;
    }
    /** attributes used for communication with the enum */
    public final AttributeSource attributes = new AttributeSource();
  
    /** return false to stop collecting */
    public abstract boolean collect(BytesRef bytes) throws IOException;
    
    /** the next segment's {@link TermsEnum} that is used to collect terms */
    public abstract void setNextEnum(TermsEnum termsEnum) throws IOException;
  }
}
