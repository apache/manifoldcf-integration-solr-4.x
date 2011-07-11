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
package org.apache.solr.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queries.function.DocValues;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.ValueSourceScorer;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.*;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.function.*;

import java.io.IOException;
import java.util.Map;

/**
 * Create a range query over a function.
 * <br>Other parameters:
 * <br><code>l</code>, the lower bound, optional)
 * <br><code>u</code>, the upper bound, optional)
 * <br><code>incl</code>, include the lower bound: true/false, optional, default=true
 * <br><code>incu</code>, include the upper bound: true/false, optional, default=true
 * <br>Example: <code>{!frange l=1000 u=50000}myfield</code>
 * <br>Filter query example: <code>fq={!frange l=0 u=2.2}sum(user_ranking,editor_ranking)</code> 
 */
public class FunctionRangeQParserPlugin extends QParserPlugin {
  public static String NAME = "frange";

  public void init(NamedList args) {
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      ValueSource vs;
      String funcStr;

      @Override
      public Query parse() throws ParseException {
        funcStr = localParams.get(QueryParsing.V, null);
        Query funcQ = subQuery(funcStr, FunctionQParserPlugin.NAME).getQuery();
        if (funcQ instanceof FunctionQuery) {
          vs = ((FunctionQuery)funcQ).getValueSource();
        } else {
          vs = new QueryValueSource(funcQ, 0.0f);
        }

        String l = localParams.get("l");
        String u = localParams.get("u");
        boolean includeLower = localParams.getBool("incl",true);
        boolean includeUpper = localParams.getBool("incu",true);

        // TODO: add a score=val option to allow score to be the value
        ValueSourceRangeFilter rf = new ValueSourceRangeFilter(vs, l, u, includeLower, includeUpper);
        FunctionRangeQuery frq = new FunctionRangeQuery(rf);
        return frq;
      }
    };
  }

}

// This class works as either a normal constant score query, or as a PostFilter using a collector
class FunctionRangeQuery extends SolrConstantScoreQuery implements PostFilter {
  final ValueSourceRangeFilter rangeFilt;

  public FunctionRangeQuery(ValueSourceRangeFilter filter) {
    super(filter);
    this.rangeFilt = filter;
  }

  @Override
  public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
    Map fcontext = ValueSource.newContext(searcher);
    return new FunctionRangeCollector(fcontext);
  }

  class FunctionRangeCollector extends DelegatingCollector {
    final Map fcontext;
    ValueSourceScorer scorer;
    int maxdoc;

    public FunctionRangeCollector(Map fcontext) {
      this.fcontext = fcontext;
    }

    @Override
    public void collect(int doc) throws IOException {
      if (doc<maxdoc && scorer.matches(doc)) {
        delegate.collect(doc);
      }
    }

    @Override
    public void setNextReader(IndexReader.AtomicReaderContext context) throws IOException {
      maxdoc = context.reader.maxDoc();
      DocValues dv = rangeFilt.getValueSource().getValues(fcontext, context);
      scorer = dv.getRangeScorer(context.reader, rangeFilt.getLowerVal(), rangeFilt.getUpperVal(), rangeFilt.isIncludeLower(), rangeFilt.isIncludeUpper());
      super.setNextReader(context);
    }
  }
}
