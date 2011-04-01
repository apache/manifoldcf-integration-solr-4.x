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
package org.apache.solr.response.transform;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.ReaderUtil;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.function.DocValues;
import org.apache.solr.search.function.ValueSource;

import java.io.IOException;
import java.util.Map;

/**
 * Add values from a ValueSource (function query etc)
 *
 * NOT really sure how or if this could work...
 *
 * @version $Id$
 * @since solr 4.0
 */
public class ValueSourceAugmenter extends DocTransformer
{
  public final String name;
  public final QParser qparser;
  public final ValueSource valueSource;



  public ValueSourceAugmenter( String name, QParser qparser, ValueSource valueSource )
  {
    this.name = name;
    this.qparser = qparser;
    this.valueSource = valueSource;
  }

  @Override
  public String getName()
  {
    return "function("+name+")";
  }

  @Override
  public void setContext( TransformContext context ) {
    IndexReader reader = qparser.getReq().getSearcher().getIndexReader();
    readerContexts = reader.getTopReaderContext().leaves();
    docValuesArr = new DocValues[readerContexts.length];

    searcher = qparser.getReq().getSearcher();
    this.fcontext = valueSource.newContext(searcher);
  }


  Map fcontext;
  SolrIndexSearcher searcher;
  IndexReader.AtomicReaderContext[] readerContexts;
  DocValues docValuesArr[];


  @Override
  public void transform(SolrDocument doc, int docid) {
    // This is only good for random-access functions

    try {

      // TODO: calculate this stuff just once across diff functions
      int idx = ReaderUtil.subIndex(docid, readerContexts);
      IndexReader.AtomicReaderContext rcontext = readerContexts[idx];
      DocValues values = docValuesArr[idx];
      if (values == null) {
        docValuesArr[idx] = values = valueSource.getValues(fcontext, rcontext);
      }

      int localId = docid - rcontext.docBase;
      float val = values.floatVal(localId);  // TODO: handle all types -- see: SOLR-2443

      doc.setField( name, val );
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "exception at docid " + docid + " for valuesource " + valueSource, e, false);
    }
  }
}
