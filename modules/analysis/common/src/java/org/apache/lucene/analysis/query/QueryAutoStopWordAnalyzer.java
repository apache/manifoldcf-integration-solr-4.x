package org.apache.lucene.analysis.query;
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.Reader;
import java.util.*;

/**
 * An {@link Analyzer} used primarily at query time to wrap another analyzer and provide a layer of protection
 * which prevents very common words from being passed into queries. 
 * <p>
 * For very large indexes the cost
 * of reading TermDocs for a very common word can be  high. This analyzer was created after experience with
 * a 38 million doc index which had a term in around 50% of docs and was causing TermQueries for 
 * this term to take 2 seconds.
 * </p>
 */
public final class QueryAutoStopWordAnalyzer extends Analyzer {

  private final Analyzer delegate;
  private final Map<String, Set<String>> stopWordsPerField = new HashMap<String, Set<String>>();
  //The default maximum percentage (40%) of index documents which
  //can contain a term, after which the term is considered to be a stop word.
  public static final float defaultMaxDocFreqPercent = 0.4f;
  private final Version matchVersion;

  /**
   * Creates a new QueryAutoStopWordAnalyzer with stopwords calculated for all
   * indexed fields from terms with a document frequency percentage greater than
   * {@link #defaultMaxDocFreqPercent}
   *
   * @param matchVersion Version to be used in {@link StopFilter}
   * @param delegate Analyzer whose TokenStream will be filtered
   * @param indexReader IndexReader to identify the stopwords from
   * @throws IOException Can be thrown while reading from the IndexReader
   */
  public QueryAutoStopWordAnalyzer(
      Version matchVersion,
      Analyzer delegate,
      IndexReader indexReader) throws IOException {
    this(matchVersion, delegate, indexReader, defaultMaxDocFreqPercent);
  }

  /**
   * Creates a new QueryAutoStopWordAnalyzer with stopwords calculated for all
   * indexed fields from terms with a document frequency greater than the given
   * maxDocFreq
   *
   * @param matchVersion Version to be used in {@link StopFilter}
   * @param delegate Analyzer whose TokenStream will be filtered
   * @param indexReader IndexReader to identify the stopwords from
   * @param maxDocFreq Document frequency terms should be above in order to be stopwords
   * @throws IOException Can be thrown while reading from the IndexReader
   */
  public QueryAutoStopWordAnalyzer(
      Version matchVersion,
      Analyzer delegate,
      IndexReader indexReader,
      int maxDocFreq) throws IOException {
    this(matchVersion, delegate, indexReader, indexReader.getFieldNames(IndexReader.FieldOption.INDEXED), maxDocFreq);
  }

  /**
   * Creates a new QueryAutoStopWordAnalyzer with stopwords calculated for all
   * indexed fields from terms with a document frequency percentage greater than
   * the given maxPercentDocs
   *
   * @param matchVersion Version to be used in {@link StopFilter}
   * @param delegate Analyzer whose TokenStream will be filtered
   * @param indexReader IndexReader to identify the stopwords from
   * @param maxPercentDocs The maximum percentage (between 0.0 and 1.0) of index documents which
   *                      contain a term, after which the word is considered to be a stop word
   * @throws IOException Can be thrown while reading from the IndexReader
   */
  public QueryAutoStopWordAnalyzer(
      Version matchVersion,
      Analyzer delegate, 
      IndexReader indexReader,
      float maxPercentDocs) throws IOException {
    this(matchVersion, delegate, indexReader, indexReader.getFieldNames(IndexReader.FieldOption.INDEXED), maxPercentDocs);
  }

  /**
   * Creates a new QueryAutoStopWordAnalyzer with stopwords calculated for the
   * given selection of fields from terms with a document frequency percentage
   * greater than the given maxPercentDocs
   *
   * @param matchVersion Version to be used in {@link StopFilter}
   * @param delegate Analyzer whose TokenStream will be filtered
   * @param indexReader IndexReader to identify the stopwords from
   * @param fields Selection of fields to calculate stopwords for
   * @param maxPercentDocs The maximum percentage (between 0.0 and 1.0) of index documents which
   *                      contain a term, after which the word is considered to be a stop word
   * @throws IOException Can be thrown while reading from the IndexReader
   */
  public QueryAutoStopWordAnalyzer(
      Version matchVersion,
      Analyzer delegate,
      IndexReader indexReader,
      Collection<String> fields,
      float maxPercentDocs) throws IOException {
    this(matchVersion, delegate, indexReader, fields, (int) (indexReader.numDocs() * maxPercentDocs));
  }

  /**
   * Creates a new QueryAutoStopWordAnalyzer with stopwords calculated for the
   * given selection of fields from terms with a document frequency greater than
   * the given maxDocFreq
   *
   * @param matchVersion Version to be used in {@link StopFilter}
   * @param delegate Analyzer whose TokenStream will be filtered
   * @param indexReader IndexReader to identify the stopwords from
   * @param fields Selection of fields to calculate stopwords for
   * @param maxDocFreq Document frequency terms should be above in order to be stopwords
   * @throws IOException Can be thrown while reading from the IndexReader
   */
  public QueryAutoStopWordAnalyzer(
      Version matchVersion,
      Analyzer delegate,
      IndexReader indexReader,
      Collection<String> fields,
      int maxDocFreq) throws IOException {
    this.matchVersion = matchVersion;
    this.delegate = delegate;
    
    for (String field : fields) {
      Set<String> stopWords = new HashSet<String>();
      Terms terms = MultiFields.getTerms(indexReader, field);
      CharsRef spare = new CharsRef();
      if (terms != null) {
        TermsEnum te = terms.iterator();
        BytesRef text;
        while ((text = te.next()) != null) {
          if (te.docFreq() > maxDocFreq) {
            stopWords.add(text.utf8ToChars(spare).toString());
          }
        }
      }
      stopWordsPerField.put(field, stopWords);
    }
  }

  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    TokenStream result;
    try {
      result = delegate.reusableTokenStream(fieldName, reader);
    } catch (IOException e) {
      result = delegate.tokenStream(fieldName, reader);
    }
    Set<String> stopWords = stopWordsPerField.get(fieldName);
    if (stopWords != null) {
      result = new StopFilter(matchVersion, result, stopWords);
    }
    return result;
  }
  
  private class SavedStreams {
    /* the underlying stream */
    TokenStream wrapped;

    /*
     * when there are no stopwords for the field, refers to wrapped.
     * if there stopwords, it is a StopFilter around wrapped.
     */
    TokenStream withStopFilter;
  }

  @SuppressWarnings("unchecked")
  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    /* map of SavedStreams for each field */
    Map<String,SavedStreams> streamMap = (Map<String,SavedStreams>) getPreviousTokenStream();
    if (streamMap == null) {
      streamMap = new HashMap<String, SavedStreams>();
      setPreviousTokenStream(streamMap);
    }

    SavedStreams streams = streamMap.get(fieldName);
    if (streams == null) {
      /* an entry for this field does not exist, create one */
      streams = new SavedStreams();
      streamMap.put(fieldName, streams);
      streams.wrapped = delegate.reusableTokenStream(fieldName, reader);

      /* if there are any stopwords for the field, save the stopfilter */
      Set<String> stopWords = stopWordsPerField.get(fieldName);
      if (stopWords != null) {
        streams.withStopFilter = new StopFilter(matchVersion, streams.wrapped, stopWords);
      } else {
        streams.withStopFilter = streams.wrapped;
      }
    } else {
      /*
      * an entry for this field exists, verify the wrapped stream has not
      * changed. if it has not, reuse it, otherwise wrap the new stream.
      */
      TokenStream result = delegate.reusableTokenStream(fieldName, reader);
      if (result == streams.wrapped) {
        /* the wrapped analyzer reused the stream */
      } else {
        /*
        * the wrapped analyzer did not. if there are any stopwords for the
        * field, create a new StopFilter around the new stream
        */
        streams.wrapped = result;
        Set<String> stopWords = stopWordsPerField.get(fieldName);
        if (stopWords != null) {
          streams.withStopFilter = new StopFilter(matchVersion, streams.wrapped, stopWords);
        } else {
          streams.withStopFilter = streams.wrapped;
        }
      }
    }

    return streams.withStopFilter;
  }

  /**
   * Provides information on which stop words have been identified for a field
   *
   * @param fieldName The field for which stop words identified in "addStopWords"
   *                  method calls will be returned
   * @return the stop words identified for a field
   */
  public String[] getStopWords(String fieldName) {
    Set<String> stopWords = stopWordsPerField.get(fieldName);
    return stopWords != null ? stopWords.toArray(new String[stopWords.size()]) : new String[0];
  }

  /**
   * Provides information on which stop words have been identified for all fields
   *
   * @return the stop words (as terms)
   */
  public Term[] getStopWords() {
    List<Term> allStopWords = new ArrayList<Term>();
    for (String fieldName : stopWordsPerField.keySet()) {
      Set<String> stopWords = stopWordsPerField.get(fieldName);
      for (String text : stopWords) {
        allStopWords.add(new Term(fieldName, text));
      }
    }
    return allStopWords.toArray(new Term[allStopWords.size()]);
	}

}
