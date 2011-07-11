package org.apache.solr.analysis;

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

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.WordnetSynonymParser;
import org.apache.lucene.analysis.util.ReusableAnalyzerBase;
import org.apache.lucene.util.Version;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.util.plugin.ResourceLoaderAware;

/**
 * @deprecated (3.4) use {@link SynonymFilterFactory} instead. this is only a backwards compatibility
 *                   mechanism that will be removed in Lucene 5.0
 */
// NOTE: rename this to "SynonymFilterFactory" and nuke that delegator in Lucene 5.0!
@Deprecated
final class FSTSynonymFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {
  private SynonymMap map;
  private boolean ignoreCase;
  
  @Override
  public TokenStream create(TokenStream input) {
    return new SynonymFilter(input, map, ignoreCase);
  }

  @Override
  public void inform(ResourceLoader loader) {
    final boolean ignoreCase = getBoolean("ignoreCase", false); 
    this.ignoreCase = ignoreCase;

    String tf = args.get("tokenizerFactory");

    final TokenizerFactory factory = tf == null ? null : loadTokenizerFactory(loader, tf, args);
    
    Analyzer analyzer = new ReusableAnalyzerBase() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = factory == null ? new WhitespaceTokenizer(Version.LUCENE_31, reader) : factory.create(reader);
        TokenStream stream = ignoreCase ? new LowerCaseFilter(Version.LUCENE_31, tokenizer) : tokenizer;
        return new TokenStreamComponents(tokenizer, stream);
      }
    };

    String format = args.get("format");
    try {
      if (format == null || format.equals("solr")) {
        // TODO: expose dedup as a parameter?
        map = loadSolrSynonyms(loader, true, analyzer);
      } else if (format.equals("wordnet")) {
        map = loadWordnetSynonyms(loader, true, analyzer);
      } else {
        // TODO: somehow make this more pluggable
        throw new RuntimeException("Unrecognized synonyms format: " + format);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Load synonyms from the solr format, "format=solr".
   */
  private SynonymMap loadSolrSynonyms(ResourceLoader loader, boolean dedup, Analyzer analyzer) throws IOException, ParseException {
    final boolean expand = getBoolean("expand", true);
    String synonyms = args.get("synonyms");
    if (synonyms == null)
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Missing required argument 'synonyms'.");
    
    CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT);
    
    SolrSynonymParser parser = new SolrSynonymParser(dedup, expand, analyzer);
    File synonymFile = new File(synonyms);
    if (synonymFile.exists()) {
      decoder.reset();
      parser.add(new InputStreamReader(loader.openResource(synonyms), decoder));
    } else {
      List<String> files = StrUtils.splitFileNames(synonyms);
      for (String file : files) {
        decoder.reset();
        parser.add(new InputStreamReader(loader.openResource(file), decoder));
      }
    }
    return parser.build();
  }
  
  /**
   * Load synonyms from the wordnet format, "format=wordnet".
   */
  private SynonymMap loadWordnetSynonyms(ResourceLoader loader, boolean dedup, Analyzer analyzer) throws IOException, ParseException {
    final boolean expand = getBoolean("expand", true);
    String synonyms = args.get("synonyms");
    if (synonyms == null)
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Missing required argument 'synonyms'.");
    
    CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT);
    
    WordnetSynonymParser parser = new WordnetSynonymParser(dedup, expand, analyzer);
    File synonymFile = new File(synonyms);
    if (synonymFile.exists()) {
      decoder.reset();
      parser.add(new InputStreamReader(loader.openResource(synonyms), decoder));
    } else {
      List<String> files = StrUtils.splitFileNames(synonyms);
      for (String file : files) {
        decoder.reset();
        parser.add(new InputStreamReader(loader.openResource(file), decoder));
      }
    }
    return parser.build();
  }
  
  private static TokenizerFactory loadTokenizerFactory(ResourceLoader loader, String cname, Map<String,String> args){
    TokenizerFactory tokFactory = (TokenizerFactory) loader.newInstance(cname);
    tokFactory.init(args);
    return tokFactory;
  }
}
