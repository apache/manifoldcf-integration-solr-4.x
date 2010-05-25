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

package org.apache.solr.analysis;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilter;
import org.apache.lucene.analysis.util.CharArraySet;

import org.apache.solr.util.plugin.ResourceLoaderAware;
import org.apache.solr.common.ResourceLoader;

import java.util.Map;
import java.io.IOException;


/**
 * @version $Id$
 */
public class WordDelimiterFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {
  public static final String PROTECTED_TOKENS = "protected";

  public void inform(ResourceLoader loader) {
    String wordFiles = args.get(PROTECTED_TOKENS);
    if (wordFiles != null) {  
      try {
        protectedWords = getWordSet(loader, wordFiles, false);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private CharArraySet protectedWords = null;

  int generateWordParts=0;
  int generateNumberParts=0;
  int catenateWords=0;
  int catenateNumbers=0;
  int catenateAll=0;
  int splitOnCaseChange=0;
  int splitOnNumerics=0;
  int preserveOriginal=0;
  int stemEnglishPossessive=0;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    generateWordParts = getInt("generateWordParts", 1);
    generateNumberParts = getInt("generateNumberParts", 1);
    catenateWords = getInt("catenateWords", 0);
    catenateNumbers = getInt("catenateNumbers", 0);
    catenateAll = getInt("catenateAll", 0);
    splitOnCaseChange = getInt("splitOnCaseChange", 1);
    splitOnNumerics = getInt("splitOnNumerics", 1);
    preserveOriginal = getInt("preserveOriginal", 0);
    stemEnglishPossessive = getInt("stemEnglishPossessive", 1);
  }

  public WordDelimiterFilter create(TokenStream input) {
    return new WordDelimiterFilter(input,
                                   generateWordParts, generateNumberParts,
                                   catenateWords, catenateNumbers, catenateAll,
                                   splitOnCaseChange, preserveOriginal,
                                   splitOnNumerics, stemEnglishPossessive, protectedWords);
  }
}
