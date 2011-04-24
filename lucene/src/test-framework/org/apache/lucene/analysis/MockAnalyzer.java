package org.apache.lucene.analysis;

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
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * Analyzer for testing
 */
public final class MockAnalyzer extends Analyzer { 
  private final CharacterRunAutomaton runAutomaton;
  private final boolean lowerCase;
  private final CharacterRunAutomaton filter;
  private final boolean enablePositionIncrements;
  private int positionIncrementGap;
  private final Random random;
  private Map<String,Integer> previousMappings = new HashMap<String,Integer>();

  /**
   * Creates a new MockAnalyzer.
   * 
   * @param random Random for payloads behavior
   * @param runAutomaton DFA describing how tokenization should happen (e.g. [a-zA-Z]+)
   * @param lowerCase true if the tokenizer should lowercase terms
   * @param filter DFA describing how terms should be filtered (set of stopwords, etc)
   * @param enablePositionIncrements true if position increments should reflect filtered terms.
   */
  public MockAnalyzer(Random random, CharacterRunAutomaton runAutomaton, boolean lowerCase, CharacterRunAutomaton filter, boolean enablePositionIncrements) {
    this.random = random;
    this.runAutomaton = runAutomaton;
    this.lowerCase = lowerCase;
    this.filter = filter;
    this.enablePositionIncrements = enablePositionIncrements;
  }

  /**
   * Calls {@link #MockAnalyzer(Random, CharacterRunAutomaton, boolean, CharacterRunAutomaton, boolean) 
   * MockAnalyzer(random, runAutomaton, lowerCase, MockTokenFilter.EMPTY_STOPSET, false}).
   */
  public MockAnalyzer(Random random, CharacterRunAutomaton runAutomaton, boolean lowerCase) {
    this(random, runAutomaton, lowerCase, MockTokenFilter.EMPTY_STOPSET, false);
  }

  /** 
   * Create a Whitespace-lowercasing analyzer with no stopwords removal.
   * <p>
   * Calls {@link #MockAnalyzer(Random, CharacterRunAutomaton, boolean, CharacterRunAutomaton, boolean) 
   * MockAnalyzer(random, MockTokenizer.WHITESPACE, true, MockTokenFilter.EMPTY_STOPSET, false}).
   */
  public MockAnalyzer(Random random) {
    this(random, MockTokenizer.WHITESPACE, true);
  }

  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    MockTokenizer tokenizer = new MockTokenizer(reader, runAutomaton, lowerCase);
    TokenFilter filt = new MockTokenFilter(tokenizer, filter, enablePositionIncrements);
    filt = maybePayload(filt, fieldName);
    return filt;
  }

  private class SavedStreams {
    MockTokenizer tokenizer;
    TokenFilter filter;
  }

  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader)
      throws IOException {
    @SuppressWarnings("unchecked") Map<String,SavedStreams> map = (Map) getPreviousTokenStream();
    if (map == null) {
      map = new HashMap<String,SavedStreams>();
      setPreviousTokenStream(map);
    }
    
    SavedStreams saved = map.get(fieldName);
    if (saved == null) {
      saved = new SavedStreams();
      saved.tokenizer = new MockTokenizer(reader, runAutomaton, lowerCase);
      saved.filter = new MockTokenFilter(saved.tokenizer, filter, enablePositionIncrements);
      saved.filter = maybePayload(saved.filter, fieldName);
      map.put(fieldName, saved);
      return saved.filter;
    } else {
      saved.tokenizer.reset(reader);
      saved.filter.reset();
      return saved.filter;
    }
  }
  
  private synchronized TokenFilter maybePayload(TokenFilter stream, String fieldName) {
    Integer val = previousMappings.get(fieldName);
    if (val == null) {
      switch(random.nextInt(3)) {
        case 0: val = -1; // no payloads
                break;
        case 1: val = Integer.MAX_VALUE; // variable length payload
                break;
        case 2: val = random.nextInt(12); // fixed length payload
                break;
      }
      previousMappings.put(fieldName, val); // save it so we are consistent for this field
    }
    
    if (val == -1)
      return stream;
    else if (val == Integer.MAX_VALUE)
      return new MockVariableLengthPayloadFilter(random, stream);
    else
      return new MockFixedLengthPayloadFilter(random, stream, val);
  }
  
  public void setPositionIncrementGap(int positionIncrementGap){
    this.positionIncrementGap = positionIncrementGap;
  }
  
  @Override
  public int getPositionIncrementGap(String fieldName){
    return positionIncrementGap;
  }
}
