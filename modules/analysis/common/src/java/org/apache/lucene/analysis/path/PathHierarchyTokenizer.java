package org.apache.lucene.analysis.path;
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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/**
 * 
 * Take something like:
 * 
 * <pre>
 *  /soemthing/something/else
 * </pre>
 * 
 * and make:
 *  
 * <pre>
 *  /soemthing
 *  /soemthing/something
 *  /soemthing/something/else
 * </pre>
 * 
 */
public class PathHierarchyTokenizer extends Tokenizer {

  public PathHierarchyTokenizer(Reader input) {
    this(input, DEFAULT_BUFFER_SIZE, DEFAULT_DELIMITER);
  }

  public PathHierarchyTokenizer(Reader input, int bufferSize, char delimiter) {
    this(input, bufferSize, delimiter, delimiter);
  }

  public PathHierarchyTokenizer(Reader input, char delimiter, char replacement) {
    this(input, DEFAULT_BUFFER_SIZE, delimiter, replacement);
  }

  public PathHierarchyTokenizer(Reader input, int bufferSize, char delimiter, char replacement) {
    super(input);
    termAtt.resizeBuffer(bufferSize);
    this.delimiter = delimiter;
    this.replacement = replacement;
    endDelimiter = false;
    resultToken = new StringBuilder(bufferSize);
  }
  
  private static final int DEFAULT_BUFFER_SIZE = 1024;
  public static final char DEFAULT_DELIMITER = '/';
  private final char delimiter;
  private final char replacement;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posAtt = addAttribute(PositionIncrementAttribute.class);
  private int finalOffset = 0;
  private boolean endDelimiter;
  private StringBuilder resultToken;

  @Override
  public final boolean incrementToken() throws IOException {
    clearAttributes();
    termAtt.append( resultToken );
    if(resultToken.length() == 0){
      posAtt.setPositionIncrement(1);
    }
    else{
      posAtt.setPositionIncrement(0);
    }
    int length = 0;
    boolean added = false;
    if( endDelimiter ){
      termAtt.append(replacement);
      length++;
      endDelimiter = false;
      added = true;
    }

    while (true) {
      int c = input.read();
      if( c < 0 ) {
        length += resultToken.length();
        termAtt.setLength(length);
        finalOffset = correctOffset(length);
        offsetAtt.setOffset(correctOffset(0), finalOffset);
        if( added ){
          resultToken.setLength(0);
          resultToken.append(termAtt.buffer(), 0, length);
        }
        return added;
      }
      added = true;
      if( c == delimiter ) {
        if( length > 0 ){
          endDelimiter = true;
          break;
        }
        else{
          termAtt.append(replacement);
          length++;
        }
      }
      else {
        termAtt.append((char)c);
        length++;
      }
    }

    length += resultToken.length();
    termAtt.setLength(length);
    finalOffset = correctOffset(length);
    offsetAtt.setOffset(correctOffset(0), finalOffset);
    resultToken.setLength(0);
    resultToken.append(termAtt.buffer(), 0, length);
    return true;
  }
  
  @Override
  public final void end() {
    // set final offset
    offsetAtt.setOffset(finalOffset, finalOffset);
  }

  @Override
  public void reset(Reader input) throws IOException {
    super.reset(input);
    resultToken.setLength(0);
    finalOffset = 0;
    endDelimiter = false;
  }
}
