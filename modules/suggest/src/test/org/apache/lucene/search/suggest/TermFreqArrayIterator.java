package org.apache.lucene.search.suggest;

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

import java.util.Arrays;
import java.util.Iterator;

import org.apache.lucene.search.spell.TermFreqIterator;

/**
 * A {@link TermFreqIterator} over a sequence of {@link TermFreq}s.
 */
public final class TermFreqArrayIterator implements TermFreqIterator {
  private final Iterator<TermFreq> i;
  private TermFreq current;

  public TermFreqArrayIterator(Iterator<TermFreq> i) {
    this.i = i;
  }

  public TermFreqArrayIterator(TermFreq [] i) {
    this(Arrays.asList(i));
  }

  public TermFreqArrayIterator(Iterable<TermFreq> i) {
    this(i.iterator());
  }
  
  public float freq() {
    return current.v;
  }
  
  public boolean hasNext() {
    return i.hasNext();
  }
  
  public String next() {
    return (current = i.next()).term;
  }

  public void remove() { throw new UnsupportedOperationException(); }
}