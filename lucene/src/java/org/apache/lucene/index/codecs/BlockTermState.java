package org.apache.lucene.index.codecs;
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

import org.apache.lucene.index.DocsEnum; // javadocs
import org.apache.lucene.index.OrdTermState;
import org.apache.lucene.index.TermState;

/**
 * Holds all state required for {@link PostingsReaderBase}
 * to produce a {@link DocsEnum} without re-seeking the
 * terms dict.
 */
public class BlockTermState extends OrdTermState {
  public int docFreq;            // how many docs have this term
  public long totalTermFreq;     // total number of occurrences of this term

  public int termCount;          // term ord are in the current block
  public long blockFilePointer;  // fp into the terms dict primary file (_X.tib) that holds this term

  public int blockTermCount;     // how many terms in current block

  @Override
  public void copyFrom(TermState _other) {
    assert _other instanceof BlockTermState : "can not copy from " + _other.getClass().getName();
    BlockTermState other = (BlockTermState) _other;
    super.copyFrom(_other);
    docFreq = other.docFreq;
    totalTermFreq = other.totalTermFreq;
    termCount = other.termCount;
    blockFilePointer = other.blockFilePointer;

    // NOTE: don't copy blockTermCount;
    // it's "transient": used only by the "primary"
    // termState, and regenerated on seek by TermState
  }

  @Override
  public String toString() {
    return "ord=" + ord + " docFreq=" + docFreq + " totalTermFreq=" + totalTermFreq + " termCount=" + termCount + " blockFP=" + blockFilePointer;
  }
}
