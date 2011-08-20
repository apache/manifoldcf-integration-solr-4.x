package org.apache.lucene.index.codecs.standard;

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

/** Consumes doc & freq, writing them using the current
 *  index file format */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.codecs.PostingsWriterBase;
import org.apache.lucene.index.codecs.TermStats;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;

/** @lucene.experimental */
public final class StandardPostingsWriter extends PostingsWriterBase {
  final static String CODEC = "StandardPostingsWriter";

  //private static boolean DEBUG = BlockTreeTermsWriter.DEBUG;
  
  // Increment version to change it:
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  final IndexOutput freqOut;
  final IndexOutput proxOut;
  final DefaultSkipListWriter skipListWriter;
  /** Expert: The fraction of TermDocs entries stored in skip tables,
   * used to accelerate {@link DocsEnum#advance(int)}.  Larger values result in
   * smaller indexes, greater acceleration, but fewer accelerable cases, while
   * smaller values result in bigger indexes, less acceleration and more
   * accelerable cases. More detailed experiments would be useful here. */
  static final int DEFAULT_SKIP_INTERVAL = 16;
  final int skipInterval;
  
  /**
   * Expert: minimum docFreq to write any skip data at all
   */
  final int skipMinimum;

  /** Expert: The maximum number of skip levels. Smaller values result in 
   * slightly smaller indexes, but slower skipping in big posting lists.
   */
  final int maxSkipLevels = 10;
  final int totalNumDocs;
  IndexOutput termsOut;

  IndexOptions indexOptions;
  boolean storePayloads;
  // Starts a new term
  long freqStart;
  long proxStart;
  FieldInfo fieldInfo;
  int lastPayloadLength;
  int lastPosition;

  // private String segment;

  public StandardPostingsWriter(SegmentWriteState state) throws IOException {
    this(state, DEFAULT_SKIP_INTERVAL);
  }
  
  public StandardPostingsWriter(SegmentWriteState state, int skipInterval) throws IOException {
    super();
    this.skipInterval = skipInterval;
    this.skipMinimum = skipInterval; /* set to the same for now */
    // this.segment = state.segmentName;
    String fileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, StandardCodec.FREQ_EXTENSION);
    freqOut = state.directory.createOutput(fileName, state.context);
    if (state.fieldInfos.hasProx()) {
      // At least one field does not omit TF, so create the
      // prox file
      fileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, StandardCodec.PROX_EXTENSION);
      proxOut = state.directory.createOutput(fileName, state.context);
    } else {
      // Every field omits TF so we will write no prox file
      proxOut = null;
    }

    totalNumDocs = state.numDocs;

    skipListWriter = new DefaultSkipListWriter(skipInterval,
                                               maxSkipLevels,
                                               state.numDocs,
                                               freqOut,
                                               proxOut);
  }

  @Override
  public void start(IndexOutput termsOut) throws IOException {
    this.termsOut = termsOut;
    CodecUtil.writeHeader(termsOut, CODEC, VERSION_CURRENT);
    termsOut.writeInt(skipInterval);                // write skipInterval
    termsOut.writeInt(maxSkipLevels);               // write maxSkipLevels
    termsOut.writeInt(skipMinimum);                 // write skipMinimum
  }

  @Override
  public void startTerm() {
    freqStart = freqOut.getFilePointer();
    //if (DEBUG) System.out.println("SPW: startTerm freqOut.fp=" + freqStart);
    if (proxOut != null) {
      proxStart = proxOut.getFilePointer();
      // force first payload to write its length
      lastPayloadLength = -1;
    }
    skipListWriter.resetSkip();
  }

  // Currently, this instance is re-used across fields, so
  // our parent calls setField whenever the field changes
  @Override
  public void setField(FieldInfo fieldInfo) {
    //System.out.println("SPW: setField");
    /*
    if (BlockTreeTermsWriter.DEBUG && fieldInfo.name.equals("id")) {
      DEBUG = true;
    } else {
      DEBUG = false;
    }
    */
    this.fieldInfo = fieldInfo;
    indexOptions = fieldInfo.indexOptions;
    storePayloads = fieldInfo.storePayloads;
    //System.out.println("  set init blockFreqStart=" + freqStart);
    //System.out.println("  set init blockProxStart=" + proxStart);
  }

  int lastDocID;
  int df;
  
  /** Adds a new doc in this term.  If this returns null
   *  then we just skip consuming positions/payloads. */
  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {
    // if (DEBUG) System.out.println("SPW:   startDoc seg=" + segment + " docID=" + docID + " tf=" + termDocFreq + " freqOut.fp=" + freqOut.getFilePointer());

    final int delta = docID - lastDocID;
    
    if (docID < 0 || (df > 0 && delta <= 0)) {
      throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " )");
    }

    if ((++df % skipInterval) == 0) {
      skipListWriter.setSkipData(lastDocID, storePayloads, lastPayloadLength);
      skipListWriter.bufferSkip(df);
    }

    assert docID < totalNumDocs: "docID=" + docID + " totalNumDocs=" + totalNumDocs;

    lastDocID = docID;
    if (indexOptions == IndexOptions.DOCS_ONLY) {
      freqOut.writeVInt(delta);
    } else if (1 == termDocFreq) {
      freqOut.writeVInt((delta<<1) | 1);
    } else {
      freqOut.writeVInt(delta<<1);
      freqOut.writeVInt(termDocFreq);
    }

    lastPosition = 0;
  }

  /** Add a new position & payload */
  @Override
  public void addPosition(int position, BytesRef payload) throws IOException {
    //if (DEBUG) System.out.println("SPW:     addPos pos=" + position + " payload=" + (payload == null ? "null" : (payload.length + " bytes")) + " proxFP=" + proxOut.getFilePointer());
    assert indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS: "invalid indexOptions: " + indexOptions;
    assert proxOut != null;

    final int delta = position - lastPosition;
    
    assert delta >= 0: "position=" + position + " lastPosition=" + lastPosition;            // not quite right (if pos=0 is repeated twice we don't catch it)

    lastPosition = position;

    if (storePayloads) {
      final int payloadLength = payload == null ? 0 : payload.length;

      if (payloadLength != lastPayloadLength) {
        lastPayloadLength = payloadLength;
        proxOut.writeVInt((delta<<1)|1);
        proxOut.writeVInt(payloadLength);
      } else {
        proxOut.writeVInt(delta << 1);
      }

      if (payloadLength > 0) {
        proxOut.writeBytes(payload.bytes, payload.offset, payloadLength);
      }
    } else {
      proxOut.writeVInt(delta);
    }
  }

  @Override
  public void finishDoc() {
  }

  private static class PendingTerm {
    public final long freqStart;
    public final long proxStart;
    public final int skipOffset;

    public PendingTerm(long freqStart, long proxStart, int skipOffset) {
      this.freqStart = freqStart;
      this.proxStart = proxStart;
      this.skipOffset = skipOffset;
    }
  }

  private final List<PendingTerm> pendingTerms = new ArrayList<PendingTerm>();

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(TermStats stats) throws IOException {

    // if (DEBUG) System.out.println("SPW: finishTerm seg=" + segment + " freqStart=" + freqStart);
    assert stats.docFreq > 0;

    // TODO: wasteful we are counting this (counting # docs
    // for this term) in two places?
    assert stats.docFreq == df;

    final int skipOffset;
    if (df >= skipMinimum) {
      skipOffset = (int) (skipListWriter.writeSkip(freqOut)-freqStart);
    } else {
      skipOffset = -1;
    }

    pendingTerms.add(new PendingTerm(freqStart, proxStart, skipOffset));

    lastDocID = 0;
    df = 0;
  }

  private final RAMOutputStream bytesWriter = new RAMOutputStream();

  @Override
  public void flushTermsBlock(int start, int count) throws IOException {
    //if (DEBUG) System.out.println("SPW: flushTermsBlock start=" + start + " count=" + count + " left=" + (pendingTerms.size()-count) + " pendingTerms.size()=" + pendingTerms.size());

    if (count == 0) {
      termsOut.writeByte((byte) 0);
      return;
    }

    assert start <= pendingTerms.size();
    assert count <= start;

    final int limit = pendingTerms.size() - start + count;
    final PendingTerm firstTerm = pendingTerms.get(limit - count);
    // First term in block is abs coded:
    bytesWriter.writeVLong(firstTerm.freqStart);

    if (firstTerm.skipOffset != -1) {
      assert firstTerm.skipOffset > 0;
      bytesWriter.writeVInt(firstTerm.skipOffset);
    }
    if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
      bytesWriter.writeVLong(firstTerm.proxStart);
    }
    long lastFreqStart = firstTerm.freqStart;
    long lastProxStart = firstTerm.proxStart;
    for(int idx=limit-count+1; idx<limit; idx++) {
      final PendingTerm term = pendingTerms.get(idx);
      //if (DEBUG) System.out.println("  write term freqStart=" + term.freqStart);
      // The rest of the terms term are delta coded:
      bytesWriter.writeVLong(term.freqStart - lastFreqStart);
      lastFreqStart = term.freqStart;
      if (term.skipOffset != -1) {
        assert term.skipOffset > 0;
        bytesWriter.writeVInt(term.skipOffset);
      }
      if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
        bytesWriter.writeVLong(term.proxStart - lastProxStart);
        lastProxStart = term.proxStart;
      }
    }

    termsOut.writeVInt((int) bytesWriter.getFilePointer());
    bytesWriter.writeTo(termsOut);
    bytesWriter.reset();

    // Remove the terms we just wrote:
    pendingTerms.subList(limit-count, limit).clear();
  }

  @Override
  public void close() throws IOException {
    try {
      freqOut.close();
    } finally {
      if (proxOut != null) {
        proxOut.close();
      }
    }
  }
}
