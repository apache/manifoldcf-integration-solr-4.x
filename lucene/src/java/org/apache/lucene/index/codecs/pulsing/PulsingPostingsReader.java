package org.apache.lucene.index.codecs.pulsing;

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

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.codecs.PostingsReaderBase;
import org.apache.lucene.index.codecs.BlockTermState;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;

/** Concrete class that reads the current doc/freq/skip
 *  postings format 
 *  @lucene.experimental */

// TODO: -- should we switch "hasProx" higher up?  and
// create two separate docs readers, one that also reads
// prox and one that doesn't?

public class PulsingPostingsReader extends PostingsReaderBase {

  // Fallback reader for non-pulsed terms:
  final PostingsReaderBase wrappedPostingsReader;
  int maxPositions;

  public PulsingPostingsReader(PostingsReaderBase wrappedPostingsReader) throws IOException {
    this.wrappedPostingsReader = wrappedPostingsReader;
  }

  @Override
  public void init(IndexInput termsIn) throws IOException {
    CodecUtil.checkHeader(termsIn, PulsingPostingsWriter.CODEC,
      PulsingPostingsWriter.VERSION_START, PulsingPostingsWriter.VERSION_START);
    maxPositions = termsIn.readVInt();
    wrappedPostingsReader.init(termsIn);
  }

  private static class PulsingTermState extends BlockTermState {
    private byte[] postings;
    private int postingsSize;                     // -1 if this term was not inlined
    private BlockTermState wrappedTermState;

    ByteArrayDataInput inlinedBytesReader;
    private byte[] inlinedBytes;

    @Override
    public Object clone() {
      PulsingTermState clone;
      clone = (PulsingTermState) super.clone();
      if (postingsSize != -1) {
        clone.postings = new byte[postingsSize];
        System.arraycopy(postings, 0, clone.postings, 0, postingsSize);
      } else {
        assert wrappedTermState != null;
        clone.wrappedTermState = (BlockTermState) wrappedTermState.clone();
      }
      return clone;
    }

    @Override
    public void copyFrom(TermState _other) {
      super.copyFrom(_other);
      PulsingTermState other = (PulsingTermState) _other;
      postingsSize = other.postingsSize;
      if (other.postingsSize != -1) {
        if (postings == null || postings.length < other.postingsSize) {
          postings = new byte[ArrayUtil.oversize(other.postingsSize, 1)];
        }
        System.arraycopy(other.postings, 0, postings, 0, other.postingsSize);
      } else {
        wrappedTermState.copyFrom(other.wrappedTermState);
      }

      // NOTE: we do not copy the
      // inlinedBytes/inlinedBytesReader; these are only
      // stored on the "primary" TermState.  They are
      // "transient" to cloned term states.
    }

    @Override
    public String toString() {
      if (postingsSize == -1) {
        return "PulsingTermState: not inlined: wrapped=" + wrappedTermState;
      } else {
        return "PulsingTermState: inlined size=" + postingsSize + " " + super.toString();
      }
    }
  }

  @Override
  public void readTermsBlock(IndexInput termsIn, FieldInfo fieldInfo, BlockTermState _termState) throws IOException {
    //System.out.println("PR.readTermsBlock state=" + _termState);
    final PulsingTermState termState = (PulsingTermState) _termState;
    if (termState.inlinedBytes == null) {
      termState.inlinedBytes = new byte[128];
      termState.inlinedBytesReader = new ByteArrayDataInput();
    }
    int len = termsIn.readVInt();
    //System.out.println("  len=" + len + " fp=" + termsIn.getFilePointer());
    if (termState.inlinedBytes.length < len) {
      termState.inlinedBytes = new byte[ArrayUtil.oversize(len, 1)];
    }
    termsIn.readBytes(termState.inlinedBytes, 0, len);
    termState.inlinedBytesReader.reset(termState.inlinedBytes);
    termState.wrappedTermState.termBlockOrd = 0;
    wrappedPostingsReader.readTermsBlock(termsIn, fieldInfo, termState.wrappedTermState);
  }

  @Override
  public BlockTermState newTermState() throws IOException {
    PulsingTermState state = new PulsingTermState();
    state.wrappedTermState = wrappedPostingsReader.newTermState();
    return state;
  }

  @Override
  public void nextTerm(FieldInfo fieldInfo, BlockTermState _termState) throws IOException {
    //System.out.println("PR nextTerm");
    PulsingTermState termState = (PulsingTermState) _termState;

    // if we have positions, its total TF, otherwise its computed based on docFreq.
    long count = fieldInfo.indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS ? termState.totalTermFreq : termState.docFreq;
    //System.out.println("  count=" + count + " threshold=" + maxPositions);

    if (count <= maxPositions) {

      // Inlined into terms dict -- just read the byte[] blob in,
      // but don't decode it now (we only decode when a DocsEnum
      // or D&PEnum is pulled):
      termState.postingsSize = termState.inlinedBytesReader.readVInt();
      if (termState.postings == null || termState.postings.length < termState.postingsSize) {
        termState.postings = new byte[ArrayUtil.oversize(termState.postingsSize, 1)];
      }
      // TODO: sort of silly to copy from one big byte[]
      // (the blob holding all inlined terms' blobs for
      // current term block) into another byte[] (just the
      // blob for this term)...
      termState.inlinedBytesReader.readBytes(termState.postings, 0, termState.postingsSize);
      //System.out.println("  inlined bytes=" + termState.postingsSize);
    } else {
      //System.out.println("  not inlined");
      termState.postingsSize = -1;
      // TODO: should we do full copyFrom?  much heavier...?
      termState.wrappedTermState.docFreq = termState.docFreq;
      termState.wrappedTermState.totalTermFreq = termState.totalTermFreq;
      wrappedPostingsReader.nextTerm(fieldInfo, termState.wrappedTermState);
      termState.wrappedTermState.termBlockOrd++;
    }
  }

  // TODO: we could actually reuse, by having TL that
  // holds the last wrapped reuse, and vice-versa
  @Override
  public DocsEnum docs(FieldInfo field, BlockTermState _termState, Bits liveDocs, DocsEnum reuse) throws IOException {
    PulsingTermState termState = (PulsingTermState) _termState;
    if (termState.postingsSize != -1) {
      PulsingDocsEnum postings;
      if (reuse instanceof PulsingDocsEnum) {
        postings = (PulsingDocsEnum) reuse;
        if (!postings.canReuse(field)) {
          postings = new PulsingDocsEnum(field);
        }
      } else {
        postings = new PulsingDocsEnum(field);
      }
      return postings.reset(liveDocs, termState);
    } else {
      // TODO: not great that we lose reuse of PulsingDocsEnum in this case:
      if (reuse instanceof PulsingDocsEnum) {
        return wrappedPostingsReader.docs(field, termState.wrappedTermState, liveDocs, null);
      } else {
        return wrappedPostingsReader.docs(field, termState.wrappedTermState, liveDocs, reuse);
      }
    }
  }

  // TODO: -- not great that we can't always reuse
  @Override
  public DocsAndPositionsEnum docsAndPositions(FieldInfo field, BlockTermState _termState, Bits liveDocs, DocsAndPositionsEnum reuse) throws IOException {
    if (field.indexOptions != IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
      return null;
    }
    //System.out.println("D&P: field=" + field.name);

    final PulsingTermState termState = (PulsingTermState) _termState;

    if (termState.postingsSize != -1) {
      PulsingDocsAndPositionsEnum postings;
      if (reuse instanceof PulsingDocsAndPositionsEnum) {
        postings = (PulsingDocsAndPositionsEnum) reuse;
        if (!postings.canReuse(field)) {
          postings = new PulsingDocsAndPositionsEnum(field);
        }
      } else {
        postings = new PulsingDocsAndPositionsEnum(field);
      }

      return postings.reset(liveDocs, termState);
    } else {
      if (reuse instanceof PulsingDocsAndPositionsEnum) {
        return wrappedPostingsReader.docsAndPositions(field, termState.wrappedTermState, liveDocs, null);
      } else {
        return wrappedPostingsReader.docsAndPositions(field, termState.wrappedTermState, liveDocs, reuse);
      }
    }
  }

  private static class PulsingDocsEnum extends DocsEnum {
    private byte[] postingsBytes;
    private final ByteArrayDataInput postings = new ByteArrayDataInput();
    private final IndexOptions indexOptions;
    private final boolean storePayloads;
    private Bits liveDocs;
    private int docID;
    private int freq;
    private int payloadLength;

    public PulsingDocsEnum(FieldInfo fieldInfo) {
      indexOptions = fieldInfo.indexOptions;
      storePayloads = fieldInfo.storePayloads;
    }

    public PulsingDocsEnum reset(Bits liveDocs, PulsingTermState termState) {
      //System.out.println("PR docsEnum termState=" + termState + " docFreq=" + termState.docFreq);
      assert termState.postingsSize != -1;

      // Must make a copy of termState's byte[] so that if
      // app does TermsEnum.next(), this DocsEnum is not affected
      if (postingsBytes == null) {
        postingsBytes = new byte[termState.postingsSize];
      } else if (postingsBytes.length < termState.postingsSize) {
        postingsBytes = ArrayUtil.grow(postingsBytes, termState.postingsSize);
      }
      System.arraycopy(termState.postings, 0, postingsBytes, 0, termState.postingsSize);
      postings.reset(postingsBytes, 0, termState.postingsSize);
      docID = 0;
      payloadLength = 0;
      freq = 1;
      this.liveDocs = liveDocs;
      return this;
    }

    boolean canReuse(FieldInfo fieldInfo) {
      return indexOptions == fieldInfo.indexOptions && storePayloads == fieldInfo.storePayloads;
    }

    @Override
    public int nextDoc() throws IOException {
      //System.out.println("PR nextDoc this= "+ this);
      while(true) {
        if (postings.eof()) {
          //System.out.println("PR   END");
          return docID = NO_MORE_DOCS;
        }

        final int code = postings.readVInt();
        //System.out.println("  read code=" + code);
        if (indexOptions == IndexOptions.DOCS_ONLY) {
          docID += code;
        } else {
          docID += code >>> 1;              // shift off low bit
          if ((code & 1) != 0) {          // if low bit is set
            freq = 1;                     // freq is one
          } else {
            freq = postings.readVInt();     // else read freq
          }

          if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
            // Skip positions
            if (storePayloads) {
              for(int pos=0;pos<freq;pos++) {
                final int posCode = postings.readVInt();
                if ((posCode & 1) != 0) {
                  payloadLength = postings.readVInt();
                }
                if (payloadLength != 0) {
                  postings.skipBytes(payloadLength);
                }
              }
            } else {
              for(int pos=0;pos<freq;pos++) {
                // TODO: skipVInt
                postings.readVInt();
              }
            }
          }
        }

        if (liveDocs == null || liveDocs.get(docID)) {
          return docID;
        }
      }
    }

    @Override
    public int freq() {
      return freq;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int advance(int target) throws IOException {
      int doc;
      while((doc=nextDoc()) != NO_MORE_DOCS) {
        if (doc >= target)
          return doc;
      }
      return docID = NO_MORE_DOCS;
    }
  }

  private static class PulsingDocsAndPositionsEnum extends DocsAndPositionsEnum {
    private byte[] postingsBytes;
    private final ByteArrayDataInput postings = new ByteArrayDataInput();
    private final boolean storePayloads;

    private Bits liveDocs;
    private int docID;
    private int freq;
    private int posPending;
    private int position;
    private int payloadLength;
    private BytesRef payload;

    private boolean payloadRetrieved;

    public PulsingDocsAndPositionsEnum(FieldInfo fieldInfo) {
      storePayloads = fieldInfo.storePayloads;
    }

    boolean canReuse(FieldInfo fieldInfo) {
      return storePayloads == fieldInfo.storePayloads;
    }

    public PulsingDocsAndPositionsEnum reset(Bits liveDocs, PulsingTermState termState) {
      assert termState.postingsSize != -1;
      if (postingsBytes == null) {
        postingsBytes = new byte[termState.postingsSize];
      } else if (postingsBytes.length < termState.postingsSize) {
        postingsBytes = ArrayUtil.grow(postingsBytes, termState.postingsSize);
      }
      System.arraycopy(termState.postings, 0, postingsBytes, 0, termState.postingsSize);
      postings.reset(postingsBytes, 0, termState.postingsSize);
      this.liveDocs = liveDocs;
      payloadLength = 0;
      posPending = 0;
      docID = 0;
      //System.out.println("PR d&p reset storesPayloads=" + storePayloads + " bytes=" + bytes.length + " this=" + this);
      return this;
    }

    @Override
    public int nextDoc() throws IOException {
      //System.out.println("PR d&p nextDoc this=" + this);

      while(true) {
        //System.out.println("  cycle skip posPending=" + posPending);

        skipPositions();

        if (postings.eof()) {
          //System.out.println("PR   END");
          return docID = NO_MORE_DOCS;
        }

        final int code = postings.readVInt();
        docID += code >>> 1;            // shift off low bit
        if ((code & 1) != 0) {          // if low bit is set
          freq = 1;                     // freq is one
        } else {
          freq = postings.readVInt();     // else read freq
        }
        posPending = freq;

        if (liveDocs == null || liveDocs.get(docID)) {
          //System.out.println("  return docID=" + docID + " freq=" + freq);
          position = 0;
          return docID;
        }
      }
    }

    @Override
    public int freq() {
      return freq;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int advance(int target) throws IOException {
      int doc;
      while((doc=nextDoc()) != NO_MORE_DOCS) {
        if (doc >= target) {
          return docID = doc;
        }
      }
      return docID = NO_MORE_DOCS;
    }

    @Override
    public int nextPosition() throws IOException {
      //System.out.println("PR d&p nextPosition posPending=" + posPending + " vs freq=" + freq);
      
      assert posPending > 0;
      posPending--;

      if (storePayloads) {
        if (!payloadRetrieved) {
          //System.out.println("PR     skip payload=" + payloadLength);
          postings.skipBytes(payloadLength);
        }
        final int code = postings.readVInt();
        //System.out.println("PR     code=" + code);
        if ((code & 1) != 0) {
          payloadLength = postings.readVInt();
          //System.out.println("PR     new payload len=" + payloadLength);
        }
        position += code >> 1;
        payloadRetrieved = false;
      } else {
        position += postings.readVInt();
      }

      //System.out.println("PR d&p nextPos return pos=" + position + " this=" + this);
      return position;
    }

    private void skipPositions() throws IOException {
      while(posPending != 0) {
        nextPosition();
      }
      if (storePayloads && !payloadRetrieved) {
        //System.out.println("  skip payload len=" + payloadLength);
        postings.skipBytes(payloadLength);
        payloadRetrieved = true;
      }
    }

    @Override
    public boolean hasPayload() {
      return storePayloads && !payloadRetrieved && payloadLength > 0;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      //System.out.println("PR  getPayload payloadLength=" + payloadLength + " this=" + this);
      if (payloadRetrieved) {
        throw new IOException("Either no payload exists at this term position or an attempt was made to load it more than once.");
      }
      payloadRetrieved = true;
      if (payloadLength > 0) {
        if (payload == null) {
          payload = new BytesRef(payloadLength);
        } else {
          payload.grow(payloadLength);
        }
        postings.readBytes(payload.bytes, 0, payloadLength);
        payload.length = payloadLength;
        return payload;
      } else {
        return null;
      }
    }
  }

  @Override
  public void close() throws IOException {
    wrappedPostingsReader.close();
  }
}
