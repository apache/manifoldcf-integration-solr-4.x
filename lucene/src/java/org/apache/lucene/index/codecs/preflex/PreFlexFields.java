package org.apache.lucene.index.codecs.preflex;

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
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Comparator;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.CompoundFileReader;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.ArrayUtil;

/** Exposes flex API on a pre-flex index, as a codec. 
 * @lucene.experimental */
public class PreFlexFields extends FieldsProducer {

  private static final boolean DEBUG_SURROGATES = false;

  public TermInfosReader tis;
  public final TermInfosReader tisNoIndex;

  public final IndexInput freqStream;
  public final IndexInput proxStream;
  final private FieldInfos fieldInfos;
  private final SegmentInfo si;
  final TreeMap<String,FieldInfo> fields = new TreeMap<String,FieldInfo>();
  private final Directory dir;
  private final int readBufferSize;
  private Directory cfsReader;

  PreFlexFields(Directory dir, FieldInfos fieldInfos, SegmentInfo info, int readBufferSize, int indexDivisor)
    throws IOException {

    si = info;

    // NOTE: we must always load terms index, even for
    // "sequential" scan during merging, because what is
    // sequential to merger may not be to TermInfosReader
    // since we do the surrogates dance:
    if (indexDivisor < 0) {
      indexDivisor = -indexDivisor;
    }

    TermInfosReader r = new TermInfosReader(dir, info.name, fieldInfos, readBufferSize, indexDivisor);    
    if (indexDivisor == -1) {
      tisNoIndex = r;
    } else {
      tisNoIndex = null;
      tis = r;
    }
    this.readBufferSize = readBufferSize;
    this.fieldInfos = fieldInfos;

    // make sure that all index files have been read or are kept open
    // so that if an index update removes them we'll still have them
    freqStream = dir.openInput(info.name + ".frq", readBufferSize);
    boolean anyProx = false;
    final int numFields = fieldInfos.size();
    for(int i=0;i<numFields;i++) {
      final FieldInfo fieldInfo = fieldInfos.fieldInfo(i);
      if (fieldInfo.isIndexed) {
        fields.put(fieldInfo.name, fieldInfo);
        if (!fieldInfo.omitTermFreqAndPositions) {
          anyProx = true;
        }
      }
    }

    if (anyProx) {
      proxStream = dir.openInput(info.name + ".prx", readBufferSize);
    } else {
      proxStream = null;
    }

    this.dir = dir;
  }

  static void files(Directory dir, SegmentInfo info, Collection<String> files) throws IOException {
    files.add(IndexFileNames.segmentFileName(info.name, "", PreFlexCodec.TERMS_EXTENSION));
    files.add(IndexFileNames.segmentFileName(info.name, "", PreFlexCodec.TERMS_INDEX_EXTENSION));
    files.add(IndexFileNames.segmentFileName(info.name, "", PreFlexCodec.FREQ_EXTENSION));
    if (info.getHasProx()) {
      // LUCENE-1739: for certain versions of 2.9-dev,
      // hasProx would be incorrectly computed during
      // indexing as true, and then stored into the segments
      // file, when it should have been false.  So we do the
      // extra check, here:
      final String prx = IndexFileNames.segmentFileName(info.name, "", PreFlexCodec.PROX_EXTENSION);
      if (dir.fileExists(prx)) {
        files.add(prx);
      }
    }
  }

  @Override
  public FieldsEnum iterator() throws IOException {
    return new PreFlexFieldsEnum();
  }

  @Override
  public Terms terms(String field) {
    FieldInfo fi = fieldInfos.fieldInfo(field);
    if (fi != null) {
      return new PreTerms(fi);
    } else {
      return null;
    }
  }

  synchronized private TermInfosReader getTermsDict() {
    if (tis != null) {
      return tis;
    } else {
      return tisNoIndex;
    }
  }

  @Override
  synchronized public void loadTermsIndex(int indexDivisor) throws IOException {
    if (tis == null) {
      Directory dir0;
      if (si.getUseCompoundFile()) {
        // In some cases, we were originally opened when CFS
        // was not used, but then we are asked to open the
        // terms reader with index, the segment has switched
        // to CFS

        if (!(dir instanceof CompoundFileReader)) {
          dir0 = cfsReader = new CompoundFileReader(dir, IndexFileNames.segmentFileName(si.name, "", IndexFileNames.COMPOUND_FILE_EXTENSION), readBufferSize);
        } else {
          dir0 = dir;
        }
        dir0 = cfsReader;
      } else {
        dir0 = dir;
      }

      tis = new TermInfosReader(dir0, si.name, fieldInfos, readBufferSize, indexDivisor);
    }
  }

  @Override
  public void close() throws IOException {
    if (tis != null) {
      tis.close();
    }
    if (tisNoIndex != null) {
      tisNoIndex.close();
    }
    if (cfsReader != null) {
      cfsReader.close();
    }
  }

  private class PreFlexFieldsEnum extends FieldsEnum {
    final Iterator<FieldInfo> it;
    private final PreTermsEnum termsEnum;
    FieldInfo current;

    public PreFlexFieldsEnum() throws IOException {
      it = fields.values().iterator();
      termsEnum = new PreTermsEnum();
    }

    @Override
    public String next() {
      if (it.hasNext()) {
        current = it.next();
        return current.name;
      } else {
        return null;
      }
    }

    @Override
    public TermsEnum terms() throws IOException {
      termsEnum.reset(current);
      return termsEnum;
    }
  }
  
  private class PreTerms extends Terms {
    final FieldInfo fieldInfo;
    PreTerms(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
    }

    @Override
    public TermsEnum iterator() throws IOException {    
      PreTermsEnum termsEnum = new PreTermsEnum();
      termsEnum.reset(fieldInfo);
      return termsEnum;
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      // Pre-flex indexes always sorted in UTF16 order, but
      // we remap on-the-fly to unicode order
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }
  }

  private class PreTermsEnum extends TermsEnum {
    private SegmentTermEnum termEnum;
    private FieldInfo fieldInfo;
    private boolean skipNext;
    private BytesRef current;
    private final BytesRef scratchBytesRef = new BytesRef();

    private int[] surrogateSeekPending = new int[1];
    private boolean[] surrogateDidSeekBack = new boolean[1];
    private int surrogateSeekUpto;
    private char[] pendingPrefix;

    private SegmentTermEnum seekTermEnum;
    private Term protoTerm;
    private int newSuffixStart;

    void reset(FieldInfo fieldInfo) throws IOException {
      this.fieldInfo = fieldInfo;
      protoTerm = new Term(fieldInfo.name);
      if (termEnum == null) {
        termEnum = getTermsDict().terms(protoTerm);
        seekTermEnum = getTermsDict().terms(protoTerm);
      } else {
        getTermsDict().seekEnum(termEnum, protoTerm);
      }
      skipNext = true;
      
      surrogateSeekUpto = 0;
      newSuffixStart = 0;

      surrogatesDance();
    }

    private void surrogatesDance() throws IOException {
      
      // Tricky: prior to 4.0, Lucene index sorted terms in
      // UTF16 order, but as of 4.0 we sort by Unicode code
      // point order.  These orders differ because of the
      // surrrogates; so we have to fixup our enum, here, by
      // carefully first seeking past the surrogates and
      // then back again at the end.  The process is
      // recursive, since any given term could have multiple
      // new occurrences of surrogate pairs, so we use a
      // stack to record the pending seek-backs.
      if (DEBUG_SURROGATES) {
        System.out.println("  dance start term=" + (termEnum.term() == null ? null : UnicodeUtil.toHexString(termEnum.term().text())));
      }

      while(popPendingSeek());
      while(pushNewSurrogate());
    }

    // only for debugging
    private String getStack() {
      if (surrogateSeekUpto == 0) {
        return "null";
      } else {
        StringBuffer sb = new StringBuffer();
        for(int i=0;i<surrogateSeekUpto;i++) {
          if (i > 0) {
            sb.append(' ');
          }
          sb.append(surrogateSeekPending[i]);
        }
        sb.append(" pendingSeekText=" + new String(pendingPrefix, 0, surrogateSeekPending[surrogateSeekUpto-1]));
        return sb.toString();
      }
    }

    private boolean popPendingSeek() throws IOException {
      if (DEBUG_SURROGATES) {
        System.out.println("  check pop newSuffix=" + newSuffixStart + " stack=" + getStack());
      }
      // if a .next() has advanced beyond the
      // after-surrogates range we had last seeked to, we
      // must seek back to the start and resume .next from
      // there.  this pops the pending seek off the stack.
      final Term t = termEnum.term();
      if (surrogateSeekUpto > 0) {
        final int seekPrefix = surrogateSeekPending[surrogateSeekUpto-1];
        if (DEBUG_SURROGATES) {
          System.out.println("    seekPrefix=" + seekPrefix);
        }
        if (newSuffixStart < seekPrefix) {
          assert pendingPrefix != null;
          assert pendingPrefix.length > seekPrefix;
          pendingPrefix[seekPrefix] = UnicodeUtil.UNI_SUR_HIGH_START;
          Term t2 = protoTerm.createTerm(new String(pendingPrefix, 0, 1+seekPrefix));
          if (DEBUG_SURROGATES) {
            System.out.println("    do pop; seek back to " + UnicodeUtil.toHexString(t2.text()));
          }
          getTermsDict().seekEnum(termEnum, t2);
          surrogateDidSeekBack[surrogateSeekUpto-1] = true;

          // +2 because we don't want to re-check the
          // surrogates we just seek'd back to
          newSuffixStart = seekPrefix + 2;
          return true;
        } else if (newSuffixStart == seekPrefix && surrogateDidSeekBack[surrogateSeekUpto-1] && t != null && t.field() == fieldInfo.name && t.text().charAt(seekPrefix) > UnicodeUtil.UNI_SUR_LOW_END) {
          assert pendingPrefix != null;
          assert pendingPrefix.length > seekPrefix;
          pendingPrefix[seekPrefix] = 0xffff;
          Term t2 = protoTerm.createTerm(new String(pendingPrefix, 0, 1+seekPrefix));
          if (DEBUG_SURROGATES) {
            System.out.println("    finish pop; seek fwd to " + UnicodeUtil.toHexString(t2.text()));
          }
          getTermsDict().seekEnum(termEnum, t2);
          if (DEBUG_SURROGATES) {
            System.out.println("    found term=" + (termEnum.term() == null ? null : UnicodeUtil.toHexString(termEnum.term().text())));
          }
          surrogateSeekUpto--;

          if (termEnum.term() == null || termEnum.term().field() != fieldInfo.name) {
            // force pop
            newSuffixStart = -1;
          } else {
            newSuffixStart = termEnum.newSuffixStart;
          }

          return true;
        }
      }

      return false;
    }

    private boolean pushNewSurrogate() throws IOException {
      if (DEBUG_SURROGATES) {
        System.out.println("  check push newSuffix=" + newSuffixStart + " stack=" + getStack());
      }
      final Term t = termEnum.term();
      if (t == null || t.field() != fieldInfo.name) {
        return false;
      }
      final String text = t.text();
      final int textLen = text.length();

      for(int i=Math.max(0,newSuffixStart);i<textLen;i++) {
        final char ch = text.charAt(i);
        if (ch >= UnicodeUtil.UNI_SUR_HIGH_START && ch <= UnicodeUtil.UNI_SUR_HIGH_END && (surrogateSeekUpto == 0 || i > surrogateSeekPending[surrogateSeekUpto-1])) {

          if (DEBUG_SURROGATES) {
            System.out.println("    found high surr 0x" + Integer.toHexString(ch) + " at pos=" + i);
          }

          // the next() that we just did read in a new
          // suffix, containing a surrogate pair

          // seek forward to see if there are any terms with
          // this same prefix, but with characters after the
          // surrogate range; if so, we must first iterate
          // them, then seek back to the surrogates

          char[] testPrefix = new char[i+1];
          for(int j=0;j<i;j++) {
            testPrefix[j] = text.charAt(j);
          }
          testPrefix[i] = 1+UnicodeUtil.UNI_SUR_LOW_END;

          getTermsDict().seekEnum(seekTermEnum, protoTerm.createTerm(new String(testPrefix)));

          Term t2 = seekTermEnum.term();
          boolean isPrefix;
          if (t2 != null && t2.field() == fieldInfo.name) {
            String seekText = t2.text();
            isPrefix = true;
            if (DEBUG_SURROGATES) {
              System.out.println("      seek found " + UnicodeUtil.toHexString(seekText));
            }
            for(int j=0;j<i;j++) {
              if (testPrefix[j] != seekText.charAt(j)) {
                isPrefix = false;
                break;
              }
            }
            if (DEBUG_SURROGATES && !isPrefix) {
              System.out.println("      no end terms");
            }
          } else {
            if (DEBUG_SURROGATES) {
              System.out.println("      no end terms");
            }
            isPrefix = false;
          }

          if (isPrefix) {
            // we found a term, sharing the same prefix,
            // with characters after the surrogates, so we
            // must first enum those, and then return the
            // the surrogates afterwards.  push that pending
            // seek on the surrogates stack now:
            pendingPrefix = testPrefix;

            getTermsDict().seekEnum(termEnum, t2);

            if (surrogateSeekUpto == surrogateSeekPending.length) {
              surrogateSeekPending = ArrayUtil.grow(surrogateSeekPending);
            }
            if (surrogateSeekUpto == surrogateDidSeekBack.length) {
              surrogateDidSeekBack = ArrayUtil.grow(surrogateDidSeekBack);
            }
            surrogateSeekPending[surrogateSeekUpto] = i;
            surrogateDidSeekBack[surrogateSeekUpto] = false;
            surrogateSeekUpto++;

            if (DEBUG_SURROGATES) {
              System.out.println("      do push " + i + "; end term=" + UnicodeUtil.toHexString(t2.text()));
            }

            newSuffixStart = i+1;

            return true;
          } else {
            // there are no terms after the surrogates, so
            // we do nothing to the enum and just step
            // through the surrogates like normal.  but we
            // must keep iterating through the term, in case
            // another surrogate pair appears later
          }
        }
      }

      return false;
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      // Pre-flex indexes always sorted in UTF16 order, but
      // we remap on-the-fly to unicode order
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    @Override
    public SeekStatus seek(long ord) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SeekStatus seek(BytesRef term, boolean useCache) throws IOException {
      if (DEBUG_SURROGATES) {
        System.out.println("TE.seek() term=" + term.utf8ToString());
      }
      skipNext = false;
      final TermInfosReader tis = getTermsDict();
      final Term t0 = protoTerm.createTerm(term.utf8ToString());

      assert termEnum != null;

      if (termEnum == null) {
        termEnum = tis.terms(t0);
      } else {
        tis.seekEnum(termEnum, t0);
      }

      surrogateSeekUpto = 0;
      surrogatesDance();

      final Term t = termEnum.term();

      final BytesRef tr;
      if (t != null) {
        tr = scratchBytesRef;
        scratchBytesRef.copy(t.text());
      } else {
        tr = null;
      }

      if (t != null && t.field() == fieldInfo.name && term.bytesEquals(tr)) {
        current = tr;
        return SeekStatus.FOUND;
      } else if (t == null || t.field() != fieldInfo.name) {
        current = null;
        return SeekStatus.END;
      } else {
        current = tr;
        return SeekStatus.NOT_FOUND;
      }
    }

    @Override
    public BytesRef next() throws IOException {
      if (DEBUG_SURROGATES) {
        System.out.println("TE.next() skipNext=" + skipNext);
      }
      if (skipNext) {
        skipNext = false;
        if (termEnum.term() == null) {
          return null;
        } else {
          scratchBytesRef.copy(termEnum.term().text());
          return current = scratchBytesRef;
        }
      }
      if (termEnum.next() && termEnum.term().field() == fieldInfo.name) {
        newSuffixStart = termEnum.newSuffixStart;
        if (DEBUG_SURROGATES) {
          System.out.println("  set newSuffixStart=" + newSuffixStart);
        }
        surrogatesDance();
        final Term t = termEnum.term();
        if (t == null || t.field() != fieldInfo.name) {
          assert t == null || !t.field().equals(fieldInfo.name); // make sure fields are in fact interned
          current = null;
        } else {
          scratchBytesRef.copy(t.text());
          current = scratchBytesRef;
        }
        return current;
      } else {
        if (DEBUG_SURROGATES) {
          System.out.println("  force pop");
        }
        // force pop
        newSuffixStart = -1;
        surrogatesDance();
        final Term t = termEnum.term();
        if (t == null || t.field() != fieldInfo.name) {
          assert t == null || !t.field().equals(fieldInfo.name); // make sure fields are in fact interned
          return null;
        } else {
          scratchBytesRef.copy(t.text());
          current = scratchBytesRef;
          return current;
        }
      }
    }

    @Override
    public BytesRef term() {
      return current;
    }

    @Override
    public int docFreq() {
      return termEnum.docFreq();
    }

    @Override
    public DocsEnum docs(Bits skipDocs, DocsEnum reuse) throws IOException {
      if (reuse != null) {
        return ((PreDocsEnum) reuse).reset(termEnum, skipDocs);        
      } else {
        return (new PreDocsEnum()).reset(termEnum, skipDocs);
      }
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
      if (reuse != null) {
        return ((PreDocsAndPositionsEnum) reuse).reset(termEnum, skipDocs);        
      } else {
        return (new PreDocsAndPositionsEnum()).reset(termEnum, skipDocs);
      }
    }
  }

  private final class PreDocsEnum extends DocsEnum {
    final private SegmentTermDocs docs;

    PreDocsEnum() throws IOException {
      docs = new SegmentTermDocs(freqStream, getTermsDict(), fieldInfos);
    }

    public PreDocsEnum reset(SegmentTermEnum termEnum, Bits skipDocs) throws IOException {
      docs.setSkipDocs(skipDocs);
      docs.seek(termEnum);
      return this;
    }

    @Override
    public int nextDoc() throws IOException {
      if (docs.next()) {
        return docs.doc();
      } else {
        return NO_MORE_DOCS;
      }
    }

    @Override
    public int advance(int target) throws IOException {
      if (docs.skipTo(target)) {
        return docs.doc();
      } else {
        return NO_MORE_DOCS;
      }
    }

    @Override
    public int freq() {
      return docs.freq();
    }

    @Override
    public int docID() {
      return docs.doc();
    }

    @Override
    public int read() throws IOException {
      if (bulkResult == null) {
        initBulkResult();
        bulkResult.docs.ints = new int[32];
        bulkResult.freqs.ints = new int[32];
      }
      return this.docs.read(bulkResult.docs.ints, bulkResult.freqs.ints);
    }
  }

  private final class PreDocsAndPositionsEnum extends DocsAndPositionsEnum {
    final private SegmentTermPositions pos;

    PreDocsAndPositionsEnum() throws IOException {
      pos = new SegmentTermPositions(freqStream, proxStream, getTermsDict(), fieldInfos);
    }

    public DocsAndPositionsEnum reset(SegmentTermEnum termEnum, Bits skipDocs) throws IOException {
      pos.setSkipDocs(skipDocs);
      pos.seek(termEnum);
      return this;
    }

    @Override
    public int nextDoc() throws IOException {
      if (pos.next()) {
        return pos.doc();
      } else {
        return NO_MORE_DOCS;
      }
    }

    @Override
    public int advance(int target) throws IOException {
      if (pos.skipTo(target)) {
        return pos.doc();
      } else {
        return NO_MORE_DOCS;
      }
    }

    @Override
    public int freq() {
      return pos.freq();
    }

    @Override
    public int docID() {
      return pos.doc();
    }

    @Override
    public int nextPosition() throws IOException {
      return pos.nextPosition();
    }

    @Override
    public boolean hasPayload() {
      return pos.isPayloadAvailable();
    }

    private BytesRef payload;

    @Override
    public BytesRef getPayload() throws IOException {
      final int len = pos.getPayloadLength();
      if (payload == null) {
        payload = new BytesRef();
        payload.bytes = new byte[len];
      } else {
        if (payload.bytes.length < len) {
          payload.grow(len);
        }
      }
      
      payload.bytes = pos.getPayload(payload.bytes, 0);
      payload.length = len;
      return payload;
    }
  }
}
