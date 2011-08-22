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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RunAutomaton;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.Util;

/** A block-based terms index and dictionary that assigns
 *  terms to variable length blocks according to how they
 *  share prefixes.  The terms index is a prefix trie
 *  whose leaves are term blocks.  The advantage of this
 *  approach is that seekExact is often able to
 *  determine a term cannot exist without doing any IO, and
 *  intersection with Automata is very fast.  Note that this
 *  terms dictionary has it's own fixed terms index (ie, it
 *  does not support a pluggable terms index
 *  implementation).
 *
 *  <p><b>NOTE</b>: this terms dictionary does not support
 *  index divisor when opening an IndexReader.  Instead, you
 *  can change the min/maxItemsPerBlock during indexing.</p>
 *
 *  <p>The data structure used by this implementation is very
 *  similar to a burst trie
 *  (http://citeseer.ist.psu.edu/viewdoc/summary?doi=10.1.1.18.3499),
 *  but with added logic to break up too-large blocks of all
 *  terms sharing a given prefix into smaller ones.</p>
 *
 *  <p>Use {@link org.apache.lucene.index.CheckIndex} with the <code>-verbose</code>
 *  option to see summary statistics on the blocks in the
 *  dictionary.
 *
 *  See {@link BlockTreeTermsWriter}.
 *
 * @lucene.experimental
 */

public class BlockTreeTermsReader extends FieldsProducer {

  // Open input to the main terms dict file (_X.tib)
  private final IndexInput in;

  //private static final boolean DEBUG = BlockTreeTermsWriter.DEBUG;

  // Reads the terms dict entries, to gather state to
  // produce DocsEnum on demand
  private final PostingsReaderBase postingsReader;

  private final TreeMap<String,FieldReader> fields = new TreeMap<String,FieldReader>();

  // keeps the dirStart offset
  protected long dirOffset;
  protected long indexDirOffset;

  private String segment;
  
  public BlockTreeTermsReader(Directory dir, FieldInfos fieldInfos, String segment,
                              PostingsReaderBase postingsReader, IOContext ioContext,
                              int codecId, int indexDivisor)
    throws IOException {
    
    this.postingsReader = postingsReader;

    this.segment = segment;
    in = dir.openInput(IndexFileNames.segmentFileName(segment, codecId, BlockTreeTermsWriter.TERMS_EXTENSION),
                       ioContext);

    boolean success = false;
    IndexInput indexIn = null;

    try {
      readHeader(in);
      if (indexDivisor != -1) {
        indexIn = dir.openInput(IndexFileNames.segmentFileName(segment, codecId, BlockTreeTermsWriter.TERMS_INDEX_EXTENSION),
                                ioContext);
        readIndexHeader(indexIn);
      }

      // Have PostingsReader init itself
      postingsReader.init(in);

      // Read per-field details
      seekDir(in, dirOffset);
      if (indexDivisor != -1) {
        seekDir(indexIn, indexDirOffset);
      }

      final int numFields = in.readVInt();

      for(int i=0;i<numFields;i++) {
        final int field = in.readVInt();
        final long numTerms = in.readVLong();
        assert numTerms >= 0;
        final int numBytes = in.readVInt();
        final BytesRef rootCode = new BytesRef(new byte[numBytes]);
        in.readBytes(rootCode.bytes, 0, numBytes);
        rootCode.length = numBytes;
        final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
        assert fieldInfo != null: "field=" + field;
        final long sumTotalTermFreq = fieldInfo.indexOptions == IndexOptions.DOCS_ONLY ? -1 : in.readVLong();
        final long sumDocFreq = in.readVLong();
        final long indexStartFP = indexDivisor != -1 ? indexIn.readVLong() : 0;
        assert !fields.containsKey(fieldInfo.name);
        fields.put(fieldInfo.name, new FieldReader(fieldInfo, numTerms, rootCode, sumTotalTermFreq, sumDocFreq, indexStartFP, indexIn));
      }
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeSafely(true, indexIn, this);
      } else if (indexDivisor != -1) {
        indexIn.close();
      }
    }
  }

  protected void readHeader(IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, BlockTreeTermsWriter.CODEC_NAME,
                          BlockTreeTermsWriter.VERSION_START,
                          BlockTreeTermsWriter.VERSION_CURRENT);
    dirOffset = input.readLong();    
  }

  protected void readIndexHeader(IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, BlockTreeTermsWriter.CODEC_NAME,
                          BlockTreeTermsWriter.VERSION_START,
                          BlockTreeTermsWriter.VERSION_CURRENT);
    indexDirOffset = input.readLong();    
  }
  
  protected void seekDir(IndexInput input, long dirOffset)
      throws IOException {
    input.seek(dirOffset);
  }

  // for debugging
  // private static String toHex(int v) {
  //   return "0x" + Integer.toHexString(v);
  // }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.closeSafely(false, in, postingsReader);
    } finally { 
      for(FieldReader field : fields.values()) {
        field.close();
      }
      // Clear so refs to terms index is GCable even if
      // app hangs onto us:
      fields.clear();
    }
  }

  public static void files(Directory dir, SegmentInfo segmentInfo, int codecID, Collection<String> files) {
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, codecID, BlockTreeTermsWriter.TERMS_EXTENSION));
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, codecID, BlockTreeTermsWriter.TERMS_INDEX_EXTENSION));
  }

  public static void getExtensions(Collection<String> extensions) {
    extensions.add(BlockTreeTermsWriter.TERMS_EXTENSION);
    extensions.add(BlockTreeTermsWriter.TERMS_INDEX_EXTENSION);
  }

  @Override
  public FieldsEnum iterator() {
    return new TermFieldsEnum();
  }

  @Override
  public Terms terms(String field) throws IOException {
    return fields.get(field);
  }

  // Iterates through all fields
  private class TermFieldsEnum extends FieldsEnum {
    final Iterator<FieldReader> it;
    FieldReader current;

    TermFieldsEnum() {
      it = fields.values().iterator();
    }

    @Override
    public String next() {
      if (it.hasNext()) {
        current = it.next();
        return current.fieldInfo.name;
      } else {
        current = null;
        return null;
      }
    }
    
    @Override
    public TermsEnum terms() throws IOException {
      return current.iterator();
    }
  }

  // for debugging
  String brToString(BytesRef b) {
    if (b == null) {
      return "null";
    } else {
      try {
        return b.utf8ToString() + " " + b;
      } catch (Throwable t) {
        // If BytesRef isn't actually UTF8, or it's eg a
        // prefix of UTF8 that ends mid-unicode-char, we
        // fallback to hex:
        return b.toString();
      }
    }
  }

  public static class Stats {
    public int indexNodeCount;
    public int indexArcCount;
    public int indexNumBytes;

    public long totalTermCount;
    public long totalTermBytes;


    public int nonFloorBlockCount;
    public int floorBlockCount;
    public int floorSubBlockCount;
    public int mixedBlockCount;
    public int termsOnlyBlockCount;
    public int subBlocksOnlyBlockCount;
    public int totalBlockCount;

    public int[] blockCountByPrefixLen = new int[10];
    private int startBlockCount;
    private int endBlockCount;
    public long totalBlockSuffixBytes;
    public long totalBlockStatsBytes;

    // Postings impl plus the other few vInts stored in
    // the frame:
    public long totalBlockOtherBytes;

    public final String segment;
    public final String field;

    public Stats(String segment, String field) {
      this.segment = segment;
      this.field = field;
    }

    void startBlock(FieldReader.SegmentTermsEnum.Frame frame, boolean isFloor) {
      totalBlockCount++;
      if (isFloor) {
        if (frame.fp == frame.fpOrig) {
          floorBlockCount++;
        }
        floorSubBlockCount++;
      } else {
        nonFloorBlockCount++;
      }

      if (blockCountByPrefixLen.length <= frame.prefix) {
        blockCountByPrefixLen = ArrayUtil.grow(blockCountByPrefixLen, 1+frame.prefix);
      }
      blockCountByPrefixLen[frame.prefix]++;
      startBlockCount++;
      totalBlockSuffixBytes += frame.suffixesReader.length();
      totalBlockStatsBytes += frame.statsReader.length();
    }

    void endBlock(FieldReader.SegmentTermsEnum.Frame frame) {
      final int termCount = frame.isLeafBlock ? frame.entCount : frame.state.termBlockOrd;
      final int subBlockCount = frame.entCount - termCount;
      totalTermCount += termCount;
      if (termCount != 0 && subBlockCount != 0) {
        mixedBlockCount++;
      } else if (termCount != 0) {
        termsOnlyBlockCount++;
      } else if (subBlockCount != 0) {
        subBlocksOnlyBlockCount++;
      } else {
        throw new IllegalStateException();
      }
      endBlockCount++;
      final long otherBytes = frame.fpEnd - frame.fp - frame.suffixesReader.length() - frame.statsReader.length();
      assert otherBytes > 0 : "otherBytes=" + otherBytes + " frame.fp=" + frame.fp + " frame.fpEnd=" + frame.fpEnd;
      totalBlockOtherBytes += otherBytes;
    }

    void term(BytesRef term) {
      totalTermBytes += term.length;
    }

    void finish() {
      assert startBlockCount == endBlockCount: "startBlockCount=" + startBlockCount + " endBlockCount=" + endBlockCount;
      assert totalBlockCount == floorSubBlockCount + nonFloorBlockCount: "floorSubBlockCount=" + floorSubBlockCount + " nonFloorBlockCount=" + nonFloorBlockCount + " totalBlockCount=" + totalBlockCount;
      assert totalBlockCount == mixedBlockCount + termsOnlyBlockCount + subBlocksOnlyBlockCount: "totalBlockCount=" + totalBlockCount + " mixedBlockCount=" + mixedBlockCount + " subBlocksOnlyBlockCount=" + subBlocksOnlyBlockCount + " termsOnlyBlockCount=" + termsOnlyBlockCount;
    }

    @Override
    public String toString() {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
      final PrintStream out = new PrintStream(bos);
      
      out.println("  index FST:");
      out.println("    " + indexNodeCount + " nodes");
      out.println("    " + indexArcCount + " arcs");
      out.println("    " + indexNumBytes + " bytes");
      out.println("  terms:");
      out.println("    " + totalTermCount + " terms");
      out.println("    " + totalTermBytes + " bytes" + (totalTermCount != 0 ? " (" + String.format("%.1f", ((double) totalTermBytes)/totalTermCount) + " bytes/term)" : ""));
      out.println("  blocks:");
      out.println("    " + totalBlockCount + " blocks");
      out.println("    " + termsOnlyBlockCount + " terms-only blocks");
      out.println("    " + subBlocksOnlyBlockCount + " sub-block-only blocks");
      out.println("    " + mixedBlockCount + " mixed blocks");
      out.println("    " + floorBlockCount + " floor blocks");
      out.println("    " + (totalBlockCount-floorSubBlockCount) + " non-floor blocks");
      out.println("    " + floorSubBlockCount + " floor sub-blocks");
      out.println("    " + totalBlockSuffixBytes + " term suffix bytes" + (totalBlockCount != 0 ? " (" + String.format("%.1f", ((double) totalBlockSuffixBytes)/totalBlockCount) + " suffix-bytes/block)" : ""));
      out.println("    " + totalBlockStatsBytes + " term stats bytes" + (totalBlockCount != 0 ? " (" + String.format("%.1f", ((double) totalBlockStatsBytes)/totalBlockCount) + " stats-bytes/block)" : ""));
      out.println("    " + totalBlockOtherBytes + " other bytes" + (totalBlockCount != 0 ? " (" + String.format("%.1f", ((double) totalBlockOtherBytes)/totalBlockCount) + " other-bytes/block)" : ""));
      if (totalBlockCount != 0) {
        out.println("    by prefix length:");
        int total = 0;
        for(int prefix=0;prefix<blockCountByPrefixLen.length;prefix++) {
          final int blockCount = blockCountByPrefixLen[prefix];
          total += blockCount;
          if (blockCount != 0) {
            out.println("      " + String.format("%2d", prefix) + ": " + blockCount);
          }
        }
        assert totalBlockCount == total;
      }

      return bos.toString();
    }
  }

  final Outputs<BytesRef> fstOutputs = ByteSequenceOutputs.getSingleton();
  final BytesRef NO_OUTPUT = fstOutputs.getNoOutput();

  public final class FieldReader extends Terms implements Closeable {
    final long numTerms;
    final FieldInfo fieldInfo;
    final long sumTotalTermFreq;
    final long sumDocFreq;
    final long indexStartFP;
    final long rootBlockFP;
    final BytesRef rootCode;
    private FST<BytesRef> index;

    //private boolean DEBUG;

    FieldReader(FieldInfo fieldInfo, long numTerms, BytesRef rootCode, long sumTotalTermFreq, long sumDocFreq, long indexStartFP, IndexInput indexIn) throws IOException {
      assert numTerms > 0;
      this.fieldInfo = fieldInfo;
      //DEBUG = BlockTreeTermsReader.DEBUG && fieldInfo.name.equals("id");
      this.numTerms = numTerms;
      this.sumTotalTermFreq = sumTotalTermFreq; 
      this.sumDocFreq = sumDocFreq; 
      this.indexStartFP = indexStartFP;
      this.rootCode = rootCode;
      // if (DEBUG) {
      //   System.out.println("BTTR: seg=" + segment + " field=" + fieldInfo.name + " rootBlockCode=" + rootCode + " divisor=" + indexDivisor);
      // }

      rootBlockFP = (new ByteArrayDataInput(rootCode.bytes, rootCode.offset, rootCode.length)).readVLong() >>> BlockTreeTermsWriter.OUTPUT_FLAGS_NUM_BITS;

      if (indexIn != null) {
        final IndexInput clone = (IndexInput) indexIn.clone();
        //System.out.println("start=" + indexStartFP + " field=" + fieldInfo.name);
        clone.seek(indexStartFP);
        index = new FST<BytesRef>(clone, ByteSequenceOutputs.getSingleton());
        
        /*
        if (false) {
          final String dotFileName = segment + "_" + fieldInfo.name + ".dot";
          Writer w = new OutputStreamWriter(new FileOutputStream(dotFileName));
          Util.toDot(index, w, false, false);
          System.out.println("FST INDEX: SAVED to " + dotFileName);
          w.close();
        }
        */
      }
    }

    /** For debugging -- used by CheckIndex too*/
    // TODO: maybe push this into Terms?
    public Stats computeStats() throws IOException {
      return new SegmentTermsEnum().computeBlockStats();
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    @Override
    public void close() {
      super.close();
    }
    
    @Override
    public TermsEnum iterator() throws IOException {
      return new SegmentTermsEnum();
    }

    @Override
    public long getUniqueTermCount() {
      return numTerms;
    }

    @Override
    public long getSumTotalTermFreq() {
      return sumTotalTermFreq;
    }

    @Override
    public long getSumDocFreq() {
      return sumDocFreq;
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
      if (compiled.type != CompiledAutomaton.AUTOMATON_TYPE.NORMAL) {
        throw new IllegalArgumentException("please use CompiledAutomaton.getTermsEnum instead");
      }
      return new IntersectEnum(compiled, startTerm);
    }
    
    // NOTE: cannot seek!
    private final class IntersectEnum extends TermsEnum {
      private final IndexInput in;

      private Frame[] stack;
      
      @SuppressWarnings("unchecked") private FST.Arc<BytesRef>[] arcs = new FST.Arc[5];

      private final RunAutomaton runAutomaton;
      private final CompiledAutomaton compiledAutomaton;

      private Frame currentFrame;

      private final BytesRef term = new BytesRef();

      // TODO: can we share this with the frame in STE?
      private final class Frame {
        final int ord;
        long fp;
        long fpOrig;
        long fpEnd;
        long lastSubFP;

        // State in automaton
        int state;

        int metaDataUpto;

        byte[] suffixBytes = new byte[128];
        final ByteArrayDataInput suffixesReader = new ByteArrayDataInput();

        byte[] statBytes = new byte[64];
        final ByteArrayDataInput statsReader = new ByteArrayDataInput();

        byte[] floorData = new byte[32];
        final ByteArrayDataInput floorDataReader = new ByteArrayDataInput();

        // Length of prefix shared by all terms in this block
        int prefix;

        // Number of entries (term or sub-block) in this block
        int entCount;

        // Which term we will next read
        int nextEnt;

        // True if this block is either not a floor block,
        // or, it's the last sub-block of a floor block
        boolean isLastInFloor;

        // True if all entries are terms
        boolean isLeafBlock;

        int numFollowFloorBlocks;
        int nextFloorLabel;
        
        Transition[] transitions;
        int curTransitionMax;
        int transitionIndex;

        FST.Arc<BytesRef> arc;

        final BlockTermState termState;

        // Cumulative output so far
        BytesRef outputPrefix;

        private int startBytePos;
        private int suffix;

        public Frame(int ord) throws IOException {
          this.ord = ord;
          termState = postingsReader.newTermState();
          termState.totalTermFreq = -1;
        }

        void loadNextFloorBlock() throws IOException {
          assert numFollowFloorBlocks > 0;
          //if (DEBUG) System.out.println("    loadNextFoorBlock trans=" + transitions[transitionIndex]);

          do {
            fp = fpOrig + (floorDataReader.readVLong() >>> 1);
            numFollowFloorBlocks--;
            // if (DEBUG) System.out.println("    skip floor block2!  nextFloorLabel=" + (char) nextFloorLabel + " vs target=" + (char) transitions[transitionIndex].getMin() + " newFP=" + fp + " numFollowFloorBlocks=" + numFollowFloorBlocks);
            if (numFollowFloorBlocks != 0) {
              nextFloorLabel = floorDataReader.readByte() & 0xff;
            } else {
              nextFloorLabel = 256;
            }
            // if (DEBUG) System.out.println("    nextFloorLabel=" + (char) nextFloorLabel);
          } while (numFollowFloorBlocks != 0 && nextFloorLabel <= transitions[transitionIndex].getMin());

          load(null);
        }

        public void setState(int state) {
          this.state = state;
          transitionIndex = 0;
          transitions = compiledAutomaton.sortedTransitions[state];
          if (transitions.length != 0) {
            curTransitionMax = transitions[0].getMax();
          } else {
            curTransitionMax = -1;
          }
        }

        void load(BytesRef frameIndexData) throws IOException {

          // if (DEBUG) System.out.println("    load fp=" + fp + " fpOrig=" + fpOrig + " frameIndexData=" + frameIndexData + " trans=" + (transitions.length != 0 ? transitions[0] : "n/a" + " state=" + state));

          if (frameIndexData != null && transitions.length != 0) {
            // Floor frame
            if (floorData.length < frameIndexData.length) {
              this.floorData = new byte[ArrayUtil.oversize(frameIndexData.length, 1)];
            }
            System.arraycopy(frameIndexData.bytes, frameIndexData.offset, floorData, 0, frameIndexData.length);
            floorDataReader.reset(floorData, 0, frameIndexData.length);
            // Skip first long -- has redundant fp, hasTerms
            // flag, isFloor flag
            final long code = floorDataReader.readVLong();
            if ((code & BlockTreeTermsWriter.OUTPUT_FLAG_IS_FLOOR) != 0) {
              numFollowFloorBlocks = floorDataReader.readVInt();
              nextFloorLabel = floorDataReader.readByte() & 0xff;
              // if (DEBUG) System.out.println("    numFollowFloorBlocks=" + numFollowFloorBlocks + " nextFloorLabel=" + nextFloorLabel);

              // If current state is accept, we must process
              // first block in case it has empty suffix:
              if (!runAutomaton.isAccept(state)) {
                // Maybe skip floor blocks:
                while (numFollowFloorBlocks != 0 && nextFloorLabel <= transitions[0].getMin()) {
                  fp = fpOrig + (floorDataReader.readVLong() >>> 1);
                  numFollowFloorBlocks--;
                  // if (DEBUG) System.out.println("    skip floor block!  nextFloorLabel=" + (char) nextFloorLabel + " vs target=" + (char) transitions[0].getMin() + " newFP=" + fp + " numFollowFloorBlocks=" + numFollowFloorBlocks);
                  if (numFollowFloorBlocks != 0) {
                    nextFloorLabel = floorDataReader.readByte() & 0xff;
                  } else {
                    nextFloorLabel = 256;
                  }
                }
              }
            }
          }

          in.seek(fp);
          int code = in.readVInt();
          entCount = code >>> 1;
          assert entCount > 0;
          isLastInFloor = (code & 1) != 0;

          // term suffixes:
          code = in.readVInt();
          isLeafBlock = (code & 1) != 0;
          int numBytes = code >>> 1;
          // if (DEBUG) System.out.println("      entCount=" + entCount + " lastInFloor?=" + isLastInFloor + " leafBlock?=" + isLeafBlock + " numSuffixBytes=" + numBytes);
          if (suffixBytes.length < numBytes) {
            suffixBytes = new byte[ArrayUtil.oversize(numBytes, 1)];
          }
          in.readBytes(suffixBytes, 0, numBytes);
          suffixesReader.reset(suffixBytes, 0, numBytes);

          // stats
          numBytes = in.readVInt();
          if (statBytes.length < numBytes) {
            statBytes = new byte[ArrayUtil.oversize(numBytes, 1)];
          }
          in.readBytes(statBytes, 0, numBytes);
          statsReader.reset(statBytes, 0, numBytes);
          metaDataUpto = 0;

          termState.termBlockOrd = 0;
          nextEnt = 0;
          
          postingsReader.readTermsBlock(in, fieldInfo, termState);

          if (!isLastInFloor) {
            // Sub-blocks of a single floor block are always
            // written one after another -- tail recurse:
            fpEnd = in.getFilePointer();
          }
        }

        // TODO: maybe add scanToLabel; should give perf boost

        public boolean next() {
          return isLeafBlock ? nextLeaf() : nextNonLeaf();
        }

        // Decodes next entry; returns true if it's a sub-block
        public boolean nextLeaf() {
          //if (DEBUG) System.out.println("  frame.next ord=" + ord + " nextEnt=" + nextEnt + " entCount=" + entCount);
          assert nextEnt != -1 && nextEnt < entCount: "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
          nextEnt++;
          suffix = suffixesReader.readVInt();
          startBytePos = suffixesReader.getPosition();
          suffixesReader.skipBytes(suffix);
          return false;
        }

        public boolean nextNonLeaf() {
          //if (DEBUG) System.out.println("  frame.next ord=" + ord + " nextEnt=" + nextEnt + " entCount=" + entCount);
          assert nextEnt != -1 && nextEnt < entCount: "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
          nextEnt++;
          final int code = suffixesReader.readVInt();
          suffix = code >>> 1;
          startBytePos = suffixesReader.getPosition();
          suffixesReader.skipBytes(suffix);
          if ((code & 1) == 0) {
            // A normal term
            termState.termBlockOrd++;
            return false;
          } else {
            // A sub-block; make sub-FP absolute:
            lastSubFP = fp - suffixesReader.readVLong();
            return true;
          }
        }

        public int getTermBlockOrd() {
          return isLeafBlock ? nextEnt : termState.termBlockOrd;
        }

        public void decodeMetaData() throws IOException {

          // lazily catch up on metadata decode:
          final int limit = getTermBlockOrd();
          assert limit > 0;

          // We must set/incr state.termCount because
          // postings impl can look at this
          termState.termBlockOrd = metaDataUpto;
      
          // TODO: better API would be "jump straight to term=N"???
          while (metaDataUpto < limit) {

            // TODO: we could make "tiers" of metadata, ie,
            // decode docFreq/totalTF but don't decode postings
            // metadata; this way caller could get
            // docFreq/totalTF w/o paying decode cost for
            // postings

            // TODO: if docFreq were bulk decoded we could
            // just skipN here:
            termState.docFreq = statsReader.readVInt();
            //if (DEBUG) System.out.println("    dF=" + state.docFreq);
            if (fieldInfo.indexOptions != IndexOptions.DOCS_ONLY) {
              termState.totalTermFreq = termState.docFreq + statsReader.readVLong();
              //if (DEBUG) System.out.println("    totTF=" + state.totalTermFreq);
            }

            postingsReader.nextTerm(fieldInfo, termState);
            metaDataUpto++;
            termState.termBlockOrd++;
          }
        }
      }

      private final BytesRef savedStartTerm;
      
      // TODO: in some cases we can filter by length?  eg
      // regexp foo*bar must be at least length 6 bytes
      public IntersectEnum(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
        // if (DEBUG) {
        //   System.out.println("\nintEnum.init seg=" + segment + " commonSuffix=" + brToString(compiled.commonSuffixRef));
        // }
        runAutomaton = compiled.runAutomaton;
        compiledAutomaton = compiled;
        in = (IndexInput) BlockTreeTermsReader.this.in.clone();
        stack = new Frame[5];
        for(int idx=0;idx<stack.length;idx++) {
          stack[idx] = new Frame(idx);
        }
        for(int arcIdx=0;arcIdx<arcs.length;arcIdx++) {
          arcs[arcIdx] = new FST.Arc<BytesRef>();
        }

        // TODO: if the automaon is "smallish" we really
        // should use the terms index to seek at least to
        // the initial term and likely to subsequent terms
        // (or, maybe just fallback to ATE for such cases).
        // Else the seek cost of loading the frames will be
        // too costly.

        final FST.Arc<BytesRef> arc = index.getFirstArc(arcs[0]);
        // Empty string prefix must have an output in the index!
        assert arc.isFinal();

        // Special pushFrame since it's the first one:
        final Frame f = stack[0];
        f.fp = f.fpOrig = rootBlockFP;
        f.prefix = 0;
        f.setState(runAutomaton.getInitialState());
        f.arc = arc;
        f.outputPrefix = arc.output;
        f.load(rootCode);

        // for assert:
        savedStartTerm = startTerm == null ? null : new BytesRef(startTerm);

        currentFrame = f;
        if (startTerm != null) {
          seekToStartTerm(startTerm);
        }
      }

      @Override
      public TermState termState() throws IOException {
        currentFrame.decodeMetaData();
        return (TermState) currentFrame.termState.clone();
      }

      private Frame getFrame(int ord) throws IOException {
        if (ord >= stack.length) {
          final Frame[] next = new Frame[ArrayUtil.oversize(1+ord, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
          System.arraycopy(stack, 0, next, 0, stack.length);
          for(int stackOrd=stack.length;stackOrd<next.length;stackOrd++) {
            next[stackOrd] = new Frame(stackOrd);
          }
          stack = next;
        }
        assert stack[ord].ord == ord;
        return stack[ord];
      }

      private FST.Arc<BytesRef> getArc(int ord) {
        if (ord >= arcs.length) {
          @SuppressWarnings("unchecked") final FST.Arc<BytesRef>[] next = new FST.Arc[ArrayUtil.oversize(1+ord, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
          System.arraycopy(arcs, 0, next, 0, arcs.length);
          for(int arcOrd=arcs.length;arcOrd<next.length;arcOrd++) {
            next[arcOrd] = new FST.Arc<BytesRef>();
          }
          arcs = next;
        }
        return arcs[ord];
      }

      private Frame pushFrame(int state) throws IOException {
        final Frame f = getFrame(currentFrame == null ? 0 : 1+currentFrame.ord);
        
        f.fp = f.fpOrig = currentFrame.lastSubFP;
        f.prefix = currentFrame.prefix + currentFrame.suffix;
        // if (DEBUG) System.out.println("    pushFrame state=" + state + " prefix=" + f.prefix);
        f.setState(state);

        // Walk the arc through the index -- we only
        // "bother" with this so we can get the floor data
        // from the index and skip floor blocks when
        // possible:
        FST.Arc<BytesRef> arc = currentFrame.arc;
        int idx = currentFrame.prefix;
        assert currentFrame.suffix > 0;
        BytesRef output = currentFrame.outputPrefix;
        while (idx < f.prefix) {
          final int target = term.bytes[idx] & 0xff;
          // TODO: we could be more efficient for the next()
          // case by using current arc as starting point,
          // passed to findTargetArc
          arc = index.findTargetArc(target, arc, getArc(1+idx));
          assert arc != null;
          output = fstOutputs.add(output, arc.output);
          idx++;
        }

        f.arc = arc;
        f.outputPrefix = output;
        assert arc.isFinal();
        f.load(fstOutputs.add(output, arc.nextFinalOutput));
        return f;
      }

      @Override
      public BytesRef term() throws IOException {
        return term;
      }

      @Override
      public int docFreq() throws IOException {
        //if (DEBUG) System.out.println("BTIR.docFreq");
        currentFrame.decodeMetaData();
        //if (DEBUG) System.out.println("  return " + currentFrame.termState.docFreq);
        return currentFrame.termState.docFreq;
      }

      @Override
      public long totalTermFreq() throws IOException {
        currentFrame.decodeMetaData();
        return currentFrame.termState.totalTermFreq;
      }

      @Override
      public DocsEnum docs(Bits skipDocs, DocsEnum reuse) throws IOException {
        currentFrame.decodeMetaData();
        return postingsReader.docs(fieldInfo, currentFrame.termState, skipDocs, reuse);
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
        if (fieldInfo.indexOptions != IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
          return null;
        } else {
          currentFrame.decodeMetaData();
          return postingsReader.docsAndPositions(fieldInfo, currentFrame.termState, skipDocs, reuse);
        }
      }

      private int getState() {
        int state = currentFrame.state;
        for(int idx=0;idx<currentFrame.suffix;idx++) {
          state = runAutomaton.step(state,  currentFrame.suffixBytes[currentFrame.startBytePos+idx] & 0xff);
          assert state != -1;
        }
        return state;
      }

      // NOTE: specialized to only doing the first-time
      // seek, but we could generalize it to allow
      // arbitrary seekExact/Ceil.  Note that this is a
      // seekFloor!
      private void seekToStartTerm(BytesRef target) throws IOException {
        //if (DEBUG) System.out.println("seek to startTerm=" + target.utf8ToString());
        assert currentFrame.ord == 0;
        if (term.length < target.length) {
          term.bytes = ArrayUtil.grow(term.bytes, target.length);
        }
        FST.Arc<BytesRef> arc = arcs[0];
        assert arc == currentFrame.arc;

        for(int idx=0;idx<=target.length;idx++) {

          boolean lastIsSubBlock = false;

          while (true) {
            final int savePos = currentFrame.suffixesReader.getPosition();
            final int saveStartBytePos = currentFrame.startBytePos;
            final int saveSuffix = currentFrame.suffix;
            final long saveLastSubFP = currentFrame.lastSubFP;
            final int saveTermBlockOrd = currentFrame.termState.termBlockOrd;

            final boolean isSubBlock = currentFrame.next();

            //if (DEBUG) System.out.println("    cycle ent=" + currentFrame.nextEnt + " (of " + currentFrame.entCount + ") prefix=" + currentFrame.prefix + " suffix=" + currentFrame.suffix + " isBlock=" + isSubBlock + " firstLabel=" + (currentFrame.suffix == 0 ? "" : (currentFrame.suffixBytes[currentFrame.startBytePos])&0xff));
            term.length = currentFrame.prefix + currentFrame.suffix;
            if (term.bytes.length < term.length) {
              term.bytes = ArrayUtil.grow(term.bytes, term.length);
            }
            System.arraycopy(currentFrame.suffixBytes, currentFrame.startBytePos, term.bytes, currentFrame.prefix, currentFrame.suffix);

            if (isSubBlock && target.startsWith(term)) {
              // Recurse
              //if (DEBUG) System.out.println("      recurse!");
              currentFrame = pushFrame(getState());
              break;
            } else {
              final int cmp = term.compareTo(target);
              if (cmp < 0) {
                if (currentFrame.nextEnt == currentFrame.entCount) {
                  if (!currentFrame.isLastInFloor) {
                    //if (DEBUG) System.out.println("  load floorBlock");
                    currentFrame.loadNextFloorBlock();
                    continue;
                  } else {
                    //if (DEBUG) System.out.println("  return term=" + brToString(term));
                    return;
                  }
                }
                lastIsSubBlock = isSubBlock;
                continue;
              } else if (cmp == 0) {
                //if (DEBUG) System.out.println("  return term=" + brToString(term));
                return;
              } else {
                // Fallback to prior entry: the semantics of
                // this method is that the first call to
                // next() will return the term after the
                // requested term
                currentFrame.nextEnt--;
                currentFrame.lastSubFP = saveLastSubFP;
                currentFrame.startBytePos = saveStartBytePos;
                currentFrame.suffix = saveSuffix;
                currentFrame.suffixesReader.setPosition(savePos);
                currentFrame.termState.termBlockOrd = saveTermBlockOrd;
                System.arraycopy(currentFrame.suffixBytes, currentFrame.startBytePos, term.bytes, currentFrame.prefix, currentFrame.suffix);
                term.length = currentFrame.prefix + currentFrame.suffix;
                // If the last entry was a block we don't
                // need to bother recursing and pushing to
                // the last term under it because the first
                // next() will simply skip the frame anyway
                return;
              }
            }
          }
        }

        assert false;
      }

      @Override
      public BytesRef next() throws IOException {

        // if (DEBUG) {
        //   System.out.println("\nintEnum.next seg=" + segment);
        //   System.out.println("  frame ord=" + currentFrame.ord + " prefix=" + brToString(new BytesRef(term.bytes, term.offset, currentFrame.prefix)) + " state=" + currentFrame.state + " lastInFloor?=" + currentFrame.isLastInFloor + " fp=" + currentFrame.fp + " trans=" + (currentFrame.transitions.length == 0 ? "n/a" : currentFrame.transitions[currentFrame.transitionIndex]) + " outputPrefix=" + currentFrame.outputPrefix);
        // }

        nextTerm:
        while(true) {
          // Pop finished frames
          while (currentFrame.nextEnt == currentFrame.entCount) {
            if (!currentFrame.isLastInFloor) {
              //if (DEBUG) System.out.println("    next-floor-block");
              currentFrame.loadNextFloorBlock();
              //if (DEBUG) System.out.println("\n  frame ord=" + currentFrame.ord + " prefix=" + brToString(new BytesRef(term.bytes, term.offset, currentFrame.prefix)) + " state=" + currentFrame.state + " lastInFloor?=" + currentFrame.isLastInFloor + " fp=" + currentFrame.fp + " trans=" + (currentFrame.transitions.length == 0 ? "n/a" : currentFrame.transitions[currentFrame.transitionIndex]) + " outputPrefix=" + currentFrame.outputPrefix);
            } else {
              //if (DEBUG) System.out.println("  pop frame");
              if (currentFrame.ord == 0) {
                return null;
              }
              final long lastFP = currentFrame.fpOrig;
              currentFrame = stack[currentFrame.ord-1];
              assert currentFrame.lastSubFP == lastFP;
              //if (DEBUG) System.out.println("\n  frame ord=" + currentFrame.ord + " prefix=" + brToString(new BytesRef(term.bytes, term.offset, currentFrame.prefix)) + " state=" + currentFrame.state + " lastInFloor?=" + currentFrame.isLastInFloor + " fp=" + currentFrame.fp + " trans=" + (currentFrame.transitions.length == 0 ? "n/a" : currentFrame.transitions[currentFrame.transitionIndex]) + " outputPrefix=" + currentFrame.outputPrefix);
            }
          }

          final boolean isSubBlock = currentFrame.next();
          // if (DEBUG) {
          //   final BytesRef suffixRef = new BytesRef();
          //   suffixRef.bytes = currentFrame.suffixBytes;
          //   suffixRef.offset = currentFrame.startBytePos;
          //   suffixRef.length = currentFrame.suffix;
          //   System.out.println("    " + (isSubBlock ? "sub-block" : "term") + " " + currentFrame.nextEnt + " (of " + currentFrame.entCount + ") suffix=" + brToString(suffixRef));
          // }

          if (currentFrame.suffix != 0) {
            final int label = currentFrame.suffixBytes[currentFrame.startBytePos] & 0xff;
            while (label > currentFrame.curTransitionMax) {
              if (currentFrame.transitionIndex >= currentFrame.transitions.length-1) {
                // Stop processing this frame -- no further
                // matches are possible because we've moved
                // beyond what the max transition will allow
                //if (DEBUG) System.out.println("      break: trans=" + (currentFrame.transitions.length == 0 ? "n/a" : currentFrame.transitions[currentFrame.transitionIndex]));

                // sneaky!  forces a pop above
                currentFrame.isLastInFloor = true;
                currentFrame.nextEnt = currentFrame.entCount;
                continue nextTerm;
              }
              currentFrame.transitionIndex++;
              currentFrame.curTransitionMax = currentFrame.transitions[currentFrame.transitionIndex].getMax();
              //if (DEBUG) System.out.println("      next trans=" + currentFrame.transitions[currentFrame.transitionIndex]);
            }
          }

          // First test the common suffix, if set:
          if (compiledAutomaton.commonSuffixRef != null && !isSubBlock) {
            final int termLen = currentFrame.prefix + currentFrame.suffix;
            if (termLen < compiledAutomaton.commonSuffixRef.length) {
              // No match
              // if (DEBUG) {
              //   System.out.println("      skip: common suffix length");
              // }
              continue nextTerm;
            }

            final byte[] suffixBytes = currentFrame.suffixBytes;
            final byte[] commonSuffixBytes = compiledAutomaton.commonSuffixRef.bytes;

            final int lenInPrefix = compiledAutomaton.commonSuffixRef.length - currentFrame.suffix;
            assert compiledAutomaton.commonSuffixRef.offset == 0;
            int suffixBytesPos;
            int commonSuffixBytesPos = 0;

            if (lenInPrefix > 0) {
              // A prefix of the common suffix overlaps with
              // the suffix of the block prefix so we first
              // test whether the prefix part matches:
              final byte[] termBytes = term.bytes;
              int termBytesPos = currentFrame.prefix - lenInPrefix;
              assert termBytesPos >= 0;
              final int termBytesPosEnd = currentFrame.prefix;
              while (termBytesPos < termBytesPosEnd) {
                if (termBytes[termBytesPos++] != commonSuffixBytes[commonSuffixBytesPos++]) {
                  // if (DEBUG) {
                  //   System.out.println("      skip: common suffix mismatch (in prefix)");
                  // }
                  continue nextTerm;
                }
              }
              suffixBytesPos = currentFrame.startBytePos;
            } else {
              suffixBytesPos = currentFrame.startBytePos + currentFrame.suffix - compiledAutomaton.commonSuffixRef.length;
            }

            // Test overlapping suffix part:
            final int commonSuffixBytesPosEnd = compiledAutomaton.commonSuffixRef.length;
            while (commonSuffixBytesPos < commonSuffixBytesPosEnd) {
              if (suffixBytes[suffixBytesPos++] != commonSuffixBytes[commonSuffixBytesPos++]) {
                // if (DEBUG) {
                //   System.out.println("      skip: common suffix mismatch");
                // }
                continue nextTerm;
              }
            }
          }

          // TODO: maybe we should do the same linear test
          // that AutomatonTermsEnum does, so that if we
          // reach a part of the automaton where .* is
          // "temporarily" accepted, we just blindly .next()
          // until the limit

          // See if the term prefix matches the automaton:
          int state = currentFrame.state;
          for (int idx=0;idx<currentFrame.suffix;idx++) {
            state = runAutomaton.step(state,  currentFrame.suffixBytes[currentFrame.startBytePos+idx] & 0xff);
            if (state == -1) {
              // No match
              //System.out.println("    no s=" + state);
              continue nextTerm;
            } else {
              //System.out.println("    c s=" + state);
            }
          }

          if (isSubBlock) {
            // Match!  Recurse:
            //if (DEBUG) System.out.println("      sub-block match to state=" + state + "; recurse fp=" + currentFrame.lastSubFP);
            copyTerm();
            currentFrame = pushFrame(state);
            //if (DEBUG) System.out.println("\n  frame ord=" + currentFrame.ord + " prefix=" + brToString(new BytesRef(term.bytes, term.offset, currentFrame.prefix)) + " state=" + currentFrame.state + " lastInFloor?=" + currentFrame.isLastInFloor + " fp=" + currentFrame.fp + " trans=" + (currentFrame.transitions.length == 0 ? "n/a" : currentFrame.transitions[currentFrame.transitionIndex]) + " outputPrefix=" + currentFrame.outputPrefix);
          } else if (runAutomaton.isAccept(state)) {
            copyTerm();
            //if (DEBUG) System.out.println("      term match to state=" + state + "; return term=" + brToString(term));
            assert savedStartTerm == null || term.compareTo(savedStartTerm) > 0: "saveStartTerm=" + savedStartTerm.utf8ToString() + " term=" + term.utf8ToString();
            return term;
          } else {
            //System.out.println("    no s=" + state);
          }
        }
      }

      private void copyTerm() {
        //System.out.println("      copyTerm cur.prefix=" + currentFrame.prefix + " cur.suffix=" + currentFrame.suffix + " first=" + (char) currentFrame.suffixBytes[currentFrame.startBytePos]);
        final int len = currentFrame.prefix + currentFrame.suffix;
        if (term.bytes.length < len) {
          term.bytes = ArrayUtil.grow(term.bytes, len);
        }
        System.arraycopy(currentFrame.suffixBytes, currentFrame.startBytePos, term.bytes, currentFrame.prefix, currentFrame.suffix);
        term.length = len;
      }

      @Override
      public Comparator<BytesRef> getComparator() {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
      }

      @Override
      public boolean seekExact(BytesRef text, boolean useCache) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void seekExact(long ord) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public long ord() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public SeekStatus seekCeil(BytesRef text, boolean useCache) throws IOException {
        throw new UnsupportedOperationException();
      }
    }

    // Iterates through terms in this field
    private final class SegmentTermsEnum extends TermsEnum {
      private final IndexInput in;

      private Frame[] stack;
      private final Frame staticFrame;
      private Frame currentFrame;
      private boolean termExists;

      private int targetBeforeCurrentLength;

      private final ByteArrayDataInput scratchReader = new ByteArrayDataInput();

      // What prefix of the current term was present in the index:
      private int validIndexPrefix;

      // assert only:
      private boolean eof;

      final BytesRef term = new BytesRef();

      @SuppressWarnings("unchecked") private FST.Arc<BytesRef>[] arcs = new FST.Arc[5];      

      public SegmentTermsEnum() throws IOException {
        //if (DEBUG) System.out.println("BTTR.init seg=" + segment);
        in = (IndexInput) BlockTreeTermsReader.this.in.clone();
        stack = new Frame[5];
        for(int stackOrd=0;stackOrd<stack.length;stackOrd++) {
          stack[stackOrd] = new Frame(stackOrd);
        }
        // Used to hold seek by TermState, or cached seek
        staticFrame = new Frame(-1);

        for(int arcIdx=0;arcIdx<arcs.length;arcIdx++) {
          arcs[arcIdx] = new FST.Arc<BytesRef>();
        }

        // Init w/ root block; don't use index since it may
        // not (and need not) have been loaded
        //final FST.Arc<BytesRef> arc = index.getFirstArc(arcs[0]);

        // Empty string prefix must have an output in the index!
        //assert arc.isFinal();

        currentFrame = staticFrame;
        final FST.Arc<BytesRef> arc;
        if (index != null) {
          arc = index.getFirstArc(arcs[0]);
          // Empty string prefix must have an output in the index!
          assert arc.isFinal();
        } else {
          arc = null;
        }
        currentFrame = pushFrame(arc, rootCode, 0);
        currentFrame.loadBlock();
        validIndexPrefix = 0;
        // if (DEBUG) {
        //   System.out.println("init frame state " + currentFrame.ord);
        //   printSeekState();
        // }

        //System.out.println();
        // computeBlockStats().print(System.out);
      }

      /** Runs next() through the entire terms dict,
       *  computing aggregate statistics. */
      public Stats computeBlockStats() throws IOException {

        Stats stats = new Stats(segment, fieldInfo.name);
        if (index != null) {
          stats.indexNodeCount = index.getNodeCount();
          stats.indexArcCount = index.getArcCount();
          stats.indexNumBytes = index.sizeInBytes();
        }
        
        currentFrame = staticFrame;
        FST.Arc<BytesRef> arc;
        if (index != null) {
          arc = index.getFirstArc(arcs[0]);
          // Empty string prefix must have an output in the index!
          assert arc.isFinal();
        } else {
          arc = null;
        }

        // Empty string prefix must have an output in the
        // index!
        currentFrame = pushFrame(arc, rootCode, 0);
        currentFrame.fpOrig = currentFrame.fp;
        currentFrame.loadBlock();
        validIndexPrefix = 0;

        stats.startBlock(currentFrame, !currentFrame.isLastInFloor);

        allTerms:
        while (true) {

          // Pop finished blocks
          while (currentFrame.nextEnt == currentFrame.entCount) {
            stats.endBlock(currentFrame);
            if (!currentFrame.isLastInFloor) {
              currentFrame.loadNextFloorBlock();
              stats.startBlock(currentFrame, true);
            } else {
              if (currentFrame.ord == 0) {
                break allTerms;
              }
              final long lastFP = currentFrame.fpOrig;
              currentFrame = stack[currentFrame.ord-1];
              assert lastFP == currentFrame.lastSubFP;
              // if (DEBUG) {
              //   System.out.println("  reset validIndexPrefix=" + validIndexPrefix);
              // }
            }
          }

          while(true) {
            if (currentFrame.next()) {
              // Push to new block:
              currentFrame = pushFrame(null, currentFrame.lastSubFP, term.length);
              currentFrame.fpOrig = currentFrame.fp;
              // This is a "next" frame -- even if it's
              // floor'd we must pretend it isn't so we don't
              // try to scan to the right floor frame:
              currentFrame.isFloor = false;
              //currentFrame.hasTerms = true;
              currentFrame.loadBlock();
              stats.startBlock(currentFrame, !currentFrame.isLastInFloor);
            } else {
              stats.term(term);
              break;
            }
          }
        }

        stats.finish();

        // Put root frame back:
        currentFrame = staticFrame;
        if (index != null) {
          arc = index.getFirstArc(arcs[0]);
          // Empty string prefix must have an output in the index!
          assert arc.isFinal();
        } else {
          arc = null;
        }
        currentFrame = pushFrame(arc, rootCode, 0);
        currentFrame.rewind();
        currentFrame.loadBlock();
        validIndexPrefix = 0;
        term.length = 0;

        return stats;
      }

      private Frame getFrame(int ord) throws IOException {
        if (ord >= stack.length) {
          final Frame[] next = new Frame[ArrayUtil.oversize(1+ord, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
          System.arraycopy(stack, 0, next, 0, stack.length);
          for(int stackOrd=stack.length;stackOrd<next.length;stackOrd++) {
            next[stackOrd] = new Frame(stackOrd);
          }
          stack = next;
        }
        assert stack[ord].ord == ord;
        return stack[ord];
      }

      private FST.Arc<BytesRef> getArc(int ord) {
        if (ord >= arcs.length) {
          @SuppressWarnings("unchecked") final FST.Arc<BytesRef>[] next = new FST.Arc[ArrayUtil.oversize(1+ord, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
          System.arraycopy(arcs, 0, next, 0, arcs.length);
          for(int arcOrd=arcs.length;arcOrd<next.length;arcOrd++) {
            next[arcOrd] = new FST.Arc<BytesRef>();
          }
          arcs = next;
        }
        return arcs[ord];
      }

      @Override
      public Comparator<BytesRef> getComparator() {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
      }

      // Pushes a frame we seek'd to
      Frame pushFrame(FST.Arc<BytesRef> arc, BytesRef frameData, int length) throws IOException {
        scratchReader.reset(frameData.bytes, frameData.offset, frameData.length);
        final long code = scratchReader.readVLong();
        final long fpSeek = code >>> BlockTreeTermsWriter.OUTPUT_FLAGS_NUM_BITS;
        final Frame f = getFrame(1+currentFrame.ord);
        f.hasTerms = (code & BlockTreeTermsWriter.OUTPUT_FLAG_HAS_TERMS) != 0;
        f.hasTermsOrig = f.hasTerms;
        f.isFloor = (code & BlockTreeTermsWriter.OUTPUT_FLAG_IS_FLOOR) != 0;
        if (f.isFloor) {
          f.setFloorData(scratchReader, frameData);
        }
        pushFrame(arc, fpSeek, length);

        return f;
      }

      // Pushes next'd frame or seek'd frame; we later
      // lazy-load the frame only when needed
      Frame pushFrame(FST.Arc<BytesRef> arc, long fp, int length) throws IOException {
        final Frame f = getFrame(1+currentFrame.ord);
        f.arc = arc;
        if (f.fpOrig == fp && f.nextEnt != -1) {
          //if (DEBUG) System.out.println("      push reused frame ord=" + f.ord + " fp=" + f.fp + " isFloor?=" + f.isFloor + " hasTerms=" + f.hasTerms + " pref=" + term + " nextEnt=" + f.nextEnt + " targetBeforeCurrentLength=" + targetBeforeCurrentLength + " term.length=" + term.length + " vs prefix=" + f.prefix);
          if (f.prefix > targetBeforeCurrentLength) {
            f.rewind();
          } else {
            // if (DEBUG) {
            //   System.out.println("        skip rewind!");
            // }
          }
          assert length == f.prefix;
        } else {
          f.nextEnt = -1;
          f.prefix = length;
          f.state.termBlockOrd = 0;
          f.fpOrig = f.fp = fp;
          f.lastSubFP = -1;
          // if (DEBUG) {
          //   final int sav = term.length;
          //   term.length = length;
          //   System.out.println("      push new frame ord=" + f.ord + " fp=" + f.fp + " hasTerms=" + f.hasTerms + " isFloor=" + f.isFloor + " pref=" + brToString(term));
          //   term.length = sav;
          // }
        }

        return f;
      }

      // asserts only
      private boolean clearEOF() {
        eof = false;
        return true;
      }

      // asserts only
      private boolean setEOF() {
        eof = true;
        return true;
      }

      @Override
      public boolean seekExact(final BytesRef target, final boolean useCache) throws IOException {

        if (index == null) {
          throw new IllegalStateException("terms index was not loaded");
        }

        if (term.bytes.length <= target.length) {
          term.bytes = ArrayUtil.grow(term.bytes, 1+target.length);
        }

        assert clearEOF();

        // if (DEBUG) {
        //   System.out.println("\nBTTR.seekExact seg=" + segment + " target=" + fieldInfo.name + ":" + brToString(target) + " current=" + brToString(term) + " (exists?=" + termExists + ") validIndexPrefix=" + validIndexPrefix);
        //   printSeekState();
        // }

        FST.Arc<BytesRef> arc;
        int targetUpto;
        BytesRef output;

        targetBeforeCurrentLength = currentFrame.ord;

        if (currentFrame != staticFrame) {

          // We are already seek'd; find the common
          // prefix of new seek term vs current term and
          // re-use the corresponding seek state.  For
          // example, if app first seeks to foobar, then
          // seeks to foobaz, we can re-use the seek state
          // for the first 5 bytes.

          // if (DEBUG) {
          //   System.out.println("  re-use current seek state validIndexPrefix=" + validIndexPrefix);
          // }

          arc = arcs[0];
          assert arc.isFinal();
          output = arc.output;
          targetUpto = 0;
          
          Frame lastFrame = stack[0];
          assert validIndexPrefix <= term.length;

          final int targetLimit = Math.min(target.length, validIndexPrefix);

          int cmp = 0;

          // TODO: reverse vLong byte order for better FST
          // prefix output sharing

          // First compare up to valid seek frames:
          while (targetUpto < targetLimit) {
            cmp = (term.bytes[targetUpto]&0xFF) - (target.bytes[target.offset + targetUpto]&0xFF);
            // if (DEBUG) {
            //   System.out.println("    cycle targetUpto=" + targetUpto + " (vs limit=" + targetLimit + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) + " vs termLabel=" + (char) (term.bytes[targetUpto]) + ")"   + " arc.output=" + arc.output + " output=" + output);
            // }
            if (cmp != 0) {
              break;
            }
            arc = arcs[1+targetUpto];
            //if (arc.label != (target.bytes[target.offset + targetUpto] & 0xFF)) {
            //System.out.println("FAIL: arc.label=" + (char) arc.label + " targetLabel=" + (char) (target.bytes[target.offset + targetUpto] & 0xFF));
            //}
            assert arc.label == (target.bytes[target.offset + targetUpto] & 0xFF): "arc.label=" + (char) arc.label + " targetLabel=" + (char) (target.bytes[target.offset + targetUpto] & 0xFF);
            if (arc.output != NO_OUTPUT) {
              output = fstOutputs.add(output, arc.output);
            }
            if (arc.isFinal()) {
              lastFrame = stack[1+lastFrame.ord];
            }
            targetUpto++;
          }

          if (cmp == 0) {
            final int targetUptoMid = targetUpto;

            // Second compare the rest of the term, but
            // don't save arc/output/frame; we only do this
            // to find out if the target term is before,
            // equal or after the current term
            final int targetLimit2 = Math.min(target.length, term.length);
            while (targetUpto < targetLimit2) {
              cmp = (term.bytes[targetUpto]&0xFF) - (target.bytes[target.offset + targetUpto]&0xFF);
              // if (DEBUG) {
              //   System.out.println("    cycle2 targetUpto=" + targetUpto + " (vs limit=" + targetLimit + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) + " vs termLabel=" + (char) (term.bytes[targetUpto]) + ")");
              // }
              if (cmp != 0) {
                break;
              }
              targetUpto++;
            }

            if (cmp == 0) {
              cmp = term.length - target.length;
            }
            targetUpto = targetUptoMid;
          }

          if (cmp < 0) {
            // Common case: target term is after current
            // term, ie, app is seeking multiple terms
            // in sorted order
            // if (DEBUG) {
            //   System.out.println("  target is after current (shares prefixLen=" + targetUpto + "); frame.ord=" + lastFrame.ord);
            // }
            currentFrame = lastFrame;

          } else if (cmp > 0) {
            // Uncommon case: target term
            // is before current term; this means we can
            // keep the currentFrame but we must rewind it
            // (so we scan from the start)
            targetBeforeCurrentLength = 0;
            // if (DEBUG) {
            //   System.out.println("  target is before current (shares prefixLen=" + targetUpto + "); rewind frame ord=" + lastFrame.ord);
            // }
            currentFrame = lastFrame;
            currentFrame.rewind();
          } else {
            // Target is exactly the same as current term
            assert term.length == target.length;
            if (termExists) {
              // if (DEBUG) {
              //   System.out.println("  target is same as current; return true");
              // }
              return true;
            } else {
              // if (DEBUG) {
              //   System.out.println("  target is same as current but term doesn't exist");
              // }
            }
            //validIndexPrefix = currentFrame.depth;
            //term.length = target.length;
            //return termExists;
          }

        } else {

          targetBeforeCurrentLength = -1;
          arc = index.getFirstArc(arcs[0]);

          // Empty string prefix must have an output (block) in the index!
          assert arc.isFinal();
          assert arc.output != null;

          // if (DEBUG) {
          //   System.out.println("    no seek state; push root frame");
          // }

          output = arc.output;

          currentFrame = staticFrame;

          //term.length = 0;
          targetUpto = 0;
          currentFrame = pushFrame(arc, fstOutputs.add(output, arc.nextFinalOutput), 0);
        }

        // if (DEBUG) {
        //   System.out.println("  start index loop targetUpto=" + targetUpto + " output=" + output + " currentFrame.ord=" + currentFrame.ord + " targetBeforeCurrentLength=" + targetBeforeCurrentLength);
        // }

        while (targetUpto < target.length) {

          final int targetLabel = target.bytes[target.offset + targetUpto] & 0xFF;

          final FST.Arc<BytesRef> nextArc = index.findTargetArc(targetLabel, arc, getArc(1+targetUpto));

          if (nextArc == null) {

            // Index is exhausted
            // if (DEBUG) {
            //   System.out.println("    index: index exhausted label=" + ((char) targetLabel) + " " + toHex(targetLabel));
            // }
            
            validIndexPrefix = currentFrame.prefix;
            //validIndexPrefix = targetUpto;

            currentFrame.scanToFloorFrame(target);

            if (!currentFrame.hasTerms) {
              termExists = false;
              term.bytes[targetUpto] = (byte) targetLabel;
              term.length = 1+targetUpto;
              // if (DEBUG) {
              //   System.out.println("  FAST NOT_FOUND term=" + brToString(term));
              // }
              return false;
            }

            currentFrame.loadBlock();

            final SeekStatus result = currentFrame.scanToTerm(target, true);            
            if (result == SeekStatus.FOUND) {
              // if (DEBUG) {
              //   System.out.println("  return FOUND term=" + term.utf8ToString() + " " + term);
              // }
              return true;
            } else {
              // if (DEBUG) {
              //   System.out.println("  got " + result + "; return NOT_FOUND term=" + brToString(term));
              // }
              return false;
            }
          } else {
            // Follow this arc
            arc = nextArc;
            term.bytes[targetUpto] = (byte) targetLabel;
            // Aggregate output as we go:
            assert arc.output != null;
            if (arc.output != NO_OUTPUT) {
              output = fstOutputs.add(output, arc.output);
            }

            // if (DEBUG) {
            //   System.out.println("    index: follow label=" + toHex(target.bytes[target.offset + targetUpto]&0xff) + " arc.output=" + arc.output + " arc.nfo=" + arc.nextFinalOutput);
            // }
            targetUpto++;

            if (arc.isFinal()) {
              //if (DEBUG) System.out.println("    arc is final!");
              currentFrame = pushFrame(arc, fstOutputs.add(output, arc.nextFinalOutput), targetUpto);
              //if (DEBUG) System.out.println("    curFrame.ord=" + currentFrame.ord + " hasTerms=" + currentFrame.hasTerms);
            }
          }
        }

        //validIndexPrefix = targetUpto;
        validIndexPrefix = currentFrame.prefix;

        currentFrame.scanToFloorFrame(target);

        // Target term is entirely contained in the index:
        if (!currentFrame.hasTerms) {
          termExists = false;
          term.length = targetUpto;
          // if (DEBUG) {
          //   System.out.println("  FAST NOT_FOUND term=" + brToString(term));
          // }
          return false;
        }

        currentFrame.loadBlock();

        final SeekStatus result = currentFrame.scanToTerm(target, true);            
        if (result == SeekStatus.FOUND) {
          // if (DEBUG) {
          //   System.out.println("  return FOUND term=" + term.utf8ToString() + " " + term);
          // }
          return true;
        } else {
          // if (DEBUG) {
          //   System.out.println("  got result " + result + "; return NOT_FOUND term=" + term.utf8ToString());
          // }

          return false;
        }
      }

      @Override
      public SeekStatus seekCeil(final BytesRef target, final boolean useCache) throws IOException {
        if (index == null) {
          throw new IllegalStateException("terms index was not loaded");
        }
   
        if (term.bytes.length <= target.length) {
          term.bytes = ArrayUtil.grow(term.bytes, 1+target.length);
        }

        assert clearEOF();

        //if (DEBUG) {
        //System.out.println("\nBTTR.seekCeil seg=" + segment + " target=" + fieldInfo.name + ":" + target.utf8ToString() + " " + target + " current=" + brToString(term) + " (exists?=" + termExists + ") validIndexPrefix=  " + validIndexPrefix);
        //printSeekState();
        //}

        FST.Arc<BytesRef> arc;
        int targetUpto;
        BytesRef output;

        targetBeforeCurrentLength = currentFrame.ord;

        if (currentFrame != staticFrame) {

          // We are already seek'd; find the common
          // prefix of new seek term vs current term and
          // re-use the corresponding seek state.  For
          // example, if app first seeks to foobar, then
          // seeks to foobaz, we can re-use the seek state
          // for the first 5 bytes.

          //if (DEBUG) {
          //System.out.println("  re-use current seek state validIndexPrefix=" + validIndexPrefix);
          //}

          arc = arcs[0];
          assert arc.isFinal();
          output = arc.output;
          targetUpto = 0;
          
          Frame lastFrame = stack[0];
          assert validIndexPrefix <= term.length;

          final int targetLimit = Math.min(target.length, validIndexPrefix);

          int cmp = 0;

          // TOOD: we should write our vLong backwards (MSB
          // first) to get better sharing from the FST

          // First compare up to valid seek frames:
          while (targetUpto < targetLimit) {
            cmp = (term.bytes[targetUpto]&0xFF) - (target.bytes[target.offset + targetUpto]&0xFF);
            //if (DEBUG) {
            //System.out.println("    cycle targetUpto=" + targetUpto + " (vs limit=" + targetLimit + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) + " vs termLabel=" + (char) (term.bytes[targetUpto]) + ")"   + " arc.output=" + arc.output + " output=" + output);
            //}
            if (cmp != 0) {
              break;
            }
            arc = arcs[1+targetUpto];
            assert arc.label == (target.bytes[target.offset + targetUpto] & 0xFF): "arc.label=" + (char) arc.label + " targetLabel=" + (char) (target.bytes[target.offset + targetUpto] & 0xFF);
            // TOOD: we could save the outputs in local
            // byte[][] instead of making new objs ever
            // seek; but, often the FST doesn't have any
            // shared bytes (but this could change if we
            // reverse vLong byte order)
            if (arc.output != NO_OUTPUT) {
              output = fstOutputs.add(output, arc.output);
            }
            if (arc.isFinal()) {
              lastFrame = stack[1+lastFrame.ord];
            }
            targetUpto++;
          }


          if (cmp == 0) {
            final int targetUptoMid = targetUpto;
            // Second compare the rest of the term, but
            // don't save arc/output/frame:
            final int targetLimit2 = Math.min(target.length, term.length);
            while (targetUpto < targetLimit2) {
              cmp = (term.bytes[targetUpto]&0xFF) - (target.bytes[target.offset + targetUpto]&0xFF);
              //if (DEBUG) {
              //System.out.println("    cycle2 targetUpto=" + targetUpto + " (vs limit=" + targetLimit + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) + " vs termLabel=" + (char) (term.bytes[targetUpto]) + ")");
              //}
              if (cmp != 0) {
                break;
              }
              targetUpto++;
            }

            if (cmp == 0) {
              cmp = term.length - target.length;
            }
            targetUpto = targetUptoMid;
          }

          if (cmp < 0) {
            // Common case: target term is after current
            // term, ie, app is seeking multiple terms
            // in sorted order
            //if (DEBUG) {
            //System.out.println("  target is after current (shares prefixLen=" + targetUpto + "); clear frame.scanned ord=" + lastFrame.ord);
            //}
            currentFrame = lastFrame;

          } else if (cmp > 0) {
            // Uncommon case: target term
            // is before current term; this means we can
            // keep the currentFrame but we must rewind it
            // (so we scan from the start)
            targetBeforeCurrentLength = 0;
            //if (DEBUG) {
            //System.out.println("  target is before current (shares prefixLen=" + targetUpto + "); rewind frame ord=" + lastFrame.ord);
            //}
            currentFrame = lastFrame;
            currentFrame.rewind();
          } else {
            // Target is exactly the same as current term
            assert term.length == target.length;
            if (termExists) {
              //if (DEBUG) {
              //System.out.println("  target is same as current; return FOUND");
              //}
              return SeekStatus.FOUND;
            } else {
              //if (DEBUG) {
              //System.out.println("  target is same as current but term doesn't exist");
              //}
            }
          }

        } else {

          targetBeforeCurrentLength = -1;
          arc = index.getFirstArc(arcs[0]);

          // Empty string prefix must have an output (block) in the index!
          assert arc.isFinal();
          assert arc.output != null;

          //if (DEBUG) {
          //System.out.println("    no seek state; push root frame");
          //}

          output = arc.output;

          currentFrame = staticFrame;

          //term.length = 0;
          targetUpto = 0;
          currentFrame = pushFrame(arc, fstOutputs.add(output, arc.nextFinalOutput), 0);
        }

        //if (DEBUG) {
        //System.out.println("  start index loop targetUpto=" + targetUpto + " output=" + output + " currentFrame.ord+1=" + currentFrame.ord + " targetBeforeCurrentLength=" + targetBeforeCurrentLength);
        //}

        while (targetUpto < target.length) {

          final int targetLabel = target.bytes[target.offset + targetUpto] & 0xFF;

          final FST.Arc<BytesRef> nextArc = index.findTargetArc(targetLabel, arc, getArc(1+targetUpto));

          if (nextArc == null) {

            // Index is exhausted
            // if (DEBUG) {
            //   System.out.println("    index: index exhausted label=" + ((char) targetLabel) + " " + toHex(targetLabel));
            // }
            
            validIndexPrefix = currentFrame.prefix;
            //validIndexPrefix = targetUpto;

            currentFrame.scanToFloorFrame(target);

            currentFrame.loadBlock();

            final SeekStatus result = currentFrame.scanToTerm(target, false);
            if (result == SeekStatus.END) {
              term.copy(target);
              termExists = false;

              if (next() != null) {
                //if (DEBUG) {
                //System.out.println("  return NOT_FOUND term=" + brToString(term) + " " + term);
                //}
                return SeekStatus.NOT_FOUND;
              } else {
                //if (DEBUG) {
                //System.out.println("  return END");
                //}
                return SeekStatus.END;
              }
            } else {
              //if (DEBUG) {
              //System.out.println("  return " + result + " term=" + brToString(term) + " " + term);
              //}
              return result;
            }
          } else {
            // Follow this arc
            term.bytes[targetUpto] = (byte) targetLabel;
            arc = nextArc;
            // Aggregate output as we go:
            assert arc.output != null;
            if (arc.output != NO_OUTPUT) {
              output = fstOutputs.add(output, arc.output);
            }

            //if (DEBUG) {
            //System.out.println("    index: follow label=" + toHex(target.bytes[target.offset + targetUpto]&0xff) + " arc.output=" + arc.output + " arc.nfo=" + arc.nextFinalOutput);
            //}
            targetUpto++;

            if (arc.isFinal()) {
              //if (DEBUG) System.out.println("    arc is final!");
              currentFrame = pushFrame(arc, fstOutputs.add(output, arc.nextFinalOutput), targetUpto);
              //if (DEBUG) System.out.println("    curFrame.ord=" + currentFrame.ord + " hasTerms=" + currentFrame.hasTerms);
            }
          }
        }

        //validIndexPrefix = targetUpto;
        validIndexPrefix = currentFrame.prefix;

        currentFrame.scanToFloorFrame(target);

        currentFrame.loadBlock();

        final SeekStatus result = currentFrame.scanToTerm(target, false);

        if (result == SeekStatus.END) {
          term.copy(target);
          termExists = false;
          if (next() != null) {
            //if (DEBUG) {
            //System.out.println("  return NOT_FOUND term=" + term.utf8ToString() + " " + term);
            //}
            return SeekStatus.NOT_FOUND;
          } else {
            //if (DEBUG) {
            //System.out.println("  return END");
            //}
            return SeekStatus.END;
          }
        } else {
          return result;
        }
      }

      private void printSeekState() throws IOException {
        if (currentFrame == staticFrame) {
          System.out.println("  no prior seek");
        } else {
          System.out.println("  prior seek state:");
          int ord = 0;
          boolean isSeekFrame = true;
          while(true) {
            Frame f = getFrame(ord);
            assert f != null;
            final BytesRef prefix = new BytesRef(term.bytes, 0, f.prefix);
            if (f.nextEnt == -1) {
              System.out.println("    frame " + (isSeekFrame ? "(seek)" : "(next)") + " ord=" + ord + " fp=" + f.fp + (f.isFloor ? (" (fpOrig=" + f.fpOrig + ")") : "") + " prefixLen=" + f.prefix + " prefix=" + prefix + (f.nextEnt == -1 ? "" : (" (of " + f.entCount + ")")) + " hasTerms=" + f.hasTerms + " isFloor=" + f.isFloor + " code=" + ((f.fp<<BlockTreeTermsWriter.OUTPUT_FLAGS_NUM_BITS) + (f.hasTerms ? BlockTreeTermsWriter.OUTPUT_FLAG_HAS_TERMS:0) + (f.isFloor ? BlockTreeTermsWriter.OUTPUT_FLAG_IS_FLOOR:0)) + " isLastInFloor=" + f.isLastInFloor + " mdUpto=" + f.metaDataUpto + " tbOrd=" + f.getTermBlockOrd());
            } else {
              System.out.println("    frame " + (isSeekFrame ? "(seek, loaded)" : "(next, loaded)") + " ord=" + ord + " fp=" + f.fp + (f.isFloor ? (" (fpOrig=" + f.fpOrig + ")") : "") + " prefixLen=" + f.prefix + " prefix=" + prefix + " nextEnt=" + f.nextEnt + (f.nextEnt == -1 ? "" : (" (of " + f.entCount + ")")) + " hasTerms=" + f.hasTerms + " isFloor=" + f.isFloor + " code=" + ((f.fp<<BlockTreeTermsWriter.OUTPUT_FLAGS_NUM_BITS) + (f.hasTerms ? BlockTreeTermsWriter.OUTPUT_FLAG_HAS_TERMS:0) + (f.isFloor ? BlockTreeTermsWriter.OUTPUT_FLAG_IS_FLOOR:0)) + " lastSubFP=" + f.lastSubFP + " isLastInFloor=" + f.isLastInFloor + " mdUpto=" + f.metaDataUpto + " tbOrd=" + f.getTermBlockOrd());
            }
            if (index != null) {
              assert !isSeekFrame || f.arc != null: "isSeekFrame=" + isSeekFrame + " f.arc=" + f.arc;
              if (f.prefix > 0 && isSeekFrame && f.arc.label != (term.bytes[f.prefix-1]&0xFF)) {
                System.out.println("      broken seek state: arc.label=" + (char) f.arc.label + " vs term byte=" + (char) (term.bytes[f.prefix-1]&0xFF));
                throw new RuntimeException("seek state is broken");
              }
              BytesRef output = Util.get(index, prefix);
              if (output == null) {
                System.out.println("      broken seek state: prefix is not final in index");
                throw new RuntimeException("seek state is broken");
              } else if (isSeekFrame && !f.isFloor) {
                final ByteArrayDataInput reader = new ByteArrayDataInput(output.bytes, output.offset, output.length);
                final long codeOrig = reader.readVLong();
                final long code = (f.fp << BlockTreeTermsWriter.OUTPUT_FLAGS_NUM_BITS) | (f.hasTerms ? BlockTreeTermsWriter.OUTPUT_FLAG_HAS_TERMS:0) | (f.isFloor ? BlockTreeTermsWriter.OUTPUT_FLAG_IS_FLOOR:0);
                if (codeOrig != code) {
                  System.out.println("      broken seek state: output code=" + codeOrig + " doesn't match frame code=" + code);
                  throw new RuntimeException("seek state is broken");
                }
              }
            }
            if (f == currentFrame) {
              break;
            }
            if (f.prefix == validIndexPrefix) {
              isSeekFrame = false;
            }
            ord++;
          }
        }
      }

      /* Decodes only the term bytes of the next term.  If caller then asks for
         metadata, ie docFreq, totalTermFreq or pulls a D/&PEnum, we then (lazily)
         decode all metadata up to the current term. */
      @Override
      public BytesRef next() throws IOException {

        targetBeforeCurrentLength = currentFrame.ord;

        assert !eof;
        //if (DEBUG) {
        //System.out.println("\nBTTR.next seg=" + segment + " term=" + brToString(term) + " termExists?=" + termExists + " field=" + fieldInfo.name + " termBlockOrd=" + currentFrame.state.termBlockOrd + " validIndexPrefix=" + validIndexPrefix);
        //printSeekState();
        //}

        if (currentFrame == staticFrame) {
          // If seek was previously called and the term was
          // cached, or seek(TermState) was called, usually
          // caller is just going to pull a D/&PEnum or get
          // docFreq, etc.  But, if they then call next(),
          // this method catches up all internal state so next()
          // works properly:
          //if (DEBUG) System.out.println("  re-seek to pending term=" + term.utf8ToString() + " " + term);
          final boolean result = seekExact(term, false);
          assert result;
        }

        // Pop finished blocks
        while (currentFrame.nextEnt == currentFrame.entCount) {
          if (!currentFrame.isLastInFloor) {
            currentFrame.loadNextFloorBlock();
          } else {
            //if (DEBUG) System.out.println("  pop frame");
            if (currentFrame.ord == 0) {
              //if (DEBUG) System.out.println("  return null");
              assert setEOF();
              term.length = 0;
              validIndexPrefix = 0;
              currentFrame.rewind();
              termExists = false;
              return null;
            }
            final long lastFP = currentFrame.fpOrig;
            currentFrame = stack[currentFrame.ord-1];

            if (currentFrame.nextEnt == -1 || currentFrame.lastSubFP != lastFP) {
              // We popped into a frame that's not loaded
              // yet or not scan'd to the right entry
              currentFrame.scanToFloorFrame(term);
              currentFrame.loadBlock();
              currentFrame.scanToSubBlock(lastFP);
            }

            // Note that the seek state (last seek) has been
            // invalidated beyond this depth
            validIndexPrefix = Math.min(validIndexPrefix, currentFrame.prefix);
            //if (DEBUG) {
            //System.out.println("  reset validIndexPrefix=" + validIndexPrefix);
            //}
          }
        }

        while(true) {
          if (currentFrame.next()) {
            // Push to new block:
            //if (DEBUG) System.out.println("  push frame");
            currentFrame = pushFrame(null, currentFrame.lastSubFP, term.length);
            // This is a "next" frame -- even if it's
            // floor'd we must pretend it isn't so we don't
            // try to scan to the right floor frame:
            currentFrame.isFloor = false;
            //currentFrame.hasTerms = true;
            currentFrame.loadBlock();
          } else {
            //if (DEBUG) System.out.println("  return term=" + term.utf8ToString() + " " + term + " currentFrame.ord=" + currentFrame.ord);
            return term;
          }
        }
      }

      @Override
      public BytesRef term() {
        assert !eof;
        return term;
      }

      @Override
      public int docFreq() throws IOException {
        assert !eof;
        //if (DEBUG) System.out.println("BTR.docFreq");
        currentFrame.decodeMetaData();
        //if (DEBUG) System.out.println("  return " + currentFrame.state.docFreq);
        return currentFrame.state.docFreq;
      }

      @Override
      public long totalTermFreq() throws IOException {
        assert !eof;
        currentFrame.decodeMetaData();
        return currentFrame.state.totalTermFreq;
      }

      @Override
      public DocsEnum docs(Bits skipDocs, DocsEnum reuse) throws IOException {
        assert !eof;
        //if (DEBUG) {
        //System.out.println("BTTR.docs seg=" + segment);
        //}
        currentFrame.decodeMetaData();
        //if (DEBUG) {
        //System.out.println("  state=" + currentFrame.state);
        //}
        final DocsEnum docsEnum = postingsReader.docs(fieldInfo, currentFrame.state, skipDocs, reuse);

        assert docsEnum != null;
        return docsEnum;
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
        assert !eof;
        //System.out.println("BTR.d&p this=" + this);
        if (fieldInfo.indexOptions != IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
          return null;
        } else {
          currentFrame.decodeMetaData();
          DocsAndPositionsEnum dpe = postingsReader.docsAndPositions(fieldInfo, currentFrame.state, skipDocs, reuse);
          //System.out.println("  return d&pe=" + dpe);
          return dpe;
        }
      }

      @Override
      public void seekExact(BytesRef target, TermState otherState) throws IOException {
        // if (DEBUG) {
        //   System.out.println("BTTR.seekExact termState seg=" + segment + " target=" + target.utf8ToString() + " " + target + " state=" + otherState);
        // }
        assert clearEOF();
        if (target.compareTo(term) != 0 || !termExists) {
          assert otherState != null && otherState instanceof BlockTermState;
          currentFrame = staticFrame;
          currentFrame.state.copyFrom(otherState);
          term.copy(target);
          currentFrame.metaDataUpto = currentFrame.getTermBlockOrd();
          assert currentFrame.metaDataUpto > 0;
          validIndexPrefix = 0;
        } else {
          // if (DEBUG) {
          //   System.out.println("  skip seek: already on target state=" + currentFrame.state);
          // }
        }
      }
      
      @Override
      public TermState termState() throws IOException {
        assert !eof;
        currentFrame.decodeMetaData();
        TermState ts = (TermState) currentFrame.state.clone();
        //if (DEBUG) System.out.println("BTTR.termState seg=" + segment + " state=" + ts);
        return ts;
      }

      @Override
      public void seekExact(long ord) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public long ord() {
        throw new UnsupportedOperationException();
      }

      // Not static -- references term, postingsReader,
      // fieldInfo, in
      private final class Frame {
        // Our index in stack[]:
        final int ord;

        boolean hasTerms;
        boolean hasTermsOrig;
        boolean isFloor;

        FST.Arc<BytesRef> arc;

        // File pointer where this block was loaded from
        long fp;
        long fpOrig;
        long fpEnd;

        byte[] suffixBytes = new byte[128];
        final ByteArrayDataInput suffixesReader = new ByteArrayDataInput();

        byte[] statBytes = new byte[64];
        final ByteArrayDataInput statsReader = new ByteArrayDataInput();

        byte[] floorData = new byte[32];
        final ByteArrayDataInput floorDataReader = new ByteArrayDataInput();

        // Length of prefix shared by all terms in this block
        int prefix;

        // Number of entries (term or sub-block) in this block
        int entCount;

        // Which term we will next read, or -1 if the block
        // isn't loaded yet
        int nextEnt;

        // True if this block is either not a floor block,
        // or, it's the last sub-block of a floor block
        boolean isLastInFloor;

        // True if all entries are terms
        boolean isLeafBlock;

        long lastSubFP;

        int nextFloorLabel;
        int numFollowFloorBlocks;

        // Next term to decode metaData; we decode metaData
        // lazily so that scanning to find the matching term is
        // fast and only if you find a match and app wants the
        // stats or docs/positions enums, will we decode the
        // metaData
        int metaDataUpto;

        final BlockTermState state;

        public Frame(int ord) throws IOException {
          this.ord = ord;
          state = postingsReader.newTermState();
          state.totalTermFreq = -1;
        }

        public void setFloorData(ByteArrayDataInput in, BytesRef source) {
          final int numBytes = source.length - (in.getPosition() - source.offset);
          if (numBytes > floorData.length) {
            floorData = new byte[ArrayUtil.oversize(numBytes, 1)];
          }
          System.arraycopy(source.bytes, source.offset+in.getPosition(), floorData, 0, numBytes);
          floorDataReader.reset(floorData, 0, numBytes);
          numFollowFloorBlocks = floorDataReader.readVInt();
          nextFloorLabel = floorDataReader.readByte() & 0xff;
          //if (DEBUG) {
          //System.out.println("    setFloorData fpOrig=" + fpOrig + " bytes=" + new BytesRef(source.bytes, source.offset + in.getPosition(), numBytes) + " numFollowFloorBlocks=" + numFollowFloorBlocks + " nextFloorLabel=" + toHex(nextFloorLabel));
          //}
        }

        public int getTermBlockOrd() {
          return isLeafBlock ? nextEnt : state.termBlockOrd;
        }

        void loadNextFloorBlock() throws IOException {
          //if (DEBUG) {
          //System.out.println("    loadNextFloorBlock fp=" + fp + " fpEnd=" + fpEnd);
          //}
          assert arc == null || isFloor: "arc=" + arc + " isFloor=" + isFloor;
          fp = fpEnd;
          nextEnt = -1;
          loadBlock();
        }

        /* Does initial decode of next block of terms; this
           doesn't actually decode the docFreq, totalTermFreq,
           postings details (frq/prx offset, etc.) metadata;
           it just loads them as byte[] blobs which are then      
           decoded on-demand if the metadata is ever requested
           for any term in this block.  This enables terms-only
           intensive consumes (eg certain MTQs, respelling) to
           not pay the price of decoding metadata they won't
           use. */
        void loadBlock() throws IOException {

          if (nextEnt != -1) {
            // Already loaded
            return;
          }
          //System.out.println("blc=" + blockLoadCount);

          in.seek(fp);
          int code = in.readVInt();
          entCount = code >>> 1;
          assert entCount > 0;
          isLastInFloor = (code & 1) != 0;
          assert arc == null || (isLastInFloor || isFloor);

          // TODO: if suffixes were stored in random-access
          // array structure, then we could do binary search
          // instead of linear scan to find target term; eg
          // we could have simple array of offsets

          // term suffixes:
          code = in.readVInt();
          isLeafBlock = (code & 1) != 0;
          int numBytes = code >>> 1;
          if (suffixBytes.length < numBytes) {
            suffixBytes = new byte[ArrayUtil.oversize(numBytes, 1)];
          }
          in.readBytes(suffixBytes, 0, numBytes);
          suffixesReader.reset(suffixBytes, 0, numBytes);

          /*if (DEBUG) {
            if (arc == null) {
              System.out.println("    loadBlock (next) fp=" + fp + " entCount=" + entCount + " prefixLen=" + prefix + " isLastInFloor=" + isLastInFloor + " leaf?=" + isLeafBlock);
            } else {
              System.out.println("    loadBlock (seek) fp=" + fp + " entCount=" + entCount + " prefixLen=" + prefix + " hasTerms?=" + hasTerms + " isFloor?=" + isFloor + " isLastInFloor=" + isLastInFloor + " leaf?=" + isLeafBlock);
            }
            }*/

          // stats
          numBytes = in.readVInt();
          if (statBytes.length < numBytes) {
            statBytes = new byte[ArrayUtil.oversize(numBytes, 1)];
          }
          in.readBytes(statBytes, 0, numBytes);
          statsReader.reset(statBytes, 0, numBytes);
          metaDataUpto = 0;

          state.termBlockOrd = 0;
          nextEnt = 0;
          lastSubFP = -1;

          // TODO: we could skip this if !hasTerms; but
          // that's rare so won't help much
          postingsReader.readTermsBlock(in, fieldInfo, state);

          // Sub-blocks of a single floor block are always
          // written one after another -- tail recurse:
          fpEnd = in.getFilePointer();
          // if (DEBUG) {
          //   System.out.println("      fpEnd=" + fpEnd);
          // }
        }

        void rewind() throws IOException {

          // Force reload:
          fp = fpOrig;
          nextEnt = -1;
          hasTerms = hasTermsOrig;
          if (isFloor) {
            floorDataReader.rewind();
            numFollowFloorBlocks = floorDataReader.readVInt();
            nextFloorLabel = floorDataReader.readByte() & 0xff;
          }

          /*
          //System.out.println("rewind");
          // Keeps the block loaded, but rewinds its state:
          if (nextEnt > 0 || fp != fpOrig) {
            if (DEBUG) {
              System.out.println("      rewind frame ord=" + ord + " fpOrig=" + fpOrig + " fp=" + fp + " hasTerms?=" + hasTerms + " isFloor?=" + isFloor + " nextEnt=" + nextEnt + " prefixLen=" + prefix);
            }
            if (fp != fpOrig) {
              fp = fpOrig;
              nextEnt = -1;
            } else {
              nextEnt = 0;
            }
            hasTerms = hasTermsOrig;
            if (isFloor) {
              floorDataReader.rewind();
              numFollowFloorBlocks = floorDataReader.readVInt();
              nextFloorLabel = floorDataReader.readByte() & 0xff;
            }
            assert suffixBytes != null;
            suffixesReader.rewind();
            assert statBytes != null;
            statsReader.rewind();
            metaDataUpto = 0;
            state.termBlockOrd = 0;
            // TODO: skip this if !hasTerms?  Then postings
            // impl wouldn't have to write useless 0 byte
            postingsReader.resetTermsBlock(fieldInfo, state);
            lastSubFP = -1;
          } else if (DEBUG) {
            System.out.println("      skip rewind fp=" + fp + " fpOrig=" + fpOrig + " nextEnt=" + nextEnt + " ord=" + ord);
          }
          */
        }

        public boolean next() {
          return isLeafBlock ? nextLeaf() : nextNonLeaf();
        }

        // Decodes next entry; returns true if it's a sub-block
        public boolean nextLeaf() {
          //if (DEBUG) System.out.println("  frame.next ord=" + ord + " nextEnt=" + nextEnt + " entCount=" + entCount);
          assert nextEnt != -1 && nextEnt < entCount: "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
          nextEnt++;
          suffix = suffixesReader.readVInt();
          startBytePos = suffixesReader.getPosition();
          term.length = prefix + suffix;
          if (term.bytes.length < term.length) {
            term.grow(term.length);
          }
          suffixesReader.readBytes(term.bytes, prefix, suffix);
          // A normal term
          termExists = true;
          return false;
        }

        public boolean nextNonLeaf() {
          //if (DEBUG) System.out.println("  frame.next ord=" + ord + " nextEnt=" + nextEnt + " entCount=" + entCount);
          assert nextEnt != -1 && nextEnt < entCount: "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
          nextEnt++;
          final int code = suffixesReader.readVInt();
          suffix = code >>> 1;
          startBytePos = suffixesReader.getPosition();
          term.length = prefix + suffix;
          if (term.bytes.length < term.length) {
            term.grow(term.length);
          }
          suffixesReader.readBytes(term.bytes, prefix, suffix);
          if ((code & 1) == 0) {
            // A normal term
            termExists = true;
            state.termBlockOrd++;
            return false;
          } else {
            // A sub-block; make sub-FP absolute:
            termExists = false;
            lastSubFP = fp - suffixesReader.readVLong();
            //if (DEBUG) {
            //System.out.println("    lastSubFP=" + lastSubFP);
            //}
            return true;
          }
        }
        
        // TODO: make this array'd so we can do bin search?
        // likely not worth it?  need to measure how many
        // floor blocks we "typically" get
        public void scanToFloorFrame(BytesRef target) {

          if (!isFloor || target.length <= prefix) {
            // if (DEBUG) {
            //   System.out.println("    scanToFloorFrame skip: isFloor=" + isFloor + " target.length=" + target.length + " vs prefix=" + prefix);
            // }
            return;
          }

          final int targetLabel = target.bytes[target.offset + prefix] & 0xFF;

          // if (DEBUG) {
          //   System.out.println("    scanToFloorFrame fpOrig=" + fpOrig + " targetLabel=" + toHex(targetLabel) + " vs nextFloorLabel=" + toHex(nextFloorLabel) + " numFollowFloorBlocks=" + numFollowFloorBlocks);
          // }

          if (targetLabel < nextFloorLabel) {
            // if (DEBUG) {
            //   System.out.println("      already on correct block");
            // }
            return;
          }

          assert numFollowFloorBlocks != 0;

          long newFP = fpOrig;
          while (true) {
            final long code = floorDataReader.readVLong();
            newFP = fpOrig + (code >>> 1);
            hasTerms = (code & 1) != 0;
            // if (DEBUG) {
            //   System.out.println("      label=" + toHex(nextFloorLabel) + " fp=" + newFP + " hasTerms?=" + hasTerms + " numFollowFloor=" + numFollowFloorBlocks);
            // }
            
            isLastInFloor = numFollowFloorBlocks == 1;
            numFollowFloorBlocks--;

            if (isLastInFloor) {
              nextFloorLabel = 256;
              // if (DEBUG) {
              //   System.out.println("        stop!  last block nextFloorLabel=" + toHex(nextFloorLabel));
              // }
              break;
            } else {
              nextFloorLabel = floorDataReader.readByte() & 0xff;
              if (targetLabel < nextFloorLabel) {
                // if (DEBUG) {
                //   System.out.println("        stop!  nextFloorLabel=" + toHex(nextFloorLabel));
                // }
                break;
              }
            }
          }

          if (newFP != fp) {
            // Force re-load of the block:
            // if (DEBUG) {
            //   System.out.println("      force switch to fp=" + newFP + " oldFP=" + fp);
            // }
            nextEnt = -1;
            fp = newFP;
          } else {
            // if (DEBUG) {
            //   System.out.println("      stay on same fp=" + newFP);
            // }
          }
        }
    
        public void decodeMetaData() throws IOException {

          //if (DEBUG) System.out.println("\nBTTR.decodeMetadata seg=" + segment + " mdUpto=" + metaDataUpto + " vs termBlockOrd=" + state.termBlockOrd);

          // lazily catch up on metadata decode:
          final int limit = getTermBlockOrd();
          assert limit > 0;

          // We must set/incr state.termCount because
          // postings impl can look at this
          state.termBlockOrd = metaDataUpto;
      
          // TODO: better API would be "jump straight to term=N"???
          while (metaDataUpto < limit) {

            // TODO: we could make "tiers" of metadata, ie,
            // decode docFreq/totalTF but don't decode postings
            // metadata; this way caller could get
            // docFreq/totalTF w/o paying decode cost for
            // postings

            // TODO: if docFreq were bulk decoded we could
            // just skipN here:
            state.docFreq = statsReader.readVInt();
            //if (DEBUG) System.out.println("    dF=" + state.docFreq);
            if (fieldInfo.indexOptions != IndexOptions.DOCS_ONLY) {
              state.totalTermFreq = state.docFreq + statsReader.readVLong();
              //if (DEBUG) System.out.println("    totTF=" + state.totalTermFreq);
            }

            postingsReader.nextTerm(fieldInfo, state);
            metaDataUpto++;
            state.termBlockOrd++;
          }
        }

        // Used only by assert
        private boolean prefixMatches(BytesRef target) {
          for(int bytePos=0;bytePos<prefix;bytePos++) {
            if (target.bytes[target.offset + bytePos] != term.bytes[bytePos]) {
              return false;
            }
          }

          return true;
        }

        // Scans to sub-block that has this target fp; only
        // called by next(); NOTE: does not set
        // startBytePos/suffix as a side effect
        public void scanToSubBlock(long subFP) {
          assert !isLeafBlock;
          //if (DEBUG) System.out.println("  scanToSubBlock fp=" + fp + " subFP=" + subFP + " entCount=" + entCount + " lastSubFP=" + lastSubFP);
          //assert nextEnt == 0;
          if (lastSubFP == subFP) {
            //if (DEBUG) System.out.println("    already positioned");
            return;
          }
          assert subFP < fp : "fp=" + fp + " subFP=" + subFP;
          final long targetSubCode = fp - subFP;
          //if (DEBUG) System.out.println("    targetSubCode=" + targetSubCode);
          while(true) {
            assert nextEnt < entCount;
            nextEnt++;
            final int code = suffixesReader.readVInt();
            suffixesReader.skipBytes(isLeafBlock ? code : code >>> 1);
            //if (DEBUG) System.out.println("    " + nextEnt + " (of " + entCount + ") ent isSubBlock=" + ((code&1)==1));
            if ((code & 1) != 0) {
              final long subCode = suffixesReader.readVLong();
              //if (DEBUG) System.out.println("      subCode=" + subCode);
              if (targetSubCode == subCode) {
                //if (DEBUG) System.out.println("        match!");
                lastSubFP = subFP;
                return;
              }
            } else {
              state.termBlockOrd++;
            }
          }
        }

        // NOTE: sets startBytePos/suffix as a side effect
        public SeekStatus scanToTerm(BytesRef target, boolean exactOnly) throws IOException {
          return isLeafBlock ? scanToTermLeaf(target, exactOnly) : scanToTermNonLeaf(target, exactOnly);
        }

        private int startBytePos;
        private int suffix;
        private long subCode;

        // Target's prefix matches this block's prefix; we
        // scan the entries check if the suffix matches.
        public SeekStatus scanToTermLeaf(BytesRef target, boolean exactOnly) throws IOException {

          // if (DEBUG) System.out.println("    scanToTermLeaf: block fp=" + fp + " prefix=" + prefix + " nextEnt=" + nextEnt + " (of " + entCount + ") target=" + brToString(target) + " term=" + brToString(term));

          assert nextEnt != -1;

          termExists = true;
          subCode = 0;

          if (nextEnt == entCount) {
            if (exactOnly) {
              fillTerm();
            }
            return SeekStatus.END;
          }

          assert prefixMatches(target);

          // Loop over each entry (term or sub-block) in this block:
          //nextTerm: while(nextEnt < entCount) {
          nextTerm: while (true) {
            nextEnt++;

            suffix = suffixesReader.readVInt();

            // if (DEBUG) {
            //   BytesRef suffixBytesRef = new BytesRef();
            //   suffixBytesRef.bytes = suffixBytes;
            //   suffixBytesRef.offset = suffixesReader.getPosition();
            //   suffixBytesRef.length = suffix;
            //   System.out.println("      cycle: term " + (nextEnt-1) + " (of " + entCount + ") suffix=" + brToString(suffixBytesRef));
            // }

            final int termLen = prefix + suffix;
            startBytePos = suffixesReader.getPosition();
            suffixesReader.skipBytes(suffix);

            final int targetLimit = target.offset + (target.length < termLen ? target.length : termLen);
            int targetPos = target.offset + prefix;

            // Loop over bytes in the suffix, comparing to
            // the target
            int bytePos = startBytePos;
            while(true) {
              final int cmp;
              final boolean stop;
              if (targetPos < targetLimit) {
                cmp = (suffixBytes[bytePos++]&0xFF) - (target.bytes[targetPos++]&0xFF);
                stop = false;
              } else {
                assert targetPos == targetLimit;
                cmp = termLen - target.length;
                stop = true;
              }

              if (cmp < 0) {
                // Current entry is still before the target;
                // keep scanning

                if (nextEnt == entCount) {
                  if (exactOnly) {
                    fillTerm();
                  }
                  // We are done scanning this block
                  break nextTerm;
                } else {
                  continue nextTerm;
                }
              } else if (cmp > 0) {

                // Done!  Current entry is after target --
                // return NOT_FOUND:
                fillTerm();

                if (!exactOnly && !termExists) {
                  // We are on a sub-block, and caller wants
                  // us to position to the next term after
                  // the target, so we must recurse into the
                  // sub-frame(s):
                  currentFrame = pushFrame(null, currentFrame.lastSubFP, termLen);
                  currentFrame.loadBlock();
                  while (currentFrame.next()) {
                    currentFrame = pushFrame(null, currentFrame.lastSubFP, term.length);
                    currentFrame.loadBlock();
                  }
                }
                
                //if (DEBUG) System.out.println("        not found");
                return SeekStatus.NOT_FOUND;
              } else if (stop) {
                // Exact match!

                // This cannot be a sub-block because we
                // would have followed the index to this
                // sub-block from the start:

                assert termExists;
                fillTerm();
                //if (DEBUG) System.out.println("        found!");
                return SeekStatus.FOUND;
              }
            }
          }

          // It is possible (and OK) that terms index pointed us
          // at this block, but, we scanned the entire block and
          // did not find the term to position to.  This happens
          // when the target is after the last term in the block
          // (but, before the next term in the index).  EG
          // target could be foozzz, and terms index pointed us
          // to the foo* block, but the last term in this block
          // was fooz (and, eg, first term in the next block will
          // bee fop).
          //if (DEBUG) System.out.println("      block end");
          if (exactOnly) {
            fillTerm();
          }

          // TODO: not consistent that in the
          // not-exact case we don't next() into the next
          // frame here
          return SeekStatus.END;
        }

        // Target's prefix matches this block's prefix; we
        // scan the entries check if the suffix matches.
        public SeekStatus scanToTermNonLeaf(BytesRef target, boolean exactOnly) throws IOException {

          //if (DEBUG) System.out.println("    scanToTermNonLeaf: block fp=" + fp + " prefix=" + prefix + " nextEnt=" + nextEnt + " (of " + entCount + ") target=" + brToString(target) + " term=" + brToString(term));

          assert nextEnt != -1;

          if (nextEnt == entCount) {
            if (exactOnly) {
              fillTerm();
              termExists = subCode == 0;
            }
            return SeekStatus.END;
          }

          assert prefixMatches(target);

          // Loop over each entry (term or sub-block) in this block:
          //nextTerm: while(nextEnt < entCount) {
          nextTerm: while (true) {
            nextEnt++;

            final int code = suffixesReader.readVInt();
            suffix = code >>> 1;
            // if (DEBUG) {
            //   BytesRef suffixBytesRef = new BytesRef();
            //   suffixBytesRef.bytes = suffixBytes;
            //   suffixBytesRef.offset = suffixesReader.getPosition();
            //   suffixBytesRef.length = suffix;
            //   System.out.println("      cycle: " + ((code&1)==1 ? "sub-block" : "term") + " " + (nextEnt-1) + " (of " + entCount + ") suffix=" + brToString(suffixBytesRef));
            // }

            termExists = (code & 1) == 0;
            final int termLen = prefix + suffix;
            startBytePos = suffixesReader.getPosition();
            suffixesReader.skipBytes(suffix);
            if (termExists) {
              state.termBlockOrd++;
              subCode = 0;
            } else {
              subCode = suffixesReader.readVLong();
              lastSubFP = fp - subCode;
            }

            final int targetLimit = target.offset + (target.length < termLen ? target.length : termLen);
            int targetPos = target.offset + prefix;

            // Loop over bytes in the suffix, comparing to
            // the target
            int bytePos = startBytePos;
            while(true) {
              final int cmp;
              final boolean stop;
              if (targetPos < targetLimit) {
                cmp = (suffixBytes[bytePos++]&0xFF) - (target.bytes[targetPos++]&0xFF);
                stop = false;
              } else {
                assert targetPos == targetLimit;
                cmp = termLen - target.length;
                stop = true;
              }

              if (cmp < 0) {
                // Current entry is still before the target;
                // keep scanning

                if (nextEnt == entCount) {
                  if (exactOnly) {
                    fillTerm();
                    //termExists = true;
                  }
                  // We are done scanning this block
                  break nextTerm;
                } else {
                  continue nextTerm;
                }
              } else if (cmp > 0) {

                // Done!  Current entry is after target --
                // return NOT_FOUND:
                fillTerm();

                if (!exactOnly && !termExists) {
                  // We are on a sub-block, and caller wants
                  // us to position to the next term after
                  // the target, so we must recurse into the
                  // sub-frame(s):
                  currentFrame = pushFrame(null, currentFrame.lastSubFP, termLen);
                  currentFrame.loadBlock();
                  while (currentFrame.next()) {
                    currentFrame = pushFrame(null, currentFrame.lastSubFP, term.length);
                    currentFrame.loadBlock();
                  }
                }
                
                //if (DEBUG) System.out.println("        not found");
                return SeekStatus.NOT_FOUND;
              } else if (stop) {
                // Exact match!

                // This cannot be a sub-block because we
                // would have followed the index to this
                // sub-block from the start:

                assert termExists;
                fillTerm();
                //if (DEBUG) System.out.println("        found!");
                return SeekStatus.FOUND;
              }
            }
          }

          // It is possible (and OK) that terms index pointed us
          // at this block, but, we scanned the entire block and
          // did not find the term to position to.  This happens
          // when the target is after the last term in the block
          // (but, before the next term in the index).  EG
          // target could be foozzz, and terms index pointed us
          // to the foo* block, but the last term in this block
          // was fooz (and, eg, first term in the next block will
          // bee fop).
          //if (DEBUG) System.out.println("      block end");
          if (exactOnly) {
            fillTerm();
          }

          // TODO: not consistent that in the
          // not-exact case we don't next() into the next
          // frame here
          return SeekStatus.END;
        }

        private void fillTerm() {
          final int termLength = prefix + suffix;
          term.length = prefix + suffix;
          if (term.bytes.length < termLength) {
            term.grow(termLength);
          }
          System.arraycopy(suffixBytes, startBytePos, term.bytes, prefix, suffix);
        }
      }
    }
  }
}
