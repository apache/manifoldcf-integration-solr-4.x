package org.apache.lucene.index;

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
import java.io.PrintStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.Comparator;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/* Tracks the stream of {@link BuffereDeletes}.
 * When DocumensWriter flushes, its buffered
 * deletes are appended to this stream.  We later
 * apply these deletes (resolve them to the actual
 * docIDs, per segment) when a merge is started
 * (only to the to-be-merged segments).  We
 * also apply to all segments when NRT reader is pulled,
 * commit/close is called, or when too many deletes are
 * buffered and must be flushed (by RAM usage or by count).
 *
 * Each packet is assigned a generation, and each flushed or
 * merged segment is also assigned a generation, so we can
 * track which BufferedDeletes packets to apply to any given
 * segment. */

class BufferedDeletesStream {

  // TODO: maybe linked list?
  private final List<FrozenBufferedDeletes> deletes = new ArrayList<FrozenBufferedDeletes>();

  // Starts at 1 so that SegmentInfos that have never had
  // deletes applied (whose bufferedDelGen defaults to 0)
  // will be correct:
  private long nextGen = 1;

  // used only by assert
  private Term lastDeleteTerm;
  
  private PrintStream infoStream;
  private final AtomicLong bytesUsed = new AtomicLong();
  private final AtomicInteger numTerms = new AtomicInteger();
  private final int messageID;

  public BufferedDeletesStream(int messageID) {
    this.messageID = messageID;
  }

  private synchronized void message(String message) {
    if (infoStream != null) {
      infoStream.println("BD " + messageID + " [" + new Date() + "; " + Thread.currentThread().getName() + "]: " + message);
    }
  }
  
  public synchronized void setInfoStream(PrintStream infoStream) {
    this.infoStream = infoStream;
  }

  // Appends a new packet of buffered deletes to the stream,
  // setting its generation:
  public synchronized void push(FrozenBufferedDeletes packet) {
    assert packet.any();
    assert checkDeleteStats();    
    assert packet.gen < nextGen;
    deletes.add(packet);
    numTerms.addAndGet(packet.numTermDeletes);
    bytesUsed.addAndGet(packet.bytesUsed);
    if (infoStream != null) {
      message("push deletes " + packet + " delGen=" + packet.gen + " packetCount=" + deletes.size());
    }
    assert checkDeleteStats();    
  }
    
  public synchronized void clear() {
    deletes.clear();
    nextGen = 1;
    numTerms.set(0);
    bytesUsed.set(0);
  }

  public boolean any() {
    return bytesUsed.get() != 0;
  }

  public int numTerms() {
    return numTerms.get();
  }

  public long bytesUsed() {
    return bytesUsed.get();
  }

  public static class ApplyDeletesResult {
    // True if any actual deletes took place:
    public final boolean anyDeletes;

    // Current gen, for the merged segment:
    public final long gen;

    ApplyDeletesResult(boolean anyDeletes, long gen) {
      this.anyDeletes = anyDeletes;
      this.gen = gen;
    }
  }

  // Sorts SegmentInfos from smallest to biggest bufferedDelGen:
  private static final Comparator<SegmentInfo> sortByDelGen = new Comparator<SegmentInfo>() {
    // @Override -- not until Java 1.6
    public int compare(SegmentInfo si1, SegmentInfo si2) {
      final long cmp = si1.getBufferedDeletesGen() - si2.getBufferedDeletesGen();
      if (cmp > 0) {
        return 1;
      } else if (cmp < 0) {
        return -1;
      } else {
        return 0;
      }
    }

    @Override
    public boolean equals(Object other) {
      return sortByDelGen == other;
    }
  };

  /** Resolves the buffered deleted Term/Query/docIDs, into
   *  actual deleted docIDs in the deletedDocs BitVector for
   *  each SegmentReader. */
  public synchronized ApplyDeletesResult applyDeletes(IndexWriter.ReaderPool readerPool, SegmentInfos infos) throws IOException {
    final long t0 = System.currentTimeMillis();

    if (infos.size() == 0) {
      return new ApplyDeletesResult(false, nextGen++);
    }

    assert checkDeleteStats();

    if (!any()) {
      message("applyDeletes: no deletes; skipping");
      return new ApplyDeletesResult(false, nextGen++);
    }

    if (infoStream != null) {
      message("applyDeletes: infos=" + infos + " packetCount=" + deletes.size());
    }

    SegmentInfos infos2 = new SegmentInfos();
    infos2.addAll(infos);
    Collections.sort(infos2, sortByDelGen);

    BufferedDeletes coalescedDeletes = null;
    boolean anyNewDeletes = false;

    int infosIDX = infos2.size()-1;
    int delIDX = deletes.size()-1;

    while (infosIDX >= 0) {
      //System.out.println("BD: cycle delIDX=" + delIDX + " infoIDX=" + infosIDX);

      final FrozenBufferedDeletes packet = delIDX >= 0 ? deletes.get(delIDX) : null;
      final SegmentInfo info = infos2.get(infosIDX);
      final long segGen = info.getBufferedDeletesGen();

      if (packet != null && segGen < packet.gen) {
        //System.out.println("  coalesce");
        if (coalescedDeletes == null) {
          coalescedDeletes = new BufferedDeletes(true);
        }
        coalescedDeletes.update(packet);
        delIDX--;
      } else if (packet != null && segGen == packet.gen) {
        //System.out.println("  eq");

        // Lock order: IW -> BD -> RP
        assert readerPool.infoIsLive(info);
        SegmentReader reader = readerPool.get(info, false);
        int delCount = 0;
        try {
          if (coalescedDeletes != null) {
            //System.out.println("    del coalesced");
            delCount += applyTermDeletes(coalescedDeletes.termsIterable(), reader);
            delCount += applyQueryDeletes(coalescedDeletes.queriesIterable(), reader);
          }
          //System.out.println("    del exact");
          // Don't delete by Term here; DocumentsWriter
          // already did that on flush:
          delCount += applyQueryDeletes(packet.queriesIterable(), reader);
        } finally {
          readerPool.release(reader);
        }
        anyNewDeletes |= delCount > 0;

        if (infoStream != null) {
          message("seg=" + info + " segGen=" + segGen + " segDeletes=[" + packet + "]; coalesced deletes=[" + (coalescedDeletes == null ? "null" : coalescedDeletes) + "] delCount=" + delCount);
        }

        if (coalescedDeletes == null) {
          coalescedDeletes = new BufferedDeletes(true);
        }
        coalescedDeletes.update(packet);
        delIDX--;
        infosIDX--;
        info.setBufferedDeletesGen(nextGen);

      } else {
        //System.out.println("  gt");

        if (coalescedDeletes != null) {
          // Lock order: IW -> BD -> RP
          assert readerPool.infoIsLive(info);
          SegmentReader reader = readerPool.get(info, false);
          int delCount = 0;
          try {
            delCount += applyTermDeletes(coalescedDeletes.termsIterable(), reader);
            delCount += applyQueryDeletes(coalescedDeletes.queriesIterable(), reader);
          } finally {
            readerPool.release(reader);
          }
          anyNewDeletes |= delCount > 0;

          if (infoStream != null) {
            message("seg=" + info + " segGen=" + segGen + " coalesced deletes=[" + (coalescedDeletes == null ? "null" : coalescedDeletes) + "] delCount=" + delCount);
          }
        }
        info.setBufferedDeletesGen(nextGen);

        infosIDX--;
      }
    }

    assert checkDeleteStats();
    if (infoStream != null) {
      message("applyDeletes took " + (System.currentTimeMillis()-t0) + " msec");
    }
    // assert infos != segmentInfos || !any() : "infos=" + infos + " segmentInfos=" + segmentInfos + " any=" + any;
    
    return new ApplyDeletesResult(anyNewDeletes, nextGen++);
  }

  public synchronized long getNextGen() {
    return nextGen++;
  }

  // Lock order IW -> BD
  /* Removes any BufferedDeletes that we no longer need to
   * store because all segments in the index have had the
   * deletes applied. */
  public synchronized void prune(SegmentInfos segmentInfos) {
    assert checkDeleteStats();
    long minGen = Long.MAX_VALUE;
    for(SegmentInfo info : segmentInfos) {
      minGen = Math.min(info.getBufferedDeletesGen(), minGen);
    }

    if (infoStream != null) {
      message("prune sis=" + segmentInfos + " minGen=" + minGen + " packetCount=" + deletes.size());
    }

    final int limit = deletes.size();
    for(int delIDX=0;delIDX<limit;delIDX++) {
      if (deletes.get(delIDX).gen >= minGen) {
        prune(delIDX);
        assert checkDeleteStats();
        return;
      }
    }

    // All deletes pruned
    prune(limit);
    assert !any();
    assert checkDeleteStats();
  }

  private synchronized void prune(int count) {
    if (count > 0) {
      if (infoStream != null) {
        message("pruneDeletes: prune " + count + " packets; " + (deletes.size() - count) + " packets remain");
      }
      for(int delIDX=0;delIDX<count;delIDX++) {
        final FrozenBufferedDeletes packet = deletes.get(delIDX);
        numTerms.addAndGet(-packet.numTermDeletes);
        assert numTerms.get() >= 0;
        bytesUsed.addAndGet(-packet.bytesUsed);
        assert bytesUsed.get() >= 0;
      }
      deletes.subList(0, count).clear();
    }
  }

  // Delete by Term
  private synchronized long applyTermDeletes(Iterable<Term> termsIter, SegmentReader reader) throws IOException {
    long delCount = 0;
    Fields fields = reader.fields();
    if (fields == null) {
      // This reader has no postings
      return 0;
    }

    TermsEnum termsEnum = null;
        
    String currentField = null;
    DocsEnum docs = null;
        
    assert checkDeleteTerm(null);

    for (Term term : termsIter) {
      // Since we visit terms sorted, we gain performance
      // by re-using the same TermsEnum and seeking only
      // forwards
      if (term.field() != currentField) {
        assert currentField == null || currentField.compareTo(term.field()) < 0;
        currentField = term.field();
        Terms terms = fields.terms(currentField);
        if (terms != null) {
          termsEnum = terms.iterator();
        } else {
          termsEnum = null;
        }
      }

      if (termsEnum == null) {
        continue;
      }
      assert checkDeleteTerm(term);

      // System.out.println("  term=" + term);
          
      if (termsEnum.seek(term.bytes(), false) == TermsEnum.SeekStatus.FOUND) {
        DocsEnum docsEnum = termsEnum.docs(reader.getDeletedDocs(), docs);
            
        if (docsEnum != null) {
          while (true) {
            final int docID = docsEnum.nextDoc();
            if (docID == DocsEnum.NO_MORE_DOCS) {
              break;
            }
            reader.deleteDocument(docID);
            // TODO: we could/should change
            // reader.deleteDocument to return boolean
            // true if it did in fact delete, because here
            // we could be deleting an already-deleted doc
            // which makes this an upper bound:
            delCount++;
          }
        }
      }
    }

    return delCount;
  }

  public static class QueryAndLimit {
    public final Query query;
    public final int limit;
    public QueryAndLimit(Query query, int limit) {
      this.query = query;       
      this.limit = limit;
    }
  }

  // Delete by query
  private synchronized long applyQueryDeletes(Iterable<QueryAndLimit> queriesIter, SegmentReader reader) throws IOException {
    long delCount = 0;
    IndexSearcher searcher = new IndexSearcher(reader);
    assert searcher.getTopReaderContext().isAtomic;
    final AtomicReaderContext readerContext = (AtomicReaderContext) searcher.getTopReaderContext();
    try {
      for (QueryAndLimit ent : queriesIter) {
        Query query = ent.query;
        int limit = ent.limit;
        Weight weight = query.weight(searcher);
        Scorer scorer = weight.scorer(readerContext, Weight.ScorerContext.def());
        if (scorer != null) {
          while(true)  {
            int doc = scorer.nextDoc();
            if (doc >= limit)
              break;

            reader.deleteDocument(doc);
            // TODO: we could/should change
            // reader.deleteDocument to return boolean
            // true if it did in fact delete, because here
            // we could be deleting an already-deleted doc
            // which makes this an upper bound:
            delCount++;
          }
        }
      }
    } finally {
      searcher.close();
    }

    return delCount;
  }

  // used only by assert
  private boolean checkDeleteTerm(Term term) {
    if (term != null) {
      assert lastDeleteTerm == null || term.compareTo(lastDeleteTerm) > 0: "lastTerm=" + lastDeleteTerm + " vs term=" + term;
    }
    lastDeleteTerm = term;
    return true;
  }
  
  // only for assert
  private boolean checkDeleteStats() {
    int numTerms2 = 0;
    long bytesUsed2 = 0;
    for(FrozenBufferedDeletes packet : deletes) {
      numTerms2 += packet.numTermDeletes;
      bytesUsed2 += packet.bytesUsed;
    }
    assert numTerms2 == numTerms.get(): "numTerms2=" + numTerms2 + " vs " + numTerms.get();
    assert bytesUsed2 == bytesUsed.get(): "bytesUsed2=" + bytesUsed2 + " vs " + bytesUsed;
    return true;
  }
}
