package org.apache.lucene.index;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_MASK;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

import java.io.IOException;
import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocumentsWriterDeleteQueue.DeleteSlice;
import org.apache.lucene.search.SimilarityProvider;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BitVector;
import org.apache.lucene.util.ByteBlockPool.Allocator;
import org.apache.lucene.util.ByteBlockPool.DirectTrackingAllocator;
import org.apache.lucene.util.RamUsageEstimator;

public class DocumentsWriterPerThread {

  /**
   * The IndexingChain must define the {@link #getChain(DocumentsWriter)} method
   * which returns the DocConsumer that the DocumentsWriter calls to process the
   * documents.
   */
  abstract static class IndexingChain {
    abstract DocConsumer getChain(DocumentsWriterPerThread documentsWriterPerThread);
  }
  

  static final IndexingChain defaultIndexingChain = new IndexingChain() {

    @Override
    DocConsumer getChain(DocumentsWriterPerThread documentsWriterPerThread) {
      /*
      This is the current indexing chain:

      DocConsumer / DocConsumerPerThread
        --> code: DocFieldProcessor / DocFieldProcessorPerThread
          --> DocFieldConsumer / DocFieldConsumerPerThread / DocFieldConsumerPerField
            --> code: DocFieldConsumers / DocFieldConsumersPerThread / DocFieldConsumersPerField
              --> code: DocInverter / DocInverterPerThread / DocInverterPerField
                --> InvertedDocConsumer / InvertedDocConsumerPerThread / InvertedDocConsumerPerField
                  --> code: TermsHash / TermsHashPerThread / TermsHashPerField
                    --> TermsHashConsumer / TermsHashConsumerPerThread / TermsHashConsumerPerField
                      --> code: FreqProxTermsWriter / FreqProxTermsWriterPerThread / FreqProxTermsWriterPerField
                      --> code: TermVectorsTermsWriter / TermVectorsTermsWriterPerThread / TermVectorsTermsWriterPerField
                --> InvertedDocEndConsumer / InvertedDocConsumerPerThread / InvertedDocConsumerPerField
                  --> code: NormsWriter / NormsWriterPerThread / NormsWriterPerField
              --> code: StoredFieldsWriter / StoredFieldsWriterPerThread / StoredFieldsWriterPerField
    */

    // Build up indexing chain:

      final TermsHashConsumer termVectorsWriter = new TermVectorsTermsWriter(documentsWriterPerThread);
      final TermsHashConsumer freqProxWriter = new FreqProxTermsWriter();

      final InvertedDocConsumer  termsHash = new TermsHash(documentsWriterPerThread, freqProxWriter, true,
                                                           new TermsHash(documentsWriterPerThread, termVectorsWriter, false, null));
      final NormsWriter normsWriter = new NormsWriter();
      final DocInverter docInverter = new DocInverter(documentsWriterPerThread.docState, termsHash, normsWriter);
      return new DocFieldProcessor(documentsWriterPerThread, docInverter);
    }
  };

  static class DocState {
    final DocumentsWriterPerThread docWriter;
    Analyzer analyzer;
    PrintStream infoStream;
    SimilarityProvider similarityProvider;
    int docID;
    Document doc;
    String maxTermPrefix;

    DocState(DocumentsWriterPerThread docWriter) {
      this.docWriter = docWriter;
    }

    // Only called by asserts
    public boolean testPoint(String name) {
      return docWriter.writer.testPoint(name);
    }

    public void clear() {
      // don't hold onto doc nor analyzer, in case it is
      // largish:
      doc = null;
      analyzer = null;
    }
  }

  static class FlushedSegment {
    final SegmentInfo segmentInfo;
    final BufferedDeletes segmentDeletes;
    final BitVector liveDocs;

    private FlushedSegment(SegmentInfo segmentInfo,
        BufferedDeletes segmentDeletes, BitVector liveDocs) {
      this.segmentInfo = segmentInfo;
      this.segmentDeletes = segmentDeletes;
      this.liveDocs = liveDocs;
    }
  }

  /** Called if we hit an exception at a bad time (when
   *  updating the index files) and must discard all
   *  currently buffered docs.  This resets our state,
   *  discarding any docs added since last flush. */
  void abort() throws IOException {
    hasAborted = aborting = true;
    try {
      if (infoStream != null) {
        message("now abort");
      }
      try {
        consumer.abort();
      } catch (Throwable t) {
      }

      pendingDeletes.clear();
      deleteSlice = deleteQueue.newSlice();
      // Reset all postings data
      doAfterFlush();

    } finally {
      aborting = false;
      if (infoStream != null) {
        message("done abort");
      }
    }
  }
  private final static boolean INFO_VERBOSE = false;
  final DocumentsWriter parent;
  final IndexWriter writer;
  final Directory directory;
  final DocState docState;
  final DocConsumer consumer;
  final AtomicLong bytesUsed;
  
  SegmentWriteState flushState;
  //Deletes for our still-in-RAM (to be flushed next) segment
  BufferedDeletes pendingDeletes;  
  String segment;     // Current segment we are working on
  boolean aborting = false;   // True if an abort is pending
  boolean hasAborted = false; // True if the last exception throws by #updateDocument was aborting

  private FieldInfos fieldInfos;
  private PrintStream infoStream;
  private int numDocsInRAM;
  private int flushedDocCount;
  DocumentsWriterDeleteQueue deleteQueue;
  DeleteSlice deleteSlice;
  private final NumberFormat nf = NumberFormat.getInstance();
  final Allocator byteBlockAllocator;

  
  public DocumentsWriterPerThread(Directory directory, DocumentsWriter parent,
      FieldInfos fieldInfos, IndexingChain indexingChain) {
    this.directory = directory;
    this.parent = parent;
    this.fieldInfos = fieldInfos;
    this.writer = parent.indexWriter;
    this.infoStream = parent.infoStream;
    this.docState = new DocState(this);
    this.docState.similarityProvider = parent.indexWriter.getConfig()
        .getSimilarityProvider();
    bytesUsed = new AtomicLong(0);
    byteBlockAllocator = new DirectTrackingAllocator(bytesUsed);
    consumer = indexingChain.getChain(this);
    pendingDeletes = new BufferedDeletes();
    initialize();
  }
  
  public DocumentsWriterPerThread(DocumentsWriterPerThread other, FieldInfos fieldInfos) {
    this(other.directory, other.parent, fieldInfos, other.parent.chain);
  }
  
  void initialize() {
    deleteQueue = parent.deleteQueue;
    assert numDocsInRAM == 0 : "num docs " + numDocsInRAM;
    pendingDeletes.clear();
    deleteSlice = null;
  }

  void setAborting() {
    aborting = true;
  }
  
  boolean checkAndResetHasAborted() {
    final boolean retval = hasAborted;
    hasAborted = false;
    return retval;
  }

  public void updateDocument(Document doc, Analyzer analyzer, Term delTerm) throws IOException {
    assert writer.testPoint("DocumentsWriterPerThread addDocument start");
    assert deleteQueue != null;
    docState.doc = doc;
    docState.analyzer = analyzer;
    docState.docID = numDocsInRAM;
    if (segment == null) {
      // this call is synchronized on IndexWriter.segmentInfos
      segment = writer.newSegmentName();
      assert numDocsInRAM == 0;
      if (INFO_VERBOSE) {
        message(Thread.currentThread().getName() + " init seg=" + segment + " delQueue=" + deleteQueue);  
      }
      
    }
    if (INFO_VERBOSE) {
      message(Thread.currentThread().getName() + " update delTerm=" + delTerm + " docID=" + docState.docID + " seg=" + segment);
    }
    boolean success = false;
    try {
      try {
        consumer.processDocument(fieldInfos);
      } finally {
        docState.clear();
      }
      success = true;
    } finally {
      if (!success) {
        if (!aborting) {
          // mark document as deleted
          deleteDocID(docState.docID);
          numDocsInRAM++;
          fieldInfos.revertUncommitted();
        } else {
          abort();
        }
      }
    }
    success = false;
    try {
      consumer.finishDocument();
      success = true;
    } finally {
      if (!success) {
        abort();
      }
    }
    finishDocument(delTerm);
  }
  
  public int updateDocuments(Iterable<Document> docs, Analyzer analyzer, Term delTerm) throws IOException {
    assert writer.testPoint("DocumentsWriterPerThread addDocuments start");
    assert deleteQueue != null;
    docState.analyzer = analyzer;
    if (segment == null) {
      // this call is synchronized on IndexWriter.segmentInfos
      segment = writer.newSegmentName();
      assert numDocsInRAM == 0;
      if (INFO_VERBOSE) {
        message(Thread.currentThread().getName() + " init seg=" + segment + " delQueue=" + deleteQueue);  
      }
    }
    if (INFO_VERBOSE) {
      message(Thread.currentThread().getName() + " update delTerm=" + delTerm + " docID=" + docState.docID + " seg=" + segment);
    }
    int docCount = 0;
    try {
      for(Document doc : docs) {
        docState.doc = doc;
        docState.docID = numDocsInRAM;
        docCount++;

        boolean success = false;
        try {
          consumer.processDocument(fieldInfos);
          success = true;
        } finally {
          if (!success) {
            // An exc is being thrown...

            if (!aborting) {
              // One of the documents hit a non-aborting
              // exception (eg something happened during
              // analysis).  We now go and mark any docs
              // from this batch that we had already indexed
              // as deleted:
              int docID = docState.docID;
              final int endDocID = docID - docCount;
              while (docID > endDocID) {
                deleteDocID(docID);
                docID--;
              }

              // Incr here because finishDocument will not
              // be called (because an exc is being thrown):
              numDocsInRAM++;
              fieldInfos.revertUncommitted();
            } else {
              abort();
            }
          }
        }
        success = false;
        try {
          consumer.finishDocument();
          success = true;
        } finally {
          if (!success) {
            abort();
          }
        }

        finishDocument(null);
      }

      // Apply delTerm only after all indexing has
      // succeeded, but apply it only to docs prior to when
      // this batch started:
      if (delTerm != null) {
        deleteQueue.add(delTerm, deleteSlice);
        assert deleteSlice.isTailItem(delTerm) : "expected the delete term as the tail item";
        deleteSlice.apply(pendingDeletes, numDocsInRAM-docCount);
      }

    } finally {
      docState.clear();
    }

    return docCount;
  }
  
  private void finishDocument(Term delTerm) throws IOException {
    /*
     * here we actually finish the document in two steps 1. push the delete into
     * the queue and update our slice. 2. increment the DWPT private document
     * id.
     * 
     * the updated slice we get from 1. holds all the deletes that have occurred
     * since we updated the slice the last time.
     */
    if (deleteSlice == null) {
      deleteSlice = deleteQueue.newSlice();
      if (delTerm != null) {
        deleteQueue.add(delTerm, deleteSlice);
        deleteSlice.reset();
      }
      
    } else {
      if (delTerm != null) {
        deleteQueue.add(delTerm, deleteSlice);
        assert deleteSlice.isTailItem(delTerm) : "expected the delete term as the tail item";
        deleteSlice.apply(pendingDeletes, numDocsInRAM);
      } else if (deleteQueue.updateSlice(deleteSlice)) {
        deleteSlice.apply(pendingDeletes, numDocsInRAM);
      }
    }
    ++numDocsInRAM;
  }

  // Buffer a specific docID for deletion.  Currently only
  // used when we hit a exception when adding a document
  void deleteDocID(int docIDUpto) {
    pendingDeletes.addDocID(docIDUpto);
    // NOTE: we do not trigger flush here.  This is
    // potentially a RAM leak, if you have an app that tries
    // to add docs but every single doc always hits a
    // non-aborting exception.  Allowing a flush here gets
    // very messy because we are only invoked when handling
    // exceptions so to do this properly, while handling an
    // exception we'd have to go off and flush new deletes
    // which is risky (likely would hit some other
    // confounding exception).
  }

  /**
   * Returns the number of delete terms in this {@link DocumentsWriterPerThread}
   */
  public int numDeleteTerms() {
    // public for FlushPolicy
    return pendingDeletes.numTermDeletes.get();
  }

  /**
   * Returns the number of RAM resident documents in this {@link DocumentsWriterPerThread}
   */
  public int getNumDocsInRAM() {
    // public for FlushPolicy
    return numDocsInRAM;
  }

  SegmentCodecs getCodec() {
    return flushState.segmentCodecs;
  }

  /** Reset after a flush */
  private void doAfterFlush() throws IOException {
    segment = null;
    consumer.doAfterFlush();
    fieldInfos = new FieldInfos(fieldInfos);
    parent.subtractFlushedNumDocs(numDocsInRAM);
    numDocsInRAM = 0;
  }
  
  /**
   * Prepares this DWPT for flushing. This method will freeze and return the
   * {@link DocumentsWriterDeleteQueue}s global buffer and apply all pending
   * deletes to this DWPT.
   */
  FrozenBufferedDeletes prepareFlush() {
    assert numDocsInRAM > 0;
    final FrozenBufferedDeletes globalDeletes = deleteQueue.freezeGlobalBuffer(deleteSlice);
    /* deleteSlice can possibly be null if we have hit non-aborting exceptions during indexing and never succeeded 
    adding a document. */
    if (deleteSlice != null) {
      // apply all deletes before we flush and release the delete slice
      deleteSlice.apply(pendingDeletes, numDocsInRAM);
      assert deleteSlice.isEmpty();
      deleteSlice = null;
    }
    return globalDeletes;
  }

  /** Flush all pending docs to a new segment */
  FlushedSegment flush() throws IOException {
    assert numDocsInRAM > 0;
    assert deleteSlice == null : "all deletes must be applied in prepareFlush";
    flushState = new SegmentWriteState(infoStream, directory, segment, fieldInfos,
        numDocsInRAM, writer.getConfig().getTermIndexInterval(),
        fieldInfos.buildSegmentCodecs(true), pendingDeletes, new IOContext(new FlushInfo(numDocsInRAM, bytesUsed())));
    final double startMBUsed = parent.flushControl.netBytes() / 1024. / 1024.;
    // Apply delete-by-docID now (delete-byDocID only
    // happens when an exception is hit processing that
    // doc, eg if analyzer has some problem w/ the text):
    if (pendingDeletes.docIDs.size() > 0) {
      flushState.liveDocs = new BitVector(numDocsInRAM);
      flushState.liveDocs.invertAll();
      for(int delDocID : pendingDeletes.docIDs) {
        flushState.liveDocs.clear(delDocID);
      }
      pendingDeletes.bytesUsed.addAndGet(-pendingDeletes.docIDs.size() * BufferedDeletes.BYTES_PER_DEL_DOCID);
      pendingDeletes.docIDs.clear();
    }

    if (infoStream != null) {
      message("flush postings as segment " + flushState.segmentName + " numDocs=" + numDocsInRAM);
    }

    if (aborting) {
      if (infoStream != null) {
        message("flush: skip because aborting is set");
      }
      return null;
    }

    boolean success = false;

    try {
      consumer.flush(flushState);
      pendingDeletes.terms.clear();
      final SegmentInfo newSegment = new SegmentInfo(segment, flushState.numDocs, directory, false, flushState.segmentCodecs, fieldInfos.asReadOnly());
      if (infoStream != null) {
        message("new segment has " + (flushState.liveDocs == null ? 0 : (flushState.numDocs - flushState.liveDocs.count())) + " deleted docs");
        message("new segment has " + (newSegment.getHasVectors() ? "vectors" : "no vectors"));
        message("flushedFiles=" + newSegment.files());
        message("flushed codecs=" + newSegment.getSegmentCodecs());
      }
      flushedDocCount += flushState.numDocs;

      final BufferedDeletes segmentDeletes;
      if (pendingDeletes.queries.isEmpty()) {
        pendingDeletes.clear();
        segmentDeletes = null;
      } else {
        segmentDeletes = pendingDeletes;
        pendingDeletes = new BufferedDeletes();
      }

      if (infoStream != null) {
        final double newSegmentSizeNoStore = newSegment.sizeInBytes(false)/1024./1024.;
        final double newSegmentSize = newSegment.sizeInBytes(true)/1024./1024.;
        message("flushed: segment=" + newSegment + 
                " ramUsed=" + nf.format(startMBUsed) + " MB" +
                " newFlushedSize=" + nf.format(newSegmentSize) + " MB" +
                " (" + nf.format(newSegmentSizeNoStore) + " MB w/o doc stores)" +
                " docs/MB=" + nf.format(flushedDocCount / newSegmentSize) +
                " new/old=" + nf.format(100.0 * newSegmentSizeNoStore / startMBUsed) + "%");
      }
      doAfterFlush();
      success = true;

      return new FlushedSegment(newSegment, segmentDeletes, flushState.liveDocs);
    } finally {
      if (!success) {
        if (segment != null) {
          synchronized(parent.indexWriter) {
            parent.indexWriter.deleter.refresh(segment);
          }
        }
        abort();
      }
    }
  }

  /** Get current segment name we are writing. */
  String getSegment() {
    return segment;
  }

  long bytesUsed() {
    return bytesUsed.get() + pendingDeletes.bytesUsed.get();
  }

  void message(String message) {
    writer.message("DWPT: " + message);
  }

  /* Initial chunks size of the shared byte[] blocks used to
     store postings data */
  final static int BYTE_BLOCK_NOT_MASK = ~BYTE_BLOCK_MASK;

  /* if you increase this, you must fix field cache impl for
   * getTerms/getTermsIndex requires <= 32768 */
  final static int MAX_TERM_LENGTH_UTF8 = BYTE_BLOCK_SIZE-2;

  /* Initial chunks size of the shared int[] blocks used to
     store postings data */
  final static int INT_BLOCK_SHIFT = 13;
  final static int INT_BLOCK_SIZE = 1 << INT_BLOCK_SHIFT;
  final static int INT_BLOCK_MASK = INT_BLOCK_SIZE - 1;

  /* Allocate another int[] from the shared pool */
  int[] getIntBlock() {
    int[] b = new int[INT_BLOCK_SIZE];
    bytesUsed.addAndGet(INT_BLOCK_SIZE*RamUsageEstimator.NUM_BYTES_INT);
    return b;
  }
  
  void recycleIntBlocks(int[][] blocks, int offset, int length) {
    bytesUsed.addAndGet(-(length *(INT_BLOCK_SIZE*RamUsageEstimator.NUM_BYTES_INT)));
  }

  PerDocWriteState newPerDocWriteState(int codecId) {
    assert segment != null;
    return new PerDocWriteState(infoStream, directory, segment, fieldInfos, bytesUsed, codecId, IOContext.DEFAULT);
  }
  
  void setInfoStream(PrintStream infoStream) {
    this.infoStream = infoStream;
    docState.infoStream = infoStream;
  }

  @Override
  public String toString() {
    return "DocumentsWriterPerThread [pendingDeletes=" + pendingDeletes
        + ", segment=" + segment + ", aborting=" + aborting + ", numDocsInRAM="
        + numDocsInRAM + ", deleteQueue=" + deleteQueue + "]";
  }
}
