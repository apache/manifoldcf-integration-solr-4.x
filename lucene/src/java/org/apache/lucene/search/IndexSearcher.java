package org.apache.lucene.search;

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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexReader.ReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Weight.ScorerContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;    // javadoc
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util.ThreadInterruptedException;

/** Implements search over a single IndexReader.
 *
 * <p>Applications usually need only call the inherited
 * {@link #search(Query,int)}
 * or {@link #search(Query,Filter,int)} methods. For performance reasons it is 
 * recommended to open only one IndexSearcher and use it for all of your searches.
 * 
 * <a name="thread-safety"></a><p><b>NOTE</b>: <code>{@link
 * IndexSearcher}</code> instances are completely
 * thread safe, meaning multiple threads can call any of its
 * methods, concurrently.  If your application requires
 * external synchronization, you should <b>not</b>
 * synchronize on the <code>IndexSearcher</code> instance;
 * use your own (non-Lucene) objects instead.</p>
 */
public class IndexSearcher {
  final IndexReader reader; // package private for testing!
  private boolean closeReader;
  
  // NOTE: these members might change in incompatible ways
  // in the next release
  protected final ReaderContext readerContext;
  protected final AtomicReaderContext[] leafContexts;
  // used with executor - each slice holds a set of leafs executed within one thread
  protected final LeafSlice[] leafSlices;

  // These are only used for multi-threaded search
  private final ExecutorService executor;

  // the default SimilarityProvider
  private static final SimilarityProvider defaultProvider = new DefaultSimilarity();
  
  /**
   * Expert: returns a default SimilarityProvider instance.
   * In general, this method is only called to initialize searchers and writers.
   * User code and query implementations should respect
   * {@link IndexSearcher#getSimilarityProvider()}.
   * @lucene.internal
   */
  public static SimilarityProvider getDefaultSimilarityProvider() {
    return defaultProvider;
  }
  
  /** The SimilarityProvider implementation used by this searcher. */
  private SimilarityProvider similarityProvider = defaultProvider;

  /** Creates a searcher searching the index in the named
   *  directory, with readOnly=true
   * @param path directory where IndexReader will be opened
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public IndexSearcher(Directory path) throws CorruptIndexException, IOException {
    this(IndexReader.open(path, true), true, null);
  }

  /** Creates a searcher searching the index in the named
   *  directory.  You should pass readOnly=true, since it
   *  gives much better concurrent performance, unless you
   *  intend to do write operations (delete documents or
   *  change norms) with the underlying IndexReader.
   * @param path directory where IndexReader will be opened
   * @param readOnly if true, the underlying IndexReader
   * will be opened readOnly
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public IndexSearcher(Directory path, boolean readOnly) throws CorruptIndexException, IOException {
    this(IndexReader.open(path, readOnly), true, null);
  }

  /** Creates a searcher searching the provided index. */
  public IndexSearcher(IndexReader r) {
    this(r, false, null);
  }

  /** Runs searches for each segment separately, using the
   *  provided ExecutorService.  IndexSearcher will not
   *  shutdown/awaitTermination this ExecutorService on
   *  close; you must do so, eventually, on your own.  NOTE:
   *  if you are using {@link NIOFSDirectory}, do not use
   *  the shutdownNow method of ExecutorService as this uses
   *  Thread.interrupt under-the-hood which can silently
   *  close file descriptors (see <a
   *  href="https://issues.apache.org/jira/browse/LUCENE-2239">LUCENE-2239</a>).
   * 
   * @lucene.experimental */
  public IndexSearcher(IndexReader r, ExecutorService executor) {
    this(r, false, executor);
  }

  /**
   * Creates a searcher searching the provided top-level {@link ReaderContext}.
   * <p>
   * Given a non-<code>null</code> {@link ExecutorService} this method runs
   * searches for each segment separately, using the provided ExecutorService.
   * IndexSearcher will not shutdown/awaitTermination this ExecutorService on
   * close; you must do so, eventually, on your own. NOTE: if you are using
   * {@link NIOFSDirectory}, do not use the shutdownNow method of
   * ExecutorService as this uses Thread.interrupt under-the-hood which can
   * silently close file descriptors (see <a
   * href="https://issues.apache.org/jira/browse/LUCENE-2239">LUCENE-2239</a>).
   * 
   * @see ReaderContext
   * @see IndexReader#getTopReaderContext()
   * @lucene.experimental
   */
  public IndexSearcher(ReaderContext context, ExecutorService executor) {
    this(context, false, executor);
  }

  /**
   * Creates a searcher searching the provided top-level {@link ReaderContext}.
   *
   * @see ReaderContext
   * @see IndexReader#getTopReaderContext()
   * @lucene.experimental
   */
  public IndexSearcher(ReaderContext context) {
    this(context, (ExecutorService) null);
  }
  
  // convenience ctor for other IR based ctors
  private IndexSearcher(IndexReader reader, boolean closeReader, ExecutorService executor) {
    this(reader.getTopReaderContext(), closeReader, executor);
  }

  private IndexSearcher(ReaderContext context, boolean closeReader, ExecutorService executor) {
    assert context.isTopLevel: "IndexSearcher's ReaderContext must be topLevel for reader" + context.reader;
    reader = context.reader;
    this.executor = executor;
    this.closeReader = closeReader;
    this.readerContext = context;
    leafContexts = ReaderUtil.leaves(context);
    this.leafSlices = executor == null ? null : slices(leafContexts);
  }
  
  /**
   * Expert: Creates an array of leaf slices each holding a subset of the given leaves.
   * Each {@link LeafSlice} is executed in a single thread. By default there
   * will be one {@link LeafSlice} per leaf ({@link AtomicReaderContext}).
   */
  protected LeafSlice[] slices(AtomicReaderContext...leaves) {
    LeafSlice[] slices = new LeafSlice[leaves.length];
    for (int i = 0; i < slices.length; i++) {
      slices[i] = new LeafSlice(leaves[i]);
    }
    return slices;
  }

  
  /** Return the {@link IndexReader} this searches. */
  public IndexReader getIndexReader() {
    return reader;
  }

  /** Expert: Returns one greater than the largest possible document number.
   * 
   * @see org.apache.lucene.index.IndexReader#maxDoc()
   */
  public int maxDoc() {
    return reader.maxDoc();
  }

  /** Returns total docFreq for this term. */
  public int docFreq(final Term term) throws IOException {
    if (executor == null) {
      return reader.docFreq(term);
    } else {
      final ExecutionHelper<Integer> runner = new ExecutionHelper<Integer>(executor);
      for(int i = 0; i < leafContexts.length; i++) {
        final IndexReader leaf = leafContexts[i].reader;
        runner.submit(new Callable<Integer>() {
            public Integer call() throws IOException {
              return Integer.valueOf(leaf.docFreq(term));
            }
          });
      }
      int docFreq = 0;
      for (Integer num : runner) {
        docFreq += num.intValue();
      }
      return docFreq;
    }
  }

  /* Sugar for .getIndexReader().document(docID) */
  public Document doc(int docID) throws CorruptIndexException, IOException {
    return reader.document(docID);
  }
  
  /* Sugar for .getIndexReader().document(docID, fieldSelector) */
  public Document doc(int docID, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
    return reader.document(docID, fieldSelector);
  }
  
  /** Expert: Set the SimilarityProvider implementation used by this Searcher.
   *
   */
  public void setSimilarityProvider(SimilarityProvider similarityProvider) {
    this.similarityProvider = similarityProvider;
  }

  public SimilarityProvider getSimilarityProvider() {
    return similarityProvider;
  }

  /**
   * Note that the underlying IndexReader is not closed, if
   * IndexSearcher was constructed with IndexSearcher(IndexReader r).
   * If the IndexReader was supplied implicitly by specifying a directory, then
   * the IndexReader is closed.
   */
  public void close() throws IOException {
    if (closeReader) {
      reader.close();
    }
  }

  /** Finds the top <code>n</code>
   * hits for <code>query</code>.
   *
   * @throws BooleanQuery.TooManyClauses
   */
  public TopDocs search(Query query, int n)
    throws IOException {
    return search(query, null, n);
  }


  /** Finds the top <code>n</code>
   * hits for <code>query</code>, applying <code>filter</code> if non-null.
   *
   * @throws BooleanQuery.TooManyClauses
   */
  public TopDocs search(Query query, Filter filter, int n)
    throws IOException {
    return search(createWeight(query), filter, n);
  }

  /** Lower-level search API.
   *
   * <p>{@link Collector#collect(int)} is called for every matching
   * document.
   * <br>Collector-based access to remote indexes is discouraged.
   *
   * <p>Applications should only use this if they need <i>all</i> of the
   * matching documents.  The high-level search API ({@link
   * IndexSearcher#search(Query, Filter, int)}) is usually more efficient, as it skips
   * non-high-scoring hits.
   *
   * @param query to match documents
   * @param filter if non-null, used to permit documents to be collected.
   * @param results to receive hits
   * @throws BooleanQuery.TooManyClauses
   */
  public void search(Query query, Filter filter, Collector results)
    throws IOException {
    search(leafContexts, createWeight(query), filter, results);
  }

  /** Lower-level search API.
  *
  * <p>{@link Collector#collect(int)} is called for every matching document.
  *
  * <p>Applications should only use this if they need <i>all</i> of the
  * matching documents.  The high-level search API ({@link
  * IndexSearcher#search(Query, int)}) is usually more efficient, as it skips
  * non-high-scoring hits.
  * <p>Note: The <code>score</code> passed to this method is a raw score.
  * In other words, the score will not necessarily be a float whose value is
  * between 0 and 1.
  * @throws BooleanQuery.TooManyClauses
  */
  public void search(Query query, Collector results)
    throws IOException {
    search(leafContexts, createWeight(query), null, results);
  }
  
  /** Search implementation with arbitrary sorting.  Finds
   * the top <code>n</code> hits for <code>query</code>, applying
   * <code>filter</code> if non-null, and sorting the hits by the criteria in
   * <code>sort</code>.
   * 
   * <p>NOTE: this does not compute scores by default; use
   * {@link IndexSearcher#setDefaultFieldSortScoring} to
   * enable scoring.
   *
   * @throws BooleanQuery.TooManyClauses
   */
  public TopFieldDocs search(Query query, Filter filter, int n,
                             Sort sort) throws IOException {
    return search(createWeight(query), filter, n, sort);
  }

  /**
   * Search implementation with arbitrary sorting and no filter.
   * @param query The query to search for
   * @param n Return only the top n results
   * @param sort The {@link org.apache.lucene.search.Sort} object
   * @return The top docs, sorted according to the supplied {@link org.apache.lucene.search.Sort} instance
   * @throws IOException
   */
  public TopFieldDocs search(Query query, int n,
                             Sort sort) throws IOException {
    return search(createWeight(query), null, n, sort);
  }

  /** Expert: Low-level search implementation.  Finds the top <code>n</code>
   * hits for <code>query</code>, applying <code>filter</code> if non-null.
   *
   * <p>Applications should usually call {@link IndexSearcher#search(Query,int)} or
   * {@link IndexSearcher#search(Query,Filter,int)} instead.
   * @throws BooleanQuery.TooManyClauses
   */
  protected TopDocs search(Weight weight, Filter filter, int nDocs) throws IOException {
    if (executor == null) {
      return search(leafContexts, weight, filter, nDocs);
    } else {
      final HitQueue hq = new HitQueue(nDocs, false);
      final Lock lock = new ReentrantLock();
      final ExecutionHelper<TopDocs> runner = new ExecutionHelper<TopDocs>(executor);
    
      for (int i = 0; i < leafSlices.length; i++) { // search each sub
        runner.submit(
                      new SearcherCallableNoSort(lock, this, leafSlices[i], weight, filter, nDocs, hq));
      }

      int totalHits = 0;
      float maxScore = Float.NEGATIVE_INFINITY;
      for (final TopDocs topDocs : runner) {
        if(topDocs.totalHits != 0) {
          totalHits += topDocs.totalHits;
          maxScore = Math.max(maxScore, topDocs.getMaxScore());
        }
      }

      final ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
      for (int i = hq.size() - 1; i >= 0; i--) // put docs in array
        scoreDocs[i] = hq.pop();

      return new TopDocs(totalHits, scoreDocs, maxScore);
    }
  }

  /** Expert: Low-level search implementation.  Finds the top <code>n</code>
   * hits for <code>query</code>, using the given leaf readers applying <code>filter</code> if non-null.
   *
   * <p>Applications should usually call {@link IndexSearcher#search(Query,int)} or
   * {@link IndexSearcher#search(Query,Filter,int)} instead.
   * @throws BooleanQuery.TooManyClauses
   */
  protected TopDocs search(AtomicReaderContext[] leaves, Weight weight, Filter filter, int nDocs) throws IOException {
    // single thread
    int limit = reader.maxDoc();
    if (limit == 0) {
      limit = 1;
    }
    nDocs = Math.min(nDocs, limit);
    TopScoreDocCollector collector = TopScoreDocCollector.create(nDocs, !weight.scoresDocsOutOfOrder());
    search(leaves, weight, filter, collector);
    return collector.topDocs();
  }

  /** Expert: Low-level search implementation with arbitrary sorting.  Finds
   * the top <code>n</code> hits for <code>query</code>, applying
   * <code>filter</code> if non-null, and sorting the hits by the criteria in
   * <code>sort</code>.
   *
   * <p>Applications should usually call {@link
   * IndexSearcher#search(Query,Filter,int,Sort)} instead.
   * 
   * @throws BooleanQuery.TooManyClauses
   */
  protected TopFieldDocs search(Weight weight, Filter filter,
      final int nDocs, Sort sort) throws IOException {
    return search(weight, filter, nDocs, sort, true);
  }

  /**
   * Just like {@link #search(Weight, Filter, int, Sort)}, but you choose
   * whether or not the fields in the returned {@link FieldDoc} instances should
   * be set by specifying fillFields.
   *
   * <p>NOTE: this does not compute scores by default.  If you
   * need scores, create a {@link TopFieldCollector}
   * instance by calling {@link TopFieldCollector#create} and
   * then pass that to {@link #search(IndexReader.AtomicReaderContext[], Weight, Filter,
   * Collector)}.</p>
   */
  protected TopFieldDocs search(Weight weight, Filter filter, int nDocs,
                             Sort sort, boolean fillFields)
      throws IOException {

    if (sort == null) throw new NullPointerException();
    
    if (executor == null) {
      // use all leaves here!
      return search (leafContexts, weight, filter, nDocs, sort, fillFields);
    } else {
      final TopFieldCollector topCollector = TopFieldCollector.create(sort, nDocs,
                                                                      fillFields,
                                                                      fieldSortDoTrackScores,
                                                                      fieldSortDoMaxScore,
                                                                      false);

      final Lock lock = new ReentrantLock();
      final ExecutionHelper<TopFieldDocs> runner = new ExecutionHelper<TopFieldDocs>(executor);
      for (int i = 0; i < leafSlices.length; i++) { // search each leaf slice
        runner.submit(
                      new SearcherCallableWithSort(lock, this, leafSlices[i], weight, filter, nDocs, topCollector, sort));
      }
      int totalHits = 0;
      float maxScore = Float.NEGATIVE_INFINITY;
      for (final TopFieldDocs topFieldDocs : runner) {
        if (topFieldDocs.totalHits != 0) {
          totalHits += topFieldDocs.totalHits;
          maxScore = Math.max(maxScore, topFieldDocs.getMaxScore());
        }
      }

      final TopFieldDocs topDocs = (TopFieldDocs) topCollector.topDocs();

      return new TopFieldDocs(totalHits, topDocs.scoreDocs, topDocs.fields, topDocs.getMaxScore());
    }
  }
  
  
  /**
   * Just like {@link #search(Weight, Filter, int, Sort)}, but you choose
   * whether or not the fields in the returned {@link FieldDoc} instances should
   * be set by specifying fillFields.
   *
   * <p>NOTE: this does not compute scores by default.  If you
   * need scores, create a {@link TopFieldCollector}
   * instance by calling {@link TopFieldCollector#create} and
   * then pass that to {@link #search(IndexReader.AtomicReaderContext[], Weight, Filter,
   * Collector)}.</p>
   */
  protected TopFieldDocs search(AtomicReaderContext[] leaves, Weight weight, Filter filter, int nDocs,
      Sort sort, boolean fillFields) throws IOException {
    // single thread
    int limit = reader.maxDoc();
    if (limit == 0) {
      limit = 1;
    }
    nDocs = Math.min(nDocs, limit);

    TopFieldCollector collector = TopFieldCollector.create(sort, nDocs,
                                                           fillFields, fieldSortDoTrackScores, fieldSortDoMaxScore, !weight.scoresDocsOutOfOrder());
    search(leaves, weight, filter, collector);
    return (TopFieldDocs) collector.topDocs();
  }

  /**
   * Lower-level search API.
   * 
   * <p>
   * {@link Collector#collect(int)} is called for every document. <br>
   * Collector-based access to remote indexes is discouraged.
   * 
   * <p>
   * Applications should only use this if they need <i>all</i> of the matching
   * documents. The high-level search API ({@link IndexSearcher#search(Query,int)}) is
   * usually more efficient, as it skips non-high-scoring hits.
   * 
   * <p>
   * NOTE: this method executes the searches on all given leaves exclusively.
   * To search across all the searchers leaves use {@link #leafContexts}.
   * 
   * @param leaves 
   *          the searchers leaves to execute the searches on
   * @param weight
   *          to match documents
   * @param filter
   *          if non-null, used to permit documents to be collected.
   * @param collector
   *          to receive hits
   * @throws BooleanQuery.TooManyClauses
   */
  protected void search(AtomicReaderContext[] leaves, Weight weight, Filter filter, Collector collector)
      throws IOException {

    // TODO: should we make this
    // threaded...?  the Collector could be sync'd?
    ScorerContext scorerContext =  ScorerContext.def().scoreDocsInOrder(true).topScorer(true);
    // always use single thread:
    if (filter == null) {
      for (int i = 0; i < leaves.length; i++) { // search each subreader
        collector.setNextReader(leaves[i]);
        scorerContext = scorerContext.scoreDocsInOrder(!collector.acceptsDocsOutOfOrder());
        Scorer scorer = weight.scorer(leaves[i], scorerContext);
        if (scorer != null) {
          scorer.score(collector);
        }
      }
    } else {
      for (int i = 0; i < leaves.length; i++) { // search each subreader
        collector.setNextReader(leaves[i]);
        searchWithFilter(leaves[i], weight, filter, collector);
      }
    }
  }

  private void searchWithFilter(AtomicReaderContext context, Weight weight,
      final Filter filter, final Collector collector) throws IOException {

    assert filter != null;
    
    Scorer scorer = weight.scorer(context, ScorerContext.def());
    if (scorer == null) {
      return;
    }

    int docID = scorer.docID();
    assert docID == -1 || docID == DocIdSetIterator.NO_MORE_DOCS;

    // CHECKME: use ConjunctionScorer here?
    DocIdSet filterDocIdSet = filter.getDocIdSet(context);
    if (filterDocIdSet == null) {
      // this means the filter does not accept any documents.
      return;
    }
    
    DocIdSetIterator filterIter = filterDocIdSet.iterator();
    if (filterIter == null) {
      // this means the filter does not accept any documents.
      return;
    }
    int filterDoc = filterIter.nextDoc();
    int scorerDoc = scorer.advance(filterDoc);
    
    collector.setScorer(scorer);
    while (true) {
      if (scorerDoc == filterDoc) {
        // Check if scorer has exhausted, only before collecting.
        if (scorerDoc == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        collector.collect(scorerDoc);
        filterDoc = filterIter.nextDoc();
        scorerDoc = scorer.advance(filterDoc);
      } else if (scorerDoc > filterDoc) {
        filterDoc = filterIter.advance(scorerDoc);
      } else {
        scorerDoc = scorer.advance(filterDoc);
      }
    }
  }

  /** Expert: called to re-write queries into primitive queries.
   * @throws BooleanQuery.TooManyClauses
   */
  public Query rewrite(Query original) throws IOException {
    Query query = original;
    for (Query rewrittenQuery = query.rewrite(reader); rewrittenQuery != query;
         rewrittenQuery = query.rewrite(reader)) {
      query = rewrittenQuery;
    }
    return query;
  }

  /** Returns an Explanation that describes how <code>doc</code> scored against
   * <code>query</code>.
   *
   * <p>This is intended to be used in developing Similarity implementations,
   * and, for good performance, should not be displayed with every hit.
   * Computing an explanation is as expensive as executing the query over the
   * entire index.
   */
  public Explanation explain(Query query, int doc) throws IOException {
    return explain(createWeight(query), doc);
  }

  /** Expert: low-level implementation method
   * Returns an Explanation that describes how <code>doc</code> scored against
   * <code>weight</code>.
   *
   * <p>This is intended to be used in developing Similarity implementations,
   * and, for good performance, should not be displayed with every hit.
   * Computing an explanation is as expensive as executing the query over the
   * entire index.
   * <p>Applications should call {@link IndexSearcher#explain(Query, int)}.
   * @throws BooleanQuery.TooManyClauses
   */
  protected Explanation explain(Weight weight, int doc) throws IOException {
    int n = ReaderUtil.subIndex(doc, leafContexts);
    int deBasedDoc = doc - leafContexts[n].docBase;
    
    return weight.explain(leafContexts[n], deBasedDoc);
  }

  private boolean fieldSortDoTrackScores;
  private boolean fieldSortDoMaxScore;

  /** By default, no scores are computed when sorting by
   *  field (using {@link #search(Query,Filter,int,Sort)}).
   *  You can change that, per IndexSearcher instance, by
   *  calling this method.  Note that this will incur a CPU
   *  cost.
   * 
   *  @param doTrackScores If true, then scores are
   *  returned for every matching document in {@link
   *  TopFieldDocs}.
   *
   *  @param doMaxScore If true, then the max score for all
   *  matching docs is computed. */
  public void setDefaultFieldSortScoring(boolean doTrackScores, boolean doMaxScore) {
    fieldSortDoTrackScores = doTrackScores;
    fieldSortDoMaxScore = doMaxScore;
  }

  /**
   * creates a weight for <code>query</code>
   * @return new weight
   */
  protected Weight createWeight(Query query) throws IOException {
    return query.weight(this);
  }

  /**
   * Returns this searchers the top-level {@link ReaderContext}.
   * @see IndexReader#getTopReaderContext()
   */
  /* sugar for #getReader().getTopReaderContext() */
  public ReaderContext getTopReaderContext() {
    return readerContext;
  }

  /**
   * A thread subclass for searching a single searchable 
   */
  private static final class SearcherCallableNoSort implements Callable<TopDocs> {

    private final Lock lock;
    private final IndexSearcher searcher;
    private final Weight weight;
    private final Filter filter;
    private final int nDocs;
    private final HitQueue hq;
    private final LeafSlice slice;

    public SearcherCallableNoSort(Lock lock, IndexSearcher searcher, LeafSlice slice,  Weight weight,
        Filter filter, int nDocs, HitQueue hq) {
      this.lock = lock;
      this.searcher = searcher;
      this.weight = weight;
      this.filter = filter;
      this.nDocs = nDocs;
      this.hq = hq;
      this.slice = slice;
    }

    public TopDocs call() throws IOException {
      final TopDocs docs = searcher.search (slice.leaves, weight, filter, nDocs);
      final ScoreDoc[] scoreDocs = docs.scoreDocs;
      for (int j = 0; j < scoreDocs.length; j++) { // merge scoreDocs into hq
        final ScoreDoc scoreDoc = scoreDocs[j];
        //it would be so nice if we had a thread-safe insert 
        lock.lock();
        try {
          if (scoreDoc == hq.insertWithOverflow(scoreDoc))
            break;
        } finally {
          lock.unlock();
        }
      }
      return docs;
    }
  }


  /**
   * A thread subclass for searching a single searchable 
   */
  private static final class SearcherCallableWithSort implements Callable<TopFieldDocs> {

    private final Lock lock;
    private final IndexSearcher searcher;
    private final Weight weight;
    private final Filter filter;
    private final int nDocs;
    private final TopFieldCollector hq;
    private final Sort sort;
    private final LeafSlice slice;

    public SearcherCallableWithSort(Lock lock, IndexSearcher searcher, LeafSlice slice, Weight weight,
        Filter filter, int nDocs, TopFieldCollector hq, Sort sort) {
      this.lock = lock;
      this.searcher = searcher;
      this.weight = weight;
      this.filter = filter;
      this.nDocs = nDocs;
      this.hq = hq;
      this.sort = sort;
      this.slice = slice;
    }

    private final class FakeScorer extends Scorer {
      float score;
      int doc;

      public FakeScorer() {
        super(null);
      }
    
      @Override
      public int advance(int target) {
        throw new UnsupportedOperationException();
      }

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public float freq() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int nextDoc() {
        throw new UnsupportedOperationException();
      }
    
      @Override
      public float score() {
        return score;
      }
    }

    private final FakeScorer fakeScorer = new FakeScorer();

    public TopFieldDocs call() throws IOException {
      assert slice.leaves.length == 1;
      final TopFieldDocs docs = searcher.search (slice.leaves, weight, filter, nDocs, sort, true);
      lock.lock();
      try {
        final int base = slice.leaves[0].docBase;
        hq.setNextReader(slice.leaves[0]);
        hq.setScorer(fakeScorer);
        for(ScoreDoc scoreDoc : docs.scoreDocs) {
          fakeScorer.doc = scoreDoc.doc - base;
          fakeScorer.score = scoreDoc.score;
          hq.collect(scoreDoc.doc-base);
        }
      } finally {
        lock.unlock();
      }
      return docs;
    }
  }

  /**
   * A helper class that wraps a {@link CompletionService} and provides an
   * iterable interface to the completed {@link Callable} instances.
   * 
   * @param <T>
   *          the type of the {@link Callable} return value
   */
  private static final class ExecutionHelper<T> implements Iterator<T>, Iterable<T> {
    private final CompletionService<T> service;
    private int numTasks;

    ExecutionHelper(final Executor executor) {
      this.service = new ExecutorCompletionService<T>(executor);
    }

    public boolean hasNext() {
      return numTasks > 0;
    }

    public void submit(Callable<T> task) {
      this.service.submit(task);
      ++numTasks;
    }

    public T next() {
      if(!this.hasNext())
        throw new NoSuchElementException();
      try {
        return service.take().get();
      } catch (InterruptedException e) {
        throw new ThreadInterruptedException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      } finally {
        --numTasks;
      }
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    public Iterator<T> iterator() {
      // use the shortcut here - this is only used in a private context
      return this;
    }
  }

  /**
   * A class holding a subset of the {@link IndexSearcher}s leaf contexts to be
   * executed within a single thread.
   * 
   * @lucene.experimental
   */
  public static class LeafSlice {
    final AtomicReaderContext[] leaves;
    
    public LeafSlice(AtomicReaderContext...leaves) {
      this.leaves = leaves;
    }
  }
}
