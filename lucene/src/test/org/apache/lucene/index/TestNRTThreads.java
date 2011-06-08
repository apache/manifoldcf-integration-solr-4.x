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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util._TestUtil;
import org.junit.Test;

// TODO
//   - mix in optimize, addIndexes
//   - randomoly mix in non-congruent docs

public class TestNRTThreads extends LuceneTestCase {

  private static class SubDocs {
    public final String packID;
    public final List<String> subIDs;
    public boolean deleted;

    public SubDocs(String packID, List<String> subIDs) {
      this.packID = packID;
      this.subIDs = subIDs;
    }
  }

  // TODO: is there a pre-existing way to do this!!!
  private Document cloneDoc(Document doc1) {
    final Document doc2 = new Document();
    for(Fieldable f : doc1.getFields()) {
      Field field1 = (Field) f;
      
      Field field2 = new Field(field1.name(),
                               field1.stringValue(),
                               field1.isStored() ? Field.Store.YES : Field.Store.NO,
                               field1.isIndexed() ? (field1.isTokenized() ? Field.Index.ANALYZED : Field.Index.NOT_ANALYZED) : Field.Index.NO);
      if (field1.getOmitNorms()) {
        field2.setOmitNorms(true);
      }
      if (field1.getOmitTermFreqAndPositions()) {
        field2.setOmitTermFreqAndPositions(true);
      }
      doc2.add(field2);
    }

    return doc2;
  }

  @Test
  public void testNRTThreads() throws Exception {

    final long t0 = System.currentTimeMillis();

    if (CodecProvider.getDefault().getDefaultFieldCodec().equals("SimpleText")) {
      // no
      CodecProvider.getDefault().setDefaultFieldCodec("Standard");
    }

    final LineFileDocs docs = new LineFileDocs(random);
    final File tempDir = _TestUtil.getTempDir("nrtopenfiles");
    final MockDirectoryWrapper dir = newFSDirectory(tempDir);
    final IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));

    if (LuceneTestCase.TEST_NIGHTLY) {
      // newIWConfig makes smallish max seg size, which
      // results in tons and tons of segments for this test
      // when run nightly:
      MergePolicy mp = conf.getMergePolicy();
      if (mp instanceof TieredMergePolicy) {
        ((TieredMergePolicy) mp).setMaxMergedSegmentMB(5000.);
      } else if (mp instanceof LogByteSizeMergePolicy) {
        ((LogByteSizeMergePolicy) mp).setMaxMergeMB(1000.);
      } else if (mp instanceof LogMergePolicy) {
        ((LogMergePolicy) mp).setMaxMergeDocs(100000);
      }
    }

    conf.setMergedSegmentWarmer(new IndexWriter.IndexReaderWarmer() {
      @Override
      public void warm(IndexReader reader) throws IOException {
        if (VERBOSE) {
          System.out.println("TEST: now warm merged reader=" + reader);
        }
        final int maxDoc = reader.maxDoc();
        final Bits delDocs = reader.getDeletedDocs();
        int sum = 0;
        final int inc = Math.max(1, maxDoc/50);
        for(int docID=0;docID<maxDoc;docID += inc) {
          if (delDocs == null || !delDocs.get(docID)) {
            final Document doc = reader.document(docID);
            sum += doc.getFields().size();
          }
        }

        IndexSearcher searcher = newSearcher(reader);
        sum += searcher.search(new TermQuery(new Term("body", "united")), 10).totalHits;
        searcher.close();

        if (VERBOSE) {
          System.out.println("TEST: warm visited " + sum + " fields");
        }
      }
      });
    
    final IndexWriter writer = new IndexWriter(dir, conf);
    if (VERBOSE) {
      writer.setInfoStream(System.out);
    }
    _TestUtil.reduceOpenFiles(writer);

    final int NUM_INDEX_THREADS = 2;
    final int NUM_SEARCH_THREADS = 3;

    final int RUN_TIME_SEC = LuceneTestCase.TEST_NIGHTLY ? 300 : 5;

    final AtomicBoolean failed = new AtomicBoolean();
    final AtomicInteger addCount = new AtomicInteger();
    final AtomicInteger delCount = new AtomicInteger();
    final AtomicInteger packCount = new AtomicInteger();

    final Set<String> delIDs = Collections.synchronizedSet(new HashSet<String>());
    final List<SubDocs> allSubDocs = Collections.synchronizedList(new ArrayList<SubDocs>());

    final long stopTime = System.currentTimeMillis() + RUN_TIME_SEC*1000;
    Thread[] threads = new Thread[NUM_INDEX_THREADS];
    for(int thread=0;thread<NUM_INDEX_THREADS;thread++) {
      threads[thread] = new Thread() {
          @Override
          public void run() {
            // TODO: would be better if this were cross thread, so that we make sure one thread deleting anothers added docs works:
            final List<String> toDeleteIDs = new ArrayList<String>();
            final List<SubDocs> toDeleteSubDocs = new ArrayList<SubDocs>();
            while(System.currentTimeMillis() < stopTime && !failed.get()) {
              try {
                Document doc = docs.nextDoc();
                if (doc == null) {
                  break;
                }
                final String addedField;
                if (random.nextBoolean()) {
                  addedField = "extra" + random.nextInt(10);
                  doc.add(new Field(addedField, "a random field", Field.Store.NO, Field.Index.ANALYZED));
                } else {
                  addedField = null;
                }
                if (random.nextBoolean()) {
                  if (VERBOSE) {
                    //System.out.println(Thread.currentThread().getName() + ": add doc id:" + doc.get("docid"));
                  }

                  if (random.nextBoolean()) {
                    // Add a pack of adjacent sub-docs
                    final String packID;
                    final SubDocs delSubDocs;
                    if (toDeleteSubDocs.size() > 0 && random.nextBoolean()) {
                      delSubDocs = toDeleteSubDocs.get(random.nextInt(toDeleteSubDocs.size()));
                      assert !delSubDocs.deleted;
                      toDeleteSubDocs.remove(delSubDocs);
                      // reuse prior packID
                      packID = delSubDocs.packID;
                    } else {
                      delSubDocs = null;
                      // make new packID
                      packID = packCount.getAndIncrement() + "";
                    }

                    final Field packIDField = newField("packID", packID, Field.Store.YES, Field.Index.NOT_ANALYZED);
                    final List<String> docIDs = new ArrayList<String>();
                    final SubDocs subDocs = new SubDocs(packID, docIDs);
                    final List<Document> docsList = new ArrayList<Document>();

                    allSubDocs.add(subDocs);
                    doc.add(packIDField);
                    docsList.add(cloneDoc(doc));
                    docIDs.add(doc.get("docid"));

                    final int maxDocCount = _TestUtil.nextInt(random, 1, 10);
                    while(docsList.size() < maxDocCount) {
                      doc = docs.nextDoc();
                      if (doc == null) {
                        break;
                      }
                      docsList.add(cloneDoc(doc));
                      docIDs.add(doc.get("docid"));
                    }
                    addCount.addAndGet(docsList.size());

                    if (delSubDocs != null) {
                      delSubDocs.deleted = true;
                      delIDs.addAll(delSubDocs.subIDs);
                      delCount.addAndGet(delSubDocs.subIDs.size());
                      if (VERBOSE) {
                        System.out.println("TEST: update pack packID=" + delSubDocs.packID + " count=" + docsList.size() + " docs=" + docIDs);
                      }
                      writer.updateDocuments(new Term("packID", delSubDocs.packID), docsList);
                      /*
                      // non-atomic:
                      writer.deleteDocuments(new Term("packID", delSubDocs.packID));
                      for(Document subDoc : docsList) {
                        writer.addDocument(subDoc);
                      }
                      */
                    } else {
                      if (VERBOSE) {
                        System.out.println("TEST: add pack packID=" + packID + " count=" + docsList.size() + " docs=" + docIDs);
                      }
                      writer.addDocuments(docsList);
                      
                      /*
                      // non-atomic:
                      for(Document subDoc : docsList) {
                        writer.addDocument(subDoc);
                      }
                      */
                    }
                    doc.removeField("packID");

                    if (random.nextInt(5) == 2) {
                      if (VERBOSE) {
                        //System.out.println(Thread.currentThread().getName() + ": buffer del id:" + packID);
                      }
                      toDeleteSubDocs.add(subDocs);
                    }

                  } else {
                    writer.addDocument(doc);
                    addCount.getAndIncrement();

                    if (random.nextInt(5) == 3) {
                      if (VERBOSE) {
                        //System.out.println(Thread.currentThread().getName() + ": buffer del id:" + doc.get("docid"));
                      }
                      toDeleteIDs.add(doc.get("docid"));
                    }
                  }
                } else {
                  // we use update but it never replaces a
                  // prior doc
                  if (VERBOSE) {
                    //System.out.println(Thread.currentThread().getName() + ": update doc id:" + doc.get("docid"));
                  }
                  writer.updateDocument(new Term("docid", doc.get("docid")), doc);
                  addCount.getAndIncrement();

                  if (random.nextInt(5) == 3) {
                    if (VERBOSE) {
                      //System.out.println(Thread.currentThread().getName() + ": buffer del id:" + doc.get("docid"));
                    }
                    toDeleteIDs.add(doc.get("docid"));
                  }
                }

                if (random.nextInt(30) == 17) {
                  if (VERBOSE) {
                    //System.out.println(Thread.currentThread().getName() + ": apply " + toDeleteIDs.size() + " deletes");
                  }
                  for(String id : toDeleteIDs) {
                    if (VERBOSE) {
                      //System.out.println(Thread.currentThread().getName() + ": del term=id:" + id);
                    }
                    writer.deleteDocuments(new Term("docid", id));
                  }
                  final int count = delCount.addAndGet(toDeleteIDs.size());
                  if (VERBOSE) {
                    //System.out.println(Thread.currentThread().getName() + ": tot " + count + " deletes");
                  }
                  delIDs.addAll(toDeleteIDs);
                  toDeleteIDs.clear();

                  for(SubDocs subDocs : toDeleteSubDocs) {
                    assert !subDocs.deleted;
                    writer.deleteDocuments(new Term("packID", subDocs.packID));
                    subDocs.deleted = true;
                    if (VERBOSE) {
                      System.out.println("  del subs: " + subDocs.subIDs + " packID=" + subDocs.packID);
                    }
                    delIDs.addAll(subDocs.subIDs);
                    delCount.addAndGet(subDocs.subIDs.size());
                  }
                  toDeleteSubDocs.clear();
                }
                if (addedField != null) {
                  doc.removeField(addedField);
                }
              } catch (Throwable t) {
                System.out.println(Thread.currentThread().getName() + ": hit exc");
                t.printStackTrace();
                failed.set(true);
                throw new RuntimeException(t);
              }
            }
            if (VERBOSE) {
              System.out.println(Thread.currentThread().getName() + ": indexing done");
            }
          }
        };
      threads[thread].setDaemon(true);
      threads[thread].start();
    }

    if (VERBOSE) {
      System.out.println("TEST: DONE start indexing threads [" + (System.currentTimeMillis()-t0) + " ms]");
    }

    // let index build up a bit
    Thread.sleep(100);

    IndexReader r = IndexReader.open(writer, true);
    boolean any = false;

    // silly starting guess:
    final AtomicInteger totTermCount = new AtomicInteger(100);

    final ExecutorService es = Executors.newCachedThreadPool(new NamedThreadFactory("NRT search threads"));

    while(System.currentTimeMillis() < stopTime && !failed.get()) {
      if (random.nextBoolean()) {
        if (VERBOSE) {
          System.out.println("TEST: now reopen r=" + r);
        }
        final IndexReader r2 = r.reopen();
        if (r != r2) {
          r.close();
          r = r2;
        }
      } else {
        if (VERBOSE) {
          System.out.println("TEST: now close reader=" + r);
        }
        r.close();
        writer.commit();
        final Set<String> openDeletedFiles = dir.getOpenDeletedFiles();
        if (openDeletedFiles.size() > 0) {
          System.out.println("OBD files: " + openDeletedFiles);
        }
        any |= openDeletedFiles.size() > 0;
        //assertEquals("open but deleted: " + openDeletedFiles, 0, openDeletedFiles.size());
        if (VERBOSE) {
          System.out.println("TEST: now open");
        }
        r = IndexReader.open(writer, true);
      }
      if (VERBOSE) {
        System.out.println("TEST: got new reader=" + r);
      }
      //System.out.println("numDocs=" + r.numDocs() + "
      //openDelFileCount=" + dir.openDeleteFileCount());

      smokeTestReader(r);

      if (r.numDocs() > 0) {

        final IndexSearcher s = new IndexSearcher(r, es);

        // run search threads
        final long searchStopTime = System.currentTimeMillis() + 500;
        final Thread[] searchThreads = new Thread[NUM_SEARCH_THREADS];
        final AtomicInteger totHits = new AtomicInteger();
        for(int thread=0;thread<NUM_SEARCH_THREADS;thread++) {
          searchThreads[thread] = new Thread() {
              @Override
                public void run() {
                try {
                  TermsEnum termsEnum = MultiFields.getTerms(s.getIndexReader(), "body").iterator();
                  int seenTermCount = 0;
                  int shift;
                  int trigger;
                  if (totTermCount.get() == 0) {
                    shift = 0;
                    trigger = 1;
                  } else {
                    shift = random.nextInt(totTermCount.get()/10);
                    trigger = totTermCount.get()/10;
                  }
                  while(System.currentTimeMillis() < searchStopTime) {
                    BytesRef term = termsEnum.next();
                    if (term == null) {
                      if (seenTermCount == 0) {
                        break;
                      }
                      totTermCount.set(seenTermCount);
                      seenTermCount = 0;
                      trigger = totTermCount.get()/10;
                      //System.out.println("trigger " + trigger);
                      shift = random.nextInt(totTermCount.get()/10);
                      termsEnum.seek(new BytesRef(""));
                      continue;
                    }
                    seenTermCount++;
                    // search 10 terms
                    if (trigger == 0) {
                      trigger = 1;
                    }
                    if ((seenTermCount + shift) % trigger == 0) {
                      //if (VERBOSE) {
                      //System.out.println(Thread.currentThread().getName() + " now search body:" + term.utf8ToString());
                      //}
                      totHits.addAndGet(runQuery(s, new TermQuery(new Term("body", term))));
                    }
                  }
                  if (VERBOSE) {
                    System.out.println(Thread.currentThread().getName() + ": search done");
                  }
                } catch (Throwable t) {
                  System.out.println(Thread.currentThread().getName() + ": hit exc");
                  failed.set(true);
                  t.printStackTrace(System.out);
                  throw new RuntimeException(t);
                }
              }
            };
          searchThreads[thread].setDaemon(true);
          searchThreads[thread].start();
        }

        for(int thread=0;thread<NUM_SEARCH_THREADS;thread++) {
          searchThreads[thread].join();
        }

        if (VERBOSE) {
          System.out.println("TEST: DONE search: totHits=" + totHits);
        }
      } else {
        Thread.sleep(100);
      }
    }

    es.shutdown();
    es.awaitTermination(1, TimeUnit.SECONDS);

    if (VERBOSE) {
      System.out.println("TEST: all searching done [" + (System.currentTimeMillis()-t0) + " ms]");
    }

    //System.out.println("numDocs=" + r.numDocs() + " openDelFileCount=" + dir.openDeleteFileCount());
    r.close();
    final Set<String> openDeletedFiles = dir.getOpenDeletedFiles();
    if (openDeletedFiles.size() > 0) {
      System.out.println("OBD files: " + openDeletedFiles);
    }
    any |= openDeletedFiles.size() > 0;

    assertFalse("saw non-zero open-but-deleted count", any);
    if (VERBOSE) {
      System.out.println("TEST: now join");
    }
    for(int thread=0;thread<NUM_INDEX_THREADS;thread++) {
      threads[thread].join();
    }
    if (VERBOSE) {
      System.out.println("TEST: done join [" + (System.currentTimeMillis()-t0) + " ms]; addCount=" + addCount + " delCount=" + delCount);
    }

    final IndexReader r2 = writer.getReader();
    final IndexSearcher s = newSearcher(r2);
    boolean doFail = false;
    for(String id : delIDs) {
      final TopDocs hits = s.search(new TermQuery(new Term("docid", id)), 1);
      if (hits.totalHits != 0) {
        System.out.println("doc id=" + id + " is supposed to be deleted, but got docID=" + hits.scoreDocs[0].doc);
        doFail = true;
      }
    }

    // Make sure each group of sub-docs are still in docID order:
    for(SubDocs subDocs : allSubDocs) {
      if (!subDocs.deleted) {
        // We sort by relevance but the scores should be identical so sort falls back to by docID:
        TopDocs hits = s.search(new TermQuery(new Term("packID", subDocs.packID)), 20);
        assertEquals(subDocs.subIDs.size(), hits.totalHits);
        int lastDocID = -1;
        int startDocID = -1;
        for(ScoreDoc scoreDoc : hits.scoreDocs) {
          final int docID = scoreDoc.doc;
          if (lastDocID != -1) {
            assertEquals(1+lastDocID, docID);
          } else {
            startDocID = docID;
          }
          lastDocID = docID;
          final Document doc = s.doc(docID);
          assertEquals(subDocs.packID, doc.get("packID"));
        }

        lastDocID = startDocID - 1;
        for(String subID : subDocs.subIDs) {
          hits = s.search(new TermQuery(new Term("docid", subID)), 1);
          assertEquals(1, hits.totalHits);
          final int docID = hits.scoreDocs[0].doc;
          if (lastDocID != -1) {
            assertEquals(1+lastDocID, docID);
          }
          lastDocID = docID;
        }          
      } else {
        for(String subID : subDocs.subIDs) {
          assertEquals(0, s.search(new TermQuery(new Term("docid", subID)), 1).totalHits);
        }
      }
    }
    
    final int endID = Integer.parseInt(docs.nextDoc().get("docid"));
    for(int id=0;id<endID;id++) {
      String stringID = ""+id;
      if (!delIDs.contains(stringID)) {
        final TopDocs hits = s.search(new TermQuery(new Term("docid", stringID)), 1);
        if (hits.totalHits != 1) {
          System.out.println("doc id=" + stringID + " is not supposed to be deleted, but got hitCount=" + hits.totalHits);
          doFail = true;
        }
      }
    }
    assertFalse(doFail);
    
    assertEquals("index=" + writer.segString() + " addCount=" + addCount + " delCount=" + delCount, addCount.get() - delCount.get(), r2.numDocs());
    r2.close();

    writer.commit();
    assertEquals("index=" + writer.segString() + " addCount=" + addCount + " delCount=" + delCount, addCount.get() - delCount.get(), writer.numDocs());

    assertFalse(writer.anyNonBulkMerges);
    writer.close(false);
    s.close();
    dir.close();
    docs.close();
    if (VERBOSE) {
      System.out.println("TEST: done [" + (System.currentTimeMillis()-t0) + " ms]");
    }
  }

  private int runQuery(IndexSearcher s, Query q) throws Exception {
    s.search(q, 10);
    return s.search(q, null, 10, new Sort(new SortField("title", SortField.STRING))).totalHits;
  }

  private void smokeTestReader(IndexReader r) throws Exception {
    IndexSearcher s = newSearcher(r);
    runQuery(s, new TermQuery(new Term("body", "united")));
    runQuery(s, new TermQuery(new Term("titleTokenized", "states")));
    PhraseQuery pq = new PhraseQuery();
    pq.add(new Term("body", "united"));
    pq.add(new Term("body", "states"));
    runQuery(s, pq);
    s.close();
  }
}
