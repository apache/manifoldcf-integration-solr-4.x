package org.apache.lucene.index;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.DocumentsWriterDeleteQueue.DeleteSlice;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Unit test for {@link DocumentsWriterDeleteQueue}
 */
public class TestDocumentsWriterDeleteQueue extends LuceneTestCase {

  public void testUpdateDelteSlices() {
    DocumentsWriterDeleteQueue queue = new DocumentsWriterDeleteQueue();
    final int size = 200 + random.nextInt(500) * RANDOM_MULTIPLIER;
    Integer[] ids = new Integer[size];
    for (int i = 0; i < ids.length; i++) {
      ids[i] = random.nextInt();
    }
    Term template = new Term("id");
    DeleteSlice slice1 = queue.newSlice();
    DeleteSlice slice2 = queue.newSlice();
    BufferedDeletes bd1 = new BufferedDeletes(false);
    BufferedDeletes bd2 = new BufferedDeletes(false);
    int last1 = 0;
    int last2 = 0;
    Set<Term> uniqueValues = new HashSet<Term>();
    for (int j = 0; j < ids.length; j++) {
      Integer i = ids[j];
      // create an array here since we compare identity below against tailItem
      Term[] term = new Term[] {template.createTerm(i.toString())};
      uniqueValues.add(term[0]);
      queue.addDelete(term);
      if (random.nextInt(20) == 0 || j == ids.length - 1) {
        queue.updateSlice(slice1);
        assertTrue(slice1.isTailItem(term));
        slice1.apply(bd1, j);
        assertAllBetween(last1, j, bd1, ids);
        last1 = j + 1;
      }
      if (random.nextInt(10) == 5 || j == ids.length - 1) {
        queue.updateSlice(slice2);
        assertTrue(slice2.isTailItem(term));
        slice2.apply(bd2, j);
        assertAllBetween(last2, j, bd2, ids);
        last2 = j + 1;
      }
      assertEquals(uniqueValues.size(), queue.numGlobalTermDeletes());
    }
    assertEquals(uniqueValues, bd1.terms.keySet());
    assertEquals(uniqueValues, bd2.terms.keySet());
    assertEquals(uniqueValues, new HashSet<Term>(Arrays.asList(queue
        .freezeGlobalBuffer(null).terms)));
    assertEquals("num deletes must be 0 after freeze", 0, queue
        .numGlobalTermDeletes());
  }

  private void assertAllBetween(int start, int end, BufferedDeletes deletes,
      Integer[] ids) {
    Term template = new Term("id");
    for (int i = start; i <= end; i++) {
      assertEquals(Integer.valueOf(end), deletes.terms.get(template
          .createTerm(ids[i].toString())));
    }
  }
  
  public void testClear() {
    DocumentsWriterDeleteQueue queue = new DocumentsWriterDeleteQueue();
    Term template = new Term("id");
    assertFalse(queue.anyChanges());
    queue.clear();
    assertFalse(queue.anyChanges());
    final int size = 200 + random.nextInt(500) * RANDOM_MULTIPLIER;
    int termsSinceFreeze = 0;
    int queriesSinceFreeze = 0;
    for (int i = 0; i < size; i++) {
      Term term = template.createTerm("" + i);
      if (random.nextInt(10) == 0) {
        queue.addDelete(new TermQuery(term));
        queriesSinceFreeze++;
      } else {
        queue.addDelete(term);
        termsSinceFreeze++;
      }
      assertTrue(queue.anyChanges());
      if (random.nextInt(10) == 0) {
        queue.clear();
        queue.tryApplyGlobalSlice();
        assertFalse(queue.anyChanges());
      }
    }
    
  }

  public void testAnyChanges() {
    DocumentsWriterDeleteQueue queue = new DocumentsWriterDeleteQueue();
    Term template = new Term("id");
    final int size = 200 + random.nextInt(500) * RANDOM_MULTIPLIER;
    int termsSinceFreeze = 0;
    int queriesSinceFreeze = 0;
    for (int i = 0; i < size; i++) {
      Term term = template.createTerm("" + i);
      if (random.nextInt(10) == 0) {
        queue.addDelete(new TermQuery(term));
        queriesSinceFreeze++;
      } else {
        queue.addDelete(term);
        termsSinceFreeze++;
      }
      assertTrue(queue.anyChanges());
      if (random.nextInt(5) == 0) {
        FrozenBufferedDeletes freezeGlobalBuffer = queue
            .freezeGlobalBuffer(null);
        assertEquals(termsSinceFreeze, freezeGlobalBuffer.terms.length);
        assertEquals(queriesSinceFreeze, freezeGlobalBuffer.queries.length);
        queriesSinceFreeze = 0;
        termsSinceFreeze = 0;
        assertFalse(queue.anyChanges());
      }
    }
  }

  public void testStressDeleteQueue() throws InterruptedException {
    DocumentsWriterDeleteQueue queue = new DocumentsWriterDeleteQueue();
    Set<Term> uniqueValues = new HashSet<Term>();
    final int size = 10000 + random.nextInt(500) * RANDOM_MULTIPLIER;
    Integer[] ids = new Integer[size];
    Term template = new Term("id");
    for (int i = 0; i < ids.length; i++) {
      ids[i] = random.nextInt();
      uniqueValues.add(template.createTerm(ids[i].toString()));
    }
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger index = new AtomicInteger(0);
    final int numThreads = 2 + random.nextInt(5);
    UpdateThread[] threads = new UpdateThread[numThreads];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new UpdateThread(queue, index, ids, latch);
      threads[i].start();
    }
    latch.countDown();
    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }

    for (UpdateThread updateThread : threads) {
      DeleteSlice slice = updateThread.slice;
      queue.updateSlice(slice);
      BufferedDeletes deletes = updateThread.deletes;
      slice.apply(deletes, BufferedDeletes.MAX_INT);
      assertEquals(uniqueValues, deletes.terms.keySet());
    }
    queue.tryApplyGlobalSlice();
    assertEquals(uniqueValues, new HashSet<Term>(Arrays.asList(queue
        .freezeGlobalBuffer(null).terms)));
    assertEquals("num deletes must be 0 after freeze", 0, queue
        .numGlobalTermDeletes());
  }

  private static class UpdateThread extends Thread {
    final DocumentsWriterDeleteQueue queue;
    final AtomicInteger index;
    final Integer[] ids;
    final DeleteSlice slice;
    final BufferedDeletes deletes;
    final CountDownLatch latch;

    protected UpdateThread(DocumentsWriterDeleteQueue queue,
        AtomicInteger index, Integer[] ids, CountDownLatch latch) {
      this.queue = queue;
      this.index = index;
      this.ids = ids;
      this.slice = queue.newSlice();
      deletes = new BufferedDeletes(false);
      this.latch = latch;
    }

    @Override
    public void run() {
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw new ThreadInterruptedException(e);
      }
      Term template = new Term("id");
      int i = 0;
      while ((i = index.getAndIncrement()) < ids.length) {
        Term term = template.createTerm(ids[i].toString());
        queue.add(term, slice);
        assertTrue(slice.isTailItem(term));
        slice.apply(deletes, BufferedDeletes.MAX_INT);
      }
    }
  }

}
