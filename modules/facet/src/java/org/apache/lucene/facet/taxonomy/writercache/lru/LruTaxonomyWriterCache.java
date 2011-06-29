package org.apache.lucene.facet.taxonomy.writercache.lru;

import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache;

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

/**
 * LRU {@link TaxonomyWriterCache} - good choice for huge taxonomies.
 * 
 * @lucene.experimental
 */
public class LruTaxonomyWriterCache implements TaxonomyWriterCache {

  public enum LRUType { LRU_HASHED, LRU_STRING }

  private NameIntCacheLRU cache;

  public LruTaxonomyWriterCache(int cacheSize) {
    // TODO (Facet): choose between NameHashIntCacheLRU and NameIntCacheLRU.
    // For guaranteed correctness - not relying on no-collisions in the hash
    // function, NameIntCacheLRU should be used:
    // On the other hand, NameHashIntCacheLRU takes less RAM but if there
    // are collisions (which we never found) two different paths would be
    // mapped to the same ordinal...
    this(cacheSize, LRUType.LRU_HASHED);
  }

  public LruTaxonomyWriterCache(int cacheSize, LRUType lruType) {
    // TODO (Facet): choose between NameHashIntCacheLRU and NameIntCacheLRU.
    // For guaranteed correctness - not relying on no-collisions in the hash
    // function, NameIntCacheLRU should be used:
    // On the other hand, NameHashIntCacheLRU takes less RAM but if there
    // are collisions (which we never found) two different paths would be
    // mapped to the same ordinal...
    if (lruType == LRUType.LRU_HASHED) {
      this.cache = new NameHashIntCacheLRU(cacheSize);
    } else {
      this.cache = new NameIntCacheLRU(cacheSize);
    }
  }

  public boolean hasRoom(int n) {
    return n<=(cache.getMaxSize()-cache.getSize());
  }

  public void close() {
    cache.clear();
    cache=null;
  }

  public int get(CategoryPath categoryPath) {
    Integer res = cache.get(categoryPath);
    if (res == null) {
      return -1;
    }

    return res.intValue();
  }

  public int get(CategoryPath categoryPath, int length) {
    if (length<0 || length>categoryPath.length()) {
      length = categoryPath.length();
    }
    // TODO (Facet): unfortunately, we make a copy here! we can avoid part of
    // the copy by creating a wrapper object (but this still creates a new
    // object). A better implementation of the cache would not use Java's
    // hash table, but rather some other hash table we can control, and
    // pass the length parameter into it...
    Integer res = cache.get(new CategoryPath(categoryPath, length));
    if (res==null) {
      return -1;
    }
    return res.intValue();
  }

  public boolean put(CategoryPath categoryPath, int ordinal) {
    boolean ret = cache.put(categoryPath, new Integer(ordinal));
    // If the cache is full, we need to clear one or more old entries
    // from the cache. However, if we delete from the cache a recent
    // addition that isn't yet in our reader, for this entry to be
    // visible to us we need to make sure that the changes have been
    // committed and we reopen the reader. Because this is a slow
    // operation, we don't delete entries one-by-one but rather in bulk
    // (put() removes the 2/3rd oldest entries).
    if (ret) {
      cache.makeRoomLRU();
    }
    return ret;
  }

  public boolean put(CategoryPath categoryPath, int prefixLen, int ordinal) {
    boolean ret = cache.put(categoryPath, prefixLen, new Integer(ordinal));
    // If the cache is full, we need to clear one or more old entries
    // from the cache. However, if we delete from the cache a recent
    // addition that isn't yet in our reader, for this entry to be
    // visible to us we need to make sure that the changes have been
    // committed and we reopen the reader. Because this is a slow
    // operation, we don't delete entries one-by-one but rather in bulk
    // (put() removes the 2/3rd oldest entries).
    if (ret) { 
      cache.makeRoomLRU(); 
    }
    return ret;
  }

}

