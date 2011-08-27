package org.apache.lucene.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;

import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.index.CategoryDocumentBuilder;
import org.apache.lucene.facet.index.params.DefaultFacetIndexingParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.lucene.LuceneTaxonomyReader;
import org.apache.lucene.facet.taxonomy.lucene.LuceneTaxonomyWriter;

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

public class FacetTestUtils {

  public static Directory[][] createIndexTaxonomyDirs(int number) throws IOException {
    Directory[][] dirs = new Directory[number][2];
    for (int i = 0; i < number; i++) {
      dirs[i][0] = LuceneTestCase.newDirectory();
      dirs[i][1] = LuceneTestCase.newDirectory();
    }
    return dirs;
  }

  public static IndexTaxonomyReaderPair[] createIndexTaxonomyReaderPair(
      Directory[][] dirs) throws IOException {
    IndexTaxonomyReaderPair[] pairs = new IndexTaxonomyReaderPair[dirs.length];
    for (int i = 0; i < dirs.length; i++) {
      IndexTaxonomyReaderPair pair = new IndexTaxonomyReaderPair();
      pair.indexReader = IndexReader.open(dirs[i][0]);
      pair.indexSearcher = new IndexSearcher(pair.indexReader);
      pair.taxReader = new LuceneTaxonomyReader(dirs[i][1]);
      pairs[i] = pair;
    }
    return pairs;
  }

  public static IndexTaxonomyWriterPair[] createIndexTaxonomyWriterPair(
      Directory[][] dirs) throws IOException {
    IndexTaxonomyWriterPair[] pairs = new IndexTaxonomyWriterPair[dirs.length];
    for (int i = 0; i < dirs.length; i++) {
      IndexTaxonomyWriterPair pair = new IndexTaxonomyWriterPair();
      pair.indexWriter = new IndexWriter(dirs[i][0], new IndexWriterConfig(
          LuceneTestCase.TEST_VERSION_CURRENT, new StandardAnalyzer(
              LuceneTestCase.TEST_VERSION_CURRENT)));
      pair.taxWriter = new LuceneTaxonomyWriter(dirs[i][1]);
      pair.indexWriter.commit();
      pair.taxWriter.commit();
      pairs[i] = pair;
    }
    return pairs;
  }

  public static Collector[] search(IndexSearcher searcher,
      TaxonomyReader taxonomyReader, DefaultFacetIndexingParams iParams,
      int k, String... facetNames) throws IOException,
      IllegalAccessException, InstantiationException {
    
    Collector[] collectors = new Collector[2];
    
    FacetSearchParams facetSearchParams = new FacetSearchParams(iParams);
    Collection<FacetRequest> fRequests = new ArrayList<FacetRequest>();
    for (String facetName : facetNames) {
      CategoryPath cp = new CategoryPath(facetName);
      FacetRequest fq = new CountFacetRequest(cp, k);
      facetSearchParams.addFacetRequest(fq);
      fRequests.add(fq);
    }

    TopScoreDocCollector topDocsCollector = TopScoreDocCollector.create(
        searcher.getIndexReader().maxDoc(), true);
    FacetsCollector facetsCollector = new FacetsCollector(
        facetSearchParams, searcher.getIndexReader(), taxonomyReader);
    Collector mColl = MultiCollector.wrap(topDocsCollector, facetsCollector);
    
    collectors[0] = topDocsCollector;
    collectors[1] = facetsCollector;

    searcher.search(new MatchAllDocsQuery(), mColl);
    return collectors;
  }
  
  public static void add(FacetIndexingParams iParams, RandomIndexWriter iw,
      TaxonomyWriter tw, String... strings) throws IOException,
      CorruptIndexException {
    ArrayList<CategoryPath> cps = new ArrayList<CategoryPath>();
    CategoryPath cp = new CategoryPath(strings);
    cps.add(cp);
    Document d = new Document();
    new CategoryDocumentBuilder(tw, iParams).setCategoryPaths(cps).build(d);
    d.add(new Field("content", TextField.TYPE_STORED, "alpha"));
    iw.addDocument(d);
  }

  public static class IndexTaxonomyReaderPair {
    public IndexReader indexReader;
    public TaxonomyReader taxReader;
    public IndexSearcher indexSearcher;

    public void close() throws IOException {
      indexSearcher.close();
      indexReader.close();
      taxReader.close();
    }

  }

  public static class IndexTaxonomyWriterPair {
    public IndexWriter indexWriter;
    public TaxonomyWriter taxWriter;

    public void close() throws IOException {
      indexWriter.close();
      taxWriter.close();
    }

    public void commit() throws IOException {
      indexWriter.commit();
      taxWriter.commit();
    }
  }

}
