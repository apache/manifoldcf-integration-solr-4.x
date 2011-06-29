package org.apache.lucene.facet.search.association;

import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.enhancements.EnhancementsDocumentBuilder;
import org.apache.lucene.facet.enhancements.association.AssociationEnhancement;
import org.apache.lucene.facet.enhancements.association.AssociationFloatProperty;
import org.apache.lucene.facet.enhancements.association.AssociationIntProperty;
import org.apache.lucene.facet.enhancements.params.DefaultEnhancementsIndexingParams;
import org.apache.lucene.facet.index.CategoryContainer;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.params.association.AssociationFloatSumFacetRequest;
import org.apache.lucene.facet.search.params.association.AssociationIntSumFacetRequest;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.taxonomy.CategoryPath;
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

/** Test for associations */
public class AssociationsFacetRequestTest extends LuceneTestCase {

  private static Directory dir = new RAMDirectory();
  private static Directory taxoDir = new RAMDirectory();
  
  private static final CategoryPath aint = new CategoryPath("int", "a");
  private static final CategoryPath bint = new CategoryPath("int", "b");
  private static final CategoryPath afloat = new CategoryPath("float", "a");
  private static final CategoryPath bfloat = new CategoryPath("float", "b");
  
  @BeforeClass
  public static void beforeClassAssociationsFacetRequestTest() throws Exception {
    // preparations - index, taxonomy, content
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new KeywordAnalyzer()));
    
    TaxonomyWriter taxoWriter = new LuceneTaxonomyWriter(taxoDir);
    
    EnhancementsDocumentBuilder builder = new EnhancementsDocumentBuilder(
        taxoWriter, new DefaultEnhancementsIndexingParams(
            new AssociationEnhancement()));
    
    // index documents, 50% have only 'b' and all have 'a'
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      CategoryContainer container = new CategoryContainer();
      container.addCategory(aint, new AssociationIntProperty(2));
      container.addCategory(afloat, new AssociationFloatProperty(0.5f));
      if (i % 2 == 0) { // 50
        container.addCategory(bint, new AssociationIntProperty(3));
        container.addCategory(bfloat, new AssociationFloatProperty(0.2f));
      }
      builder.setCategories(container).build(doc);
      writer.addDocument(doc);
    }
    
    taxoWriter.close();
    writer.close();
  }
  
  @AfterClass
  public static void afterClassAssociationsFacetRequestTest() throws Exception {
    dir.close();
    taxoDir.close();
  }
  
  @Test
  public void testIntSumAssociation() throws Exception {
    IndexReader reader = IndexReader.open(dir, true);
    LuceneTaxonomyReader taxo = new LuceneTaxonomyReader(taxoDir);

    // facet requests for two facets
    FacetSearchParams fsp = new FacetSearchParams();
    fsp.addFacetRequest(new AssociationIntSumFacetRequest(aint, 10));
    fsp.addFacetRequest(new AssociationIntSumFacetRequest(bint, 10));
    
    Query q = new MatchAllDocsQuery();

    FacetsCollector fc = new FacetsCollector(fsp, reader, taxo);
    
    new IndexSearcher(reader).search(q, fc);
    List<FacetResult> res = fc.getFacetResults();
    
    assertNotNull("No results!",res);
    assertEquals("Wrong number of results!",2, res.size());
    assertEquals("Wrong count for category 'a'!",200, (int) res.get(0).getFacetResultNode().getValue());
    assertEquals("Wrong count for category 'b'!",150, (int) res.get(1).getFacetResultNode().getValue());
    
    taxo.close();
    reader.close();
  }
  
  @Test
  public void testFloatSumAssociation() throws Exception {
    
    IndexReader reader = IndexReader.open(dir, true);
    LuceneTaxonomyReader taxo = new LuceneTaxonomyReader(taxoDir);

    // facet requests for two facets
    FacetSearchParams fsp = new FacetSearchParams();
    fsp.addFacetRequest(new AssociationFloatSumFacetRequest(afloat, 10));
    fsp.addFacetRequest(new AssociationFloatSumFacetRequest(bfloat, 10));
    
    Query q = new MatchAllDocsQuery();

    FacetsCollector fc = new FacetsCollector(fsp, reader, taxo);
    
    new IndexSearcher(reader).search(q, fc);
    List<FacetResult> res = fc.getFacetResults();
    
    assertNotNull("No results!",res);
    assertEquals("Wrong number of results!",2, res.size());
    assertEquals("Wrong count for category 'a'!",50f, (float) res.get(0).getFacetResultNode().getValue(), 0.00001);
    assertEquals("Wrong count for category 'b'!",10f, (float) res.get(1).getFacetResultNode().getValue(), 0.00001);
    
    taxo.close();
    reader.close();
  }  
    
  @Test
  public void testDifferentAggregatorsSameCategoryList() throws Exception {
    // Same category list cannot be aggregated by two different aggregators. If
    // you want to do that, you need to separate the categories into two
    // category list (you'll still have one association list).
    IndexReader reader = IndexReader.open(dir, true);
    LuceneTaxonomyReader taxo = new LuceneTaxonomyReader(taxoDir);

    // facet requests for two facets
    FacetSearchParams fsp = new FacetSearchParams();
    fsp.addFacetRequest(new AssociationIntSumFacetRequest(aint, 10));
    fsp.addFacetRequest(new AssociationIntSumFacetRequest(bint, 10));
    fsp.addFacetRequest(new AssociationFloatSumFacetRequest(afloat, 10));
    fsp.addFacetRequest(new AssociationFloatSumFacetRequest(bfloat, 10));
    
    Query q = new MatchAllDocsQuery();

    FacetsCollector fc = new FacetsCollector(fsp, reader, taxo);
    
    new IndexSearcher(reader).search(q, fc);
    try {
      fc.getFacetResults();
      fail("different aggregators for same category list should not be supported");
    } catch (RuntimeException e) {
      // ok - expected
    }
  }  

}
