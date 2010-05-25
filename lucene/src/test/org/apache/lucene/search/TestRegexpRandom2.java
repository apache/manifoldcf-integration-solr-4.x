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
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;

/**
 * Create an index with random unicode terms
 * Generates random regexps, and validates against a simple impl.
 */
public class TestRegexpRandom2 extends LuceneTestCase {
  private IndexSearcher searcher;
  private Random random;
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    random = newRandom();
    RAMDirectory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new MockAnalyzer(MockTokenizer.KEYWORD, false),
        IndexWriter.MaxFieldLength.UNLIMITED);
    
    Document doc = new Document();
    Field field = new Field("field", "", Field.Store.NO, Field.Index.ANALYZED);
    doc.add(field);
    
    for (int i = 0; i < 2000*_TestUtil.getRandomMultiplier(); i++) {
      field.setValue(_TestUtil.randomUnicodeString(random));
      writer.addDocument(doc);
    }
    
    writer.optimize();
    writer.close();
    searcher = new IndexSearcher(dir);
  }

  @Override
  protected void tearDown() throws Exception {
    searcher.close();
    super.tearDown();
  }
  
  /** a stupid regexp query that just blasts thru the terms */
  private class DumbRegexpQuery extends MultiTermQuery {
    private final Automaton automaton;
    
    DumbRegexpQuery(Term term) {
      super(term.field());
      RegExp re = new RegExp(term.text());
      automaton = re.toAutomaton();
    }
    
    @Override
    protected TermsEnum getTermsEnum(IndexReader reader) throws IOException {
      return new SimpleAutomatonTermsEnum(reader, field);
    }

    private class SimpleAutomatonTermsEnum extends FilteredTermsEnum {
      CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton);
      UnicodeUtil.UTF16Result utf16 = new UnicodeUtil.UTF16Result();

      private SimpleAutomatonTermsEnum(IndexReader reader, String field) throws IOException {
        super(reader, field);
        setInitialSeekTerm(new BytesRef(""));
      }
      
      @Override
      protected AcceptStatus accept(BytesRef term) throws IOException {
        UnicodeUtil.UTF8toUTF16(term.bytes, term.offset, term.length, utf16);
        return runAutomaton.run(utf16.result, 0, utf16.length) ? 
            AcceptStatus.YES : AcceptStatus.NO;
      }
    }

    @Override
    public String toString(String field) {
      return field.toString() + automaton.toString();
    }
  }
  
  /** test a bunch of random regular expressions */
  public void testRegexps() throws Exception {
      for (int i = 0; i < 1000*_TestUtil.getRandomMultiplier(); i++)
        assertSame(AutomatonTestUtil.randomRegexp(random).toString());
  }
  
  /** check that the # of hits is the same as from a very
   * simple regexpquery implementation.
   */
  private void assertSame(String regexp) throws IOException {   
    RegexpQuery smart = new RegexpQuery(new Term("field", regexp));
    DumbRegexpQuery dumb = new DumbRegexpQuery(new Term("field", regexp));
    
    // we can't compare the two if automaton rewrites to a simpler enum.
    // for example: "a\uda07\udcc7?.*?" gets rewritten to a simpler query:
    // a\uda07* prefixquery. Prefixquery then does the "wrong" thing, which
    // isn't really wrong as the query was undefined to begin with... but not
    // automatically comparable.
    if (!(smart.getTermsEnum(searcher.getIndexReader()) instanceof AutomatonTermsEnum))
      return;
    
    TopDocs smartDocs = searcher.search(smart, 25);
    TopDocs dumbDocs = searcher.search(dumb, 25);
    
    assertEquals("for re:" + regexp, dumbDocs.totalHits, smartDocs.totalHits);
  }
}
