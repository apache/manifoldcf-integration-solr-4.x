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

import java.io.Reader;
import java.io.StringReader;
import java.util.Iterator;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericField.DataType;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.values.PerDocFieldValues;
import org.apache.lucene.index.values.ValueType;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestIndexableField extends LuceneTestCase {

  private class MyField implements IndexableField {

    private final int counter;
    private final IndexableFieldType fieldType = new IndexableFieldType() {
      @Override
      public boolean indexed() {
        return (counter % 10) != 3;
      }

      @Override
      public boolean stored() {
        return (counter & 1) == 0 || (counter % 10) == 3;
      }

      @Override
      public boolean tokenized() {
        return true;
      }

      @Override
      public boolean storeTermVectors() {
        return counter % 2 == 1 && counter % 10 != 9;
      }

      @Override
      public boolean storeTermVectorOffsets() {
        return counter % 2 == 1 && counter % 10 != 9;
      }

      @Override
      public boolean storeTermVectorPositions() {
        return counter % 2 == 1 && counter % 10 != 9;
      }

      @Override
      public boolean omitNorms() {
        return false;
      }

      @Override
      public FieldInfo.IndexOptions indexOptions() {
        return FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
      }
    };

    public MyField(int counter) {
      this.counter = counter;
    }

    @Override
    public String name() {
      return "f" + counter;
    }

    @Override
    public float boost() {
      return 1.0f + random.nextFloat();
    }

    @Override
    public BytesRef binaryValue() {
      if ((counter%10) == 3) {
        final byte[] bytes = new byte[10];
        for(int idx=0;idx<bytes.length;idx++) {
          bytes[idx] = (byte) (counter+idx);
        }
        return new BytesRef(bytes, 0, bytes.length);
      } else {
        return null;
      }
    }

    @Override
    public String stringValue() {
      final int fieldID = counter%10;
      if (fieldID != 3 && fieldID != 7 && fieldID != 9) {
        return "text " + counter;
      } else {
        return null;
      }
    }

    @Override
    public Reader readerValue() {
      if (counter%10 == 7) {
        return new StringReader("text " + counter);
      } else {
        return null;
      }
    }

    @Override
    public TokenStream tokenStreamValue() {
      if (numeric()) {
        return new NumericField(name()).setIntValue(counter).tokenStreamValue();
      } else {
        return null;
      }
    }

    // Numeric field:
    @Override
    public boolean numeric() {
      return counter%10 == 9;
    }

    @Override
    public DataType numericDataType() {
      return DataType.INT;
    }

    @Override
    public Number numericValue() {
      return counter;
    }

    @Override
    public IndexableFieldType fieldType() {
      return fieldType;
    }

    // TODO: randomly enable doc values
    @Override
    public PerDocFieldValues docValues() {
      return null;
    }

    @Override
    public ValueType docValuesType() {
      return null;
    }
  }

  // Silly test showing how to index documents w/o using Lucene's core
  // Document nor Field class
  public void testArbitraryFields() throws Exception {

    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random, dir);

    final int NUM_DOCS = atLeast(27);
    if (VERBOSE) {
      System.out.println("TEST: " + NUM_DOCS + " docs");
    }
    final int[] fieldsPerDoc = new int[NUM_DOCS];
    int baseCount = 0;

    for(int docCount=0;docCount<NUM_DOCS;docCount++) {
      final int fieldCount = _TestUtil.nextInt(random, 1, 17);
      fieldsPerDoc[docCount] = fieldCount-1;

      final int finalDocCount = docCount;
      if (VERBOSE) {
        System.out.println("TEST: " + fieldCount + " fields in doc " + docCount);
      }

      final int finalBaseCount = baseCount;
      baseCount += fieldCount-1;

      w.addDocument(new Iterable<IndexableField>() {
        @Override
        public Iterator<IndexableField> iterator() {
          return new Iterator<IndexableField>() {
            int fieldUpto;

            @Override
            public boolean hasNext() {
              return fieldUpto < fieldCount;
            }

            @Override
            public IndexableField next() {
              assert fieldUpto < fieldCount;
              if (fieldUpto == 0) {
                fieldUpto = 1;
                return newField("id", ""+finalDocCount, StringField.TYPE_STORED);
              } else {
                return new MyField(finalBaseCount + (fieldUpto++-1));
              }
            }

            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }
          };
        }
        });
    }

    final IndexReader r = w.getReader();
    w.close();

    final IndexSearcher s = new IndexSearcher(r);
    int counter = 0;
    for(int id=0;id<NUM_DOCS;id++) {
      if (VERBOSE) {
        System.out.println("TEST: verify doc id=" + id + " (" + fieldsPerDoc[id] + " fields) counter=" + counter);
      }
      final TopDocs hits = s.search(new TermQuery(new Term("id", ""+id)), 1);
      assertEquals(1, hits.totalHits);
      final int docID = hits.scoreDocs[0].doc;
      final Document doc = s.doc(docID);
      final int endCounter = counter + fieldsPerDoc[id];
      while(counter < endCounter) {
        final String name = "f" + counter;
        final int fieldID = counter % 10;

        final boolean stored = (counter&1) == 0 || fieldID == 3;
        final boolean binary = fieldID == 3;
        final boolean indexed = fieldID != 3;
        final boolean numeric = fieldID == 9;

        final String stringValue;
        if (fieldID != 3 && fieldID != 9) {
          stringValue = "text " + counter;
        } else {
          stringValue = null;
        }

        // stored:
        if (stored) {
          IndexableField f = doc.getField(name);
          assertNotNull("doc " + id + " doesn't have field f" + counter, f);
          if (binary) {
            assertNotNull("doc " + id + " doesn't have field f" + counter, f);
            final BytesRef b = f.binaryValue();
            assertNotNull(b);
            assertEquals(10, b.length);
            for(int idx=0;idx<10;idx++) {
              assertEquals((byte) (idx+counter), b.bytes[b.offset+idx]);
            }
          } else if (numeric) {
            assertTrue(f instanceof NumericField);
            final NumericField nf = (NumericField) f;
            assertEquals(NumericField.DataType.INT, nf.numericDataType());
            assertEquals(counter, nf.numericValue().intValue());
          } else {
            assert stringValue != null;
            assertEquals(stringValue, f.stringValue());
          }
        }
        
        if (indexed) {
          final boolean tv = counter % 2 == 1 && fieldID != 9;
          final TermFreqVector tfv = r.getTermFreqVector(docID, name);
          if (tv) {
            assertNotNull(tfv);
            assertTrue(tfv instanceof TermPositionVector);
            final TermPositionVector tpv = (TermPositionVector) tfv;
            final BytesRef[] terms = tpv.getTerms();
            assertEquals(2, terms.length);
            assertEquals(new BytesRef(""+counter), terms[0]);
            assertEquals(new BytesRef("text"), terms[1]);

            final int[] freqs = tpv.getTermFrequencies();
            assertEquals(2, freqs.length);
            assertEquals(1, freqs[0]);
            assertEquals(1, freqs[1]);

            int[] positions = tpv.getTermPositions(0);
            assertEquals(1, positions.length);
            assertEquals(1, positions[0]);

            positions = tpv.getTermPositions(1);
            assertEquals(1, positions.length);
            assertEquals(0, positions[0]);

            // TODO: offsets
            
          } else {
            assertNull(tfv);
          }

          if (numeric) {
            NumericRangeQuery nrq = NumericRangeQuery.newIntRange(name, counter, counter, true, true);
            final TopDocs hits2 = s.search(nrq, 1);
            assertEquals(1, hits2.totalHits);
            assertEquals(docID, hits2.scoreDocs[0].doc);
          } else {
            BooleanQuery bq = new BooleanQuery();
            bq.add(new TermQuery(new Term("id", ""+id)), BooleanClause.Occur.MUST);
            bq.add(new TermQuery(new Term(name, "text")), BooleanClause.Occur.MUST);
            final TopDocs hits2 = s.search(bq, 1);
            assertEquals(1, hits2.totalHits);
            assertEquals(docID, hits2.scoreDocs[0].doc);

            bq = new BooleanQuery();
            bq.add(new TermQuery(new Term("id", ""+id)), BooleanClause.Occur.MUST);
            bq.add(new TermQuery(new Term(name, ""+counter)), BooleanClause.Occur.MUST);
            final TopDocs hits3 = s.search(bq, 1);
            assertEquals(1, hits3.totalHits);
            assertEquals(docID, hits3.scoreDocs[0].doc);
          }
        }

        counter++;
      }
    }

    r.close();
    dir.close();
  }
}
