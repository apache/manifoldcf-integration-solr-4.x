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

package org.apache.solr.schema;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ReaderUtil;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.function.DocValues;
import org.apache.solr.search.function.IntDocValues;
import org.apache.solr.search.function.ValueSource;

/**
 * Utility Field used for random sorting.  It should not be passed a value.
 * <p>
 * This random sorting implementation uses the dynamic field name to set the
 * random 'seed'.  To get random sorting order, you need to use a random
 * dynamic field name.  For example, you will need to configure schema.xml:
 * <pre>
 * &lt;types&gt;
 *  ...
 *  &lt;fieldType name="random" class="solr.RandomSortField" /&gt;
 *  ... 
 * &lt;/types&gt;
 * &lt;fields&gt;
 *  ...
 *  &lt;dynamicField name="random*" type="random" indexed="true" stored="false"/&gt;
 *  ...
 * &lt;/fields&gt;
 * </pre>
 * 
 * Examples of queries:
 * <ul>
 * <li>http://localhost:8983/solr/select/?q=*:*&fl=name&sort=rand_1234%20desc</li>
 * <li>http://localhost:8983/solr/select/?q=*:*&fl=name&sort=rand_2345%20desc</li>
 * <li>http://localhost:8983/solr/select/?q=*:*&fl=name&sort=rand_ABDC%20desc</li>
 * <li>http://localhost:8983/solr/select/?q=*:*&fl=name&sort=rand_21%20desc</li>
 * </ul>
 * Note that multiple calls to the same URL will return the same sorting order.
 * 
 *
 * @since solr 1.3
 */
public class RandomSortField extends FieldType {
  // Thomas Wang's hash32shift function, from http://www.cris.com/~Ttwang/tech/inthash.htm
  // slightly modified to return only positive integers.
  private static int hash(int key) {
    key = ~key + (key << 15); // key = (key << 15) - key - 1;
    key = key ^ (key >>> 12);
    key = key + (key << 2);
    key = key ^ (key >>> 4);
    key = key * 2057; // key = (key + (key << 3)) + (key << 11);
    key = key ^ (key >>> 16);
    return key >>> 1; 
  }

  /** 
   * Given a field name and an IndexReader, get a random hash seed.  
   * Using dynamic fields, you can force the random order to change 
   */
  private static int getSeed(String fieldName, AtomicReaderContext context) {
    final IndexReader top = ReaderUtil.getTopLevelContext(context).reader;
    // calling getVersion() on a segment will currently give you a null pointer exception, so
    // we use the top-level reader.
    return fieldName.hashCode() + context.docBase + (int)top.getVersion();
  }
  
  @Override
  public SortField getSortField(SchemaField field, boolean reverse) {
    return new SortField(field.getName(), randomComparatorSource, reverse);
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    return new RandomValueSource(field.getName());
  }

  @Override
  public void write(TextResponseWriter writer, String name, Fieldable f) throws IOException { }


  private static FieldComparatorSource randomComparatorSource = new FieldComparatorSource() {
    @Override
    public FieldComparator newComparator(final String fieldname, final int numHits, int sortPos, boolean reversed) throws IOException {
      return new FieldComparator() {
        int seed;
        private final int[] values = new int[numHits];
        int bottomVal;

        @Override
        public int compare(int slot1, int slot2) {
          return values[slot1] - values[slot2];  // values will be positive... no overflow possible.
        }

        @Override
        public void setBottom(int slot) {
          bottomVal = values[slot];
        }

        @Override
        public int compareBottom(int doc) throws IOException {
          return bottomVal - hash(doc+seed);
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
          values[slot] = hash(doc+seed);
        }

        @Override
        public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
          seed = getSeed(fieldname, context);
          return this;
        }

        @Override
        public Comparable value(int slot) {
          return values[slot];
        }
      };
    }
  };



  public class RandomValueSource extends ValueSource {
    private final String field;

    public RandomValueSource(String field) {
      this.field=field;
    }

    @Override
    public String description() {
      return field;
    }

    @Override
    public DocValues getValues(Map context, final AtomicReaderContext readerContext) throws IOException {
      return new IntDocValues(this) {
          private final int seed = getSeed(field, readerContext);
          @Override
          public int intVal(int doc) {
            return hash(doc+seed);
          }
        };
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof RandomValueSource)) return false;
      RandomValueSource other = (RandomValueSource)o;
      return this.field.equals(other.field);
    }

    @Override
    public int hashCode() {
      return field.hashCode();
    };
  }
}





