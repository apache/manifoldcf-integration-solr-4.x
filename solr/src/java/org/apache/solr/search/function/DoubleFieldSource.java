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

package org.apache.solr.search.function;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.cache.DoubleValuesCreator;
import org.apache.lucene.search.cache.FloatValuesCreator;
import org.apache.lucene.search.cache.CachedArray.DoubleValues;
import org.apache.lucene.search.cache.CachedArray.FloatValues;
import org.apache.solr.search.MutableValue;
import org.apache.solr.search.MutableValueDouble;

import java.io.IOException;
import java.util.Map;

/**
 * Obtains float field values from the {@link org.apache.lucene.search.FieldCache}
 * using <code>getFloats()</code>
 * and makes those values available as other numeric types, casting as needed.
 *
 * @version $Id$
 */

public class DoubleFieldSource extends NumericFieldCacheSource<DoubleValues> {

  public DoubleFieldSource(DoubleValuesCreator creator) {
    super(creator);
  }

  public String description() {
    return "double(" + field + ')';
  }

  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    final DoubleValues vals = cache.getDoubles(reader, field, creator);
    final double[] arr = vals.values;
    
    return new DocValues() {
      public float floatVal(int doc) {
        return (float) arr[doc];
      }

      public int intVal(int doc) {
        return (int) arr[doc];
      }

      public long longVal(int doc) {
        return (long) arr[doc];
      }

      public double doubleVal(int doc) {
        return arr[doc];
      }

      public String strVal(int doc) {
        return Double.toString(arr[doc]);
      }

      public String toString(int doc) {
        return description() + '=' + doubleVal(doc);
      }

      @Override
      public ValueSourceScorer getRangeScorer(IndexReader reader, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper) {
        double lower,upper;

        if (lowerVal==null) {
          lower = Double.NEGATIVE_INFINITY;
        } else {
          lower = Double.parseDouble(lowerVal);
        }

         if (upperVal==null) {
          upper = Double.POSITIVE_INFINITY;
        } else {
          upper = Double.parseDouble(upperVal);
        }

        final double l = lower;
        final double u = upper;


        if (includeLower && includeUpper) {
          return new ValueSourceScorer(reader, this) {
            @Override
            public boolean matchesValue(int doc) {
              double docVal = doubleVal(doc);
              return docVal >= l && docVal <= u;
            }
          };
        }
        else if (includeLower && !includeUpper) {
          return new ValueSourceScorer(reader, this) {
            @Override
            public boolean matchesValue(int doc) {
              double docVal = doubleVal(doc);
              return docVal >= l && docVal < u;
            }
          };
        }
        else if (!includeLower && includeUpper) {
          return new ValueSourceScorer(reader, this) {
            @Override
            public boolean matchesValue(int doc) {
              double docVal = doubleVal(doc);
              return docVal > l && docVal <= u;
            }
          };
        }
        else {
          return new ValueSourceScorer(reader, this) {
            @Override
            public boolean matchesValue(int doc) {
              double docVal = doubleVal(doc);
              return docVal > l && docVal < u;
            }
          };
        }
      }

            @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final double[] doubleArr = arr;
          private final MutableValueDouble mval = new MutableValueDouble();

          @Override
          public MutableValue getValue() {
            return mval;
          }

          @Override
          public void fillValue(int doc) {
            mval.value = doubleArr[doc];
          }
        };
      }


      };

  }
}
