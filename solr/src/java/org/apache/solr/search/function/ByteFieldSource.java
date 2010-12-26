package org.apache.solr.search.function;
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.cache.ByteValuesCreator;
import org.apache.lucene.search.cache.CachedArray.ByteValues;

import java.io.IOException;
import java.util.Map;

/**
 * Obtains int field values from the {@link org.apache.lucene.search.FieldCache}
 * using <code>getInts()</code>
 * and makes those values available as other numeric types, casting as needed. *
 *
 * @version $Id$
 */

public class ByteFieldSource extends NumericFieldCacheSource<ByteValues> {

  public ByteFieldSource(ByteValuesCreator creator) {
    super(creator);
  }

  public String description() {
    return "byte(" + field + ')';
  }

  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    final ByteValues vals = cache.getBytes(reader, field, creator);
    final byte[] arr = vals.values;
    
    return new DocValues() {
      @Override
      public byte byteVal(int doc) {
        return arr[doc];
      }

      @Override
      public short shortVal(int doc) {
        return (short) arr[doc];
      }

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
        return (double) arr[doc];
      }

      public String strVal(int doc) {
        return Byte.toString(arr[doc]);
      }

      public String toString(int doc) {
        return description() + '=' + byteVal(doc);
      }

    };
  }
}
