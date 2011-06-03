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

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DefFunction extends MultiFunction {
  public DefFunction(List<ValueSource> sources) {
    super(sources);
  }

  @Override
  protected String name() {
    return "def";
  }


  @Override
  public DocValues getValues(Map fcontext, AtomicReaderContext readerContext) throws IOException {


    return new Values(valsArr(sources, fcontext, readerContext)) {
      final int upto = valsArr.length - 1;

      private DocValues get(int doc) {
        for (int i=0; i<upto; i++) {
          DocValues vals = valsArr[i];
          if (vals.exists(doc)) {
            return vals;
          }
        }
        return valsArr[upto];
      }

      @Override
      public byte byteVal(int doc) {
        return get(doc).byteVal(doc);
      }

      @Override
      public short shortVal(int doc) {
        return get(doc).shortVal(doc);
      }

      @Override
      public float floatVal(int doc) {
        return get(doc).floatVal(doc);
      }

      @Override
      public int intVal(int doc) {
        return get(doc).intVal(doc);
      }

      @Override
      public long longVal(int doc) {
        return get(doc).longVal(doc);
      }

      @Override
      public double doubleVal(int doc) {
        return get(doc).doubleVal(doc);
      }

      @Override
      public String strVal(int doc) {
        return get(doc).strVal(doc);
      }

      @Override
      public boolean boolVal(int doc) {
        return get(doc).boolVal(doc);
      }

      @Override
      public boolean bytesVal(int doc, BytesRef target) {
        return get(doc).bytesVal(doc, target);
      }

      @Override
      public Object objectVal(int doc) {
        return get(doc).objectVal(doc);
      }

      @Override
      public boolean exists(int doc) {
        // return true if any source is exists?
        for (DocValues vals : valsArr) {
          if (vals.exists(doc)) {
            return true;
          }
        }
        return false;
      }

      @Override
      public ValueFiller getValueFiller() {
        // TODO: need ValueSource.type() to determine correct type
        return super.getValueFiller();
      }
    };
  }
}