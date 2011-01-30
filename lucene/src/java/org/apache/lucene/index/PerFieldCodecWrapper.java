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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.TermsConsumer;
import org.apache.lucene.store.Directory;

/**
 * Enables native per field codec support. This class selects the codec used to
 * write a field depending on the provided {@link SegmentCodecs}. For each field
 * seen it resolves the codec based on the {@link FieldInfo#codecId} which is
 * only valid during a segment merge. See {@link SegmentCodecs} javadoc for
 * details.
 * 
 * @lucene.internal
 */
final class PerFieldCodecWrapper extends Codec {
  private final SegmentCodecs segmentCodecs;

  PerFieldCodecWrapper(SegmentCodecs segmentCodecs) {
    name = "PerField";
    this.segmentCodecs = segmentCodecs;
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state)
      throws IOException {
    return new FieldsWriter(state);
  }

  private class FieldsWriter extends FieldsConsumer {
    private final ArrayList<FieldsConsumer> consumers = new ArrayList<FieldsConsumer>();

    public FieldsWriter(SegmentWriteState state) throws IOException {
      assert segmentCodecs == state.segmentCodecs;
      final Codec[] codecs = segmentCodecs.codecs;
      for (int i = 0; i < codecs.length; i++) {
        consumers.add(codecs[i].fieldsConsumer(new SegmentWriteState(state, "" + i)));
      }
    }

    @Override
    public TermsConsumer addField(FieldInfo field) throws IOException {
      final FieldsConsumer fields = consumers.get(field.codecId);
      return fields.addField(field);
    }

    @Override
    public void close() throws IOException {
      Iterator<FieldsConsumer> it = consumers.iterator();
      IOException err = null;
      while (it.hasNext()) {
        try {
          it.next().close();
        } catch (IOException ioe) {
          // keep first IOException we hit but keep
          // closing the rest
          if (err == null) {
            err = ioe;
          }
        }
      }
      if (err != null) {
        throw err;
      }
    }
  }

  private class FieldsReader extends FieldsProducer {

    private final Set<String> fields = new TreeSet<String>();
    private final Map<String, FieldsProducer> codecs = new HashMap<String, FieldsProducer>();

    public FieldsReader(Directory dir, FieldInfos fieldInfos, SegmentInfo si,
        int readBufferSize, int indexDivisor) throws IOException {

      final int fieldCount = fieldInfos.size();
      final Map<Codec, FieldsProducer> producers = new HashMap<Codec, FieldsProducer>();
      boolean success = false;
      try {
        for (int i = 0; i < fieldCount; i++) {
          FieldInfo fi = fieldInfos.fieldInfo(i);
          if (fi.isIndexed) { // TODO this does not work for non-indexed fields
            fields.add(fi.name);
            Codec codec = segmentCodecs.codecs[fi.codecId];
            if (!producers.containsKey(codec)) {
              producers.put(codec, codec.fieldsProducer(new SegmentReadState(dir,
                                                                             si, fieldInfos, readBufferSize, indexDivisor, ""+fi.codecId)));
            }
            codecs.put(fi.name, producers.get(codec));
          }
        }
        success = true;
      } finally {
        if (!success) {
          // If we hit exception (eg, IOE because writer was
          // committing, or, for any other reason) we must
          // go back and close all FieldsProducers we opened:
          for(FieldsProducer fp : producers.values()) {
            try {
              fp.close();
            } catch (Throwable t) {
              // Suppress all exceptions here so we continue
              // to throw the original one
            }
          }
        }
      }
    }

    private final class FieldsIterator extends FieldsEnum {
      private final Iterator<String> it;
      private String current;

      public FieldsIterator() {
        it = fields.iterator();
      }

      @Override
      public String next() {
        if (it.hasNext()) {
          current = it.next();
        } else {
          current = null;
        }

        return current;
      }

      @Override
      public TermsEnum terms() throws IOException {
        Terms terms = codecs.get(current).terms(current);
        if (terms != null) {
          return terms.iterator();
        } else {
          return TermsEnum.EMPTY;
        }
      }
    }

    @Override
    public FieldsEnum iterator() throws IOException {
      return new FieldsIterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
      FieldsProducer fields = codecs.get(field);
      return fields == null ? null : fields.terms(field);
    }

    @Override
    public void close() throws IOException {
      Iterator<FieldsProducer> it = codecs.values().iterator();
      IOException err = null;
      while (it.hasNext()) {
        try {
          it.next().close();
        } catch (IOException ioe) {
          // keep first IOException we hit but keep
          // closing the rest
          if (err == null) {
            err = ioe;
          }
        }
      }
      if (err != null) {
        throw err;
      }
    }

    @Override
    public void loadTermsIndex(int indexDivisor) throws IOException {
      Iterator<FieldsProducer> it = codecs.values().iterator();
      while (it.hasNext()) {
        it.next().loadTermsIndex(indexDivisor);
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state)
      throws IOException {
    return new FieldsReader(state.dir, state.fieldInfos, state.segmentInfo,
        state.readBufferSize, state.termsIndexDivisor);
  }

  @Override
  public void files(Directory dir, SegmentInfo info, String codecId, Set<String> files)
      throws IOException {
    // ignore codecid sicne segmentCodec will assign it per codec
    segmentCodecs.files(dir, info, files);
  }

  @Override
  public void getExtensions(Set<String> extensions) {
    for (Codec codec : segmentCodecs.codecs) {
      codec.getExtensions(extensions);
    }
  }
}
