package org.apache.lucene.index.codecs;

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
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.values.Writer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

public class DefaultDocValuesConsumer extends PerDocConsumer {
  private final String segmentName;
  private final int codecId;
  private final Directory directory;
  private final AtomicLong bytesUsed;
  private final Comparator<BytesRef> comparator;

  public DefaultDocValuesConsumer(PerDocWriteState state, Comparator<BytesRef> comparator) {
    this.segmentName = state.segmentName;
    this.codecId = state.codecId;
    this.bytesUsed = state.bytesUsed;
    this.directory = state.directory;
    this.comparator = comparator;
  }
  
  public void close() throws IOException {
  }

  @Override
  public DocValuesConsumer addValuesField(FieldInfo field) throws IOException {
    return Writer.create(field.getDocValues(),
        docValuesId(segmentName, codecId, field.number),
        // TODO can we have a compound file per segment and codec for
        // docvalues?
        directory, comparator, bytesUsed);
  }
  
  @SuppressWarnings("fallthrough")
  public static void files(Directory dir, SegmentInfo segmentInfo, int codecId,
      Set<String> files) throws IOException {
    FieldInfos fieldInfos = segmentInfo.getFieldInfos();
    for (FieldInfo fieldInfo : fieldInfos) {
      if (fieldInfo.getCodecId() == codecId && fieldInfo.hasDocValues()) {
        String filename = docValuesId(segmentInfo.name, codecId,
            fieldInfo.number);
        switch (fieldInfo.getDocValues()) {
        case BYTES_FIXED_DEREF:
        case BYTES_VAR_DEREF:
        case BYTES_VAR_SORTED:
        case BYTES_FIXED_SORTED:
        case BYTES_VAR_STRAIGHT:
          files.add(IndexFileNames.segmentFileName(filename, "",
              Writer.INDEX_EXTENSION));
          assert dir.fileExists(IndexFileNames.segmentFileName(filename, "",
              Writer.INDEX_EXTENSION));
          // until here all types use an index
        case BYTES_FIXED_STRAIGHT:
        case FLOAT_32:
        case FLOAT_64:
        case INTS:
          files.add(IndexFileNames.segmentFileName(filename, "",
              Writer.DATA_EXTENSION));
          assert dir.fileExists(IndexFileNames.segmentFileName(filename, "",
              Writer.DATA_EXTENSION));
          break;
        default:
          assert false;
        }
      }
    }
  }
  
  static String docValuesId(String segmentsName, int codecID, int fieldId) {
    return segmentsName + "_" + codecID + "-" + fieldId;
  }

  public static void getDocValuesExtensions(Set<String> extensions) {
    extensions.add(Writer.DATA_EXTENSION);
    extensions.add(Writer.INDEX_EXTENSION);
  }
}
