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

final class DocFieldConsumersPerField extends DocFieldConsumerPerField {

  final DocFieldConsumerPerField one;
  final DocFieldConsumerPerField two;
  final DocFieldConsumers parent;
  final FieldInfo fieldInfo;

  public DocFieldConsumersPerField(DocFieldConsumers parent, FieldInfo fi, DocFieldConsumerPerField one, DocFieldConsumerPerField two) {
    this.parent = parent;
    this.one = one;
    this.two = two;
    this.fieldInfo = fi;
  }

  @Override
  public void processFields(IndexableField[] fields, int count) throws IOException {
    one.processFields(fields, count);
    two.processFields(fields, count);
  }

  @Override
  public void abort() {
    try {
      one.abort();
    } finally {
      two.abort();
    }
  }

  @Override
  FieldInfo getFieldInfo() {
    return fieldInfo;
  }
}
