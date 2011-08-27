package org.apache.lucene.facet.taxonomy.lucene;

import java.io.IOException;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.IndexInput;

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

/**
 * @lucene.experimental
 */
abstract class Consts {

  static final String FULL = "$full_path$";
  static final String FIELD_PAYLOADS = "$payloads$";
  static final String PAYLOAD_PARENT = "p";
  static final char[] PAYLOAD_PARENT_CHARS = PAYLOAD_PARENT.toCharArray();

  /**
   * The following is a "stored field visitor", an object
   * which tells Lucene to extract only a single field
   * rather than a whole document.
   */
  public static final class LoadFullPathOnly extends StoredFieldVisitor {
    private String fullPath;

    public boolean stringField(FieldInfo fieldInfo, IndexInput in, int numUTF8Bytes) throws IOException {
      final byte[] bytes = new byte[numUTF8Bytes];
      in.readBytes(bytes, 0, bytes.length);
      fullPath = new String(bytes, "UTF-8");

      // Stop loading:
      return true;
    }

    public String getFullPath() {
      return fullPath;
    }
  }

  /**
   * Delimiter used for creating the full path of a category from the list of
   * its labels from root. It is forbidden for labels to contain this
   * character.
   * <P>
   * Originally, we used \uFFFE, officially a "unicode noncharacter" (invalid
   * unicode character) for this purpose. Recently, we switched to the
   * "private-use" character \uF749.
   */
  //static final char DEFAULT_DELIMITER = '\uFFFE';
  static final char DEFAULT_DELIMITER = '\uF749';
  
}
