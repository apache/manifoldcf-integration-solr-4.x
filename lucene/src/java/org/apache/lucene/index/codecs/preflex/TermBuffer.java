package org.apache.lucene.index.codecs.preflex;

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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.FieldInfos;

final class TermBuffer implements Cloneable {

  private String field;
  private Term term;                            // cached
  private boolean dirty;                          // true if text was set externally (ie not read via UTF8 bytes)

  private UnicodeUtil.UTF16Result text = new UnicodeUtil.UTF16Result();
  private BytesRef bytes = new BytesRef(10);

  int newSuffixStart;

  public final int compareTo(TermBuffer other) {
    if (field == other.field) 	  // fields are interned
      return compareChars(text.result, text.length, other.text.result, other.text.length);
    else
      return field.compareTo(other.field);
  }

  private static int compareChars(char[] chars1, int len1,
                                  char[] chars2, int len2) {
    final int end = len1 < len2 ? len1:len2;
    for (int k = 0; k < end; k++) {
      char c1 = chars1[k];
      char c2 = chars2[k];
      if (c1 != c2) {
        return c1 - c2;
      }
    }
    return len1 - len2;
  }

  public final void read(IndexInput input, FieldInfos fieldInfos)
    throws IOException {
    this.term = null;                           // invalidate cache
    int start = input.readVInt();
    int length = input.readVInt();
    int totalLength = start + length;
    if (bytes.bytes.length < totalLength) {
      bytes.grow(totalLength);
    }
    if (dirty) {
      // Fully convert all bytes since bytes is dirty
      UnicodeUtil.UTF16toUTF8(text.result, 0, text.length, bytes);
      bytes.length = totalLength;
      input.readBytes(bytes.bytes, start, length);
      UnicodeUtil.UTF8toUTF16(bytes.bytes, 0, totalLength, text);
      dirty = false;
    } else {
      // Incrementally convert only the UTF8 bytes that are new:
      bytes.length = totalLength;
      input.readBytes(bytes.bytes, start, length);
      UnicodeUtil.UTF8toUTF16(bytes.bytes, start, length, text);
    }

    while(true) {
      newSuffixStart = text.offsets[start];
      if (newSuffixStart != -1) {
        break;
      }
      if (--start == 0) {
        newSuffixStart = 0;
        break;
      }
    }
    this.field = fieldInfos.fieldName(input.readVInt());
  }

  public final void set(Term term) {
    if (term == null) {
      reset();
      return;
    }
    final String termText = term.text();
    final int termLen = termText.length();
    text.setLength(termLen);
    termText.getChars(0, termLen, text.result, 0);
    dirty = true;
    field = term.field();
    this.term = term;
  }

  public final void set(TermBuffer other) {
    text.copyText(other.text);
    dirty = true;
    field = other.field;
    term = other.term;
  }

  public void reset() {
    field = null;
    text.setLength(0);
    term = null;
    dirty = true;
  }

  public Term toTerm() {
    if (field == null)                            // unset
      return null;

    if (term == null)
      term = new Term(field, new String(text.result, 0, text.length), false);

    return term;
  }

  @Override
  protected Object clone() {
    TermBuffer clone = null;
    try {
      clone = (TermBuffer)super.clone();
    } catch (CloneNotSupportedException e) {}
    clone.dirty = true;
    clone.bytes = new BytesRef(10);
    clone.text = new UnicodeUtil.UTF16Result();
    clone.text.offsets = new int[text.offsets.length];
    System.arraycopy(text.offsets, 0, clone.text.offsets, 0, text.offsets.length);
    clone.text.copyText(text);
    return clone;
  }
}
