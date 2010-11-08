package org.apache.lucene.index.codecs.simpletext;

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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.UnicodeUtil;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.SortedMap;
import java.util.Iterator;

class SimpleTextFieldsReader extends FieldsProducer {

  private final IndexInput in;
  private final FieldInfos fieldInfos;

  final static byte NEWLINE     = SimpleTextFieldsWriter.NEWLINE;
  final static byte ESCAPE      = SimpleTextFieldsWriter.ESCAPE;
  final static BytesRef END     = SimpleTextFieldsWriter.END;
  final static BytesRef FIELD   = SimpleTextFieldsWriter.FIELD;
  final static BytesRef TERM    = SimpleTextFieldsWriter.TERM;
  final static BytesRef DOC     = SimpleTextFieldsWriter.DOC;
  final static BytesRef POS     = SimpleTextFieldsWriter.POS;
  final static BytesRef PAYLOAD = SimpleTextFieldsWriter.PAYLOAD;

  public SimpleTextFieldsReader(SegmentReadState state) throws IOException {
    in = state.dir.openInput(SimpleTextCodec.getPostingsFileName(state.segmentInfo.name));
    fieldInfos = state.fieldInfos;
  }

  static void readLine(IndexInput in, BytesRef scratch) throws IOException {
    int upto = 0;
    while(true) {
      byte b = in.readByte();
      if (scratch.bytes.length == upto) {
        scratch.grow(1+upto);
      }
      if (b == ESCAPE) {
        scratch.bytes[upto++] = in.readByte();
      } else {
        if (b == NEWLINE) {
          break;
        } else {
          scratch.bytes[upto++] = b;
        }
      }
    }
    scratch.offset = 0;
    scratch.length = upto;
  }

  private class SimpleTextFieldsEnum extends FieldsEnum {
    private final IndexInput in;
    private final BytesRef scratch = new BytesRef(10);
    private String current;

    public SimpleTextFieldsEnum() {
      this.in = (IndexInput) SimpleTextFieldsReader.this.in.clone();
    }

    @Override
    public String next() throws IOException {
      while(true) {
        readLine(in, scratch);
        if (scratch.equals(END)) {
          current = null;
          return null;
        }
        if (scratch.startsWith(FIELD)) {
          String field = StringHelper.intern(new String(scratch.bytes, scratch.offset + FIELD.length, scratch.length - FIELD.length, "UTF-8"));
          current = field;
          return field;
        }
      }
    }

    @Override
    public TermsEnum terms() throws IOException {
      return SimpleTextFieldsReader.this.terms(current).iterator();
    }
  }

  private class SimpleTextTermsEnum extends TermsEnum {
    private final IndexInput in;
    private final boolean omitTF;
    private BytesRef current;
    private int docFreq;
    private long docsStart;
    private boolean ended;
    private final TreeMap<BytesRef,TermData> allTerms;
    private Iterator<Map.Entry<BytesRef,TermData>> iter;

    public SimpleTextTermsEnum(TreeMap<BytesRef,TermData> allTerms, boolean omitTF) throws IOException {
      this.in = (IndexInput) SimpleTextFieldsReader.this.in.clone();
      this.allTerms = allTerms;
      this.omitTF = omitTF;
      iter = allTerms.entrySet().iterator();
    }

    public SeekStatus seek(BytesRef text, boolean useCache /* ignored */) throws IOException {
      
      final SortedMap<BytesRef,TermData> tailMap = allTerms.tailMap(text);

      if (tailMap.isEmpty()) {
        current = null;
        return SeekStatus.END;
      } else {
        current = tailMap.firstKey();
        final TermData td = tailMap.get(current);
        docsStart = td.docsStart;
        docFreq = td.docFreq;
        iter = tailMap.entrySet().iterator();
        assert iter.hasNext();
        iter.next();
        if (current.equals(text)) {
          return SeekStatus.FOUND;
        } else {
          return SeekStatus.NOT_FOUND;
        }
      }

      /*
      if (current != null) {
        final int cmp = current.compareTo(text);
        if (cmp == 0) {
          return SeekStatus.FOUND;
        } else if (cmp > 0) {
          ended = false;
          in.seek(fieldStart);
        }
      } else {
        ended = false;
        in.seek(fieldStart);
      }

      // Naive!!  This just scans... would be better to do
      // up-front scan to build in-RAM index
      BytesRef b;
      while((b = next()) != null) {
        final int cmp = b.compareTo(text);
        if (cmp == 0) {
          ended = false;
          return SeekStatus.FOUND;
        } else if (cmp > 0) {
          ended = false;
          return SeekStatus.NOT_FOUND;
        }
      }
      current = null;
      ended = true;
      return SeekStatus.END;
      */
    }

    @Override
    public void cacheCurrentTerm() {
    }

    @Override
    public BytesRef next() throws IOException {
      assert !ended;

      if (iter.hasNext()) {
        Map.Entry<BytesRef,TermData> ent = iter.next();
        current = ent.getKey();
        TermData td = ent.getValue();
        docFreq = td.docFreq;
        docsStart = td.docsStart;
        return current;
      } else {
        current = null;
        return null;
      }

      /*
      readLine(in, scratch);
      if (scratch.equals(END) || scratch.startsWith(FIELD)) {
        ended = true;
        current = null;
        return null;
      } else {
        assert scratch.startsWith(TERM): "got " + scratch.utf8ToString();
        docsStart = in.getFilePointer();
        final int len = scratch.length - TERM.length;
        if (len > scratch2.length) {
          scratch2.grow(len);
        }
        System.arraycopy(scratch.bytes, TERM.length, scratch2.bytes, 0, len);
        scratch2.length = len;
        current = scratch2;
        docFreq = 0;
        long lineStart = 0;
        while(true) {
          lineStart = in.getFilePointer();
          readLine(in, scratch);
          if (scratch.equals(END) || scratch.startsWith(FIELD) || scratch.startsWith(TERM)) {
            break;
          }
          if (scratch.startsWith(DOC)) {
            docFreq++;
          }
        }
        in.seek(lineStart);
        return current;
      }
      */
    }

    @Override
    public BytesRef term() {
      return current;
    }

    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SeekStatus seek(long ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() {
      return docFreq;
    }

    @Override
    public DocsEnum docs(Bits skipDocs, DocsEnum reuse) throws IOException {
      SimpleTextDocsEnum docsEnum;
      if (reuse != null && reuse instanceof SimpleTextDocsEnum && ((SimpleTextDocsEnum) reuse).canReuse(in)) {
        docsEnum = (SimpleTextDocsEnum) reuse;
      } else {
        docsEnum = new SimpleTextDocsEnum();
      }
      return docsEnum.reset(docsStart, skipDocs, omitTF);
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
      if (omitTF) {
        return null;
      }

      SimpleTextDocsAndPositionsEnum docsAndPositionsEnum;
      if (reuse != null && reuse instanceof SimpleTextDocsAndPositionsEnum && ((SimpleTextDocsAndPositionsEnum) reuse).canReuse(in)) {
        docsAndPositionsEnum = (SimpleTextDocsAndPositionsEnum) reuse;
      } else {
        docsAndPositionsEnum = new SimpleTextDocsAndPositionsEnum();
      } 
      return docsAndPositionsEnum.reset(docsStart, skipDocs);
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }
  }

  private class SimpleTextDocsEnum extends DocsEnum {
    private final IndexInput inStart;
    private final IndexInput in;
    private boolean omitTF;
    private int docID;
    private int tf;
    private Bits skipDocs;
    private final BytesRef scratch = new BytesRef(10);
    private final UnicodeUtil.UTF16Result scratchUTF16 = new UnicodeUtil.UTF16Result();
    
    public SimpleTextDocsEnum() {
      this.inStart = SimpleTextFieldsReader.this.in;
      this.in = (IndexInput) this.inStart.clone();
    }

    public boolean canReuse(IndexInput in) {
      return in == inStart;
    }

    public SimpleTextDocsEnum reset(long fp, Bits skipDocs, boolean omitTF) throws IOException {
      this.skipDocs = skipDocs;
      in.seek(fp);
      this.omitTF = omitTF;
      if (omitTF) {
        tf = 1;
      }
      return this;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int freq() {
      return tf;
    }

    @Override
    public int nextDoc() throws IOException {
      if (docID == NO_MORE_DOCS) {
        return docID;
      }
      boolean first = true;
      int termFreq = 0;
      while(true) {
        final long lineStart = in.getFilePointer();
        readLine(in, scratch);
        if (scratch.startsWith(DOC)) {
          if (!first && (skipDocs == null || !skipDocs.get(docID))) {
            in.seek(lineStart);
            if (!omitTF) {
              tf = termFreq;
            }
            return docID;
          }
          UnicodeUtil.UTF8toUTF16(scratch.bytes, scratch.offset+DOC.length, scratch.length-DOC.length, scratchUTF16);
          docID = ArrayUtil.parseInt(scratchUTF16.result, 0, scratchUTF16.length);
          termFreq = 0;
          first = false;
        } else if (scratch.startsWith(POS)) {
          termFreq++;
        } else if (scratch.startsWith(PAYLOAD)) {
          // skip
        } else {
          assert scratch.startsWith(TERM) || scratch.startsWith(FIELD) || scratch.startsWith(END): "scratch=" + scratch.utf8ToString();
          if (!first && (skipDocs == null || !skipDocs.get(docID))) {
            in.seek(lineStart);
            if (!omitTF) {
              tf = termFreq;
            }
            return docID;
          }
          return docID = NO_MORE_DOCS;
        }
      }
    }

    @Override
    public int advance(int target) throws IOException {
      // Naive -- better to index skip data
      while(nextDoc() < target);
      return docID;
    }
  }

  private class SimpleTextDocsAndPositionsEnum extends DocsAndPositionsEnum {
    private final IndexInput inStart;
    private final IndexInput in;
    private int docID;
    private int tf;
    private Bits skipDocs;
    private final BytesRef scratch = new BytesRef(10);
    private final BytesRef scratch2 = new BytesRef(10);
    private final UnicodeUtil.UTF16Result scratchUTF16 = new UnicodeUtil.UTF16Result();
    private final UnicodeUtil.UTF16Result scratchUTF16_2 = new UnicodeUtil.UTF16Result();
    private BytesRef payload;
    private long nextDocStart;

    public SimpleTextDocsAndPositionsEnum() {
      this.inStart = SimpleTextFieldsReader.this.in;
      this.in = (IndexInput) inStart.clone();
    }

    public boolean canReuse(IndexInput in) {
      return in == inStart;
    }

    public SimpleTextDocsAndPositionsEnum reset(long fp, Bits skipDocs) {
      this.skipDocs = skipDocs;
      nextDocStart = fp;
      return this;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int freq() {
      return tf;
    }

    @Override
    public int nextDoc() throws IOException {
      boolean first = true;
      in.seek(nextDocStart);
      long posStart = 0;
      while(true) {
        final long lineStart = in.getFilePointer();
        readLine(in, scratch);
        if (scratch.startsWith(DOC)) {
          if (!first && (skipDocs == null || !skipDocs.get(docID))) {
            nextDocStart = lineStart;
            in.seek(posStart);
            return docID;
          }
          UnicodeUtil.UTF8toUTF16(scratch.bytes, scratch.offset+DOC.length, scratch.length-DOC.length, scratchUTF16);
          docID = ArrayUtil.parseInt(scratchUTF16.result, 0, scratchUTF16.length);
          tf = 0;
          posStart = in.getFilePointer();
          first = false;
        } else if (scratch.startsWith(POS)) {
          tf++;
        } else if (scratch.startsWith(PAYLOAD)) {
          // skip
        } else {
          assert scratch.startsWith(TERM) || scratch.startsWith(FIELD) || scratch.startsWith(END);
          if (!first && (skipDocs == null || !skipDocs.get(docID))) {
            nextDocStart = lineStart;
            in.seek(posStart);
            return docID;
          }
          return docID = NO_MORE_DOCS;
        }
      }
    }

    @Override
    public int advance(int target) throws IOException {
      // Naive -- better to index skip data
      while(nextDoc() < target);
      return docID;
    }

    @Override
    public int nextPosition() throws IOException {
      readLine(in, scratch);
      assert scratch.startsWith(POS): "got line=" + scratch.utf8ToString();
      UnicodeUtil.UTF8toUTF16(scratch.bytes, scratch.offset+POS.length, scratch.length-POS.length, scratchUTF16_2);
      final int pos = ArrayUtil.parseInt(scratchUTF16_2.result, 0, scratchUTF16_2.length);
      final long fp = in.getFilePointer();
      readLine(in, scratch);
      if (scratch.startsWith(PAYLOAD)) {
        final int len = scratch.length - PAYLOAD.length;
        if (scratch2.bytes.length < len) {
          scratch2.grow(len);
        }
        System.arraycopy(scratch.bytes, PAYLOAD.length, scratch2.bytes, 0, len);
        scratch2.length = len;
        payload = scratch2;
      } else {
        payload = null;
        in.seek(fp);
      }
      return pos;
    }

    @Override
    public BytesRef getPayload() {
      // Some tests rely on only being able to retrieve the
      // payload once
      try {
        return payload;
      } finally {
        payload = null;
      }
    }

    @Override
    public boolean hasPayload() {
      return payload != null;
    }
  }

  static class TermData {
    public long docsStart;
    public int docFreq;

    public TermData(long docsStart, int docFreq) {
      this.docsStart = docsStart;
      this.docFreq = docFreq;
    }
  }

  private class SimpleTextTerms extends Terms {
    private final String field;
    private final long termsStart;
    private final boolean omitTF;

    // NOTE: horribly, horribly RAM consuming, but then
    // SimpleText should never be used in production
    private final TreeMap<BytesRef,TermData> allTerms = new TreeMap<BytesRef,TermData>();

    private final BytesRef scratch = new BytesRef(10);

    public SimpleTextTerms(String field, long termsStart) throws IOException {
      this.field = StringHelper.intern(field);
      this.termsStart = termsStart;
      omitTF = fieldInfos.fieldInfo(field).omitTermFreqAndPositions;
      loadTerms();
    }

    private void loadTerms() throws IOException {
      IndexInput in = (IndexInput) SimpleTextFieldsReader.this.in.clone();
      in.seek(termsStart);
      final BytesRef lastTerm = new BytesRef(10);
      long lastDocsStart = -1;
      int docFreq = 0;
      while(true) {
        readLine(in, scratch);
        if (scratch.equals(END) || scratch.startsWith(FIELD)) {
          if (lastDocsStart != -1) {
            allTerms.put(new BytesRef(lastTerm),
                         new TermData(lastDocsStart, docFreq));
          }
          break;
        } else if (scratch.startsWith(DOC)) {
          docFreq++;
        } else if (scratch.startsWith(TERM)) {
          if (lastDocsStart != -1) {
            allTerms.put(new BytesRef(lastTerm),
                         new TermData(lastDocsStart, docFreq));
          }
          lastDocsStart = in.getFilePointer();
          final int len = scratch.length - TERM.length;
          if (len > lastTerm.length) {
            lastTerm.grow(len);
          }
          System.arraycopy(scratch.bytes, TERM.length, lastTerm.bytes, 0, len);
          lastTerm.length = len;
          docFreq = 0;
        }
      }
    }

    @Override
    public TermsEnum iterator() throws IOException {
      return new SimpleTextTermsEnum(allTerms, omitTF);
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }
  }

  @Override
  public FieldsEnum iterator() throws IOException {
    return new SimpleTextFieldsEnum();
  }

  private final Map<String,Terms> termsCache = new HashMap<String,Terms>();

  @Override
  synchronized public Terms terms(String field) throws IOException {
    Terms terms = termsCache.get(field);
    if (terms == null) {
      SimpleTextFieldsEnum fe = (SimpleTextFieldsEnum) iterator();
      String fieldUpto;
      while((fieldUpto = fe.next()) != null) {
        if (fieldUpto.equals(field)) {
          terms = new SimpleTextTerms(field, fe.in.getFilePointer());
          break;
        }
      }
      termsCache.put(field, terms);
    }
    return terms;
  }

  @Override
  public void loadTermsIndex(int indexDivisor) {
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
