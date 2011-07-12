package org.apache.lucene.search;

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
import java.util.*;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexReader.ReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.search.Similarity.SloppyDocScorer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TermContext;
import org.apache.lucene.util.ToStringUtils;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.Bits;

/**
 * MultiPhraseQuery is a generalized version of PhraseQuery, with an added
 * method {@link #add(Term[])}.
 * To use this class, to search for the phrase "Microsoft app*" first use
 * add(Term) on the term "Microsoft", then find all terms that have "app" as
 * prefix using IndexReader.terms(Term), and use MultiPhraseQuery.add(Term[]
 * terms) to add them to the query.
 *
 */
public class MultiPhraseQuery extends Query {
  private String field;
  private ArrayList<Term[]> termArrays = new ArrayList<Term[]>();
  private ArrayList<Integer> positions = new ArrayList<Integer>();

  private int slop = 0;

  /** Sets the phrase slop for this query.
   * @see PhraseQuery#setSlop(int)
   */
  public void setSlop(int s) { slop = s; }

  /** Sets the phrase slop for this query.
   * @see PhraseQuery#getSlop()
   */
  public int getSlop() { return slop; }

  /** Add a single term at the next position in the phrase.
   * @see PhraseQuery#add(Term)
   */
  public void add(Term term) { add(new Term[]{term}); }

  /** Add multiple terms at the next position in the phrase.  Any of the terms
   * may match.
   *
   * @see PhraseQuery#add(Term)
   */
  public void add(Term[] terms) {
    int position = 0;
    if (positions.size() > 0)
      position = positions.get(positions.size()-1).intValue() + 1;

    add(terms, position);
  }

  /**
   * Allows to specify the relative position of terms within the phrase.
   * 
   * @see PhraseQuery#add(Term, int)
   * @param terms
   * @param position
   */
  public void add(Term[] terms, int position) {
    if (termArrays.size() == 0)
      field = terms[0].field();

    for (int i = 0; i < terms.length; i++) {
      if (!terms[i].field().equals(field)) {
        throw new IllegalArgumentException(
            "All phrase terms must be in the same field (" + field + "): "
                + terms[i]);
      }
    }

    termArrays.add(terms);
    positions.add(Integer.valueOf(position));
  }

  /**
   * Returns a List of the terms in the multiphrase.
   * Do not modify the List or its contents.
   */
  public List<Term[]> getTermArrays() {
	  return Collections.unmodifiableList(termArrays);
  }

  /**
   * Returns the relative positions of terms in this phrase.
   */
  public int[] getPositions() {
    int[] result = new int[positions.size()];
    for (int i = 0; i < positions.size(); i++)
      result[i] = positions.get(i).intValue();
    return result;
  }

  // inherit javadoc
  @Override
  public void extractTerms(Set<Term> terms) {
    for (final Term[] arr : termArrays) {
      for (final Term term: arr) {
        terms.add(term);
      }
    }
  }


  private class MultiPhraseWeight extends Weight {
    private final Similarity similarity;
    private final Similarity.Stats stats;

    public MultiPhraseWeight(IndexSearcher searcher)
      throws IOException {
      this.similarity = searcher.getSimilarityProvider().get(field);
      final ReaderContext context = searcher.getTopReaderContext();
      
      // compute idf
      ArrayList<TermContext> allTerms = new ArrayList<TermContext>();
      for(final Term[] terms: termArrays) {
        for (Term term: terms) {
          allTerms.add(TermContext.build(context, term, true));
        }
      }
      stats = similarity.computeStats(searcher, field, getBoost(), allTerms.toArray(new TermContext[allTerms.size()]));
    }

    @Override
    public Query getQuery() { return MultiPhraseQuery.this; }

    @Override
    public float getValueForNormalization() {
      return stats.getValueForNormalization();
    }

    @Override
    public void normalize(float queryNorm, float topLevelBoost) {
      stats.normalize(queryNorm, topLevelBoost);
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, ScorerContext scorerContext) throws IOException {
      if (termArrays.size() == 0)                  // optimize zero-term case
        return null;
      final IndexReader reader = context.reader;
      final Bits liveDocs = reader.getLiveDocs();
      
      PhraseQuery.PostingsAndFreq[] postingsFreqs = new PhraseQuery.PostingsAndFreq[termArrays.size()];

      for (int pos=0; pos<postingsFreqs.length; pos++) {
        Term[] terms = termArrays.get(pos);

        final DocsAndPositionsEnum postingsEnum;
        int docFreq;

        if (terms.length > 1) {
          postingsEnum = new UnionDocsAndPositionsEnum(reader, terms);

          // coarse -- this overcounts since a given doc can
          // have more than one terms:
          docFreq = 0;
          for(int termIdx=0;termIdx<terms.length;termIdx++) {
            docFreq += reader.docFreq(terms[termIdx]);
          }
        } else {
          final Term term = terms[0];
          postingsEnum = reader.termPositionsEnum(liveDocs,
                                                  term.field(),
                                                  term.bytes());

          if (postingsEnum == null) {
            if (reader.termDocsEnum(liveDocs, term.field(), term.bytes()) != null) {
              // term does exist, but has no positions
              throw new IllegalStateException("field \"" + term.field() + "\" was indexed without position data; cannot run PhraseQuery (term=" + term.text() + ")");
            } else {
              // term does not exist
              return null;
            }
          }

          docFreq = reader.docFreq(term.field(), term.bytes());
        }

        postingsFreqs[pos] = new PhraseQuery.PostingsAndFreq(postingsEnum, docFreq, positions.get(pos).intValue(), terms[0]);
      }

      // sort by increasing docFreq order
      if (slop == 0) {
        ArrayUtil.mergeSort(postingsFreqs);
      }

      if (slop == 0) {
        ExactPhraseScorer s = new ExactPhraseScorer(this, postingsFreqs, similarity.exactDocScorer(stats, field, context));
        if (s.noDocs) {
          return null;
        } else {
          return s;
        }
      } else {
        return new SloppyPhraseScorer(this, postingsFreqs, slop, similarity.sloppyDocScorer(stats, field, context));
      }
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      Scorer scorer = scorer(context, ScorerContext.def());
      if (scorer != null) {
        int newDoc = scorer.advance(doc);
        if (newDoc == doc) {
          float freq = scorer.freq();
          SloppyDocScorer docScorer = similarity.sloppyDocScorer(stats, field, context);
          ComplexExplanation result = new ComplexExplanation();
          result.setDescription("weight("+getQuery()+" in "+doc+") [" + similarity.getClass().getSimpleName() + "], result of:");
          Explanation scoreExplanation = docScorer.explain(doc, new Explanation(freq, "phraseFreq=" + freq));
          result.addDetail(scoreExplanation);
          result.setValue(scoreExplanation.getValue());
          result.setMatch(true);          
          return result;
        }
      }
      
      return new ComplexExplanation(false, 0.0f, "no matching term");
    }
  }

  @Override
  public Query rewrite(IndexReader reader) {
    if (termArrays.size() == 1) {                 // optimize one-term case
      Term[] terms = termArrays.get(0);
      BooleanQuery boq = new BooleanQuery(true);
      for (int i=0; i<terms.length; i++) {
        boq.add(new TermQuery(terms[i]), BooleanClause.Occur.SHOULD);
      }
      boq.setBoost(getBoost());
      return boq;
    } else {
      return this;
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new MultiPhraseWeight(searcher);
  }

  /** Prints a user-readable version of this query. */
  @Override
  public final String toString(String f) {
    StringBuilder buffer = new StringBuilder();
    if (field == null || !field.equals(f)) {
      buffer.append(field);
      buffer.append(":");
    }

    buffer.append("\"");
    Iterator<Term[]> i = termArrays.iterator();
    while (i.hasNext()) {
      Term[] terms = i.next();
      if (terms.length > 1) {
        buffer.append("(");
        for (int j = 0; j < terms.length; j++) {
          buffer.append(terms[j].text());
          if (j < terms.length-1)
            buffer.append(" ");
        }
        buffer.append(")");
      } else {
        buffer.append(terms[0].text());
      }
      if (i.hasNext())
        buffer.append(" ");
    }
    buffer.append("\"");

    if (slop != 0) {
      buffer.append("~");
      buffer.append(slop);
    }

    buffer.append(ToStringUtils.boost(getBoost()));

    return buffer.toString();
  }


  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MultiPhraseQuery)) return false;
    MultiPhraseQuery other = (MultiPhraseQuery)o;
    return this.getBoost() == other.getBoost()
      && this.slop == other.slop
      && termArraysEquals(this.termArrays, other.termArrays)
      && this.positions.equals(other.positions);
  }

  /** Returns a hash code value for this object.*/
  @Override
  public int hashCode() {
    return Float.floatToIntBits(getBoost())
      ^ slop
      ^ termArraysHashCode()
      ^ positions.hashCode()
      ^ 0x4AC65113;
  }
  
  // Breakout calculation of the termArrays hashcode
  private int termArraysHashCode() {
    int hashCode = 1;
    for (final Term[] termArray: termArrays) {
      hashCode = 31 * hashCode
          + (termArray == null ? 0 : Arrays.hashCode(termArray));
    }
    return hashCode;
  }

  // Breakout calculation of the termArrays equals
  private boolean termArraysEquals(List<Term[]> termArrays1, List<Term[]> termArrays2) {
    if (termArrays1.size() != termArrays2.size()) {
      return false;
    }
    ListIterator<Term[]> iterator1 = termArrays1.listIterator();
    ListIterator<Term[]> iterator2 = termArrays2.listIterator();
    while (iterator1.hasNext()) {
      Term[] termArray1 = iterator1.next();
      Term[] termArray2 = iterator2.next();
      if (!(termArray1 == null ? termArray2 == null : Arrays.equals(termArray1,
          termArray2))) {
        return false;
      }
    }
    return true;
  }
}

/**
 * Takes the logical union of multiple DocsEnum iterators.
 */

// TODO: if ever we allow subclassing of the *PhraseScorer
class UnionDocsAndPositionsEnum extends DocsAndPositionsEnum {

  private static final class DocsQueue extends PriorityQueue<DocsAndPositionsEnum> {
    DocsQueue(List<DocsAndPositionsEnum> docsEnums) throws IOException {
      super(docsEnums.size());

      Iterator<DocsAndPositionsEnum> i = docsEnums.iterator();
      while (i.hasNext()) {
        DocsAndPositionsEnum postings = i.next();
        if (postings.nextDoc() != DocsAndPositionsEnum.NO_MORE_DOCS) {
          add(postings);
        }
      }
    }

    final public DocsEnum peek() {
      return top();
    }

    @Override
    public final boolean lessThan(DocsAndPositionsEnum a, DocsAndPositionsEnum b) {
      return a.docID() < b.docID();
    }
  }

  private static final class IntQueue {
    private int _arraySize = 16;
    private int _index = 0;
    private int _lastIndex = 0;
    private int[] _array = new int[_arraySize];
    
    final void add(int i) {
      if (_lastIndex == _arraySize)
        growArray();

      _array[_lastIndex++] = i;
    }

    final int next() {
      return _array[_index++];
    }

    final void sort() {
      Arrays.sort(_array, _index, _lastIndex);
    }

    final void clear() {
      _index = 0;
      _lastIndex = 0;
    }

    final int size() {
      return (_lastIndex - _index);
    }

    private void growArray() {
      int[] newArray = new int[_arraySize * 2];
      System.arraycopy(_array, 0, newArray, 0, _arraySize);
      _array = newArray;
      _arraySize *= 2;
    }
  }

  private int _doc;
  private int _freq;
  private DocsQueue _queue;
  private IntQueue _posList;

  public UnionDocsAndPositionsEnum(IndexReader indexReader, Term[] terms) throws IOException {
    List<DocsAndPositionsEnum> docsEnums = new LinkedList<DocsAndPositionsEnum>();
    final Bits liveDocs = indexReader.getLiveDocs();
    for (int i = 0; i < terms.length; i++) {
      DocsAndPositionsEnum postings = indexReader.termPositionsEnum(liveDocs,
                                                                    terms[i].field(),
                                                                    terms[i].bytes());
      if (postings != null) {
        docsEnums.add(postings);
      } else {
        if (indexReader.termDocsEnum(liveDocs, terms[i].field(), terms[i].bytes()) != null) {
          // term does exist, but has no positions
          throw new IllegalStateException("field \"" + terms[i].field() + "\" was indexed without position data; cannot run PhraseQuery (term=" + terms[i].text() + ")");
        }
      }
    }

    _queue = new DocsQueue(docsEnums);
    _posList = new IntQueue();
  }

  @Override
  public final int nextDoc() throws IOException {
    if (_queue.size() == 0) {
      return NO_MORE_DOCS;
    }

    // TODO: move this init into positions(): if the search
    // doesn't need the positions for this doc then don't
    // waste CPU merging them:
    _posList.clear();
    _doc = _queue.top().docID();

    // merge sort all positions together
    DocsAndPositionsEnum postings;
    do {
      postings = _queue.top();

      final int freq = postings.freq();
      for (int i = 0; i < freq; i++) {
        _posList.add(postings.nextPosition());
      }

      if (postings.nextDoc() != NO_MORE_DOCS) {
        _queue.updateTop();
      } else {
        _queue.pop();
      }
    } while (_queue.size() > 0 && _queue.top().docID() == _doc);

    _posList.sort();
    _freq = _posList.size();

    return _doc;
  }

  @Override
  public int nextPosition() {
    return _posList.next();
  }

  @Override
  public BytesRef getPayload() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasPayload() {
    throw new UnsupportedOperationException();
  }

  @Override
  public final int advance(int target) throws IOException {
    while (_queue.top() != null && target > _queue.top().docID()) {
      DocsAndPositionsEnum postings = _queue.pop();
      if (postings.advance(target) != NO_MORE_DOCS) {
        _queue.add(postings);
      }
    }
    return nextDoc();
  }

  @Override
  public final int freq() {
    return _freq;
  }

  @Override
  public final int docID() {
    return _doc;
  }
}
