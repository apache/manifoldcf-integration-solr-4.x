package org.apache.lucene.search.cache;

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

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCache.Parser;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;

public abstract class CachedArrayCreator<T extends CachedArray> extends EntryCreatorWithOptions<T>
{
  public static final int OPTION_VALIDATE     = 1;
  public static final int OPTION_CACHE_VALUES = 2;
  public static final int OPTION_CACHE_BITS   = 4;
  
  // Composite Options Fields
  public static final int CACHE_VALUES_AND_BITS = OPTION_CACHE_VALUES ^ OPTION_CACHE_BITS;
  public static final int CACHE_VALUES_AND_BITS_VALIDATE = OPTION_CACHE_VALUES ^ OPTION_CACHE_BITS ^ OPTION_VALIDATE;

  public final String field;

  public CachedArrayCreator( String field )
  {
    super( OPTION_CACHE_VALUES ^ OPTION_VALIDATE );
    if( field == null ) {
      throw new IllegalArgumentException( "field can not be null" );
    }
    this.field = field;
  }

  public CachedArrayCreator( String field, int flags )
  {
    super( flags );
    if( field == null ) {
      throw new IllegalArgumentException( "field can not be null" );
    }
    this.field = field;
  }

  /**
   * Note that the 'flags' are not part of the key -- subsequent calls to the cache
   * with different options will use the same cache entry.
   */
  @Override
  public EntryKey getCacheKey() {
    return new SimpleEntryKey( CachedArray.class, getArrayType(), field );
    //return new Integer( CachedArrayCreator.class.hashCode() ^ getArrayType().hashCode() ^ field.hashCode() );
  }
  
  /** Return the type that the array will hold */
  public abstract Class getArrayType();
  public abstract Parser getParser();
  public abstract int getSortTypeID();

  protected void assertSameParserAndResetCounts(T value, Parser parser)
  {
    int parserHashCode = parser.hashCode();
    if( value.parserHashCode != null && value.parserHashCode != parserHashCode ) {
      throw new RuntimeException( "Parser changed in subsequet call.  "
          +value.parserHashCode+" != "+parserHashCode + " :: " + parser );
    }
    value.parserHashCode = parserHashCode;
    value.numDocs = value.numTerms = 0;
  }

  /**
   * Utility function to help check what bits are valid
   */
  protected Bits checkMatchAllBits( Bits deleted, OpenBitSet valid, int maxDocs, int numDocs )
  {
    if( numDocs != maxDocs ) {
      if( hasOption( OPTION_CACHE_BITS ) ) {
        if( deleted == null ) {
          for( int i=0; i<maxDocs; i++ ) {
            if( !valid.get(i) ) {
              return valid;
            }
          }
        }
        else {
          for( int i=0; i<maxDocs; i++ ) {
            if( !deleted.get(i) && !valid.get(i) ) {
              return valid;
            }
          }
        }
      }
      else {
        return null;
      }
    }
    return new Bits.MatchAllBits( maxDocs );
  }

  public void fillValidBits( T vals, IndexReader reader, String field ) throws IOException
  {
    vals.numDocs = vals.numTerms = 0;
    Terms terms = MultiFields.getTerms(reader, field);
    if (terms != null) {
      final TermsEnum termsEnum = terms.iterator();
      final Bits delDocs = MultiFields.getDeletedDocs(reader);
      OpenBitSet validBits = new OpenBitSet( reader.maxDoc() );
      DocsEnum docs = null;
      while(true) {
        final BytesRef term = termsEnum.next();
        if (term == null) {
          break;
        }
        docs = termsEnum.docs(delDocs, docs);
        while (true) {
          final int docID = docs.nextDoc();
          if (docID == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
          validBits.set( docID );
          vals.numDocs++;
        }
        vals.numTerms++;
      }

      vals.valid = checkMatchAllBits( delDocs, validBits, vals.numDocs, reader.maxDoc() );
    }
    if( vals.numDocs < 1 ) {
      vals.valid = new Bits.MatchNoBits( reader.maxDoc() );
    }
  }
}
