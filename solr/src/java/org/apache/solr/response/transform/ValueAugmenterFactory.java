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
package org.apache.solr.response.transform;

import java.util.Map;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

/**
 * @version $Id$
 * @since solr 4.0
 */
public class ValueAugmenterFactory extends TransformerFactory
{
  protected Object value = null;
  protected Object defaultValue = null;

  @Override
  public void init(NamedList args) {
    value = args.get( "value" );
    if( value == null ) {
      defaultValue = args.get( "deaultValue" );
    }
  }

  public static Object getObjectFrom( String val, String type )
  {
    if( type != null ) {
      try {
        if( "int".equals( type ) ) return Integer.valueOf( val );
        if( "double".equals( type ) ) return Double.valueOf( val );
        if( "float".equals( type ) ) return Float.valueOf( val );
        if( "date".equals( type ) ) return DateUtil.parseDate(val);
      }
      catch( Exception ex ) {
        throw new SolrException( ErrorCode.BAD_REQUEST,
            "Unable to parse "+type+"="+val, ex );
      }
    }
    return val;
  }

  @Override
  public DocTransformer create(String field, Map<String,String> args, SolrQueryRequest req) {
    Object val = value;
    if( val == null ) {
      String v = args.get("v");
      if( v == null ) {
        val = defaultValue;
      }
      else {
        val = getObjectFrom(v, args.get("t"));
      }
      if( val == null ) {
        throw new SolrException( ErrorCode.BAD_REQUEST,
            "ValueAugmenter is missing a value -- should be defined in solrconfig or inline" );
      }
    }
    return new ValueAugmenter( field, val );
  }
}

class ValueAugmenter extends DocTransformer
{
  final String name;
  final Object value;

  public ValueAugmenter( String name, Object value )
  {
    this.name = name;
    this.value = value;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void transform(SolrDocument doc, int docid) {
    doc.setField( name, value );
  }
}

