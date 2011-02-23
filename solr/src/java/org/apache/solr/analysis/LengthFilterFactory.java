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

package org.apache.solr.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;

import java.util.Map;

/**
 * Factory for {@link LengthFilter}. 
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_lngth" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.LengthFilterFactory" min="0" max="1" enablePositionIncrements="false"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre> 
 * @version $Id$
 */
public class LengthFilterFactory extends BaseTokenFilterFactory {
  int min,max;
  boolean enablePositionIncrements;
  public static final String MIN_KEY = "min";
  public static final String MAX_KEY = "max";

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    min=Integer.parseInt(args.get(MIN_KEY));
    max=Integer.parseInt(args.get(MAX_KEY));
    enablePositionIncrements = getBoolean("enablePositionIncrements",false);
  }
  
  public LengthFilter create(TokenStream input) {
    return new LengthFilter(enablePositionIncrements, input,min,max);
  }
}
