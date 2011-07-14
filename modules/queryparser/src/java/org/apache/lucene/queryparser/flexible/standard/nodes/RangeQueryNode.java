package org.apache.lucene.queryparser.flexible.standard.nodes;

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

import org.apache.lucene.queryparser.flexible.core.nodes.ParametricQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ParametricRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.processors.ParametricRangeQueryNodeProcessor;

/**
 * This query node represents a range query. 
 * 
 * @see ParametricRangeQueryNodeProcessor
 * @see org.apache.lucene.search.TermRangeQuery
 */
public class RangeQueryNode extends ParametricRangeQueryNode {

  /**
   * @param lower
   * @param upper
   */
  public RangeQueryNode(ParametricQueryNode lower, ParametricQueryNode upper) {
    super(lower, upper);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("<range>\n\t");
    sb.append(this.getUpperBound()).append("\n\t");
    sb.append(this.getLowerBound()).append("\n");
    sb.append("</range>\n");

    return sb.toString();

  }
}
