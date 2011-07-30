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

package org.apache.solr.update;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

/**
 *
 */
public class DeleteUpdateCommand extends UpdateCommand {
  public String id;    // external (printable) id, for delete-by-id
  public String query; // query string for delete-by-query
  private BytesRef indexedId;


  public DeleteUpdateCommand(SolrQueryRequest req) {
    super("delete", req);
  }

  public void clear() {
    id = null;
    query = null;
    indexedId = null;
  }

  /** Returns the indexed ID for this delete.  The returned BytesRef is retained across multiple calls, and should not be modified. */
  public BytesRef getIndexedId() {
    if (indexedId == null) {
      IndexSchema schema = req.getSchema();
      SchemaField sf = schema.getUniqueKeyField();
      if (sf != null && id != null) {
        indexedId = new BytesRef();
        sf.getType().readableToIndexed(id, indexedId);
      }
    }
    return indexedId;
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(commandName);
    sb.append(':');
    if (id!=null) sb.append("id=").append(id);
    else sb.append("query=`").append(query).append('`');
    return sb.toString();
  }
}
