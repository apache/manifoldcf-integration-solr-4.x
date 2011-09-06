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

package org.apache.solr.handler;

import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Update handler which uses the JavaBin format
 *
 *
 * @see org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec
 * @see org.apache.solr.common.util.JavaBinCodec
 * @since solr 1.4
 */
public class BinaryUpdateRequestHandler extends ContentStreamHandlerBase {


  @Override
  protected ContentStreamLoader newLoader(SolrQueryRequest req, final UpdateRequestProcessor processor) {
    return new ContentStreamLoader() {
      @Override
      public void load(SolrQueryRequest req, SolrQueryResponse rsp, ContentStream stream) throws Exception {
        InputStream is = null;
        try {
          is = stream.getStream();
          parseAndLoadDocs(req, rsp, is, processor);
        } finally {
          if(is != null) {
            is.close();
          }
        }
      }
    };
  }

  private void parseAndLoadDocs(final SolrQueryRequest req, SolrQueryResponse rsp, InputStream stream,
                                final UpdateRequestProcessor processor) throws IOException {
    UpdateRequest update = null;
    update = new JavaBinUpdateRequestCodec().unmarshal(stream,
            new JavaBinUpdateRequestCodec.StreamingDocumentHandler() {
              private AddUpdateCommand addCmd = null;

              public void document(SolrInputDocument document, UpdateRequest updateRequest) {
                if (addCmd == null) {
                  addCmd = getAddCommand(req, updateRequest.getParams());
                }
                addCmd.solrDoc = document;
                try {
                  processor.processAdd(addCmd);
                  addCmd.clear();
                } catch (IOException e) {
                  throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "ERROR adding document " + document);
                }
              }
            });
    if (update.getDeleteById() != null) {
      delete(req, update.getDeleteById(), processor, true);
    }
    if (update.getDeleteQuery() != null) {
      delete(req, update.getDeleteQuery(), processor, false);
    }

  }

  private AddUpdateCommand getAddCommand(SolrQueryRequest req, SolrParams params) {
    AddUpdateCommand addCmd = new AddUpdateCommand(req);

    addCmd.overwrite = params.getBool(UpdateParams.OVERWRITE, true);
    addCmd.commitWithin = params.getInt(UpdateParams.COMMIT_WITHIN, -1);
    return addCmd;
  }

  private void delete(SolrQueryRequest req, List<String> l, UpdateRequestProcessor processor, boolean isId) throws IOException {
    for (String s : l) {
      DeleteUpdateCommand delcmd = new DeleteUpdateCommand(req);
      if (isId) {
        delcmd.id = s;
      } else {
        delcmd.query = s;
      }
      processor.processDelete(delcmd);
    }
  }

  @Override
  public String getDescription() {
    return "Add/Update multiple documents with javabin format";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }

  @Override
  public String getVersion() {
    return "$Revision$";
  }
}
