/**
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.solr.mcf;

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.queries.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QParser;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.slf4j.*;

import java.io.*;
import java.util.*;
import java.net.*;

/**
* Query parser plugin for ManifoldCF-specific document-level access control.
*/
public class ManifoldCFQParserPlugin extends QParserPlugin
{
  /** The component name */
  static final public String COMPONENT_NAME = "mcf";
  /** The parameter that is supposed to contain the authenticated user name, possibly including the domain */
  static final public String AUTHENTICATED_USER_NAME = "AuthenticatedUserName";
  /** This parameter is an array of strings, which contain the tokens to use if there is no authenticated user name.
   * It's meant to work with mod_authz_annotate,
   * running under Apache */
  static final public String USER_TOKENS = "UserTokens";
  
  /** Special token for null security fields */
  static final public String NOSECURITY_TOKEN = "__nosecurity__";
  
  /** The queries that we will not attempt to interfere with */
  static final private String[] globalAllowed = { "solrpingquery" };
  
  /** A logger we can use */
  private static final Logger LOG = LoggerFactory.getLogger(ManifoldCFQParserPlugin.class);

  // Member variables
  String authorityBaseURL = null;
  String fieldAllowDocument = null;
  String fieldDenyDocument = null;
  String fieldAllowShare = null;
  String fieldDenyShare = null;
  int socketTimeOut;
  
  public ManifoldCFQParserPlugin()
  {
    super();
  }

  @Override
  public void init(NamedList args)
  {
    authorityBaseURL = (String)args.get("AuthorityServiceBaseURL");
    if (authorityBaseURL == null)
      authorityBaseURL = "http://localhost:8345/mcf-authority-service";
    Integer timeOut = (Integer)args.get("SocketTimeOut");
    socketTimeOut = timeOut == null ? 300000 : timeOut;
    String allowAttributePrefix = (String)args.get("AllowAttributePrefix");
    String denyAttributePrefix = (String)args.get("DenyAttributePrefix");
    if (allowAttributePrefix == null)
      allowAttributePrefix = "allow_token_";
    if (denyAttributePrefix == null)
      denyAttributePrefix = "deny_token_";
    fieldAllowDocument = allowAttributePrefix+"document";
    fieldDenyDocument = denyAttributePrefix+"document";
    fieldAllowShare = allowAttributePrefix+"share";
    fieldDenyShare = denyAttributePrefix+"share";
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req)
  {
    return new ManifoldCFQueryParser(qstr,localParams,params,req);
  }

  protected class ManifoldCFQueryParser extends QParser
  {
    public ManifoldCFQueryParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req)
    {
      super(qstr,localParams,params,req);
    }
        
    @Override
    /** Create and return the <code>Query</code> object represented by <code>qstr</code>
    * @see #getQuery()
    **/
    public Query parse() throws ParseException
    {
      SolrParams params = req.getParams();

      List<String> userAccessTokens;
      
      // Get the authenticated user name from the parameters
      String authenticatedUserName = params.get(AUTHENTICATED_USER_NAME);
      
      // If this parameter is empty or does not exist, we have to presume this is a guest, and treat them accordingly
      if (authenticatedUserName == null || authenticatedUserName.length() == 0)
      {
        // No authenticated user name.
        // mod_authz_annotate may be in use upstream, so look for tokens from it.
        userAccessTokens = new ArrayList<String>();
        String[] passedTokens = params.getParams(USER_TOKENS);
        if (passedTokens == null)
        {
          // Only return 'public' documents (those with no security tokens at all)
          LOG.info("Default no-user response (open documents only)");
        }
        else
        {
          // Only return 'public' documents (those with no security tokens at all)
          LOG.info("Group tokens received from caller");
          for (String passedToken : passedTokens)
          {
            userAccessTokens.add(passedToken);
          }
        }
      }
      else
      {
        LOG.info("Trying to match docs for user '"+authenticatedUserName+"'");
        // Valid authenticated user name.  Look up access tokens for the user.
        // Check the configuration arguments for validity
        if (authorityBaseURL == null)
        {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error initializing ManifoldCFSecurityFilter component: 'AuthorityServiceBaseURL' init parameter required");
        }
        try
        {
          userAccessTokens = getAccessTokens(authenticatedUserName);
        }
        catch (IOException e)
        {
          LOG.error("IO exception communicating with MCF authority service: "+e.getMessage(),e);
          throw new ParseException("IO exception communicating with MCF authority service: "+e.getMessage());
        }
      }

      BooleanQuery bq = new BooleanQuery();
      //bf.setMaxClauseCount(100000);
      
      Query allowShareOpen = new TermQuery(new Term(fieldAllowShare,NOSECURITY_TOKEN));
      Query denyShareOpen = new WildcardQuery(new Term(fieldDenyShare,NOSECURITY_TOKEN));
      Query allowDocumentOpen = new WildcardQuery(new Term(fieldAllowDocument,NOSECURITY_TOKEN));
      Query denyDocumentOpen = new WildcardQuery(new Term(fieldDenyDocument,NOSECURITY_TOKEN));
      
      if (userAccessTokens.size() == 0)
      {
        // Only open documents can be included.
        // That query is:
        // (fieldAllowShare is empty AND fieldDenyShare is empty AND fieldAllowDocument is empty AND fieldDenyDocument is empty)
        // We're trying to map to:  -(fieldAllowShare:*) , which should be pretty efficient in Solr because it is negated.  If this turns out not to be so, then we should
        // have the SolrConnector inject a special token into these fields when they otherwise would be empty, and we can trivially match on that token.
        bq.add(allowShareOpen,BooleanClause.Occur.MUST);
        bq.add(denyShareOpen,BooleanClause.Occur.MUST);
        bq.add(allowDocumentOpen,BooleanClause.Occur.MUST);
        bq.add(denyDocumentOpen,BooleanClause.Occur.MUST);
      }
      else
      {
        // Extend the query appropriately for each user access token.
        bq.add(calculateCompleteSubquery(fieldAllowShare,fieldDenyShare,allowShareOpen,denyShareOpen,userAccessTokens),
          BooleanClause.Occur.MUST);
        bq.add(calculateCompleteSubquery(fieldAllowDocument,fieldDenyDocument,allowDocumentOpen,denyDocumentOpen,userAccessTokens),
          BooleanClause.Occur.MUST);
      }

      return new ConstantScoreQuery(bq);
    }

    /** Calculate a complete subclause, representing something like:
    * ((fieldAllowShare is empty AND fieldDenyShare is empty) OR fieldAllowShare HAS token1 OR fieldAllowShare HAS token2 ...)
    *     AND fieldDenyShare DOESN'T_HAVE token1 AND fieldDenyShare DOESN'T_HAVE token2 ...
    */
    protected Query calculateCompleteSubquery(String allowField, String denyField, Query allowOpen, Query denyOpen, List<String> userAccessTokens)
    {
      BooleanQuery bq = new BooleanQuery();
      bq.setMaxClauseCount(1000000);
      
      // Add the empty-acl case
      BooleanQuery subUnprotectedClause = new BooleanQuery();
      subUnprotectedClause.add(new MatchAllDocsQuery(),BooleanClause.Occur.SHOULD);
      subUnprotectedClause.add(allowOpen,BooleanClause.Occur.MUST);
      subUnprotectedClause.add(denyOpen,BooleanClause.Occur.MUST);
      bq.add(subUnprotectedClause,BooleanClause.Occur.SHOULD);
      for (String accessToken : userAccessTokens)
      {
        bq.add(new TermQuery(new Term(allowField,accessToken)),BooleanClause.Occur.SHOULD);
        bq.add(new TermQuery(new Term(denyField,accessToken)),BooleanClause.Occur.MUST_NOT);
      }
      return bq;
    }
    
    // Protected methods
    
    /** Get access tokens given a username */
    protected List<String> getAccessTokens(String authenticatedUserName)
      throws IOException
    {
      // We can make this more complicated later, with support for https etc., but this is enough to demonstrate how it all should work.
      HttpClient client = new HttpClient();
      String theURL = authorityBaseURL + "/UserACLs?username="+URLEncoder.encode(authenticatedUserName,"utf-8");
        
      GetMethod method = new GetMethod(theURL);
      try
      {
        method.getParams().setParameter("http.socket.timeout", socketTimeOut);
        method.setFollowRedirects(true);
        int rval = client.executeMethod(method);
        if (rval != 200)
        {
          String response = method.getResponseBodyAsString();
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"Couldn't fetch user's access tokens from ManifoldCF authority service: "+Integer.toString(rval)+"; "+response);
        }
        InputStream is = method.getResponseBodyAsStream();
        try
        {
          Reader r = new InputStreamReader(is,"utf-8");
          try
          {
            BufferedReader br = new BufferedReader(r);
            try
            {
              // Read the tokens, one line at a time.  If any authorities are down, we have no current way to note that, but someday we will.
              List<String> tokenList = new ArrayList<String>();
              while (true)
              {
                String line = br.readLine();
                if (line == null)
                  break;
                if (line.startsWith("TOKEN:"))
                {
                  tokenList.add(line.substring("TOKEN:".length()));
                }
                else
                {
                  // It probably says something about the state of the authority(s) involved, so log it
                  LOG.info("For user '"+authenticatedUserName+"', saw authority response "+line);
                }
              }
              return tokenList;
            }
            finally
            {
              br.close();
            }
          }
          finally
          {
            r.close();
          }
        }
        finally
        {
          is.close();
        }
      }
      finally
      {
        method.releaseConnection();
      }
    }
  }
  
}