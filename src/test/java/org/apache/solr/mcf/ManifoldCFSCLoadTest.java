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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class ManifoldCFSCLoadTest extends SolrTestCaseJ4 {
  
  static MockMCFAuthorityService service;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-auth-load.xml","schema-auth.xml");
    service = new MockMCFAuthorityService();
    service.start();

    //             |     share    |   document
    //             |--------------|--------------
    //             | allow | deny | allow | deny
    // ------------+-------+------+-------+------
    // da12        |       |      | 1, 2  |
    // ------------+-------+------+-------+------
    // da13-dd3    |       |      | 1,3   | 3
    // ------------+-------+------+-------+------
    // sa123-sd13  | 1,2,3 | 1, 3 |       |
    // ------------+-------+------+-------+------
    // sa3-sd1-da23| 3     | 1    | 2,3   |
    // ------------+-------+------+-------+------
    // notoken     |       |      |       |
    // ------------+-------+------+-------+------
    //
    int i = 0;
    while (i < 1000)
    {
      assertU(adoc("id", "da12-"+i, "allow_token_document", "token1", "allow_token_document", "token2"));
      assertU(adoc("id", "da13-dd3-"+i, "allow_token_document", "token1", "allow_token_document", "token3", "deny_token_document", "token3"));
      assertU(adoc("id", "sa123-sd13-"+i, "allow_token_share", "token1", "allow_token_share", "token2", "allow_token_share", "token3", "deny_token_share", "token1", "deny_token_share", "token3"));
      assertU(adoc("id", "sa3-sd1-da23-"+i, "allow_token_document", "token2", "allow_token_document", "token3", "allow_token_share", "token3", "deny_token_share", "token1"));
      assertU(adoc("id", "notoken-"+i));
      i++;
    }
    assertU(optimize());
    assertU(commit());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    service.stop();
  }
  
  @Test
  public void testTimeQueries() throws Exception {
    int i = 0;
    long startTime = System.nanoTime();
    while (i < 1000)
    {
      assertQ(req("qt", "/mcf", "q", "*:*", "fl", "id", "AuthenticatedUserName", "user1"),
          "//*[@numFound='3000']");

      assertQ(req("qt", "/mcf", "q", "*:*", "fl", "id", "AuthenticatedUserName", "user2"),
          "//*[@numFound='3000']");

      assertQ(req("qt", "/mcf", "q", "*:*", "fl", "id", "AuthenticatedUserName", "user3"),
          "//*[@numFound='2000']");
      
      i++;
    }
    System.out.println("Query time (milliseconds) = " +  TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-startTime));
  }

  static class MockMCFAuthorityService {
    
    Server server;
    
    public MockMCFAuthorityService() {
      server = new Server(8346);
      ContextHandlerCollection contexts = new ContextHandlerCollection();
      server.setHandler(contexts);

      ServletContextHandler asContext = new ServletContextHandler(contexts,"/mcf-as",ServletContextHandler.SESSIONS);
      asContext.addServlet(new ServletHolder(new UserACLServlet()), "/UserACLs");
      contexts.addHandler(asContext);
    }
    
    public void start() throws Exception {
      server.start();
    }
    
    public void stop() throws Exception {
      server.stop();
    }

    // username | tokens rewarded
    // ---------+-------------------------------
    // null     | (no tokens)
    // user1    | token1
    // user2    | token1, token2
    // user3    | token1, token2, token3
    public static class UserACLServlet extends HttpServlet {
      @Override
      public void service(HttpServletRequest req, HttpServletResponse res)
          throws IOException {
        String user = req.getParameter("username");
        res.setStatus(HttpServletResponse.SC_OK);
        if(user.equals("user1") || user.equals("user2") || user.equals("user3"))
          res.getWriter().printf("TOKEN:token1\n");
        if(user.equals("user2") || user.equals("user3"))
          res.getWriter().printf("TOKEN:token2\n");
        if(user.equals("user3"))
          res.getWriter().printf("TOKEN:token3\n");
        int i = 0;
        while (i < 100)
        {
          res.getWriter().printf("TOKEN:dummy"+i+"\n");
          i++;
        }
      }
    }
  }
}
