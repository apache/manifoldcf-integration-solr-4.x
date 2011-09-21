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

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public class ManifoldCFSecurityFilterTest extends SolrTestCaseJ4 {
  
  static MockMCFAuthorityService service;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-auth.xml","schema-auth.xml");
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
    assertU(adoc("id", "da12", "allow_token_document", "token1", "allow_token_document", "token2"));
    assertU(adoc("id", "da13-dd3", "allow_token_document", "token1", "allow_token_document", "token3", "deny_token_document", "token3"));
    assertU(adoc("id", "sa123-sd13", "allow_token_share", "token1", "allow_token_share", "token2", "allow_token_share", "token3", "deny_token_share", "token1", "deny_token_share", "token3"));
    assertU(adoc("id", "sa3-sd1-da23", "allow_token_document", "token2", "allow_token_document", "token3", "allow_token_share", "token3", "deny_token_share", "token1"));
    assertU(adoc("id", "notoken"));
    assertU(commit());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    service.stop();
  }
  
  @Test
  public void testParameters() throws Exception {
    ManifoldCFSecurityFilter mcfFilter = (ManifoldCFSecurityFilter)h.getCore().getSearchComponent("mcf-param");
    assertEquals("http://localhost:8345/mcf-as", mcfFilter.authorityBaseURL);
    assertEquals(3000, mcfFilter.socketTimeOut);
    assertEquals("aap-document", mcfFilter.fieldAllowDocument);
    assertEquals("dap-document", mcfFilter.fieldDenyDocument);
    assertEquals("aap-share", mcfFilter.fieldAllowShare);
    assertEquals("dap-share", mcfFilter.fieldDenyShare);
  }

  @Test
  public void testNullUsers() throws Exception {
    assertQ(req("qt", "/mcf", "q", "*:*", "fl", "id"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='notoken']");
    assertQ(req("qt", "/mcf", "q", "*:*", "fl", "id", "AuthenticatedUserName", "anonymous"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='notoken']");
  }

  // da12
  // da13-dd3
  // sa123-sd13
  // sa3-sd1-da23
  // notoken
  @Test
  public void testAuthUsers() throws Exception {
    assertQ(req("qt", "/mcf", "q", "*:*", "fl", "id", "AuthenticatedUserName", "user1"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='da12']",
        "//result/doc[2]/str[@name='id'][.='da13-dd3']",
        "//result/doc[3]/str[@name='id'][.='notoken']");

    assertQ(req("qt", "/mcf", "q", "*:*", "fl", "id", "AuthenticatedUserName", "user2"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='da12']",
        "//result/doc[2]/str[@name='id'][.='da13-dd3']",
        "//result/doc[3]/str[@name='id'][.='notoken']");

    assertQ(req("qt", "/mcf", "q", "*:*", "fl", "id", "AuthenticatedUserName", "user3"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='da12']",
        "//result/doc[2]/str[@name='id'][.='notoken']");
  }

  // da12
  // da13-dd3
  // sa123-sd13
  // sa3-sd1-da23
  // notoken
  @Test
  public void testUserTokens() throws Exception {

    assertQ(req("qt", "/mcf", "q", "*:*", "fl", "id", "UserTokens", "token1"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='da12']",
        "//result/doc[2]/str[@name='id'][.='da13-dd3']",
        "//result/doc[3]/str[@name='id'][.='notoken']");

    assertQ(req("qt", "/mcf", "q", "*:*", "fl", "id", "UserTokens", "token2"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='da12']",
        "//result/doc[2]/str[@name='id'][.='sa123-sd13']",
        "//result/doc[3]/str[@name='id'][.='notoken']");

    assertQ(req("qt", "/mcf", "q", "*:*", "fl", "id", "UserTokens", "token3"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='sa3-sd1-da23']",
        "//result/doc[2]/str[@name='id'][.='notoken']");

    assertQ(req("qt", "/mcf", "q", "*:*", "fl", "id", "UserTokens", "token2", "UserTokens", "token3"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='da12']",
        "//result/doc[2]/str[@name='id'][.='sa3-sd1-da23']",
        "//result/doc[3]/str[@name='id'][.='notoken']");
  }
  
  static class MockMCFAuthorityService {
    
    Server server;
    
    public MockMCFAuthorityService() {
      server = new Server(8345);
      Context asContext = new Context(server,"/mcf-authority-service",Context.SESSIONS);
      asContext.addServlet(new ServletHolder(new UserACLServlet()), "/UserACLs");
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
      }
    }
  }
}
