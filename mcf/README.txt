# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

Compatibility
------------

This version of this component is fully functional with Apache ManifoldCF 1.6 and
above.  It is backwards compatible with earlier versions as well, except for the fact
that two additional Solr fields are required for this plugin to work.


Getting Started
---------------

There are two ways to hook up security to Solr in this package.  The first is using
a Query Parser plugin.  The second is using a Search Component.  In both cases,
the first step is to have ManifoldCF installed and running.  See:
http://manifoldcf.apache.org/release/trunk/en_US/how-to-build-and-deploy.html

Then, you will need to add fields to your Solr schema.xml file that can be used
to contain document authorization information.  There will need to be six of these
fields, an 'allow' field for documents, parents, and shares, and a 'deny' field for
documents, parents, and shares.  For example:

  <field name="allow_token_document" type="string" indexed="true" stored="false"
    multiValued="true" required="false" default="__nosecurity__"/>
  <field name="allow_token_parent" type="string" indexed="true" stored="false"
    multiValued="true" required="false" default="__nosecurity__"/>
  <field name="allow_token_share" type="string" indexed="true" stored="false"
    multiValued="true" required="false" default="__nosecurity__"/>
  <field name="deny_token_document" type="string" indexed="true" stored="false"
    multiValued="true" required="false" default="__nosecurity__"/>
  <field name="deny_token_parent" type="string" indexed="true" stored="false"
    multiValued="true" required="false" default="__nosecurity__"/>
  <field name="deny_token_share" type="string" indexed="true" stored="false"
    multiValued="true" required="false" default="__nosecurity__"/>

The default value of "__nosecurity__" is required by this plugin, so do not forget
to include it.


Upgrading from releases earlier than 1.2
--------------------------------------

Earlier releases of this plugin only required four special fields.  Since this version
of the plugin requires six, when upgrading it will be necessary to reindex all
documents, so all six fields have the correct values.  Otherwise, the plugin
will prevent you from viewing any documents.


Using the Query Parser Plugin
----------------------------

To set up the query parser plugin, modify your solrconfig.xml to add the query parser:

  <!-- ManifoldCF document security enforcement component -->
  <queryParser name="manifoldCFSecurity"
    class="org.apache.solr.mcf.ManifoldCFQParserPlugin">
    <str name="AuthorityServiceBaseURL">http://localhost:8345/mcf-authority-service</str>
    <int name="ConnectionPoolSize">50</int>
  </queryParser>

Hook up the search component in the solrconfig.xml file wherever you want it, e.g.:

<requestHandler name="search" class="solr.SearchHandler" default="true">
  <lst name="appends">
    <str name="fq">{!manifoldCFSecurity}</str>
  </lst>
  ...
</requestHandler>


Using the Search Component
----------------------------

To set up the search component, modify your solrconfig.xml to add the search component:

  <!-- ManifoldCF document security enforcement component -->
  <searchComponent name="manifoldCFSecurity"
    class="org.apache.solr.mcf.ManifoldCFSearchComponent">
    <str name="AuthorityServiceBaseURL">http://localhost:8345/mcf-authority-service</str>
    <int name="ConnectionPoolSize">50</int>
  </searchComponent>

Hook up the search component in the solrconfig.xml file wherever you want it, e.g.:

<requestHandler name="search" class="solr.SearchHandler" default="true">
  <arr name="last-components">
    <str>manifoldCFSecurity</str>
  </arr>
  ...
</requestHandler>


Supplying authenticated usernames and domains
----------------------------------------------

This component looks for the following parameters in the Solr request object:

AuthenticatedUserName
AuthenticatedUserDomain
AuthenticatedUserName_XX
AuthenticatedUserDomain_XX

At a minimum, AuthenticatedUserName must be present in order for the component to communicate with
the ManifoldCF Authority Service and obtain user access tokens.  In that case, the user identity will consist
of one user and one authorization domain.  If AuthenticatedUserDomain is not set, then the authorization domain
chosen will be the standard default domain, an empty string.

If you need multiple user/domain tuples for the user identity, you may pass these as parameter pairs starting with
AuthenticatedUserName_0 and AuthenticatedUserDomain_0, and counting up as high as you like.

Operation in conjunction with mod-authz-annotate
------------------------------------------------

An optional component of ManifoldCF can be built and deployed as part of Apache - mod-authz-annotate.  The
mod-authz-annotate module obtains the Kerberos principal from the Kerberos tickets present if mod-auth-kerb is used, and uses
the MCF Authority Service to look up the appropriate access tokens.  If you choose to use that architecture,
you will still need to use this Solr component to modify the user query.  All you have to do is the following:

- Make sure you do not set AuthenticatedUserName or AuthenticatedUserName_0 in the request
- Make sure the HTTP request from Apache to Solr translates all AAAGRP header values into "UserToken" parameters
   for the Solr request

