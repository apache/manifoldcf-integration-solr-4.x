package org.apache.lucene.benchmark.byTask.feeds;

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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.lucene.benchmark.BenchmarkTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ContentSourceTest extends BenchmarkTestCase {
  private static final String TEXT = "Some-Text..."; 
  private File testDir;
  private CompressorStreamFactory csFactory = new CompressorStreamFactory();
  
  @Test
  public void testGetInputStreamPlainText() throws Exception {
    assertReadText(textFile("txt"));
    assertReadText(textFile("TXT"));
  }

  @Test
  public void testGetInputStreamGzip() throws Exception {
    assertReadText(gzipFile("gz"));
    assertReadText(gzipFile("gzip"));
    assertReadText(gzipFile("GZ"));
    assertReadText(gzipFile("GZIP"));
  }

  @Test
  public void testGetInputStreamBzip2() throws Exception {
  	assertReadText(bzip2File("bz2"));
  	assertReadText(bzip2File("bzip"));
  	assertReadText(bzip2File("BZ2"));
  	assertReadText(bzip2File("BZIP"));
  }
  
  private File textFile(String ext) throws Exception {
    File f = new File(testDir,"testfile." +	ext);
    BufferedWriter w = new BufferedWriter(new FileWriter(f));
    w.write(TEXT);
    w.newLine();
    w.close();
    return f;
  }
  
  private File gzipFile(String ext) throws Exception {
    File f = new File(testDir,"testfile." +	ext);
    OutputStream os = csFactory.createCompressorOutputStream(CompressorStreamFactory.GZIP, new FileOutputStream(f));
    BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os));
    w.write(TEXT);
    w.newLine();
    w.close();
    return f;
  }

  private File bzip2File(String ext) throws Exception {
  	File f = new File(testDir,"testfile." +	ext);
  	OutputStream os = csFactory.createCompressorOutputStream(CompressorStreamFactory.BZIP2, new FileOutputStream(f));
  	BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os));
  	w.write(TEXT);
  	w.newLine();
  	w.close();
  	return f;
  }

  private void assertReadText(File f) throws Exception {
    ContentSource src = new ContentSource() {
      @Override
      public void close() throws IOException { 
      }
      @Override
      public DocData getNextDocData(DocData docData) throws NoMoreDataException,
      IOException { 
        return null;
      }
    };
    InputStream ir = src.getInputStream(f);
    InputStreamReader in = new InputStreamReader(ir);
    BufferedReader r = new BufferedReader(in);
    String line = r.readLine();
    assertEquals("Wrong text found in "+f.getName(), TEXT, line);
    r.close();
  }
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    testDir = new File(getWorkDir(),"ContentSourceTest");
    _TestUtil.rmDir(testDir);
    assertTrue(testDir.mkdirs());
  }

  @After
  public void tearDown() throws Exception {
    _TestUtil.rmDir(testDir);
    super.tearDown();
  }

}
