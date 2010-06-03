package org.apache.lucene.util;

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

import java.io.File;
import java.io.IOException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.store.Directory;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Random;

public class _TestUtil {

  /** Returns temp dir, containing String arg in its name;
   *  does not create the directory. */
  public static File getTempDir(String desc) {
    return new File(LuceneTestCaseJ4.TEMP_DIR, desc + "." + new Random().nextLong());
  }

  public static void rmDir(File dir) throws IOException {
    if (dir.exists()) {
      File[] files = dir.listFiles();
      for (int i = 0; i < files.length; i++) {
        if (!files[i].delete()) {
          throw new IOException("could not delete " + files[i]);
        }
      }
      dir.delete();
    }
  }

  public static void rmDir(String dir) throws IOException {
    rmDir(new File(dir));
  }

  public static void syncConcurrentMerges(IndexWriter writer) {
    syncConcurrentMerges(writer.getConfig().getMergeScheduler());
  }

  public static void syncConcurrentMerges(MergeScheduler ms) {
    if (ms instanceof ConcurrentMergeScheduler)
      ((ConcurrentMergeScheduler) ms).sync();
  }

  /** This runs the CheckIndex tool on the index in.  If any
   *  issues are hit, a RuntimeException is thrown; else,
   *  true is returned. */
  public static boolean checkIndex(Directory dir) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);

    CheckIndex checker = new CheckIndex(dir);
    checker.setInfoStream(new PrintStream(bos));
    CheckIndex.Status indexStatus = checker.checkIndex();
    if (indexStatus == null || indexStatus.clean == false) {
      System.out.println("CheckIndex failed");
      System.out.println(bos.toString());
      throw new RuntimeException("CheckIndex failed");
    } else
      return true;
  }

  /** Use only for testing.
   *  @deprecated -- in 3.0 we can use Arrays.toString
   *  instead */
  @Deprecated
  public static String arrayToString(int[] array) {
    StringBuilder buf = new StringBuilder();
    buf.append("[");
    for(int i=0;i<array.length;i++) {
      if (i > 0) {
        buf.append(" ");
      }
      buf.append(array[i]);
    }
    buf.append("]");
    return buf.toString();
  }

  /** Use only for testing.
   *  @deprecated -- in 3.0 we can use Arrays.toString
   *  instead */
  @Deprecated
  public static String arrayToString(Object[] array) {
    StringBuilder buf = new StringBuilder();
    buf.append("[");
    for(int i=0;i<array.length;i++) {
      if (i > 0) {
        buf.append(" ");
      }
      buf.append(array[i]);
    }
    buf.append("]");
    return buf.toString();
  }
  /** start and end are BOTH inclusive */
  public static int nextInt(Random r, int start, int end) {
    return start + r.nextInt(end-start+1);
  }

  /** Returns random string, including full unicode range. */
  public static String randomUnicodeString(Random r) {
    return randomUnicodeString(r, 20);
  }

  public static String randomUnicodeString(Random r, int maxLength) {
    final int end = r.nextInt(maxLength);
    if (end == 0) {
      // allow 0 length
      return "";
    }
    final char[] buffer = new char[end];
    for (int i = 0; i < end; i++) {
      int t = r.nextInt(5);
      //buffer[i] = (char) (97 + r.nextInt(26));
      if (0 == t && i < end - 1) {
        // Make a surrogate pair
        // High surrogate
        buffer[i++] = (char) nextInt(r, 0xd800, 0xdbff);
        // Low surrogate
        buffer[i] = (char) nextInt(r, 0xdc00, 0xdfff);
      }
      else if (t <= 1) buffer[i] = (char) r.nextInt(0x80);
      else if (2 == t) buffer[i] = (char) nextInt(r, 0x80, 0x800);
      else if (3 == t) buffer[i] = (char) nextInt(r, 0x800, 0xd7ff);
      else if (4 == t) buffer[i] = (char) nextInt(r, 0xe000, 0xffff);
    }
    return new String(buffer, 0, end);
  }

  /** gets a random multiplier, which you should use when writing
   *  random tests: multiply it by the number of iterations */
  public static int getRandomMultiplier() {
    return Integer.parseInt(System.getProperty("random.multiplier", "1"));
  }
}
