package org.apache.lucene.analysis.core;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.util.ReusableAnalyzerBase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

public class TestStandardAnalyzer extends BaseTokenStreamTestCase {
  
  public void testHugeDoc() throws IOException {
    StringBuilder sb = new StringBuilder();
    char whitespace[] = new char[4094];
    Arrays.fill(whitespace, ' ');
    sb.append(whitespace);
    sb.append("testing 1234");
    String input = sb.toString();
    StandardTokenizer tokenizer = new StandardTokenizer(TEST_VERSION_CURRENT, new StringReader(input));
    BaseTokenStreamTestCase.assertTokenStreamContents(tokenizer, new String[] { "testing", "1234" });
  }

  private Analyzer a = new ReusableAnalyzerBase() {
    @Override
    protected TokenStreamComponents createComponents
      (String fieldName, Reader reader) {

      Tokenizer tokenizer = new StandardTokenizer(TEST_VERSION_CURRENT, reader);
      return new TokenStreamComponents(tokenizer);
    }
  };

  /** Passes through tokens with type "<URL>" and blocks all other types. */
  private class URLFilter extends TokenFilter {
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    public URLFilter(TokenStream in) {
      super(in);
    }
    @Override
    public final boolean incrementToken() throws java.io.IOException {
      boolean isTokenAvailable = false;
      while (input.incrementToken()) {
        if (typeAtt.type() == StandardTokenizer.TOKEN_TYPES[StandardTokenizer.URL]) {
          isTokenAvailable = true;
          break;
        }
      }
      return isTokenAvailable;
    }
  }
  
  /** Passes through tokens with type "<EMAIL>" and blocks all other types. */
  private class EmailFilter extends TokenFilter {
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    public EmailFilter(TokenStream in) {
      super(in);
    }
    @Override
    public final boolean incrementToken() throws java.io.IOException {
      boolean isTokenAvailable = false;
      while (input.incrementToken()) {
        if (typeAtt.type() == StandardTokenizer.TOKEN_TYPES[StandardTokenizer.EMAIL]) {
          isTokenAvailable = true;
          break;
        }
      }
      return isTokenAvailable;
    }
  }

  private Analyzer urlAnalyzer = new ReusableAnalyzerBase() {
    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
      StandardTokenizer tokenizer = new StandardTokenizer(TEST_VERSION_CURRENT, reader);
      tokenizer.setMaxTokenLength(Integer.MAX_VALUE);  // Tokenize arbitrary length URLs
      TokenFilter filter = new URLFilter(tokenizer);
      return new TokenStreamComponents(tokenizer, filter);
    }
  };

  private Analyzer emailAnalyzer = new ReusableAnalyzerBase() {
    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer tokenizer = new StandardTokenizer(TEST_VERSION_CURRENT, reader);
      TokenFilter filter = new EmailFilter(tokenizer);
      return new TokenStreamComponents(tokenizer, filter);
    }
  };

  public void testArmenian() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "Վիքիպեդիայի 13 միլիոն հոդվածները (4,600` հայերեն վիքիպեդիայում) գրվել են կամավորների կողմից ու համարյա բոլոր հոդվածները կարող է խմբագրել ցանկաց մարդ ով կարող է բացել Վիքիպեդիայի կայքը։",
        new String[] { "Վիքիպեդիայի", "13", "միլիոն", "հոդվածները", "4,600", "հայերեն", "վիքիպեդիայում", "գրվել", "են", "կամավորների", "կողմից", 
        "ու", "համարյա", "բոլոր", "հոդվածները", "կարող", "է", "խմբագրել", "ցանկաց", "մարդ", "ով", "կարող", "է", "բացել", "Վիքիպեդիայի", "կայքը" } );
  }
  
  public void testAmharic() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "ዊኪፔድያ የባለ ብዙ ቋንቋ የተሟላ ትክክለኛና ነጻ መዝገበ ዕውቀት (ኢንሳይክሎፒዲያ) ነው። ማንኛውም",
        new String[] { "ዊኪፔድያ", "የባለ", "ብዙ", "ቋንቋ", "የተሟላ", "ትክክለኛና", "ነጻ", "መዝገበ", "ዕውቀት", "ኢንሳይክሎፒዲያ", "ነው", "ማንኛውም" } );
  }
  
  public void testArabic() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "الفيلم الوثائقي الأول عن ويكيبيديا يسمى \"الحقيقة بالأرقام: قصة ويكيبيديا\" (بالإنجليزية: Truth in Numbers: The Wikipedia Story)، سيتم إطلاقه في 2008.",
        new String[] { "الفيلم", "الوثائقي", "الأول", "عن", "ويكيبيديا", "يسمى", "الحقيقة", "بالأرقام", "قصة", "ويكيبيديا",
        "بالإنجليزية", "Truth", "in", "Numbers", "The", "Wikipedia", "Story", "سيتم", "إطلاقه", "في", "2008" } ); 
  }
  
  public void testAramaic() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "ܘܝܩܝܦܕܝܐ (ܐܢܓܠܝܐ: Wikipedia) ܗܘ ܐܝܢܣܩܠܘܦܕܝܐ ܚܐܪܬܐ ܕܐܢܛܪܢܛ ܒܠܫܢ̈ܐ ܣܓܝܐ̈ܐ܂ ܫܡܗ ܐܬܐ ܡܢ ܡ̈ܠܬܐ ܕ\"ܘܝܩܝ\" ܘ\"ܐܝܢܣܩܠܘܦܕܝܐ\"܀",
        new String[] { "ܘܝܩܝܦܕܝܐ", "ܐܢܓܠܝܐ", "Wikipedia", "ܗܘ", "ܐܝܢܣܩܠܘܦܕܝܐ", "ܚܐܪܬܐ", "ܕܐܢܛܪܢܛ", "ܒܠܫܢ̈ܐ", "ܣܓܝܐ̈ܐ", "ܫܡܗ",
        "ܐܬܐ", "ܡܢ", "ܡ̈ܠܬܐ", "ܕ", "ܘܝܩܝ", "ܘ", "ܐܝܢܣܩܠܘܦܕܝܐ"});
  }
  
  public void testBengali() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "এই বিশ্বকোষ পরিচালনা করে উইকিমিডিয়া ফাউন্ডেশন (একটি অলাভজনক সংস্থা)। উইকিপিডিয়ার শুরু ১৫ জানুয়ারি, ২০০১ সালে। এখন পর্যন্ত ২০০টিরও বেশী ভাষায় উইকিপিডিয়া রয়েছে।",
        new String[] { "এই", "বিশ্বকোষ", "পরিচালনা", "করে", "উইকিমিডিয়া", "ফাউন্ডেশন", "একটি", "অলাভজনক", "সংস্থা", "উইকিপিডিয়ার",
        "শুরু", "১৫", "জানুয়ারি", "২০০১", "সালে", "এখন", "পর্যন্ত", "২০০টিরও", "বেশী", "ভাষায়", "উইকিপিডিয়া", "রয়েছে" });
  }
  
  public void testFarsi() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "ویکی پدیای انگلیسی در تاریخ ۲۵ دی ۱۳۷۹ به صورت مکملی برای دانشنامهٔ تخصصی نوپدیا نوشته شد.",
        new String[] { "ویکی", "پدیای", "انگلیسی", "در", "تاریخ", "۲۵", "دی", "۱۳۷۹", "به", "صورت", "مکملی",
        "برای", "دانشنامهٔ", "تخصصی", "نوپدیا", "نوشته", "شد" });
  }
  
  public void testGreek() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "Γράφεται σε συνεργασία από εθελοντές με το λογισμικό wiki, κάτι που σημαίνει ότι άρθρα μπορεί να προστεθούν ή να αλλάξουν από τον καθένα.",
        new String[] { "Γράφεται", "σε", "συνεργασία", "από", "εθελοντές", "με", "το", "λογισμικό", "wiki", "κάτι", "που",
        "σημαίνει", "ότι", "άρθρα", "μπορεί", "να", "προστεθούν", "ή", "να", "αλλάξουν", "από", "τον", "καθένα" });
  }

  public void testThai() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "การที่ได้ต้องแสดงว่างานดี. แล้วเธอจะไปไหน? ๑๒๓๔",
        new String[] { "การที่ได้ต้องแสดงว่างานดี", "แล้วเธอจะไปไหน", "๑๒๓๔" });
  }
  
  public void testLao() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "ສາທາລະນະລັດ ປະຊາທິປະໄຕ ປະຊາຊົນລາວ", 
        new String[] { "ສາທາລະນະລັດ", "ປະຊາທິປະໄຕ", "ປະຊາຊົນລາວ" });
  }
  
  public void testTibetan() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "སྣོན་མཛོད་དང་ལས་འདིས་བོད་ཡིག་མི་ཉམས་གོང་འཕེལ་དུ་གཏོང་བར་ཧ་ཅང་དགེ་མཚན་མཆིས་སོ། །",
                     new String[] { "སྣོན", "མཛོད", "དང", "ལས", "འདིས", "བོད", "ཡིག", 
                                    "མི", "ཉམས", "གོང", "འཕེལ", "དུ", "གཏོང", "བར", 
                                    "ཧ", "ཅང", "དགེ", "མཚན", "མཆིས", "སོ" });
  }
  
  /*
   * For chinese, tokenize as char (these can later form bigrams or whatever)
   */
  public void testChinese() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "我是中国人。 １２３４ Ｔｅｓｔｓ ",
        new String[] { "我", "是", "中", "国", "人", "１２３４", "Ｔｅｓｔｓ"});
  }
  
  public void testEmpty() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "", new String[] {});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, ".", new String[] {});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, " ", new String[] {});
  }
  
  /* test various jira issues this analyzer is related to */
  
  public void testLUCENE1545() throws Exception {
    /*
     * Standard analyzer does not correctly tokenize combining character U+0364 COMBINING LATIN SMALL LETTRE E.
     * The word "moͤchte" is incorrectly tokenized into "mo" "chte", the combining character is lost.
     * Expected result is only on token "moͤchte".
     */
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "moͤchte", new String[] { "moͤchte" }); 
  }
  
  /* Tests from StandardAnalyzer, just to show behavior is similar */
  public void testAlphanumericSA() throws Exception {
    // alphanumeric tokens
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "B2B", new String[]{"B2B"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "2B", new String[]{"2B"});
  }

  public void testDelimitersSA() throws Exception {
    // other delimiters: "-", "/", ","
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "some-dashed-phrase", new String[]{"some", "dashed", "phrase"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "dogs,chase,cats", new String[]{"dogs", "chase", "cats"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "ac/dc", new String[]{"ac", "dc"});
  }

  public void testApostrophesSA() throws Exception {
    // internal apostrophes: O'Reilly, you're, O'Reilly's
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "O'Reilly", new String[]{"O'Reilly"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "you're", new String[]{"you're"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "she's", new String[]{"she's"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "Jim's", new String[]{"Jim's"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "don't", new String[]{"don't"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "O'Reilly's", new String[]{"O'Reilly's"});
  }

  public void testNumericSA() throws Exception {
    // floating point, serial, model numbers, ip addresses, etc.
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "21.35", new String[]{"21.35"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "R2D2 C3PO", new String[]{"R2D2", "C3PO"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "216.239.63.104", new String[]{"216.239.63.104"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "216.239.63.104", new String[]{"216.239.63.104"});
  }

  public void testTextWithNumbersSA() throws Exception {
    // numbers
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "David has 5000 bones", new String[]{"David", "has", "5000", "bones"});
  }

  public void testVariousTextSA() throws Exception {
    // various
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "C embedded developers wanted", new String[]{"C", "embedded", "developers", "wanted"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "foo bar FOO BAR", new String[]{"foo", "bar", "FOO", "BAR"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "foo      bar .  FOO <> BAR", new String[]{"foo", "bar", "FOO", "BAR"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "\"QUOTED\" word", new String[]{"QUOTED", "word"});
  }

  public void testKoreanSA() throws Exception {
    // Korean words
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "안녕하세요 한글입니다", new String[]{"안녕하세요", "한글입니다"});
  }
  
  public void testOffsets() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "David has 5000 bones", 
        new String[] {"David", "has", "5000", "bones"},
        new int[] {0, 6, 10, 15},
        new int[] {5, 9, 14, 20});
  }
  
  public void testTypes() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "David has 5000 bones", 
        new String[] {"David", "has", "5000", "bones"},
        new String[] { "<ALPHANUM>", "<ALPHANUM>", "<NUM>", "<ALPHANUM>" });
  }
  
  public void testWikiURLs() throws Exception {
    Reader reader = null;
    String luceneResourcesWikiPage;
    try {
      reader = new InputStreamReader
        (getClass().getResourceAsStream("LuceneResourcesWikiPage.html"), "UTF-8");
      StringBuilder builder = new StringBuilder();
      char[] buffer = new char[1024];
      int numCharsRead;
      while (-1 != (numCharsRead = reader.read(buffer))) {
        builder.append(buffer, 0, numCharsRead);
      }
      luceneResourcesWikiPage = builder.toString(); 
    } finally {
      if (null != reader) {
        reader.close();
      }
    }
    assertTrue(null != luceneResourcesWikiPage 
               && luceneResourcesWikiPage.length() > 0);
    BufferedReader bufferedReader = null;
    String[] urls;
    try {
      List<String> urlList = new ArrayList<String>();
      bufferedReader = new BufferedReader(new InputStreamReader
        (getClass().getResourceAsStream("LuceneResourcesWikiPageURLs.txt"), "UTF-8"));
      String line;
      while (null != (line = bufferedReader.readLine())) {
        line = line.trim();
        if (line.length() > 0) {
          urlList.add(line);
        }
      }
      urls = urlList.toArray(new String[urlList.size()]);
    } finally {
      if (null != bufferedReader) {
        bufferedReader.close();
      }
    }
    assertTrue(null != urls && urls.length > 0);
    BaseTokenStreamTestCase.assertAnalyzesTo
      (urlAnalyzer, luceneResourcesWikiPage, urls);
  }
  
  public void testEmails() throws Exception {
    Reader reader = null;
    String randomTextWithEmails;
    try {
      reader = new InputStreamReader
        (getClass().getResourceAsStream("random.text.with.email.addresses.txt"), "UTF-8");
      StringBuilder builder = new StringBuilder();
      char[] buffer = new char[1024];
      int numCharsRead;
      while (-1 != (numCharsRead = reader.read(buffer))) {
        builder.append(buffer, 0, numCharsRead);
      }
      randomTextWithEmails = builder.toString(); 
    } finally {
      if (null != reader) {
        reader.close();
      }
    }
    assertTrue(null != randomTextWithEmails 
               && randomTextWithEmails.length() > 0);
    BufferedReader bufferedReader = null;
    String[] emails;
    try {
      List<String> emailList = new ArrayList<String>();
      bufferedReader = new BufferedReader(new InputStreamReader
        (getClass().getResourceAsStream("email.addresses.from.random.text.with.email.addresses.txt"), "UTF-8"));
      String line;
      while (null != (line = bufferedReader.readLine())) {
        line = line.trim();
        if (line.length() > 0) {
          emailList.add(line);
        }
      }
      emails = emailList.toArray(new String[emailList.size()]);
    } finally {
      if (null != bufferedReader) {
        bufferedReader.close();
      }
    }
    assertTrue(null != emails && emails.length > 0);
    BaseTokenStreamTestCase.assertAnalyzesTo
      (emailAnalyzer, randomTextWithEmails, emails);
  }

  public void testURLs() throws Exception {
    Reader reader = null;
    String randomTextWithURLs;
    try {
      reader = new InputStreamReader
        (getClass().getResourceAsStream("random.text.with.urls.txt"), "UTF-8");
      StringBuilder builder = new StringBuilder();
      char[] buffer = new char[1024];
      int numCharsRead;
      while (-1 != (numCharsRead = reader.read(buffer))) {
        builder.append(buffer, 0, numCharsRead);
      }
      randomTextWithURLs = builder.toString(); 
    } finally {
      if (null != reader) {
        reader.close();
      }
    }
    assertTrue(null != randomTextWithURLs 
               && randomTextWithURLs.length() > 0);
    BufferedReader bufferedReader = null;
    String[] urls;
    try {
      List<String> urlList = new ArrayList<String>();
      bufferedReader = new BufferedReader(new InputStreamReader
        (getClass().getResourceAsStream("urls.from.random.text.with.urls.txt"), "UTF-8"));
      String line;
      while (null != (line = bufferedReader.readLine())) {
        line = line.trim();
        if (line.length() > 0) {
          urlList.add(line);
        }
      }
      urls = urlList.toArray(new String[urlList.size()]);
    } finally {
      if (null != bufferedReader) {
        bufferedReader.close();
      }
    }
    assertTrue(null != urls && urls.length > 0);
    BaseTokenStreamTestCase.assertAnalyzesTo
      (urlAnalyzer, randomTextWithURLs, urls);
  }

  public void testUnicodeWordBreaks() throws Exception {
    WordBreakTestUnicode_6_0_0 wordBreakTest = new WordBreakTestUnicode_6_0_0();
    wordBreakTest.test(a);
  }
}
