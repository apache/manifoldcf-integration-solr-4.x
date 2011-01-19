package org.apache.lucene.analysis.tokenattributes;

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

import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.LuceneTestCase;

import java.util.Collections;
import java.util.HashMap;

public class TestSimpleAttributeImpl extends LuceneTestCase {

  // this checks using reflection API if the defaults are correct
  public void testAttributes() {
    _TestUtil.assertAttributeReflection(new PositionIncrementAttributeImpl(),
      Collections.singletonMap(PositionIncrementAttribute.class.getName()+"#positionIncrement", 1));
    _TestUtil.assertAttributeReflection(new FlagsAttributeImpl(),
      Collections.singletonMap(FlagsAttribute.class.getName()+"#flags", 0));
    _TestUtil.assertAttributeReflection(new TypeAttributeImpl(),
      Collections.singletonMap(TypeAttribute.class.getName()+"#type", TypeAttribute.DEFAULT_TYPE));
    _TestUtil.assertAttributeReflection(new PayloadAttributeImpl(),
      Collections.singletonMap(PayloadAttribute.class.getName()+"#payload", null));
    _TestUtil.assertAttributeReflection(new KeywordAttributeImpl(),
      Collections.singletonMap(KeywordAttribute.class.getName()+"#keyword", false));
    _TestUtil.assertAttributeReflection(new OffsetAttributeImpl(), new HashMap<String,Object>() {{
      put(OffsetAttribute.class.getName()+"#startOffset", 0);
      put(OffsetAttribute.class.getName()+"#endOffset", 0);
    }});
  }

}
