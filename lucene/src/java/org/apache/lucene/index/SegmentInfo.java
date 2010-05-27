package org.apache.lucene.index;

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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BitVector;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.CodecProvider;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Information about a segment such as it's name, directory, and files related
 * to the segment.
 * 
 * @lucene.experimental
 */
public final class SegmentInfo {

  static final int NO = -1;          // e.g. no norms; no deletes;
  static final int YES = 1;          // e.g. have norms; have deletes;
  static final int CHECK_DIR = 0;    // e.g. must check dir to see if there are norms/deletions
  static final int WITHOUT_GEN = 0;  // a file name that has no GEN in it. 

  public String name;				  // unique name in dir
  public int docCount;				  // number of docs in seg
  public Directory dir;				  // where segment resides

  private boolean preLockless;                    // true if this is a segments file written before
                                                  // lock-less commits (2.1)

  private long delGen;                            // current generation of del file; NO if there
                                                  // are no deletes; CHECK_DIR if it's a pre-2.1 segment
                                                  // (and we must check filesystem); YES or higher if
                                                  // there are deletes at generation N
   
  private long[] normGen;                         // current generation of each field's norm file.
                                                  // If this array is null, for lockLess this means no 
                                                  // separate norms.  For preLockLess this means we must 
                                                  // check filesystem. If this array is not null, its 
                                                  // values mean: NO says this field has no separate  
                                                  // norms; CHECK_DIR says it is a preLockLess segment and    
                                                  // filesystem must be checked; >= YES says this field  
                                                  // has separate norms with the specified generation

  private byte isCompoundFile;                    // NO if it is not; YES if it is; CHECK_DIR if it's
                                                  // pre-2.1 (ie, must check file system to see
                                                  // if <name>.cfs and <name>.nrm exist)         

  private boolean hasSingleNormFile;              // true if this segment maintains norms in a single file; 
                                                  // false otherwise
                                                  // this is currently false for segments populated by DocumentWriter
                                                  // and true for newly created merged segments (both
                                                  // compound and non compound).
  
  private List<String> files;                             // cached list of files that this segment uses
                                                  // in the Directory

  long sizeInBytes = -1;                          // total byte size of all of our files (computed on demand)

  private int docStoreOffset;                     // if this segment shares stored fields & vectors, this
                                                  // offset is where in that file this segment's docs begin
  private String docStoreSegment;                 // name used to derive fields/vectors file we share with
                                                  // other segments
  private boolean docStoreIsCompoundFile;         // whether doc store files are stored in compound file (*.cfx)

  private int delCount;                           // How many deleted docs in this segment, or -1 if not yet known
                                                  // (if it's an older index)

  private boolean hasProx;                        // True if this segment has any fields with omitTermFreqAndPositions==false
  
  private Codec codec;


  private Map<String,String> diagnostics;

  public SegmentInfo(String name, int docCount, Directory dir, Codec codec) {
    this.name = name;
    this.docCount = docCount;
    this.dir = dir;
    delGen = NO;
    isCompoundFile = CHECK_DIR;
    preLockless = true;
    hasSingleNormFile = false;
    docStoreOffset = -1;
    docStoreSegment = name;
    docStoreIsCompoundFile = false;
    delCount = 0;
    hasProx = true;
    this.codec = codec;
  }

  public SegmentInfo(String name, int docCount, Directory dir, boolean isCompoundFile, boolean hasSingleNormFile, 
                     int docStoreOffset, String docStoreSegment, boolean docStoreIsCompoundFile, boolean hasProx,
                     Codec codec) { 
    this(name, docCount, dir, codec);
    this.isCompoundFile = (byte) (isCompoundFile ? YES : NO);
    this.hasSingleNormFile = hasSingleNormFile;
    preLockless = false;
    this.docStoreOffset = docStoreOffset;
    this.docStoreSegment = docStoreSegment;
    this.docStoreIsCompoundFile = docStoreIsCompoundFile;
    this.hasProx = hasProx;
    this.codec = codec;
    delCount = 0;
    assert docStoreOffset == -1 || docStoreSegment != null: "dso=" + docStoreOffset + " dss=" + docStoreSegment + " docCount=" + docCount;
  }

  /**
   * Copy everything from src SegmentInfo into our instance.
   */
  void reset(SegmentInfo src) {
    clearFiles();
    name = src.name;
    docCount = src.docCount;
    dir = src.dir;
    preLockless = src.preLockless;
    delGen = src.delGen;
    docStoreOffset = src.docStoreOffset;
    docStoreIsCompoundFile = src.docStoreIsCompoundFile;
    if (src.normGen == null) {
      normGen = null;
    } else {
      normGen = new long[src.normGen.length];
      System.arraycopy(src.normGen, 0, normGen, 0, src.normGen.length);
    }
    isCompoundFile = src.isCompoundFile;
    hasSingleNormFile = src.hasSingleNormFile;
    delCount = src.delCount;
    codec = src.codec;
  }

  void setDiagnostics(Map<String, String> diagnostics) {
    this.diagnostics = diagnostics;
  }

  public Map<String, String> getDiagnostics() {
    return diagnostics;
  }

  /**
   * Construct a new SegmentInfo instance by reading a
   * previously saved SegmentInfo from input.
   *
   * @param dir directory to load from
   * @param format format of the segments info file
   * @param input input handle to read segment info from
   */
  SegmentInfo(Directory dir, int format, IndexInput input, CodecProvider codecs) throws IOException {
    this.dir = dir;
    name = input.readString();
    docCount = input.readInt();
    final String codecName;
    if (format <= SegmentInfos.FORMAT_LOCKLESS) {
      delGen = input.readLong();
      if (format <= SegmentInfos.FORMAT_SHARED_DOC_STORE) {
        docStoreOffset = input.readInt();
        if (docStoreOffset != -1) {
          docStoreSegment = input.readString();
          docStoreIsCompoundFile = (1 == input.readByte());
        } else {
          docStoreSegment = name;
          docStoreIsCompoundFile = false;
        }
      } else {
        docStoreOffset = -1;
        docStoreSegment = name;
        docStoreIsCompoundFile = false;
      }
      if (format <= SegmentInfos.FORMAT_SINGLE_NORM_FILE) {
        hasSingleNormFile = (1 == input.readByte());
      } else {
        hasSingleNormFile = false;
      }
      int numNormGen = input.readInt();
      if (numNormGen == NO) {
        normGen = null;
      } else {
        normGen = new long[numNormGen];
        for(int j=0;j<numNormGen;j++) {
          normGen[j] = input.readLong();
        }
      }
      isCompoundFile = input.readByte();
      preLockless = (isCompoundFile == CHECK_DIR);
      if (format <= SegmentInfos.FORMAT_DEL_COUNT) {
        delCount = input.readInt();
        assert delCount <= docCount;
      } else
        delCount = -1;
      if (format <= SegmentInfos.FORMAT_HAS_PROX)
        hasProx = input.readByte() == 1;
      else
        hasProx = true;

      // System.out.println(Thread.currentThread().getName() + ": si.read hasProx=" + hasProx + " seg=" + name);
      
      if (format <= SegmentInfos.FORMAT_FLEX_POSTINGS)
        codecName = input.readString();
      else
        codecName = "PreFlex";

      if (format <= SegmentInfos.FORMAT_DIAGNOSTICS) {
        diagnostics = input.readStringStringMap();
      } else {
        diagnostics = Collections.<String,String>emptyMap();
      }
    } else {
      delGen = CHECK_DIR;
      normGen = null;
      isCompoundFile = CHECK_DIR;
      preLockless = true;
      hasSingleNormFile = false;
      docStoreOffset = -1;
      docStoreIsCompoundFile = false;
      docStoreSegment = null;
      delCount = -1;
      hasProx = true;
      codecName = "PreFlex";
      diagnostics = Collections.<String,String>emptyMap();
    }
    codec = codecs.lookup(codecName);
  }
  
  void setNumFields(int numFields) {
    if (normGen == null) {
      // normGen is null if we loaded a pre-2.1 segment
      // file, or, if this segments file hasn't had any
      // norms set against it yet:
      normGen = new long[numFields];

      if (preLockless) {
        // Do nothing: thus leaving normGen[k]==CHECK_DIR (==0), so that later we know  
        // we have to check filesystem for norm files, because this is prelockless.
        
      } else {
        // This is a FORMAT_LOCKLESS segment, which means
        // there are no separate norms:
        for(int i=0;i<numFields;i++) {
          normGen[i] = NO;
        }
      }
    }
  }

  /** Returns total size in bytes of all of files used by
   *  this segment. */
  public long sizeInBytes() throws IOException {
    if (sizeInBytes == -1) {
      List<String> files = files();
      final int size = files.size();
      sizeInBytes = 0;
      for(int i=0;i<size;i++) {
        final String fileName = files.get(i);
        // We don't count bytes used by a shared doc store
        // against this segment:
        if (docStoreOffset == -1 || !IndexFileNames.isDocStoreFile(fileName))
          sizeInBytes += dir.fileLength(fileName);
      }
    }
    return sizeInBytes;
  }

  public boolean hasDeletions()
    throws IOException {
    // Cases:
    //
    //   delGen == NO: this means this segment was written
    //     by the LOCKLESS code and for certain does not have
    //     deletions yet
    //
    //   delGen == CHECK_DIR: this means this segment was written by
    //     pre-LOCKLESS code which means we must check
    //     directory to see if .del file exists
    //
    //   delGen >= YES: this means this segment was written by
    //     the LOCKLESS code and for certain has
    //     deletions
    //
    if (delGen == NO) {
      return false;
    } else if (delGen >= YES) {
      return true;
    } else {
      return dir.fileExists(getDelFileName());
    }
  }

  void advanceDelGen() {
    // delGen 0 is reserved for pre-LOCKLESS format
    if (delGen == NO) {
      delGen = YES;
    } else {
      delGen++;
    }
    clearFiles();
  }

  void clearDelGen() {
    delGen = NO;
    clearFiles();
  }

  @Override
  public Object clone () {
    SegmentInfo si = new SegmentInfo(name, docCount, dir, codec);
    si.isCompoundFile = isCompoundFile;
    si.delGen = delGen;
    si.delCount = delCount;
    si.hasProx = hasProx;
    si.preLockless = preLockless;
    si.hasSingleNormFile = hasSingleNormFile;
    si.diagnostics = new HashMap<String, String>(diagnostics);
    if (normGen != null) {
      si.normGen = normGen.clone();
    }
    si.docStoreOffset = docStoreOffset;
    si.docStoreSegment = docStoreSegment;
    si.docStoreIsCompoundFile = docStoreIsCompoundFile;
    si.codec = codec;
    return si;
  }

  public String getDelFileName() {
    if (delGen == NO) {
      // In this case we know there is no deletion filename
      // against this segment
      return null;
    } else {
      // If delGen is CHECK_DIR, it's the pre-lockless-commit file format
      return IndexFileNames.fileNameFromGeneration(name, IndexFileNames.DELETES_EXTENSION, delGen); 
    }
  }

  /**
   * Returns true if this field for this segment has saved a separate norms file (_<segment>_N.sX).
   *
   * @param fieldNumber the field index to check
   */
  public boolean hasSeparateNorms(int fieldNumber)
    throws IOException {
    if ((normGen == null && preLockless) || (normGen != null && normGen[fieldNumber] == CHECK_DIR)) {
      // Must fallback to directory file exists check:
      String fileName = name + ".s" + fieldNumber;
      return dir.fileExists(fileName);
    } else if (normGen == null || normGen[fieldNumber] == NO) {
      return false;
    } else {
      return true;
    }
  }

  /**
   * Returns true if any fields in this segment have separate norms.
   */
  public boolean hasSeparateNorms()
    throws IOException {
    if (normGen == null) {
      if (!preLockless) {
        // This means we were created w/ LOCKLESS code and no
        // norms are written yet:
        return false;
      } else {
        // This means this segment was saved with pre-LOCKLESS
        // code.  So we must fallback to the original
        // directory list check:
        String[] result = dir.listAll();
        if (result == null)
          throw new IOException("cannot read directory " + dir + ": listAll() returned null");

        final String pattern = name + ".s\\d+";
        for(int i = 0; i < result.length; i++){
          String fileName = result[i];
          if (fileName.matches(pattern)) {
            return true;
          }
        }
        return false;
      }
    } else {
      // This means this segment was saved with LOCKLESS
      // code so we first check whether any normGen's are >= 1
      // (meaning they definitely have separate norms):
      for(int i=0;i<normGen.length;i++) {
        if (normGen[i] >= YES) {
          return true;
        }
      }
      // Next we look for any == 0.  These cases were
      // pre-LOCKLESS and must be checked in directory:
      for(int i=0;i<normGen.length;i++) {
        if (normGen[i] == CHECK_DIR) {
          if (hasSeparateNorms(i)) {
            return true;
          }
        }
      }
    }

    return false;
  }

  /**
   * Increment the generation count for the norms file for
   * this field.
   *
   * @param fieldIndex field whose norm file will be rewritten
   */
  void advanceNormGen(int fieldIndex) {
    if (normGen[fieldIndex] == NO) {
      normGen[fieldIndex] = YES;
    } else {
      normGen[fieldIndex]++;
    }
    clearFiles();
  }

  /**
   * Get the file name for the norms file for this field.
   *
   * @param number field index
   */
  public String getNormFileName(int number) throws IOException {
    long gen;
    if (normGen == null) {
      gen = CHECK_DIR;
    } else {
      gen = normGen[number];
    }
    
    if (hasSeparateNorms(number)) {
      // case 1: separate norm
      return IndexFileNames.fileNameFromGeneration(name, "s" + number, gen);
    }

    if (hasSingleNormFile) {
      // case 2: lockless (or nrm file exists) - single file for all norms 
      return IndexFileNames.fileNameFromGeneration(name, IndexFileNames.NORMS_EXTENSION, WITHOUT_GEN);
    }
      
    // case 3: norm file for each field
    return IndexFileNames.fileNameFromGeneration(name, "f" + number, WITHOUT_GEN);
  }

  /**
   * Mark whether this segment is stored as a compound file.
   *
   * @param isCompoundFile true if this is a compound file;
   * else, false
   */
  void setUseCompoundFile(boolean isCompoundFile) {
    if (isCompoundFile) {
      this.isCompoundFile = YES;
    } else {
      this.isCompoundFile = NO;
    }
    clearFiles();
  }

  /**
   * Returns true if this segment is stored as a compound
   * file; else, false.
   */
  public boolean getUseCompoundFile() throws IOException {
    if (isCompoundFile == NO) {
      return false;
    } else if (isCompoundFile == YES) {
      return true;
    } else {
      return dir.fileExists(IndexFileNames.segmentFileName(name, "", IndexFileNames.COMPOUND_FILE_EXTENSION));
    }
  }

  public int getDelCount() throws IOException {
    if (delCount == -1) {
      if (hasDeletions()) {
        final String delFileName = getDelFileName();
        delCount = new BitVector(dir, delFileName).count();
      } else
        delCount = 0;
    }
    assert delCount <= docCount;
    return delCount;
  }

  void setDelCount(int delCount) {
    this.delCount = delCount;
    assert delCount <= docCount;
  }

  public int getDocStoreOffset() {
    return docStoreOffset;
  }
  
  public boolean getDocStoreIsCompoundFile() {
    return docStoreIsCompoundFile;
  }
  
  void setDocStoreIsCompoundFile(boolean v) {
    docStoreIsCompoundFile = v;
    clearFiles();
  }
  
  public String getDocStoreSegment() {
    return docStoreSegment;
  }
  
  void setDocStoreOffset(int offset) {
    docStoreOffset = offset;
    clearFiles();
  }

  void setDocStore(int offset, String segment, boolean isCompoundFile) {        
    docStoreOffset = offset;
    docStoreSegment = segment;
    docStoreIsCompoundFile = isCompoundFile;
    clearFiles();
  }
  
  /**
   * Save this segment's info.
   */
  void write(IndexOutput output)
    throws IOException {
    assert delCount <= docCount: "delCount=" + delCount + " docCount=" + docCount + " segment=" + name;
    output.writeString(name);
    output.writeInt(docCount);
    output.writeLong(delGen);
    output.writeInt(docStoreOffset);
    if (docStoreOffset != -1) {
      output.writeString(docStoreSegment);
      output.writeByte((byte) (docStoreIsCompoundFile ? 1:0));
    }

    output.writeByte((byte) (hasSingleNormFile ? 1:0));
    if (normGen == null) {
      output.writeInt(NO);
    } else {
      output.writeInt(normGen.length);
      for(int j = 0; j < normGen.length; j++) {
        output.writeLong(normGen[j]);
      }
    }
    output.writeByte(isCompoundFile);
    output.writeInt(delCount);
    output.writeByte((byte) (hasProx ? 1:0));
    output.writeString(codec.name);
    output.writeStringStringMap(diagnostics);
  }

  void setHasProx(boolean hasProx) {
    this.hasProx = hasProx;
    clearFiles();
  }

  public boolean getHasProx() {
    return hasProx;
  }

  /** Can only be called once. */
  public void setCodec(Codec codec) {
    assert this.codec == null;
    if (codec == null) {
      throw new IllegalArgumentException("codec must be non-null");
    }
    this.codec = codec;
  }

  Codec getCodec() {
    return codec;
  }

  private void addIfExists(Set<String> files, String fileName) throws IOException {
    if (dir.fileExists(fileName))
      files.add(fileName);
  }

  /*
   * Return all files referenced by this SegmentInfo.  The
   * returns List is a locally cached List so you should not
   * modify it.
   */

  public List<String> files() throws IOException {

    if (files != null) {
      // Already cached:
      return files;
    }
    
    Set<String> fileSet = new HashSet<String>();
    
    boolean useCompoundFile = getUseCompoundFile();

    if (useCompoundFile) {
      fileSet.add(IndexFileNames.segmentFileName(name, "", IndexFileNames.COMPOUND_FILE_EXTENSION));
    } else {
      for(String ext : IndexFileNames.NON_STORE_INDEX_EXTENSIONS) {
        addIfExists(fileSet, IndexFileNames.segmentFileName(name, "", ext));
      }
      codec.files(dir, this, fileSet);
    }

    if (docStoreOffset != -1) {
      // We are sharing doc stores (stored fields, term
      // vectors) with other segments
      assert docStoreSegment != null;
      if (docStoreIsCompoundFile) {
        fileSet.add(IndexFileNames.segmentFileName(docStoreSegment, "", IndexFileNames.COMPOUND_FILE_STORE_EXTENSION));
      } else {
        for (String ext : IndexFileNames.STORE_INDEX_EXTENSIONS)
          addIfExists(fileSet, IndexFileNames.segmentFileName(docStoreSegment, "", ext));
      }
    } else if (!useCompoundFile) {
      for (String ext : IndexFileNames.STORE_INDEX_EXTENSIONS)
        addIfExists(fileSet, IndexFileNames.segmentFileName(name, "", ext));
    }

    String delFileName = IndexFileNames.fileNameFromGeneration(name, IndexFileNames.DELETES_EXTENSION, delGen);
    if (delFileName != null && (delGen >= YES || dir.fileExists(delFileName))) {
      fileSet.add(delFileName);
    }

    // Careful logic for norms files    
    if (normGen != null) {
      for(int i=0;i<normGen.length;i++) {
        long gen = normGen[i];
        if (gen >= YES) {
          // Definitely a separate norm file, with generation:
          fileSet.add(IndexFileNames.fileNameFromGeneration(name, IndexFileNames.SEPARATE_NORMS_EXTENSION + i, gen));
        } else if (NO == gen) {
          // No separate norms but maybe plain norms
          // in the non compound file case:
          if (!hasSingleNormFile && !useCompoundFile) {
            String fileName = IndexFileNames.segmentFileName(name, "", IndexFileNames.PLAIN_NORMS_EXTENSION + i);
            if (dir.fileExists(fileName)) {
              fileSet.add(fileName);
            }
          }
        } else if (CHECK_DIR == gen) {
          // Pre-2.1: we have to check file existence
          String fileName = null;
          if (useCompoundFile) {
            fileName = IndexFileNames.segmentFileName(name, "", IndexFileNames.SEPARATE_NORMS_EXTENSION + i);
          } else if (!hasSingleNormFile) {
            fileName = IndexFileNames.segmentFileName(name, "", IndexFileNames.PLAIN_NORMS_EXTENSION + i);
          }
          if (fileName != null && dir.fileExists(fileName)) {
            fileSet.add(fileName);
          }
        }
      }
    } else if (preLockless || (!hasSingleNormFile && !useCompoundFile)) {
      // Pre-2.1: we have to scan the dir to find all
      // matching _X.sN/_X.fN files for our segment:
      String prefix;
      if (useCompoundFile) {
        prefix = IndexFileNames.segmentFileName(name, "", IndexFileNames.SEPARATE_NORMS_EXTENSION);
      } else {
        prefix = IndexFileNames.segmentFileName(name, "", IndexFileNames.PLAIN_NORMS_EXTENSION);
      }
      final String pattern = prefix + "\\d+";

      String[] allFiles = dir.listAll();
      for(int i=0;i<allFiles.length;i++) {
        String fileName = allFiles[i];
        if (fileName.matches(pattern)) {
          fileSet.add(fileName);
        }
      }
    }

    files = new ArrayList<String>(fileSet);

    return files;
  }

  /* Called whenever any change is made that affects which
   * files this segment has. */
  private void clearFiles() {
    files = null;
    sizeInBytes = -1;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return toString(dir, 0);
  }

  /** Used for debugging.  Format may suddenly change.
   * 
   *  <p>Current format looks like
   *  <code>_a:c45/4->_1</code>, which means the segment's
   *  name is <code>_a</code>; it's using compound file
   *  format (would be <code>C</code> if not compound); it
   *  has 45 documents; it has 4 deletions (this part is
   *  left off when there are no deletions); it's using the
   *  shared doc stores named <code>_1</code> (this part is
   *  left off if doc stores are private).</p>
   */
  public String toString(Directory dir, int pendingDelCount) {

    StringBuilder s = new StringBuilder();
    s.append(name).append(':');

    char cfs;
    try {
      if (getUseCompoundFile()) {
        cfs = 'c';
      } else {
        cfs = 'C';
      }
    } catch (IOException ioe) {
      cfs = '?';
    }
    s.append(cfs);

    if (this.dir != dir) {
      s.append('x');
    }
    s.append(docCount);

    int delCount;
    try {
      delCount = getDelCount();
    } catch (IOException ioe) {
      delCount = -1;
    }
    if (delCount != -1) {
      delCount += pendingDelCount;
    }
    if (delCount != 0) {
      s.append('/');
      if (delCount == -1) {
        s.append('?');
      } else {
        s.append(delCount);
      }
    }
    
    if (docStoreOffset != -1) {
      s.append("->").append(docStoreSegment);
    }

    return s.toString();
  }

  /** We consider another SegmentInfo instance equal if it
   *  has the same dir and same name. */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj instanceof SegmentInfo) {
      final SegmentInfo other = (SegmentInfo) obj;
      return other.dir == dir && other.name.equals(name);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return dir.hashCode() + name.hashCode();
  }
}
