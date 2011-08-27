package org.apache.lucene.document;

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
import java.io.Reader;
import java.util.Comparator;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.values.PerDocFieldValues;
import org.apache.lucene.index.values.ValueType;
import org.apache.lucene.util.BytesRef;

/**
 * <p>
 * This class provides a {@link Field} that enables storing of typed
 * per-document values for scoring, sorting or value retrieval. Here's an
 * example usage, adding an int value:
 * 
 * <pre>
 * document.add(new IndexDocValuesField(name).setInt(value));
 * </pre>
 * 
 * For optimal performance, re-use the <code>DocValuesField</code> and
 * {@link Document} instance for more than one document:
 * 
 * <pre>
 *  IndexDocValuesField field = new IndexDocValuesField(name);
 *  Document document = new Document();
 *  document.add(field);
 * 
 *  for(all documents) {
 *    ...
 *    field.setInt(value)
 *    writer.addDocument(document);
 *    ...
 *  }
 * </pre>
 * 
 * <p>
 * If doc values are stored in addition to an indexed ({@link FieldType#setIndexed(boolean)}) or stored
 * ({@link FieldType#setStored(boolean)}) value it's recommended to pass the appropriate {@link FieldType}
 * when creating the field:
 * 
 * <pre>
 *  IndexDocValuesField field = new IndexDocValuesField(name, StringField.TYPE_STORED);
 *  Document document = new Document();
 *  document.add(field);
 *  for(all documents) {
 *    ...
 *    field.setInt(value)
 *    writer.addDocument(document);
 *    ...
 *  }
 * </pre>
 * 
 * */
// TODO: maybe rename to DocValuesField?
public class IndexDocValuesField extends Field implements PerDocFieldValues {

  protected BytesRef bytes;
  protected double doubleValue;
  protected long longValue;
  protected ValueType type;
  protected Comparator<BytesRef> bytesComparator;

  /**
   * Creates a new {@link IndexDocValuesField} with the given name.
   */
  public IndexDocValuesField(String name) {
    this(name, new FieldType());
  }

  public IndexDocValuesField(String name, FieldType type) {
    this(name, type, null);
  }

  public IndexDocValuesField(String name, FieldType type, String value) {
    super(name, type);
    fieldsData = value;
  }

  @Override
  public PerDocFieldValues docValues() {
    return this;
  }

  /**
   * Sets the given <code>long</code> value and sets the field's {@link ValueType} to
   * {@link ValueType#VAR_INTS} unless already set. If you want to change the
   * default type use {@link #setDocValuesType(ValueType)}.
   */
  public void setInt(long value) {
    setInt(value, false);
  }
  
  /**
   * Sets the given <code>long</code> value as a 64 bit signed integer.
   * 
   * @param value
   *          the value to set
   * @param fixed
   *          if <code>true</code> {@link ValueType#FIXED_INTS_64} is used
   *          otherwise {@link ValueType#VAR_INTS}
   */
  public void setInt(long value, boolean fixed) {
    if (type == null) {
      type = fixed ? ValueType.FIXED_INTS_64 : ValueType.VAR_INTS;
    }
    longValue = value;
  }

  /**
   * Sets the given <code>int</code> value and sets the field's {@link ValueType} to
   * {@link ValueType#VAR_INTS} unless already set. If you want to change the
   * default type use {@link #setDocValuesType(ValueType)}.
   */
  public void setInt(int value) {
    setInt(value, false);
  }

  /**
   * Sets the given <code>int</code> value as a 32 bit signed integer.
   * 
   * @param value
   *          the value to set
   * @param fixed
   *          if <code>true</code> {@link ValueType#FIXED_INTS_32} is used
   *          otherwise {@link ValueType#VAR_INTS}
   */
  public void setInt(int value, boolean fixed) {
    if (type == null) {
      type = fixed ? ValueType.FIXED_INTS_32 : ValueType.VAR_INTS;
    }
    longValue = value;
  }

  /**
   * Sets the given <code>short</code> value and sets the field's {@link ValueType} to
   * {@link ValueType#VAR_INTS} unless already set. If you want to change the
   * default type use {@link #setDocValuesType(ValueType)}.
   */
  public void setInt(short value) {
    setInt(value, false);
  }

  /**
   * Sets the given <code>short</code> value as a 16 bit signed integer.
   * 
   * @param value
   *          the value to set
   * @param fixed
   *          if <code>true</code> {@link ValueType#FIXED_INTS_16} is used
   *          otherwise {@link ValueType#VAR_INTS}
   */
  public void setInt(short value, boolean fixed) {
    if (type == null) {
      type = fixed ? ValueType.FIXED_INTS_16 : ValueType.VAR_INTS;
    }
    longValue = value;
  }

  /**
   * Sets the given <code>byte</code> value and sets the field's {@link ValueType} to
   * {@link ValueType#VAR_INTS} unless already set. If you want to change the
   * default type use {@link #setDocValuesType(ValueType)}.
   */
  public void setInt(byte value) {
    setInt(value, false);
  }

  /**
   * Sets the given <code>byte</code> value as a 8 bit signed integer.
   * 
   * @param value
   *          the value to set
   * @param fixed
   *          if <code>true</code> {@link ValueType#FIXED_INTS_8} is used
   *          otherwise {@link ValueType#VAR_INTS}
   */
  public void setInt(byte value, boolean fixed) {
    if (type == null) {
      type = fixed ? ValueType.FIXED_INTS_8 : ValueType.VAR_INTS;
    }
    longValue = value;
  }

  /**
   * Sets the given <code>float</code> value and sets the field's {@link ValueType}
   * to {@link ValueType#FLOAT_32} unless already set. If you want to
   * change the type use {@link #setDocValuesType(ValueType)}.
   */
  public void setFloat(float value) {
    if (type == null) {
      type = ValueType.FLOAT_32;
    }
    doubleValue = value;
  }

  /**
   * Sets the given <code>double</code> value and sets the field's {@link ValueType}
   * to {@link ValueType#FLOAT_64} unless already set. If you want to
   * change the default type use {@link #setDocValuesType(ValueType)}.
   */
  public void setFloat(double value) {
    if (type == null) {
      type = ValueType.FLOAT_64;
    }
    doubleValue = value;
  }

  /**
   * Sets the given {@link BytesRef} value and the field's {@link ValueType}. The
   * comparator for this field is set to <code>null</code>. If a
   * <code>null</code> comparator is set the default comparator for the given
   * {@link ValueType} is used.
   */
  public void setBytes(BytesRef value, ValueType type) {
    setBytes(value, type, null);
  }

  /**
   * Sets the given {@link BytesRef} value, the field's {@link ValueType} and the
   * field's comparator. If the {@link Comparator} is set to <code>null</code>
   * the default for the given {@link ValueType} is used instead.
   * 
   * @throws IllegalArgumentException
   *           if the value or the type are null
   */
  public void setBytes(BytesRef value, ValueType type, Comparator<BytesRef> comp) {
    if (value == null) {
      throw new IllegalArgumentException("value must not be null");
    }
    setDocValuesType(type);
    if (bytes == null) {
      bytes = new BytesRef(value);
    } else {
      bytes.copy(value);
    }
    bytesComparator = comp;
  }

  /**
   * Returns the set {@link BytesRef} or <code>null</code> if not set.
   */
  public BytesRef getBytes() {
    return bytes;
  }

  /**
   * Returns the set {@link BytesRef} comparator or <code>null</code> if not set
   */
  public Comparator<BytesRef> bytesComparator() {
    return bytesComparator;
  }

  /**
   * Returns the set floating point value or <code>0.0d</code> if not set.
   */
  public double getFloat() {
    return doubleValue;
  }

  /**
   * Returns the set <code>long</code> value of <code>0</code> if not set.
   */
  public long getInt() {
    return longValue;
  }

  /**
   * Sets the {@link BytesRef} comparator for this field. If the field has a
   * numeric {@link ValueType} the comparator will be ignored.
   */
  public void setBytesComparator(Comparator<BytesRef> comp) {
    this.bytesComparator = comp;
  }

  /**
   * Sets the {@link ValueType} for this field.
   */
  public void setDocValuesType(ValueType type) {
    if (type == null) {
      throw new IllegalArgumentException("Type must not be null");
    }
    this.type = type;
  }

  /**
   * Returns always <code>null</code>
   */
  public Reader readerValue() {
    return null;
  }

  /**
   * Returns always <code>null</code>
   */
  public TokenStream tokenStreamValue() {
    return null;
  }

  @Override
  public ValueType docValuesType() {
    return type;
  }

  @Override
  public String toString() {
    final String value;
    switch (type) {
    case BYTES_FIXED_DEREF:
    case BYTES_FIXED_SORTED:
    case BYTES_FIXED_STRAIGHT:
    case BYTES_VAR_DEREF:
    case BYTES_VAR_SORTED:
    case BYTES_VAR_STRAIGHT:
      value = "bytes:bytes.utf8ToString();";
      break;
    case VAR_INTS:
      value = "int:" + longValue;
      break;
    case FLOAT_32:
      value = "float32:" + doubleValue;
      break;
    case FLOAT_64:
      value = "float64:" + doubleValue;
      break;
    default:
      throw new IllegalArgumentException("unknown type: " + type);
    }
    return "<" + name() + ": IndexDocValuesField " + value + ">";
  }

  /**
   * Returns an IndexDocValuesField holding the value from
   * the provided string field, as the specified type.  The
   * incoming field must have a string value.  The name, {@link
   * FieldType} and string value are carried over from the
   * incoming Field.
   */
  public static IndexDocValuesField build(Field field, ValueType type) {
    if (field instanceof IndexDocValuesField) {
      return (IndexDocValuesField) field;
    }
    final IndexDocValuesField valField = new IndexDocValuesField(field.name(), field.getFieldType(), field.stringValue());
    switch (type) {
    case BYTES_FIXED_DEREF:
    case BYTES_FIXED_SORTED:
    case BYTES_FIXED_STRAIGHT:
    case BYTES_VAR_DEREF:
    case BYTES_VAR_SORTED:
    case BYTES_VAR_STRAIGHT:
      BytesRef ref = field.isBinary() ? field.binaryValue() : new BytesRef(field.stringValue());
      valField.setBytes(ref, type);
      break;
    case VAR_INTS:
      valField.setInt(Long.parseLong(field.stringValue()));
      break;
    case FLOAT_32:
      valField.setFloat(Float.parseFloat(field.stringValue()));
      break;
    case FLOAT_64:
      valField.setFloat(Double.parseDouble(field.stringValue()));
      break;
    default:
      throw new IllegalArgumentException("unknown type: " + type);
    }
    return valField;
  }
}
