/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink.dynamic;

import java.util.List;
import java.util.Set;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.StructProjection;

/**
 * Hashes the equality fields of a row with int and float values widened to long and double, so that
 * the rows of a key go to the same writer before and after a type promotion of an equality field.
 * Writers of one table can then retire each other's rows of the key within a checkpoint.
 *
 * <p>Keys hash as with {@code EqualityFieldKeySelector}, except for negative int values and float
 * values: an int from 0 up has the same hash as the long it widens to, and values of other types
 * are not converted.
 */
class WidenedEqualityFieldKeySelector implements KeySelector<RowData, Integer> {

  private final Schema schema;
  private final RowType flinkSchema;
  private final Schema deleteSchema;

  private transient RowDataWrapper rowDataWrapper;
  private transient StructProjection structProjection;
  private transient Type.TypeID[] keyTypes;
  private transient KeyValues widenedKey;
  private transient StructLikeWrapper structLikeWrapper;

  WidenedEqualityFieldKeySelector(
      Schema schema, RowType flinkSchema, Set<Integer> equalityFieldIds) {
    this.schema = schema;
    this.flinkSchema = flinkSchema;
    this.deleteSchema = TypeUtil.select(schema, equalityFieldIds);
  }

  @Override
  public Integer getKey(RowData row) {
    if (rowDataWrapper == null) {
      initialize();
    }

    StructLike key = structProjection.wrap(rowDataWrapper.wrap(row));
    for (int pos = 0; pos < widenedKey.size(); pos += 1) {
      Object value = key.get(pos, Object.class);
      if (value != null && keyTypes[pos] == Type.TypeID.INTEGER) {
        widenedKey.set(pos, ((Integer) value).longValue());
      } else if (value != null && keyTypes[pos] == Type.TypeID.FLOAT) {
        widenedKey.set(pos, ((Float) value).doubleValue());
      } else {
        widenedKey.set(pos, value);
      }
    }

    return structLikeWrapper.set(widenedKey).hashCode();
  }

  // The members are built on first use because some of them are not serializable
  private void initialize() {
    List<Types.NestedField> keyFields = deleteSchema.asStruct().fields();
    List<Types.NestedField> widenedFields = Lists.newArrayListWithCapacity(keyFields.size());
    this.keyTypes = new Type.TypeID[keyFields.size()];
    for (int pos = 0; pos < keyFields.size(); pos += 1) {
      Types.NestedField field = keyFields.get(pos);
      keyTypes[pos] = field.type().typeId();
      widenedFields.add(
          Types.NestedField.of(
              field.fieldId(), field.isOptional(), field.name(), widen(field.type())));
    }

    this.structProjection = StructProjection.create(schema, deleteSchema);
    this.widenedKey = new KeyValues(keyFields.size());
    this.structLikeWrapper = StructLikeWrapper.forType(Types.StructType.of(widenedFields));
    this.rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
  }

  private static Type widen(Type type) {
    switch (type.typeId()) {
      case INTEGER:
        return Types.LongType.get();
      case FLOAT:
        return Types.DoubleType.get();
      default:
        return type;
    }
  }

  // The values of a key, reused for every row
  private static class KeyValues implements StructLike {
    private final Object[] values;

    private KeyValues(int size) {
      this.values = new Object[size];
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      values[pos] = value;
    }
  }
}
