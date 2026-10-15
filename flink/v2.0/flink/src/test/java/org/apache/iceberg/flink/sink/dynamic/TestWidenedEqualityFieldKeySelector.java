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

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.Set;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.InstantiationUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.EqualityFieldKeySelector;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestWidenedEqualityFieldKeySelector {

  private static final Set<Integer> KEY = Sets.newHashSet(1);

  @Test
  void testIntKeyHashesLikeItsLongPromotion() throws Exception {
    Schema narrow = schema(Types.IntegerType.get());
    Schema wide = schema(Types.LongType.get());
    WidenedEqualityFieldKeySelector narrowSelector = selector(narrow, KEY);
    WidenedEqualityFieldKeySelector wideSelector = selector(wide, KEY);

    for (int id : new int[] {Integer.MIN_VALUE, -5, -1, 0, 1, 42, Integer.MAX_VALUE}) {
      assertThat(narrowSelector.getKey(row(id)))
          .as("Key %s", id)
          .isEqualTo(wideSelector.getKey(row((long) id)));
    }

    // without widening, the two hashes of a negative key differ
    assertThat(
            new EqualityFieldKeySelector(narrow, FlinkSchemaUtil.convert(narrow), KEY)
                .getKey(row(-5)))
        .isNotEqualTo(
            new EqualityFieldKeySelector(wide, FlinkSchemaUtil.convert(wide), KEY)
                .getKey(row(-5L)));
  }

  @Test
  void testFloatKeyHashesLikeItsDoublePromotion() throws Exception {
    WidenedEqualityFieldKeySelector narrowSelector = selector(schema(Types.FloatType.get()), KEY);
    WidenedEqualityFieldKeySelector wideSelector = selector(schema(Types.DoubleType.get()), KEY);

    for (float value : new float[] {0.1f, 1.5f, -0.0f, Float.NaN, Float.MAX_VALUE}) {
      assertThat(narrowSelector.getKey(row(value)))
          .as("Key %s", value)
          .isEqualTo(wideSelector.getKey(row((double) value)));
    }

    assertThat(narrowSelector.getKey(row((Object) null)))
        .isEqualTo(wideSelector.getKey(row((Object) null)));
  }

  @Test
  void testKeyOfSeveralFieldsHashesLikeItsPromotions() throws Exception {
    Schema narrow =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "score", Types.FloatType.get()),
            Types.NestedField.optional(3, "data", Types.StringType.get()));
    Schema wide =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "score", Types.DoubleType.get()),
            Types.NestedField.optional(3, "data", Types.StringType.get()));
    Set<Integer> key = Sets.newHashSet(1, 2, 3);

    assertThat(
            selector(narrow, key).getKey(GenericRowData.of(-7, 0.3f, StringData.fromString("a"))))
        .isEqualTo(
            selector(wide, key)
                .getKey(GenericRowData.of(-7L, (double) 0.3f, StringData.fromString("a"))));
  }

  @Test
  void testKeysWithoutNegativeIntOrFloatValuesHashAsBefore() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "day", Types.DateType.get()),
            Types.NestedField.optional(3, "data", Types.StringType.get()),
            Types.NestedField.optional(4, "amount", Types.DecimalType.of(10, 2)),
            Types.NestedField.optional(5, "seq", Types.LongType.get()));
    Set<Integer> key = Sets.newHashSet(1, 2, 3, 4, 5);
    WidenedEqualityFieldKeySelector widened = selector(schema, key);
    EqualityFieldKeySelector original =
        new EqualityFieldKeySelector(schema, FlinkSchemaUtil.convert(schema), key);

    // dates are ints too, but are not promoted, so a negative day keeps its hash
    for (int id = 0; id <= 1000; id += 1) {
      RowData row =
          GenericRowData.of(
              id,
              id - 500,
              StringData.fromString("data-" + id),
              DecimalData.fromBigDecimal(BigDecimal.valueOf(id - 500, 2), 10, 2),
              (long) -id);
      assertThat(widened.getKey(row)).as("Key %s", id).isEqualTo(original.getKey(row));
    }

    RowData nulls = GenericRowData.of(3, null, null, null, null);
    assertThat(widened.getKey(nulls)).isEqualTo(original.getKey(nulls));
  }

  @Test
  void testSelectorIsSerializable() throws Exception {
    WidenedEqualityFieldKeySelector selector = selector(schema(Types.IntegerType.get()), KEY);
    int key = selector.getKey(row(-5));

    assertThat(InstantiationUtil.clone(selector).getKey(row(-5))).isEqualTo(key);
  }

  private static Schema schema(Type keyType) {
    return new Schema(
        Types.NestedField.optional(1, "key", keyType),
        Types.NestedField.optional(2, "data", Types.StringType.get()));
  }

  private static WidenedEqualityFieldKeySelector selector(Schema schema, Set<Integer> key) {
    return new WidenedEqualityFieldKeySelector(schema, FlinkSchemaUtil.convert(schema), key);
  }

  private static RowData row(Object key) {
    return GenericRowData.of(key, StringData.fromString("data"));
  }
}
