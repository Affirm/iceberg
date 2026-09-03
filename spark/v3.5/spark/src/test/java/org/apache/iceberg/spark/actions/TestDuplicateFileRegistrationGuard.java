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
package org.apache.iceberg.spark.actions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * AFFIRM: direct unit coverage for {@link DuplicateFileRegistrationGuard}'s V3 deletion-vector
 * refusal.
 *
 * <p>This branch cannot be driven end-to-end through any action, because Iceberg 1.11 refuses to
 * create the state it defends against in the first place: {@code BaseRowDelta#validate} calls
 * {@code validateAddedDVs} unconditionally, so adding a second DV for a data file that already has
 * one fails at commit with "Found concurrently added DV for file..." (pinned by {@code
 * TestRewritePositionDeleteFilesAction#testIcebergItselfRefusesToCreateADuplicateDvRegistration}).
 * The branch is kept as defence-in-depth for a state arriving by some other route, so it is tested
 * here at the decision function rather than left unverified.
 */
public class TestDuplicateFileRegistrationGuard {

  private static final StructType DUPLICATE_SCHEMA =
      new StructType()
          .add("file_path", DataTypes.StringType)
          .add("content", DataTypes.IntegerType)
          .add("keep_sequence_number", DataTypes.LongType)
          .add("distinct_sequence_numbers", DataTypes.LongType);

  private static Row duplicate(String path, FileContent content) {
    return new GenericRowWithSchema(
        new Object[] {path, content.id(), 1L, 2L}, DUPLICATE_SCHEMA);
  }

  @Test
  public void testDataFileDuplicateIsSafeToAutoRepairOnEveryFormatVersion() {
    List<Row> duplicates = ImmutableList.of(duplicate("/path/data-a.parquet", FileContent.DATA));

    for (int formatVersion : new int[] {1, 2, 3, 4}) {
      assertThat(
              DuplicateFileRegistrationGuard.hasUnsafeDeletionVectorDuplicate(
                  formatVersion, duplicates))
          .as(
              "A duplicated DATA file is always safe to repair -- DataFileSet keys on location() "
                  + "alone, so there is no Puffin-style sharing to confuse it (v%s)",
              formatVersion)
          .isFalse();
    }
  }

  @Test
  public void testDeleteFileDuplicateIsSafeToAutoRepairBelowV3() {
    List<Row> positionDeletes =
        ImmutableList.of(duplicate("/path/deletes.parquet", FileContent.POSITION_DELETES));
    List<Row> equalityDeletes =
        ImmutableList.of(duplicate("/path/eq-deletes.parquet", FileContent.EQUALITY_DELETES));

    for (int formatVersion : new int[] {1, 2}) {
      assertThat(
              DuplicateFileRegistrationGuard.hasUnsafeDeletionVectorDuplicate(
                  formatVersion, positionDeletes))
          .as("Below v3 a delete file owns its whole location, so repair is safe (v%s)", formatVersion)
          .isFalse();
      assertThat(
              DuplicateFileRegistrationGuard.hasUnsafeDeletionVectorDuplicate(
                  formatVersion, equalityDeletes))
          .as("Same for equality deletes (v%s)", formatVersion)
          .isFalse();
    }
  }

  @Test
  public void testDeleteFileDuplicateIsRefusedFromV3Onward() {
    List<Row> positionDeletes =
        ImmutableList.of(duplicate("/path/deletes.puffin", FileContent.POSITION_DELETES));
    List<Row> equalityDeletes =
        ImmutableList.of(duplicate("/path/eq-deletes.puffin", FileContent.EQUALITY_DELETES));

    for (int formatVersion : new int[] {3, 4}) {
      assertThat(
              DuplicateFileRegistrationGuard.hasUnsafeDeletionVectorDuplicate(
                  formatVersion, positionDeletes))
          .as(
              "From v3 a Puffin file can legitimately hold several distinct delete files at one "
                  + "location, which location-only matching cannot tell from a real duplicate, so "
                  + "auto-repair must be refused (v%s)",
              formatVersion)
          .isTrue();
      assertThat(
              DuplicateFileRegistrationGuard.hasUnsafeDeletionVectorDuplicate(
                  formatVersion, equalityDeletes))
          .as("Refused for any non-DATA content on v%s", formatVersion)
          .isTrue();
    }
  }

  @Test
  public void testOneUnsafeDeleteDuplicatePoisonsTheWholeBatch() {
    // The repair commits every duplicated path together, so a single unsafe entry has to veto the
    // batch -- repairing "just the safe ones" would still be a partial commit built on a set the
    // guard has already decided it cannot fully reason about.
    List<Row> mixed =
        ImmutableList.of(
            duplicate("/path/data-a.parquet", FileContent.DATA),
            duplicate("/path/data-b.parquet", FileContent.DATA),
            duplicate("/path/deletes.puffin", FileContent.POSITION_DELETES));

    assertThat(DuplicateFileRegistrationGuard.hasUnsafeDeletionVectorDuplicate(3, mixed))
        .as("One unsafe delete-file duplicate must veto auto-repair for the whole batch")
        .isTrue();
    assertThat(DuplicateFileRegistrationGuard.hasUnsafeDeletionVectorDuplicate(2, mixed))
        .as("...but only from v3 onward; on v2 the same batch is fully repairable")
        .isFalse();
  }

  @Test
  public void testEmptyDuplicateListIsNeverUnsafe() {
    assertThat(DuplicateFileRegistrationGuard.hasUnsafeDeletionVectorDuplicate(4, ImmutableList.of()))
        .isFalse();
  }
}
