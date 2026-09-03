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

import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.min;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DuplicateRegistrationRepair;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AFFIRM: shared duplicate-file-registration detection and repair, used by both {@link
 * RewriteDataFilesSparkAction} and {@link RewritePositionDeleteFilesSparkAction}.
 *
 * <p>Pulled out of {@code RewriteDataFilesSparkAction} per review feedback on Affirm/iceberg#6
 * (pullrequestreview-5105776910): the underlying defect -- a file registered twice, at two data
 * sequence numbers, silently corrupting the next compaction because {@code DataFileSet}/{@code
 * DeleteFileSet} collapse it by location alone -- is reachable through any action that commits
 * through {@code ManifestFilterManager}, not just {@code rewrite_data_files}. {@code
 * rewrite_position_delete_files} is run standalone in production oncall/backfill notebooks with
 * no dependency on {@code rewrite_data_files} ever having run against the same table, so it needs
 * the identical guard rather than a second, drifting copy of this logic.
 */
final class DuplicateFileRegistrationGuard {

  private static final Logger LOG = LoggerFactory.getLogger(DuplicateFileRegistrationGuard.class);
  private static final int DUPLICATE_REGISTRATION_REPORT_LIMIT = 10;

  private DuplicateFileRegistrationGuard() {}

  /**
   * Detects live file paths (data or delete) registered at more than one data sequence number
   * and, depending on {@code resolve}, either repairs them or throws.
   *
   * @param spark the caller's Spark session
   * @param table the table being scanned
   * @param validate if false, skip the check entirely
   * @param resolve if true, repair a detected duplicate instead of failing; forced to a no-op
   *     for any duplicate this guard cannot safely auto-repair (see {@link
   *     #hasUnsafeDeletionVectorDuplicate})
   * @param validatePropertyName the caller's validate-option name, used only in error messages
   * @param resolvePropertyName the caller's resolve-option name, used only in error messages
   */
  static void validateOrRepair(
      SparkSession spark,
      Table table,
      boolean validate,
      boolean resolve,
      String validatePropertyName,
      String resolvePropertyName) {
    if (!validate) {
      return;
    }

    List<Row> duplicates = findDuplicateFileRegistrations(spark, table);
    if (duplicates.isEmpty()) {
      return;
    }

    boolean unsafeForAutoRepair =
        hasUnsafeDeletionVectorDuplicate(TableUtil.formatVersion(table), duplicates);
    if (resolve && !unsafeForAutoRepair) {
      repairDuplicateFileRegistrations(table, duplicates, resolvePropertyName);
      return;
    }

    throwDuplicateRegistrationError(
        table, duplicates, unsafeForAutoRepair, validatePropertyName, resolvePropertyName);
  }

  /**
   * AFFIRM: returns one row per live file path (data or delete) that is registered at more than
   * one distinct live data sequence number, with columns {@code file_path}, {@code content} (the
   * lowest live registration's content type -- content does not vary across duplicate
   * registrations of the same physical file), {@code keep_sequence_number} (the lowest live data
   * sequence number registered for that path), and {@code distinct_sequence_numbers} (how many
   * distinct sequence numbers that path is live at). Empty if the table has no such path.
   *
   * <p>Uses the {@code Dataset} groupBy/agg API (same shape as {@link
   * RemoveDanglingDeletesSparkAction#findDanglingDeletes}), not a temp view plus raw SQL --
   * cheaper (no extra catalog round-trip) and compile-time column-safe.
   */
  private static List<Row> findDuplicateFileRegistrations(SparkSession spark, Table table) {
    Dataset<Row> liveEntries =
        SparkTableUtil.loadMetadataTable(spark, table, MetadataTableType.ENTRIES)
            .filter("status < 2") // live entries only; status 2 is DELETED
            .selectExpr(
                "data_file.file_path as file_path",
                "data_file.content as content",
                "sequence_number")
            .distinct();

    return liveEntries
        .groupBy("file_path")
        .agg(
            min("content").as("content"),
            min("sequence_number").as("keep_sequence_number"),
            countDistinct("sequence_number").as("distinct_sequence_numbers"))
        .filter("distinct_sequence_numbers > 1")
        .collectAsList();
  }

  /**
   * AFFIRM: true if any duplicated path in {@code duplicates} is a delete file on a table that
   * may carry V3+ deletion vectors.
   *
   * <p>A V3 deletion vector packs multiple DISTINCT delete files (one per referenced data file)
   * into a single Puffin file that they all share as {@code location()} -- {@code
   * DeleteFileSet}'s equality key is {@code (location, contentOffset, contentSizeInBytes)}
   * specifically because of this; {@code DataFileSet} has no such concern and keys on {@code
   * location()} alone. This guard's matching ({@code ManifestFilterManager
   * #isDuplicateRegistrationToDrop}) is location-only, so on a V3+ table it cannot distinguish
   * those legitimately-distinct DVs sharing one Puffin path from an actual duplicate registration
   * of that whole path. Auto-repairing in that case risks dropping a DV that still legitimately
   * covers a different data file than the one actually duplicated -- reproducing the exact class
   * of defect this guard exists to prevent, through a path this guard cannot see. Refuse to
   * auto-repair the whole batch when this is possible; always require manual remediation for it,
   * regardless of the caller's resolve option.
   *
   * <p><b>This is defence-in-depth, not a live scenario on 1.11.</b> Verified while testing:
   * {@code BaseRowDelta#validate} calls {@code validateAddedDVs} UNCONDITIONALLY on every
   * {@code RowDelta} with a parent snapshot, so Iceberg 1.11 already refuses at commit time to
   * add a second DV for a data file that has one ("Found concurrently added DV for file..."). A
   * duplicated-DV registration therefore cannot be produced through the public write path at
   * all -- an attempt to construct one in a test is rejected by that validation. This branch
   * exists for a state arriving by some other route (a lower-level writer, a legacy table
   * migrated in, or a future regression in that validation), where refusing is strictly better
   * than guessing. Kept deliberately rather than deleted as dead code.
   */
  @VisibleForTesting
  static boolean hasUnsafeDeletionVectorDuplicate(int formatVersion, List<Row> duplicates) {
    if (formatVersion < 3) {
      return false;
    }

    return duplicates.stream()
        .anyMatch(row -> ((int) row.getAs("content")) != FileContent.DATA.id());
  }

  /**
   * AFFIRM: repairs every duplicated file path found by {@link
   * #findDuplicateFileRegistrations(SparkSession, Table)} in a single commit, then refreshes
   * {@code table} so the caller plans its rewrite against the corrected metadata.
   *
   * <p>For each path, keeps the lowest live data sequence number and drops every other live
   * registration of that path. This is metadata-only: no physical file is touched, and the
   * underlying file is never at risk of being treated as orphaned, because the kept registration
   * still references its exact location. See {@link DuplicateRegistrationRepair} for why the
   * lowest sequence number, specifically, must be the one kept.
   */
  private static void repairDuplicateFileRegistrations(
      Table table, List<Row> duplicates, String resolvePropertyName) {
    DuplicateRegistrationRepair repair =
        new DuplicateRegistrationRepair(table.name(), ((HasTableOperations) table).operations());

    for (Row row : duplicates) {
      String path = row.getAs("file_path");
      int content = row.getAs("content");
      long keepSequenceNumber = row.getAs("keep_sequence_number");
      if (content == FileContent.DATA.id()) {
        repair.keepDataFile(path, keepSequenceNumber);
      } else {
        // FileContent.POSITION_DELETES or FileContent.EQUALITY_DELETES
        repair.keepDeleteFile(path, keepSequenceNumber);
      }
    }

    repair.commit();
    table.refresh();

    LOG.warn(
        "Repaired {} duplicate file registration(s) in {} before rewriting: kept the lowest "
            + "live data sequence number for each path and dropped every other live "
            + "registration. This usually follows a commit retried after its outcome became "
            + "unknown. Set {}=false to require manual remediation instead of automatic repair.",
        duplicates.size(),
        table.name(),
        resolvePropertyName);
  }

  private static void throwDuplicateRegistrationError(
      Table table,
      List<Row> duplicates,
      boolean unsafeForAutoRepair,
      String validatePropertyName,
      String resolvePropertyName) {
    boolean truncated = duplicates.size() > DUPLICATE_REGISTRATION_REPORT_LIMIT;
    String sample =
        duplicates.stream()
            .limit(DUPLICATE_REGISTRATION_REPORT_LIMIT)
            .map(
                row -> {
                  String path = row.getAs("file_path");
                  long distinctSequenceNumbers = row.getAs("distinct_sequence_numbers");
                  return String.format("%s (%d sequence numbers)", path, distinctSequenceNumbers);
                })
            .collect(Collectors.joining(", "));

    String remediation =
        unsafeForAutoRepair
            ? String.format(
                "At least one duplicated path is a delete file on a table that may carry V3+ "
                    + "deletion vectors, where a single Puffin file legitimately holds multiple "
                    + "distinct delete files at the same location and cannot safely auto-repair "
                    + "using location-only matching -- refusing regardless of %s. Resolve the "
                    + "duplicate registrations manually.",
                resolvePropertyName)
            : String.format(
                "Resolve the duplicate registrations first, set %s=true to repair "
                    + "automatically, or set %s=false to proceed anyway and accept the risk.",
                resolvePropertyName, validatePropertyName);

    throw new ValidationException(
        "Cannot rewrite %s: %d file path(s) (data or delete) are registered at more than one "
            + "data sequence number. Compaction cannot handle that safely and would turn it into "
            + "duplicate rows. This usually follows a commit retried after its outcome became "
            + "unknown. Affected (showing up to %d): %s%s. %s",
        table.name(),
        duplicates.size(),
        DUPLICATE_REGISTRATION_REPORT_LIMIT,
        sample,
        truncated ? ", ..." : "",
        remediation);
  }
}
