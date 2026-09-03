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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * AFFIRM: core-level coverage for {@link DuplicateRegistrationRepair} and the {@link
 * ManifestFilterManager} primitive it drives.
 *
 * <p>Exists because the Spark-side tests in {@code TestRewriteDataFilesAction} exercise this
 * through a full {@code rewrite_data_files} run, which cannot reach the argument validation, the
 * retry-safety check, or the "keep the lowest sequence number" invariant directly -- and cannot
 * construct the concurrent-modification state that the retry-safety check exists to catch.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestDuplicateRegistrationRepair extends TestBase {

  private DuplicateRegistrationRepair repair() {
    return new DuplicateRegistrationRepair(table.name(), table.ops());
  }

  private Set<Long> liveSequenceNumbersFor(String path) {
    Set<Long> sequenceNumbers = Sets.newHashSet();
    Snapshot snapshot = table.currentSnapshot();
    if (snapshot == null) {
      return sequenceNumbers;
    }

    for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
      for (ManifestEntry<DataFile> entry :
          ManifestFiles.read(manifest, table.io(), table.specs()).entries()) {
        if (entry.isLive() && entry.file().location().equals(path)) {
          sequenceNumbers.add(entry.dataSequenceNumber());
        }
      }
    }

    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      for (ManifestEntry<DeleteFile> entry :
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs()).entries()) {
        if (entry.isLive() && entry.file().location().equals(path)) {
          sequenceNumbers.add(entry.dataSequenceNumber());
        }
      }
    }

    return sequenceNumbers;
  }

  // ---------------------------------------------------------------------------
  // Argument validation -- unreachable from the Spark path, which always passes
  // values straight out of a metadata-table scan.
  // ---------------------------------------------------------------------------

  @TestTemplate
  public void testKeepDataFileRejectsNullPath() {
    assertThatThrownBy(() -> repair().keepDataFile(null, 1L))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("path cannot be null");
  }

  @TestTemplate
  public void testKeepDeleteFileRejectsNullPath() {
    assertThatThrownBy(() -> repair().keepDeleteFile(null, 1L))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("path cannot be null");
  }

  @TestTemplate
  public void testKeepDataFileRejectsNegativeSequenceNumber() {
    assertThatThrownBy(() -> repair().keepDataFile("/path/to/data-a.parquet", -1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sequenceNumber must not be negative");
  }

  @TestTemplate
  public void testKeepDeleteFileRejectsNegativeSequenceNumber() {
    assertThatThrownBy(() -> repair().keepDeleteFile("/path/to/data-a-deletes.parquet", -1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sequenceNumber must not be negative");
  }

  @TestTemplate
  public void testEmptyRepairIsANoOpAndDoesNotCommit() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotIdBefore = table.currentSnapshot().snapshotId();
    int snapshotsBefore = Lists.newArrayList(table.snapshots()).size();

    // No keepDataFile/keepDeleteFile calls at all: dropDuplicateRegistrations short-circuits on
    // an empty map, so this must not produce a snapshot that drops everything.
    repair().commit();
    table.refresh();

    assertThat(Lists.newArrayList(table.snapshots())).hasSize(snapshotsBefore + 1);
    assertThat(liveSequenceNumbersFor(FILE_A.location()))
        .as("An empty repair must leave the existing registration untouched")
        .hasSize(1);
    assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(snapshotIdBefore);
  }

  // ---------------------------------------------------------------------------
  // The core invariant: keep the lowest, drop the rest.
  // ---------------------------------------------------------------------------

  @TestTemplate
  public void testDropsHigherRegistrationAndKeepsDesignatedSurvivor() {
    table.newAppend().appendFile(FILE_A).commit();
    long firstSeq = table.currentSnapshot().sequenceNumber();

    // Re-append the identical file: a second live registration of the same path at a higher
    // data sequence number. This is the CommitStateUnknownException-retry shape.
    table.newAppend().appendFile(FILE_A).commit();
    long secondSeq = table.currentSnapshot().sequenceNumber();
    assertThat(secondSeq).isGreaterThan(firstSeq);
    assertThat(liveSequenceNumbersFor(FILE_A.location()))
        .as("Both registrations must be live before the repair")
        .containsExactlyInAnyOrder(firstSeq, secondSeq);

    repair().keepDataFile(FILE_A.location(), firstSeq).commit();
    table.refresh();

    assertThat(liveSequenceNumbersFor(FILE_A.location()))
        .as("Only the designated (lowest) registration may survive")
        .containsExactly(firstSeq);
  }

  @TestTemplate
  public void testKeepingTheHigherSequenceNumberIsPossibleButWrong() {
    // Documents that this primitive does NOT enforce the lowest-sequence-number invariant -- the
    // caller does (RewriteDataFiles/DuplicateFileRegistrationGuard, via min(sequence_number)).
    // Pinning that here so a future change that starts silently "correcting" the caller's choice
    // is a deliberate decision with a failing test, not an accident.
    table.newAppend().appendFile(FILE_A).commit();
    long firstSeq = table.currentSnapshot().sequenceNumber();
    table.newAppend().appendFile(FILE_A).commit();
    long secondSeq = table.currentSnapshot().sequenceNumber();

    repair().keepDataFile(FILE_A.location(), secondSeq).commit();
    table.refresh();

    assertThat(liveSequenceNumbersFor(FILE_A.location()))
        .as("The primitive honours the caller's choice verbatim, even when it is the wrong one")
        .containsExactly(secondSeq);
    assertThat(secondSeq).isNotEqualTo(firstSeq);
  }

  @TestTemplate
  public void testCollapsesThreeRegistrationsToOne() {
    table.newAppend().appendFile(FILE_A).commit();
    long firstSeq = table.currentSnapshot().sequenceNumber();
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_A).commit();

    assertThat(liveSequenceNumbersFor(FILE_A.location()))
        .as("Three live registrations before repair")
        .hasSize(3);

    repair().keepDataFile(FILE_A.location(), firstSeq).commit();
    table.refresh();

    assertThat(liveSequenceNumbersFor(FILE_A.location())).containsExactly(firstSeq);
  }

  @TestTemplate
  public void testLeavesUnrelatedPathsAlone() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    long firstSeq = table.currentSnapshot().sequenceNumber();
    table.newAppend().appendFile(FILE_A).commit();

    repair().keepDataFile(FILE_A.location(), firstSeq).commit();
    table.refresh();

    assertThat(liveSequenceNumbersFor(FILE_A.location())).containsExactly(firstSeq);
    assertThat(liveSequenceNumbersFor(FILE_B.location()))
        .as("A path that was never duplicated must be untouched by the repair")
        .containsExactly(firstSeq);
    validateTableFiles(table, FILE_A, FILE_B);
  }

  @TestTemplate
  public void testRepairsMultiplePathsInOneCommit() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    long firstSeq = table.currentSnapshot().sequenceNumber();
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();

    assertThat(liveSequenceNumbersFor(FILE_A.location())).hasSize(2);
    assertThat(liveSequenceNumbersFor(FILE_B.location())).hasSize(2);

    int snapshotsBefore = Lists.newArrayList(table.snapshots()).size();

    repair()
        .keepDataFile(FILE_A.location(), firstSeq)
        .keepDataFile(FILE_B.location(), firstSeq)
        .commit();
    table.refresh();

    assertThat(liveSequenceNumbersFor(FILE_A.location())).containsExactly(firstSeq);
    assertThat(liveSequenceNumbersFor(FILE_B.location())).containsExactly(firstSeq);
    assertThat(Lists.newArrayList(table.snapshots()))
        .as("Both paths must be repaired in a single commit, not one snapshot per path")
        .hasSize(snapshotsBefore + 1);
  }

  // ---------------------------------------------------------------------------
  // Delete files: routed through a separate filter manager, so covered separately.
  // ---------------------------------------------------------------------------

  @TestTemplate
  public void testRepairsDuplicateDeleteFileRegistration() {
    assumeThat(formatVersion).isEqualTo(2);

    table.newAppend().appendFile(FILE_A).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    long deleteSeq = table.currentSnapshot().sequenceNumber();

    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    assertThat(liveSequenceNumbersFor(FILE_A_DELETES.location()))
        .as("Both delete-file registrations must be live before the repair")
        .hasSize(2);

    repair().keepDeleteFile(FILE_A_DELETES.location(), deleteSeq).commit();
    table.refresh();

    assertThat(liveSequenceNumbersFor(FILE_A_DELETES.location())).containsExactly(deleteSeq);
  }

  @TestTemplate
  public void testRepairingADataFileDoesNotDropItsDeleteCoverage() {
    // The actual incident mechanism: the surviving data registration must still be covered by
    // its delete file after the repair. If the repair leaked the dropped registration's path
    // into the data-side deleteFiles set, 1.11's removeDanglingDeletesFor/isDanglingDV path
    // would drop that delete file and resurrect the suppressed rows.
    assumeThat(formatVersion).isEqualTo(2);

    table.newAppend().appendFile(FILE_A).commit();
    long dataSeq = table.currentSnapshot().sequenceNumber();
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    long deleteSeq = table.currentSnapshot().sequenceNumber();

    table.newAppend().appendFile(FILE_A).commit();
    assertThat(liveSequenceNumbersFor(FILE_A.location())).hasSize(2);

    repair().keepDataFile(FILE_A.location(), dataSeq).commit();
    table.refresh();

    assertThat(liveSequenceNumbersFor(FILE_A.location())).containsExactly(dataSeq);
    assertThat(liveSequenceNumbersFor(FILE_A_DELETES.location()))
        .as("The delete file covering the surviving data registration must still be live")
        .containsExactly(deleteSeq);
  }

  // ---------------------------------------------------------------------------
  // Retry safety: the check that exists specifically for the concurrent-commit race.
  // ---------------------------------------------------------------------------

  @TestTemplate
  public void testFailsWhenDesignatedSurvivorSequenceNumberDoesNotExist() {
    table.newAppend().appendFile(FILE_A).commit();
    long realSeq = table.currentSnapshot().sequenceNumber();
    table.newAppend().appendFile(FILE_A).commit();

    // Stand-in for the real race: by the time the repair's filtering pass runs, no live entry
    // exists at the sequence number the earlier scan designated as the survivor. Proceeding
    // would drop BOTH registrations (survivor gone, others don't match) -- silent data loss with
    // the commit reporting success. Must abort instead.
    long nonexistentSeq = realSeq + 9999L;

    assertThatThrownBy(() -> repair().keepDataFile(FILE_A.location(), nonexistentSeq).commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("the intended surviving registration was not found live")
        .hasMessageContaining(FILE_A.location());

    table.refresh();
    assertThat(liveSequenceNumbersFor(FILE_A.location()))
        .as("A failed repair must leave every registration intact, not partially applied")
        .hasSize(2);
  }

  @TestTemplate
  public void testFailsWhenDesignatedPathDoesNotExistAtAll() {
    table.newAppend().appendFile(FILE_A).commit();
    long seq = table.currentSnapshot().sequenceNumber();

    assertThatThrownBy(() -> repair().keepDataFile("/path/to/never-registered.parquet", seq).commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("the intended surviving registration was not found live");

    table.refresh();
    assertThat(liveSequenceNumbersFor(FILE_A.location())).hasSize(1);
  }

  @TestTemplate
  public void testFailsWholeBatchWhenAnySurvivorIsMissing() {
    // Documents the accepted batch-atomicity tradeoff raised in review: one bad path fails the
    // entire batch rather than repairing the still-valid paths and skipping the bad one. If this
    // ever changes to partial repair, this test must be the thing that forces the discussion.
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    long firstSeq = table.currentSnapshot().sequenceNumber();
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();

    assertThatThrownBy(
            () ->
                repair()
                    .keepDataFile(FILE_A.location(), firstSeq) // valid
                    .keepDataFile(FILE_B.location(), firstSeq + 9999L) // bogus
                    .commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("the intended surviving registration was not found live");

    table.refresh();
    assertThat(liveSequenceNumbersFor(FILE_A.location()))
        .as("The valid path must NOT be repaired when another path in the batch is bad")
        .hasSize(2);
    assertThat(liveSequenceNumbersFor(FILE_B.location())).hasSize(2);
  }

  @TestTemplate
  public void testSurvivorTrackingIsNotDefeatedByAnOverlappingDelete() {
    // Ray's short-circuit landmine: isDuplicateRegistrationToDrop records the survivor as a side
    // effect, and it used to be called inline in a || chain. A caller that ALSO issues an
    // ordinary delete() naming the same path would short-circuit that call away, leaving the
    // survivor unrecorded and making the commit wrongly throw. Combine the two here so the
    // hoisted-to-a-local-variable fix is actually pinned by a test.
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    long firstSeq = table.currentSnapshot().sequenceNumber();
    table.newAppend().appendFile(FILE_A).commit();

    DuplicateRegistrationRepair combined = repair();
    combined.keepDataFile(FILE_A.location(), firstSeq);
    // ordinary delete of a DIFFERENT path, which populates deletePaths and therefore makes the
    // earlier terms of the || chain match on the entry for FILE_B
    combined.delete(FILE_B.location());

    combined.commit();
    table.refresh();

    assertThat(liveSequenceNumbersFor(FILE_A.location()))
        .as("Repair must still apply correctly alongside an ordinary delete")
        .containsExactly(firstSeq);
    assertThat(liveSequenceNumbersFor(FILE_B.location()))
        .as("The ordinary delete must still take effect")
        .isEmpty();
  }

  @TestTemplate
  public void testRepairIsRetriedSuccessfullyAfterAConcurrentUnrelatedCommit() {
    // A concurrent commit that does NOT touch the duplicated path must not block the repair --
    // it should lose the CAS, re-run validate()/apply() against the refreshed base, and succeed.
    table.newAppend().appendFile(FILE_A).commit();
    long firstSeq = table.currentSnapshot().sequenceNumber();
    table.newAppend().appendFile(FILE_A).commit();
    assertThat(liveSequenceNumbersFor(FILE_A.location())).hasSize(2);

    DuplicateRegistrationRepair pending = repair();
    pending.keepDataFile(FILE_A.location(), firstSeq);
    // force apply() to run once, then land an unrelated commit underneath it
    List<ManifestFile> ignored = pending.apply(table.ops().refresh(), table.currentSnapshot());
    assertThat(ignored).isNotNull();

    table.newAppend().appendFile(FILE_C).commit();

    pending.commit();
    table.refresh();

    assertThat(liveSequenceNumbersFor(FILE_A.location()))
        .as("Repair must still succeed across an unrelated concurrent commit")
        .containsExactly(firstSeq);
    assertThat(liveSequenceNumbersFor(FILE_C.location()))
        .as("The concurrent commit's file must survive the repair")
        .hasSize(1);
  }

  @TestTemplate
  public void testConcurrentUpsertWritesDoNotStarveTheRepair() {
    // Pins the load-bearing claim in validateDuplicateRegistrationSurvivorsPresent's javadoc.
    // Modelled on the actual production writer, which is NOT an append-only sink: it is a Flink
    // UPSERT sink committing every ~5 minutes that emits an equality delete for every row, so
    // every one of its commits touches the delete side. The claim under test is that such a
    // writer still cannot make a designated survivor vanish, because a RowDelta that only calls
    // addRows/addDeletes leaves deletePaths/deleteFiles/dropPartitions empty, which makes
    // canContainDeletedFiles false for every pre-existing manifest -- so nothing is reopened and
    // no existing entry is marked DELETED.
    assumeThat(formatVersion).isEqualTo(2);

    table.newAppend().appendFile(FILE_A).commit();
    long survivorSeq = table.currentSnapshot().sequenceNumber();
    table.newAppend().appendFile(FILE_A).commit();
    assertThat(liveSequenceNumbersFor(FILE_A.location())).hasSize(2);

    DuplicateRegistrationRepair pending = repair();
    pending.keepDataFile(FILE_A.location(), survivorSeq);
    pending.apply(table.ops().refresh(), table.currentSnapshot());

    // Stand in for several upsert checkpoints landing under the pending repair: each adds data
    // AND an equality delete, exactly as upsert mode does for every row.
    table.newRowDelta().addRows(FILE_B).addDeletes(FILE_B_DELETES).commit();
    table.newRowDelta().addRows(FILE_C).addDeletes(FILE_C2_DELETES).commit();
    table.newRowDelta().addRows(FILE_D).commit();

    pending.commit();
    table.refresh();

    assertThat(liveSequenceNumbersFor(FILE_A.location()))
        .as("Repeated concurrent upsert commits must not starve or fail the repair")
        .containsExactly(survivorSeq);
    assertThat(liveSequenceNumbersFor(FILE_B.location())).hasSize(1);
    assertThat(liveSequenceNumbersFor(FILE_C.location())).hasSize(1);
    assertThat(liveSequenceNumbersFor(FILE_D.location()))
        .as("Every upsert's data file must survive the repair commit")
        .hasSize(1);
    assertThat(liveSequenceNumbersFor(FILE_B_DELETES.location()))
        .as("Every upsert's equality delete must survive the repair commit")
        .hasSize(1);
  }

  @TestTemplate
  public void testOperationIsReportedAsDelete() {
    table.newAppend().appendFile(FILE_A).commit();
    long seq = table.currentSnapshot().sequenceNumber();
    table.newAppend().appendFile(FILE_A).commit();

    repair().keepDataFile(FILE_A.location(), seq).commit();
    table.refresh();

    assertThat(table.currentSnapshot().operation())
        .as("A repair drops manifest entries, so it must be summarised as a DELETE")
        .isEqualTo(DataOperations.DELETE);
  }
}
