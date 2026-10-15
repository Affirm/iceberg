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
package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergFilesCommitter extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<FlinkWriteResult, Void>, BoundedOneInput {

  private static final long serialVersionUID = 1L;
  private static final long INITIAL_CHECKPOINT_ID = -1L;
  private static final byte[] EMPTY_MANIFEST_DATA = new byte[0];

  private static final Logger LOG = LoggerFactory.getLogger(IcebergFilesCommitter.class);
  private static final String FLINK_JOB_ID = "flink.job-id";
  private static final String OPERATOR_ID = "flink.operator-id";

  // The max checkpoint id we've committed to iceberg table. As the flink's checkpoint is always
  // increasing, so we could correctly commit all the data files whose checkpoint id is greater than
  // the max committed one to iceberg table, for avoiding committing the same data files twice. This
  // id will be attached to iceberg's meta when committing the iceberg transaction.
  private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";
  static final String MAX_CONTINUOUS_EMPTY_COMMITS = "flink.max-continuous-empty-commits";

  // AFFIRM: how many of the most recent ancestor snapshots (for this flinkJobId/operatorId) we
  // inspect, by actual file path, before committing. This guards against the case documented in
  // apache/iceberg#10765: a commit's response is lost/ambiguous (e.g. a network timeout), the
  // committer doesn't observe success, and a subsequent commit re-appends the exact same files
  // because getMaxCommittedCheckpointId() was evaluated against a table snapshot that didn't yet
  // reflect the prior, already-successful commit. That check trusts the flink.job-id /
  // flink.operator-id / flink.max-committed-checkpoint-id snapshot summary properties; this check
  // additionally verifies by content (file path) against a small, bounded window of recent
  // history, so a lost-response race can't silently double-register a file. Bounded to a small
  // constant so cost stays flat regardless of total table/snapshot history size.
  static final String RECENT_SNAPSHOT_LOOKBACK_PROP = "flink.recent-snapshot-lookback";
  private static final int RECENT_SNAPSHOT_LOOKBACK_DEFAULT = 5;

  // AFFIRM: apache/iceberg's REST client maps a 500/502/504 from the catalog on a commit request
  // to CommitStateUnknownException (see ErrorHandlers#commitErrorHandler) -- the request may have
  // actually succeeded server-side; the client just couldn't confirm it. SnapshotProducer#commit
  // deliberately does no cleanup and rethrows immediately in this case, by design, leaving the
  // caller responsible for deciding whether it's safe to retry. Left unhandled, that exception
  // propagates out of notifyCheckpointComplete, fails the Flink task, and Flink's restart-strategy
  // blindly retries the same checkpoint -- exactly what CommitStateUnknownException's own javadoc
  // warns against ("retrying an already successful operation will result in duplicate records").
  // Rather than rely on that restart happening to be slow enough for the catalog to catch up (it
  // wasn't, in production: see the 17s-apart duplicate on chrono.user_updates_status), poll the
  // catalog directly for a bounded window to find out whether the commit actually landed before
  // letting the exception propagate into an uncontrolled restart. Configurable via table
  // properties (same pattern as MAX_CONTINUOUS_EMPTY_COMMITS below) so this can be tuned without a
  // code change, and so tests don't have to sleep through the real default budget.
  static final String COMMIT_STATE_UNKNOWN_MAX_VERIFY_ATTEMPTS_PROP =
      "flink.commit-state-unknown-max-verify-attempts";
  static final String COMMIT_STATE_UNKNOWN_VERIFY_INITIAL_DELAY_MS_PROP =
      "flink.commit-state-unknown-verify-initial-delay-ms";
  private static final int COMMIT_STATE_UNKNOWN_MAX_VERIFY_ATTEMPTS_DEFAULT = 5;
  private static final long COMMIT_STATE_UNKNOWN_VERIFY_INITIAL_DELAY_MS_DEFAULT = 1000L;

  // TableLoader to load iceberg table lazily.
  private final TableLoader tableLoader;
  private final boolean replacePartitions;
  private final Map<String, String> snapshotProperties;

  // A sorted map to maintain the completed data files for each pending checkpointId (which have not
  // been committed to iceberg table). We need a sorted map here because there's possible that few
  // checkpoints snapshot failed, for example: the 1st checkpoint have 2 data files <1, <file0,
  // file1>>, the 2st checkpoint have 1 data files <2, <file3>>. Snapshot for checkpoint#1
  // interrupted because of network/disk failure etc, while we don't expect any data loss in iceberg
  // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
  // table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final Map<Long, List<WriteResult>> writeResultsSinceLastSnapshot = Maps.newHashMap();
  private final String branch;

  // It will have an unique identifier for one job.
  private transient String flinkJobId;
  private transient String operatorUniqueId;
  private transient Table table;
  private transient IcebergFilesCommitterMetrics committerMetrics;
  private transient ManifestOutputFileFactory manifestOutputFileFactory;
  private transient long maxCommittedCheckpointId;
  private transient int continuousEmptyCheckpoints;
  private transient int maxContinuousEmptyCommits;
  private transient int recentSnapshotLookback;
  private transient int commitStateUnknownMaxVerifyAttempts;
  private transient long commitStateUnknownVerifyInitialDelayMs;
  // There're two cases that we restore from flink checkpoints: the first case is restoring from
  // snapshot created by the same flink job; another case is restoring from snapshot created by
  // another different job. For the second case, we need to maintain the old flink job's id in flink
  // state backend to find the max-committed-checkpoint-id when traversing iceberg table's
  // snapshots.
  private static final ListStateDescriptor<String> JOB_ID_DESCRIPTOR =
      new ListStateDescriptor<>("iceberg-flink-job-id", BasicTypeInfo.STRING_TYPE_INFO);
  private transient ListState<String> jobIdState;
  // All pending checkpoints states for this function.
  private static final ListStateDescriptor<SortedMap<Long, byte[]>> STATE_DESCRIPTOR =
      buildStateDescriptor();
  private transient ListState<SortedMap<Long, byte[]>> checkpointsState;

  private final Integer workerPoolSize;
  private final PartitionSpec spec;
  private transient ExecutorService workerPool;

  IcebergFilesCommitter(
      StreamOperatorParameters<Void> parameters,
      TableLoader tableLoader,
      boolean replacePartitions,
      Map<String, String> snapshotProperties,
      Integer workerPoolSize,
      String branch,
      PartitionSpec spec) {
    super(parameters);
    this.tableLoader = tableLoader;
    this.replacePartitions = replacePartitions;
    this.snapshotProperties = snapshotProperties;
    this.workerPoolSize = workerPoolSize;
    this.branch = branch;
    this.spec = spec;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
    this.operatorUniqueId = getRuntimeContext().getOperatorUniqueID();

    // Open the table loader and load the table.
    this.tableLoader.open();
    this.table = tableLoader.loadTable();
    this.committerMetrics = new IcebergFilesCommitterMetrics(super.metrics, table.name());

    maxContinuousEmptyCommits =
        PropertyUtil.propertyAsInt(table.properties(), MAX_CONTINUOUS_EMPTY_COMMITS, 10);
    Preconditions.checkArgument(
        maxContinuousEmptyCommits > 0, MAX_CONTINUOUS_EMPTY_COMMITS + " must be positive");
    recentSnapshotLookback =
        PropertyUtil.propertyAsInt(
            table.properties(), RECENT_SNAPSHOT_LOOKBACK_PROP, RECENT_SNAPSHOT_LOOKBACK_DEFAULT);
    commitStateUnknownMaxVerifyAttempts =
        PropertyUtil.propertyAsInt(
            table.properties(),
            COMMIT_STATE_UNKNOWN_MAX_VERIFY_ATTEMPTS_PROP,
            COMMIT_STATE_UNKNOWN_MAX_VERIFY_ATTEMPTS_DEFAULT);
    commitStateUnknownVerifyInitialDelayMs =
        PropertyUtil.propertyAsLong(
            table.properties(),
            COMMIT_STATE_UNKNOWN_VERIFY_INITIAL_DELAY_MS_PROP,
            COMMIT_STATE_UNKNOWN_VERIFY_INITIAL_DELAY_MS_DEFAULT);

    int subTaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    int attemptId = getRuntimeContext().getTaskInfo().getAttemptNumber();
    this.manifestOutputFileFactory =
        FlinkManifestUtil.createOutputFileFactory(
            () -> table, table.properties(), flinkJobId, operatorUniqueId, subTaskId, attemptId);
    this.maxCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    this.checkpointsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
    this.jobIdState = context.getOperatorStateStore().getListState(JOB_ID_DESCRIPTOR);
    if (context.isRestored()) {
      Iterable<String> jobIdIterable = jobIdState.get();
      if (jobIdIterable == null || !jobIdIterable.iterator().hasNext()) {
        LOG.warn(
            "Failed to restore committer state. This can happen when operator uid changed and Flink "
                + "allowNonRestoredState is enabled. Best practice is to explicitly set the operator id "
                + "via FlinkSink#Builder#uidPrefix() so that the committer operator uid is stable. "
                + "Otherwise, Flink auto generate an operator uid based on job topology."
                + "With that, operator uid is subjective to change upon topology change.");
        return;
      }

      String restoredFlinkJobId = jobIdIterable.iterator().next();
      Preconditions.checkState(
          !Strings.isNullOrEmpty(restoredFlinkJobId),
          "Flink job id parsed from checkpoint snapshot shouldn't be null or empty");

      // Since flink's checkpoint id will start from the max-committed-checkpoint-id + 1 in the new
      // flink job even if it's restored from a snapshot created by another different flink job, so
      // it's safe to assign the max committed checkpoint id from restored flink job to the current
      // flink job.
      this.maxCommittedCheckpointId =
          SinkUtil.getMaxCommittedCheckpointId(table, restoredFlinkJobId, operatorUniqueId, branch);

      NavigableMap<Long, byte[]> uncommittedDataFiles =
          Maps.newTreeMap(checkpointsState.get().iterator().next())
              .tailMap(maxCommittedCheckpointId, false);
      if (!uncommittedDataFiles.isEmpty()) {
        // Committed all uncommitted data files from the old flink job to iceberg table.
        long maxUncommittedCheckpointId = uncommittedDataFiles.lastKey();
        commitUpToCheckpoint(
            uncommittedDataFiles, restoredFlinkJobId, operatorUniqueId, maxUncommittedCheckpointId);
      }
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    long checkpointId = context.getCheckpointId();
    LOG.info(
        "Start to flush snapshot state to state backend, table: {}, checkpointId: {}",
        table,
        checkpointId);

    // Update the checkpoint state.
    long startNano = System.nanoTime();
    writeToManifestUptoLatestCheckpoint(checkpointId);

    // Reset the snapshot state to the latest state.
    checkpointsState.clear();
    checkpointsState.add(dataFilesPerCheckpoint);

    jobIdState.clear();
    jobIdState.add(flinkJobId);

    committerMetrics.checkpointDuration(
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    // It's possible that we have the following events:
    //   1. snapshotState(ckpId);
    //   2. snapshotState(ckpId+1);
    //   3. notifyCheckpointComplete(ckpId+1);
    //   4. notifyCheckpointComplete(ckpId);
    // For step#4, we don't need to commit iceberg table again because in step#3 we've committed all
    // the files,
    // Besides, we need to maintain the max-committed-checkpoint-id to be increasing.
    if (checkpointId > maxCommittedCheckpointId) {
      LOG.info("Checkpoint {} completed. Attempting commit.", checkpointId);
      commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, operatorUniqueId, checkpointId);
      this.maxCommittedCheckpointId = checkpointId;
    } else {
      LOG.info(
          "Skipping committing checkpoint {}. {} is already committed.",
          checkpointId,
          maxCommittedCheckpointId);
    }

    // reload the table in case new configuration is needed
    this.table = tableLoader.loadTable();
  }

  private void commitUpToCheckpoint(
      NavigableMap<Long, byte[]> deltaManifestsMap,
      String newFlinkJobId,
      String operatorId,
      long checkpointId)
      throws IOException {
    NavigableMap<Long, byte[]> pendingMap = deltaManifestsMap.headMap(checkpointId, true);
    List<ManifestFile> manifests = Lists.newArrayList();
    NavigableMap<Long, WriteResult> pendingResults = Maps.newTreeMap();
    for (Map.Entry<Long, byte[]> e : pendingMap.entrySet()) {
      if (Arrays.equals(EMPTY_MANIFEST_DATA, e.getValue())) {
        // Skip the empty flink manifest.
        continue;
      }

      DeltaManifests deltaManifests =
          SimpleVersionedSerialization.readVersionAndDeSerialize(
              DeltaManifestsSerializer.INSTANCE, e.getValue());
      pendingResults.put(
          e.getKey(),
          FlinkManifestUtil.readCompletedFiles(deltaManifests, table.io(), table.specs()));
      manifests.addAll(deltaManifests.manifests());
    }

    NavigableMap<Long, WriteResult> dedupedResults =
        dropAlreadyCommittedFiles(pendingResults, newFlinkJobId, operatorId);

    CommitSummary summary = new CommitSummary(dedupedResults);
    commitPendingResult(dedupedResults, summary, newFlinkJobId, operatorId, checkpointId);
    committerMetrics.updateCommitSummary(summary);
    pendingMap.clear();
    FlinkManifestUtil.deleteCommittedManifests(table, manifests, newFlinkJobId, checkpointId);
  }

  /**
   * AFFIRM: defense-in-depth against apache/iceberg#10765. Drops any data/delete file from {@code
   * pendingResults} whose exact path already appears as an added file in one of the last {@code
   * recentSnapshotLookback} ancestor snapshots committed by this same flinkJobId/operatorId. This
   * does not replace {@link SinkUtil#getMaxCommittedCheckpointId}; it's an additional,
   * content-based check for the narrow window where that metadata-only check can be fooled by a
   * commit whose response was lost/ambiguous to the client but which actually succeeded on the
   * catalog.
   */
  @VisibleForTesting
  @SuppressWarnings("CollectionUndefinedEquality") // CharSequenceSet defines path equality itself
  NavigableMap<Long, WriteResult> dropAlreadyCommittedFiles(
      NavigableMap<Long, WriteResult> pendingResults, String newFlinkJobId, String operatorId) {
    CharSequenceSet recentlyCommittedPaths =
        collectRecentlyCommittedFilePaths(newFlinkJobId, operatorId);
    if (recentlyCommittedPaths.isEmpty()) {
      return pendingResults;
    }

    NavigableMap<Long, WriteResult> deduped = Maps.newTreeMap();
    for (Map.Entry<Long, WriteResult> e : pendingResults.entrySet()) {
      long checkpointId = e.getKey();
      WriteResult result = e.getValue();
      WriteResult.Builder builder =
          WriteResult.builder().addReferencedDataFiles(result.referencedDataFiles());

      for (DataFile file : result.dataFiles()) {
        if (logAndSkipIfAlreadyCommitted(
            "data", file.path(), recentlyCommittedPaths, newFlinkJobId, operatorId, checkpointId)) {
          builder.addDataFiles(file);
        }
      }

      for (DeleteFile file : result.deleteFiles()) {
        if (logAndSkipIfAlreadyCommitted(
            "delete",
            file.path(),
            recentlyCommittedPaths,
            newFlinkJobId,
            operatorId,
            checkpointId)) {
          builder.addDeleteFiles(file);
        }
      }

      deduped.put(checkpointId, builder.build());
    }

    return deduped;
  }

  /**
   * Returns {@code true} if {@code path} should be kept (i.e. it's NOT already committed), {@code
   * false} if it should be dropped -- logging the drop either way. Shared by the data-file and
   * delete-file loops in {@link #dropAlreadyCommittedFiles} so the two stay in sync.
   */
  @SuppressWarnings("CollectionUndefinedEquality") // CharSequenceSet defines path equality itself
  private boolean logAndSkipIfAlreadyCommitted(
      String fileKind,
      CharSequence path,
      CharSequenceSet recentlyCommittedPaths,
      String newFlinkJobId,
      String operatorId,
      long checkpointId) {
    if (!recentlyCommittedPaths.contains(path)) {
      return true;
    }

    LOG.warn(
        "Dropping {} file already present in a recent snapshot for table {} branch {} "
            + "flinkJobId {} operatorId {} checkpoint {}: {}. This indicates a prior commit "
            + "for this file already succeeded even though this committer didn't observe "
            + "that success (see apache/iceberg#10765).",
        fileKind,
        table.name(),
        branch,
        newFlinkJobId,
        operatorId,
        checkpointId,
        path);
    return false;
  }

  // AFFIRM: hard ceiling on total ancestor snapshots walked in collectRecentlyCommittedFilePaths,
  // independent of recentSnapshotLookback (which now only counts *matching* snapshots -- see that
  // method's comment). Purely a runaway-cost guard for the pathological case of a huge run of
  // non-Flink-commit snapshots (e.g. a table under heavy external maintenance) with no matching
  // snapshot anywhere nearby; not expected to bind in normal operation.
  private static final int RECENT_SNAPSHOT_WALK_LIMIT = 200;

  /**
   * Walks back from the current snapshot on {@link #branch}, collecting the paths of data/delete
   * files added by snapshots committed by this exact flinkJobId/operatorId, stopping once {@code
   * recentSnapshotLookback} *matching* ancestor snapshots have been found (or {@link
   * #RECENT_SNAPSHOT_WALK_LIMIT} total ancestors have been visited) to keep this bounded and cheap.
   * Refreshes the table first so this observes the freshest metadata available.
   *
   * <p>AFFIRM: the budget is intentionally scoped to matching snapshots only. Non-Flink maintenance
   * snapshots (compaction, dangling-delete removal, etc.) interleaved on the branch don't carry
   * this flinkJobId/operatorId and must not silently consume the lookback window meant for this
   * committer's own recent history -- otherwise a run of 5+ such snapshots between an ambiguous
   * commit and its retry would defeat this check for the exact #10765 scenario it exists to catch.
   */
  private CharSequenceSet collectRecentlyCommittedFilePaths(
      String newFlinkJobId, String operatorId) {
    table.refresh();

    CharSequenceSet paths = CharSequenceSet.empty();
    Snapshot snapshot = table.snapshot(branch);
    int matched = 0;
    int visited = 0;
    while (snapshot != null
        && matched < recentSnapshotLookback
        && visited < RECENT_SNAPSHOT_WALK_LIMIT) {
      Map<String, String> summary = snapshot.summary();
      if (newFlinkJobId.equals(summary.get(FLINK_JOB_ID))
          && (summary.get(OPERATOR_ID) == null || operatorId.equals(summary.get(OPERATOR_ID)))) {
        for (DataFile file : snapshot.addedDataFiles(table.io())) {
          paths.add(file.path());
        }
        for (DeleteFile file : snapshot.addedDeleteFiles(table.io())) {
          paths.add(file.path());
        }
        matched++;
      }

      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
      visited++;
    }

    return paths;
  }

  private void commitPendingResult(
      NavigableMap<Long, WriteResult> pendingResults,
      CommitSummary summary,
      String newFlinkJobId,
      String operatorId,
      long checkpointId) {
    long totalFiles = summary.dataFilesCount() + summary.deleteFilesCount();
    continuousEmptyCheckpoints = totalFiles == 0 ? continuousEmptyCheckpoints + 1 : 0;
    if (totalFiles != 0 || continuousEmptyCheckpoints % maxContinuousEmptyCommits == 0) {
      if (replacePartitions) {
        replacePartitions(pendingResults, summary, newFlinkJobId, operatorId, checkpointId);
      } else {
        commitDeltaTxn(pendingResults, summary, newFlinkJobId, operatorId, checkpointId);
      }
      continuousEmptyCheckpoints = 0;
    } else {
      LOG.info("Skip commit for checkpoint {} due to no data files or delete files.", checkpointId);
    }
  }

  private void replacePartitions(
      NavigableMap<Long, WriteResult> pendingResults,
      CommitSummary summary,
      String newFlinkJobId,
      String operatorId,
      long checkpointId) {
    Preconditions.checkState(
        summary.deleteFilesCount() == 0, "Cannot overwrite partitions with delete files.");
    // Commit the overwrite transaction.
    ReplacePartitions dynamicOverwrite = table.newReplacePartitions().scanManifestsWith(workerPool);
    for (WriteResult result : pendingResults.values()) {
      Preconditions.checkState(
          result.referencedDataFiles().length == 0, "Should have no referenced data files.");
      Arrays.stream(result.dataFiles()).forEach(dynamicOverwrite::addFile);
    }

    commitOperation(
        dynamicOverwrite,
        summary,
        "dynamic partition overwrite",
        newFlinkJobId,
        operatorId,
        checkpointId);
  }

  private void commitDeltaTxn(
      NavigableMap<Long, WriteResult> pendingResults,
      CommitSummary summary,
      String newFlinkJobId,
      String operatorId,
      long checkpointId) {
    if (summary.deleteFilesCount() == 0) {
      // To be compatible with iceberg format V1.
      AppendFiles appendFiles = table.newAppend().scanManifestsWith(workerPool);
      for (WriteResult result : pendingResults.values()) {
        Preconditions.checkState(
            result.referencedDataFiles().length == 0,
            "Should have no referenced data files for append.");
        Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
      }
      commitOperation(appendFiles, summary, "append", newFlinkJobId, operatorId, checkpointId);
    } else {
      // To be compatible with iceberg format V2.
      for (Map.Entry<Long, WriteResult> e : pendingResults.entrySet()) {
        // We don't commit the merged result into a single transaction because for the sequential
        // transaction txn1 and txn2, the equality-delete files of txn2 are required to be applied
        // to data files from txn1. Committing the merged one will lead to the incorrect delete
        // semantic.
        WriteResult result = e.getValue();

        // AFFIRM: dropAlreadyCommittedFiles can leave a checkpoint entry with zero data files and
        // zero delete files (a full duplicate). Committing an empty RowDelta here would be a
        // pointless no-op snapshot at best; at worst, if that empty commit is ever rejected by
        // something other than CommitStateUnknownException, the exception propagates out of
        // commitUpToCheckpoint before pendingMap.clear() runs, leaving later, genuinely-new entries
        // in this same batch stuck pending for a retry. Skip it entirely instead.
        if (result.dataFiles().length == 0 && result.deleteFiles().length == 0) {
          LOG.info(
              "Skipping commit for checkpoint {} to table {} branch {}: fully deduped as already "
                  + "committed, nothing left to write.",
              e.getKey(),
              table.name(),
              branch);
          continue;
        }

        // Row delta validations are not needed for streaming changes that write equality deletes.
        // Equality deletes are applied to data in all previous sequence numbers, so retries may
        // push deletes further in the future, but do not affect correctness. Position deletes
        // committed to the table in this path are used only to delete rows from data files that are
        // being added in this commit. There is no way for data files added along with the delete
        // files to be concurrently removed, so there is no need to validate the files referenced by
        // the position delete files that are being committed.
        RowDelta rowDelta = table.newRowDelta().scanManifestsWith(workerPool);

        Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
        Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
        commitOperation(rowDelta, summary, "rowDelta", newFlinkJobId, operatorId, e.getKey());
      }
    }
  }

  private void commitOperation(
      SnapshotUpdate<?> operation,
      CommitSummary summary,
      String description,
      String newFlinkJobId,
      String operatorId,
      long checkpointId) {
    LOG.info(
        "Committing {} for checkpoint {} to table {} branch {} with summary: {}",
        description,
        checkpointId,
        table.name(),
        branch,
        summary);
    snapshotProperties.forEach(operation::set);
    // custom snapshot metadata properties will be overridden if they conflict with internal ones
    // used by the sink.
    operation.set(MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
    operation.set(FLINK_JOB_ID, newFlinkJobId);
    operation.set(OPERATOR_ID, operatorId);
    operation.toBranch(branch);

    long startNano = System.nanoTime();
    try {
      operation.commit(); // abort is automatically called if this fails, EXCEPT on
      // CommitStateUnknownException -- see the catch block below.
    } catch (CommitStateUnknownException e) {
      // AFFIRM: see apache/iceberg#10765 and the class-level comment on
      // COMMIT_STATE_UNKNOWN_MAX_VERIFY_ATTEMPTS_PROP. Don't let Flink's restart-strategy be the
      // thing that decides whether this ambiguous commit gets blindly retried; check ourselves.
      if (verifyCommitEventuallySucceeded(newFlinkJobId, operatorId, checkpointId, description)) {
        LOG.warn(
            "Commit {} for checkpoint {} to table {} branch {} returned an ambiguous response "
                + "(CommitStateUnknownException) but verification found it actually succeeded; "
                + "treating this checkpoint as committed instead of failing the task.",
            description,
            checkpointId,
            table.name(),
            branch,
            e);
        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
        committerMetrics.commitDuration(durationMs);
        return;
      }

      LOG.error(
          "Commit {} for checkpoint {} to table {} branch {} returned an ambiguous response "
              + "(CommitStateUnknownException) and could not be verified as successful within "
              + "the retry budget ({} attempts). Rethrowing so Flink can restart and re-attempt. "
              + "If the commit actually did succeed after this budget was exhausted, "
              + "dropAlreadyCommittedFiles should still catch and drop the duplicate on retry.",
          description,
          checkpointId,
          table.name(),
          branch,
          commitStateUnknownMaxVerifyAttempts,
          e);
      throw e;
    }
    long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
    LOG.info(
        "Committed {} to table: {}, branch: {}, checkpointId {} in {} ms",
        description,
        table.name(),
        branch,
        checkpointId,
        durationMs);
    committerMetrics.commitDuration(durationMs);
  }

  /**
   * AFFIRM: polls, with exponential backoff, for up to {@code commitStateUnknownMaxVerifyAttempts}
   * attempts to determine whether a commit that just threw {@link CommitStateUnknownException}
   * actually landed on the catalog, by refreshing the table and checking whether {@code
   * checkpointId} is now covered by {@link SinkUtil#getMaxCommittedCheckpointId}. Deliberately
   * blocks the calling thread (inside notifyCheckpointComplete): a bounded wait here is preferable
   * to unconditionally failing the task and paying for a full restart-and-restore cycle only to hit
   * the same ambiguity check again. Returns false (not verified) if the budget is exhausted or the
   * wait is interrupted.
   */
  @VisibleForTesting
  boolean verifyCommitEventuallySucceeded(
      String newFlinkJobId, String operatorId, long checkpointId, String description) {
    long delayMs = commitStateUnknownVerifyInitialDelayMs;
    for (int attempt = 1; attempt <= commitStateUnknownMaxVerifyAttempts; attempt++) {
      try {
        Thread.sleep(delayMs);
      } catch (InterruptedException interruptedException) {
        Thread.currentThread().interrupt();
        return false;
      }

      table.refresh();
      long observedCheckpointId =
          SinkUtil.getMaxCommittedCheckpointId(table, newFlinkJobId, operatorId, branch);
      LOG.info(
          "Verifying ambiguous {} commit for checkpoint {} on table {} branch {}: attempt {}/{}, "
              + "observed max-committed-checkpoint-id {} for flinkJobId {} operatorId {}",
          description,
          checkpointId,
          table.name(),
          branch,
          attempt,
          commitStateUnknownMaxVerifyAttempts,
          observedCheckpointId,
          newFlinkJobId,
          operatorId);
      if (observedCheckpointId >= checkpointId) {
        return true;
      }

      delayMs *= 2;
    }

    return false;
  }

  @Override
  public void processElement(StreamRecord<FlinkWriteResult> element) {
    FlinkWriteResult flinkWriteResult = element.getValue();
    List<WriteResult> writeResults =
        writeResultsSinceLastSnapshot.computeIfAbsent(
            flinkWriteResult.checkpointId(), k -> Lists.newArrayList());
    writeResults.add(flinkWriteResult.writeResult());
  }

  @Override
  public void endInput() throws IOException {
    // Flush the buffered data files into 'dataFilesPerCheckpoint' firstly.
    long currentCheckpointId = IcebergStreamWriter.END_INPUT_CHECKPOINT_ID;
    writeToManifestUptoLatestCheckpoint(currentCheckpointId);
    commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, operatorUniqueId, currentCheckpointId);
  }

  private void writeToManifestUptoLatestCheckpoint(long checkpointId) throws IOException {
    if (!writeResultsSinceLastSnapshot.containsKey(checkpointId)) {
      dataFilesPerCheckpoint.put(checkpointId, EMPTY_MANIFEST_DATA);
    }

    for (Map.Entry<Long, List<WriteResult>> writeResultsOfCheckpoint :
        writeResultsSinceLastSnapshot.entrySet()) {
      dataFilesPerCheckpoint.put(
          writeResultsOfCheckpoint.getKey(),
          writeToManifest(writeResultsOfCheckpoint.getKey(), writeResultsOfCheckpoint.getValue()));
    }

    // Clear the local buffer for current checkpoint.
    writeResultsSinceLastSnapshot.clear();
  }

  /**
   * Write all the complete data files to a newly created manifest file and return the manifest's
   * avro serialized bytes.
   */
  private byte[] writeToManifest(long checkpointId, List<WriteResult> writeResults)
      throws IOException {
    WriteResult result = WriteResult.builder().addAll(writeResults).build();
    DeltaManifests deltaManifests =
        FlinkManifestUtil.writeCompletedFiles(
            result,
            () -> manifestOutputFileFactory.create(checkpointId),
            spec,
            TableUtil.formatVersion(table));

    return SimpleVersionedSerialization.writeVersionAndSerialize(
        DeltaManifestsSerializer.INSTANCE, deltaManifests);
  }

  @Override
  public void open() throws Exception {
    super.open();

    final String operatorID = getRuntimeContext().getOperatorUniqueID();
    this.workerPool =
        ThreadPools.newFixedThreadPool("iceberg-worker-pool-" + operatorID, workerPoolSize);
  }

  @Override
  public void close() throws Exception {
    if (tableLoader != null) {
      tableLoader.close();
    }

    if (workerPool != null) {
      workerPool.shutdown();
    }
  }

  @VisibleForTesting
  static ListStateDescriptor<SortedMap<Long, byte[]>> buildStateDescriptor() {
    Comparator<Long> longComparator = Comparators.forType(Types.LongType.get());
    // Construct a SortedMapTypeInfo.
    SortedMapTypeInfo<Long, byte[]> sortedMapTypeInfo =
        new SortedMapTypeInfo<>(
            BasicTypeInfo.LONG_TYPE_INFO,
            PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
            longComparator);
    return new ListStateDescriptor<>("iceberg-files-committer-state", sortedMapTypeInfo);
  }
}
