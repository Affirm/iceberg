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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.ManifestFileUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionSet;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class ManifestFilterManager<F extends ContentFile<F>> {
  private static final Logger LOG = LoggerFactory.getLogger(ManifestFilterManager.class);
  private static final Joiner COMMA = Joiner.on(",");

  protected static class DeleteException extends ValidationException {
    private final String partition;

    private DeleteException(String partition) {
      super("Operation would delete existing data");
      this.partition = partition;
    }

    public String partition() {
      return partition;
    }
  }

  private final Map<Integer, PartitionSpec> specsById;
  private final PartitionSet deleteFilePartitions;
  private final Set<F> deleteFiles = newFileSet();
  private final Set<String> manifestsWithDeletes = Sets.newHashSet();
  private final PartitionSet dropPartitions;
  private final CharSequenceSet deletePaths = CharSequenceSet.empty();
  // count of manifests that were rewritten with different manifest entry status during filtering
  private final AtomicInteger replacedManifestsCount = new AtomicInteger(0);

  // AFFIRM: path -> the one data sequence number at that path which must survive; every other
  // live registration of that same path is a duplicate to be dropped. See
  // #dropDuplicateRegistrations for why the lowest sequence number is always the one kept.
  private final Map<String, Long> duplicateRegistrationKeepSequence = Maps.newHashMap();
  // AFFIRM: locations passed to delete(F file) by the CALLER, as distinct from deleteFiles,
  // which the filtering loop also writes to and which is never cleared between commit
  // attempts. Only used by validateNoContradictoryDuplicateRegistrationIntent.
  private final Set<String> callerDeletedFilePaths = Sets.newHashSet();
  // AFFIRM: paths from duplicateRegistrationKeepSequence actually observed live, at their
  // designated keep-sequence-number, during the CURRENT filterManifests() pass. Manifests are
  // processed in parallel (see filterManifests' Tasks.range(...).run(...)), so this must be
  // thread-safe. Cleared at the start of every filterManifests() call -- see
  // validateDuplicateRegistrationSurvivorsPresent for why a stale entry from a prior, failed
  // commit attempt must never satisfy this check on a later retry.
  private final Set<String> duplicateRegistrationSurvivorsSeen = Sets.newConcurrentHashSet();
  private Expression deleteExpression = Expressions.alwaysFalse();
  private long minSequenceNumber = 0;
  private boolean failAnyDelete = false;
  private boolean failMissingDeletePaths = false;
  private int duplicateDeleteCount = 0;
  private boolean caseSensitive = true;
  private boolean allDeletesReferenceManifests = true;
  // this is only being used for the DeleteManifestFilterManager to detect orphaned DVs for removed
  // data file paths
  private Set<String> removedDataFilePaths = Sets.newHashSet();

  // cache filtered manifests to avoid extra work when commits fail.
  private final Map<ManifestFile, ManifestFile> filteredManifests = Maps.newConcurrentMap();

  // tracking where files were deleted to validate retries quickly
  private final Map<ManifestFile, Iterable<F>> filteredManifestToDeletedFiles =
      Maps.newConcurrentMap();

  private final Supplier<ExecutorService> workerPoolSupplier;

  protected ManifestFilterManager(
      Map<Integer, PartitionSpec> specsById, Supplier<ExecutorService> executorSupplier) {
    this.specsById = specsById;
    this.deleteFilePartitions = PartitionSet.create(specsById);
    this.dropPartitions = PartitionSet.create(specsById);
    this.workerPoolSupplier = executorSupplier;
  }

  protected abstract void deleteFile(String location);

  protected abstract ManifestWriter<F> newManifestWriter(PartitionSpec spec);

  protected abstract ManifestReader<F> newManifestReader(ManifestFile manifest);

  protected abstract Set<F> newFileSet();

  protected void failAnyDelete() {
    this.failAnyDelete = true;
  }

  protected void failMissingDeletePaths() {
    this.failMissingDeletePaths = true;
  }

  protected Set<F> filesToBeDeleted() {
    return deleteFiles;
  }

  /**
   * Add a filter to match files to delete. A file will be deleted if all of the rows it contains
   * match this or any other filter passed to this method.
   *
   * @param expr an expression to match rows.
   */
  protected void deleteByRowFilter(Expression expr) {
    Preconditions.checkNotNull(expr, "Cannot delete files using filter: null");
    invalidateFilteredCache();
    this.deleteExpression = Expressions.or(deleteExpression, expr);
    this.allDeletesReferenceManifests = false;
  }

  /** Add a partition tuple to drop from the table during the delete phase. */
  protected void dropPartition(int specId, StructLike partition) {
    Preconditions.checkNotNull(partition, "Cannot delete files in invalid partition: null");
    invalidateFilteredCache();
    dropPartitions.add(specId, partition);
    this.allDeletesReferenceManifests = false;
  }

  /**
   * Set the sequence number used to remove old delete files.
   *
   * <p>Delete files with a sequence number older than the given value will be removed. By setting
   * this to the sequence number of the oldest data file in the table, this will continuously remove
   * delete files that are no longer needed because deletes cannot match any existing rows in the
   * table.
   *
   * @param sequenceNumber a sequence number used to remove old delete files
   */
  protected void dropDeleteFilesOlderThan(long sequenceNumber) {
    Preconditions.checkArgument(
        sequenceNumber >= 0, "Invalid minimum data sequence number: %s", sequenceNumber);
    this.minSequenceNumber = sequenceNumber;
  }

  void caseSensitive(boolean newCaseSensitive) {
    this.caseSensitive = newCaseSensitive;
  }

  protected void removeDanglingDeletesFor(Set<DataFile> dataFiles) {
    this.removedDataFilePaths =
        dataFiles.stream().map(ContentFile::location).collect(Collectors.toSet());
  }

  /** Add a specific path to be deleted in the new snapshot. */
  void delete(F file) {
    Preconditions.checkNotNull(file, "Cannot delete file: null");
    invalidateFilteredCache();

    if (file.manifestLocation() == null) {
      this.allDeletesReferenceManifests = false;
    } else {
      manifestsWithDeletes.add(file.manifestLocation());
    }

    deleteFiles.add(file);
    // AFFIRM: caller-supplied deletions only. deleteFiles itself is ALSO written by the
    // filtering loop (filterManifestWithDeletedFiles) and is never cleared -- not by
    // invalidateFilteredCache, not anywhere -- so on a commit retry it contains paths this
    // caller never asked to delete. validateNoContradictoryDuplicateRegistrationIntent must
    // read this set instead, or it would blame the caller for the loop's own bookkeeping.
    callerDeletedFilePaths.add(file.location().toString());
    deleteFilePartitions.add(file.specId(), file.partition());
  }

  /** Add a specific path to be deleted in the new snapshot. */
  void delete(CharSequence path) {
    Preconditions.checkNotNull(path, "Cannot delete file path: null");
    invalidateFilteredCache();
    this.allDeletesReferenceManifests = false;
    deletePaths.add(path);
  }

  /**
   * AFFIRM: repairs a file that was registered more than once at different data sequence
   * numbers, keeping exactly one registration per path and dropping the rest.
   *
   * <p>A single physical file registered twice is not something a healthy writer produces. It
   * arises when a commit is retried after its outcome became unknown (for example {@link
   * org.apache.iceberg.exceptions.CommitStateUnknownException}) and the retry re-registers a
   * WriteResult that had in fact already been applied. Left alone, this corrupts the next
   * compaction: {@code DataFileSet}/{@code DeleteFileSet} key file identity on location alone, so
   * the data side collapses the two registrations into one manifest reference and only removes
   * one of them, while the delete side has no sequence number to key on at all and removes both
   * -- leaving the surviving data registration with no delete coverage and resurrecting rows that
   * were correctly suppressed.
   *
   * <p>The value in {@code keepSequenceNumberByPath} for each path MUST be the lowest live data
   * sequence number registered for that path, not an arbitrary choice. Any delete file committed
   * between two duplicate registrations has a sequence number greater than the lower one, so it
   * continues to cover a registration kept at the lower sequence number. Keeping the higher one
   * instead would silently drop that coverage and reproduce the exact defect this repairs.
   *
   * <p>Applies uniformly regardless of content type: a duplicate registration of a delete file is
   * exactly as dangerous to whatever later reads it as a duplicate data file registration is, so
   * both filter managers ({@code DataFile} and {@code DeleteFile}) are driven the same way.
   *
   * @param keepSequenceNumberByPath a map from file location to the one live data sequence
   *     number at that location which must be kept; every other live registration at that
   *     location is deleted from the new snapshot
   */
  protected void dropDuplicateRegistrations(Map<String, Long> keepSequenceNumberByPath) {
    Preconditions.checkNotNull(
        keepSequenceNumberByPath, "Cannot drop duplicate registrations using: null");
    if (keepSequenceNumberByPath.isEmpty()) {
      return;
    }
    invalidateFilteredCache();
    this.duplicateRegistrationKeepSequence.putAll(keepSequenceNumberByPath);
    this.allDeletesReferenceManifests = false;
  }

  // AFFIRM: true if this exact live registration (this path at this data sequence number) is a
  // duplicate that must be dropped, i.e. a *different* sequence number was chosen to survive for
  // this same path. As a side effect, records the path as confirmed-live when THIS is the
  // survivor entry (see duplicateRegistrationSurvivorsSeen and
  // validateDuplicateRegistrationSurvivorsPresent for why: a commit retry against a base a
  // concurrent writer already changed must not silently drop every registration of a path whose
  // survivor vanished out from under it).
  //
  // Every caller MUST evaluate this method unconditionally into a local variable before
  // combining it into a || chain, never call it inline as one term of that chain. Java
  // short-circuits ||, so an earlier term in the chain matching the same entry (e.g.
  // deletePaths/deleteFiles/dropPartitions) would skip this call, and skip its survivor-seen
  // side effect, even though the entry IS the survivor. See both call sites below for the
  // pattern to follow.
  /**
   * AFFIRM: rejects a commit that both designates a survivor for a path AND separately asks for
   * that same path to be deleted outright.
   *
   * <p>Those two instructions contradict each other, and without this check the contradiction
   * resolves silently in the destructive direction: the survivor entry is visited (so {@link
   * #duplicateRegistrationSurvivorsSeen} records it and {@link
   * #validateDuplicateRegistrationSurvivorsPresent} is satisfied), but is then dropped anyway by
   * the {@code deletePaths}/{@code deleteFiles} term in the same predicate. Every live
   * registration of the path disappears and the commit reports success -- exactly the silent-loss
   * shape the survivor check exists to prevent, arrived at from the other direction.
   *
   * <p>Worth recording how this was found, because it is a genuine cost of an earlier fix. Before
   * {@code isDuplicateRegistrationToDrop} was hoisted out of the {@code ||} chains (done so its
   * survivor-recording side effect could not be short-circuited away), this case happened to
   * abort loudly: {@code deletePaths} matched first, the hoisted call never ran, no survivor was
   * recorded, and the survivor check threw. That was accidental safety, not designed safety, and
   * hoisting removed it. This check replaces it with the real thing -- a specific error about
   * contradictory intent, rather than a misleading "survivor not found live" message blaming
   * concurrent modification for what is a caller bug.
   *
   * <p>Not covered: {@code dropPartitions}. Deciding whether a dropped partition contains a
   * duplicated path needs the partition tuple for that path, which is only available once the
   * manifests are open -- by which point the filtering decision has already been made. No caller
   * combines the two today ({@link DuplicateRegistrationRepair} sets neither), and doing so would
   * hit the silent path described above. Left as a known gap rather than a half-check.
   */
  // CollectionUndefinedEquality: deletePaths is a CharSequenceSet, which wraps its elements for
  // comparison, so a String lookup is well-defined here. Same suppression and reason as
  // validateRequiredDeletes and the two filtering methods below.
  @SuppressWarnings("CollectionUndefinedEquality")
  private void validateNoContradictoryDuplicateRegistrationIntent() {
    if (duplicateRegistrationKeepSequence.isEmpty()) {
      return;
    }

    Set<String> conflicting = Sets.newTreeSet();
    for (String path : duplicateRegistrationKeepSequence.keySet()) {
      if (deletePaths.contains(path)) {
        conflicting.add(path);
      }
    }
    for (F file : deleteFiles) {
      String location = file.location().toString();
      if (duplicateRegistrationKeepSequence.containsKey(location)) {
        conflicting.add(location);
      }
    }

    ValidationException.check(
        conflicting.isEmpty(),
        "Cannot both keep a duplicate registration and delete the same path outright: %s. "
            + "These instructions contradict each other -- the designated survivor would be "
            + "dropped by the explicit delete, leaving no live registration of the path at all. "
            + "Issue the repair and the delete as separate commits if both are genuinely "
            + "intended.",
        COMMA.join(conflicting));
  }

  private boolean isDuplicateRegistrationToDrop(F file, ManifestEntry<F> entry) {
    if (duplicateRegistrationKeepSequence.isEmpty()) {
      return false;
    }
    String path = file.location().toString();
    Long keepSequenceNumber = duplicateRegistrationKeepSequence.get(path);
    if (keepSequenceNumber == null) {
      return false;
    }
    if (keepSequenceNumber.equals(entry.dataSequenceNumber())) {
      duplicateRegistrationSurvivorsSeen.add(path);
      return false;
    }
    return true;
  }

  boolean containsDeletes() {
    return !deletePaths.isEmpty()
        || !deleteFiles.isEmpty()
        || deleteExpression != Expressions.alwaysFalse()
        || !dropPartitions.isEmpty()
        || !duplicateRegistrationKeepSequence.isEmpty();
  }

  /**
   * Filter deleted files out of a list of manifests.
   *
   * @param tableSchema the current table schema
   * @param manifests a list of manifests to be filtered
   * @return an array of filtered manifests
   */
  List<ManifestFile> filterManifests(Schema tableSchema, List<ManifestFile> manifests) {
    // AFFIRM: cleared per call, not per commit attempt -- apply()/filterManifests() re-runs on
    // every optimistic-concurrency retry against a freshly refreshed base, and a survivor seen
    // during a PRIOR (failed/superseded) attempt must never satisfy this retry's check.
    duplicateRegistrationSurvivorsSeen.clear();

    // AFFIRM: must run BEFORE the filtering loop below, which itself adds entries to
    // deleteFiles (see filterManifestWithDeletedFiles); at this point deleteFiles still holds
    // only what the caller supplied.
    validateNoContradictoryDuplicateRegistrationIntent();

    if (manifests == null || manifests.isEmpty()) {
      validateRequiredDeletes();
      validateDuplicateRegistrationSurvivorsPresent();
      return ImmutableList.of();
    }

    boolean trustManifestReferences = canTrustManifestReferences(manifests);
    ManifestFile[] filtered = new ManifestFile[manifests.size()];
    // open all of the manifest files in parallel, use index to avoid reordering
    Tasks.range(filtered.length)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(workerPoolSupplier.get())
        .run(
            index -> {
              ManifestFile manifest =
                  filterManifest(tableSchema, manifests.get(index), trustManifestReferences);
              filtered[index] = manifest;
            });

    validateRequiredDeletes(filtered);
    validateDuplicateRegistrationSurvivorsPresent();

    return Arrays.asList(filtered);
  }

  /**
   * AFFIRM: throws if the intended surviving registration for any duplicated path was not
   * actually observed live while filtering manifests for this attempt.
   *
   * <p>{@code dropDuplicateRegistrations}'s keep-sequence-number map is computed once, before
   * this repair's commit is attempted, from a scan that already happened. {@code
   * MergingSnapshotProducer}'s optimistic-concurrency retry loop re-runs {@code validate()}/{@code
   * apply()} -- and therefore this filtering pass -- against a freshly refreshed base on every
   * CAS conflict, but does NOT recompute that map. If a concurrent commit changed this exact path
   * in the narrow window between the original scan and a retry (for example, a different
   * operation legitimately replaced the file the "keep" sequence number pointed at), silently
   * proceeding would drop every live registration of that path -- the survivor because it no
   * longer exists at that sequence number, and every other registration because it doesn't match
   * either. That is silent data loss with the commit reporting success. Failing loudly here
   * instead means the whole commit attempt aborts; a subsequent retry (or a fresh invocation of
   * the guard) re-evaluates against the latest state rather than acting on stale intent.
   *
   * <p><b>What does NOT trigger this: a concurrent streaming writer, however frequent --
   * including an UPSERT writer.</b> This matters because our CDC tables are fed by a Flink upsert
   * sink that commits every ~5 minutes and emits an equality delete for every row, so "every
   * commit touches the delete side" is the steady state here, not an edge case. It is still safe,
   * and the reason is structural rather than a property of appends:
   *
   * <ul>
   *   <li>An upsert commits a {@code RowDelta} that only calls {@code addRows}/{@code addDeletes}
   *       -- never {@code removeRows}/{@code removeDeletes}. So {@code deletePaths}, {@code
   *       deleteFiles} and {@code dropPartitions} all stay EMPTY on that commit.
   *   <li>With all three empty and no delete expression, {@code canContainDeletedFiles} returns
   *       false for every pre-existing manifest, so those manifests are never opened or
   *       rewritten and no existing entry can be marked DELETED. Note that {@code
   *       canContainDeletedFiles} has no {@code minSequenceNumber} term -- which is also why
   *       ordinary writer commits never clean up dangling equality deletes, i.e. the very reason
   *       they accumulate on these tables. That accumulation is empirical confirmation of this
   *       reading.
   *   <li>Even when a commit does merge manifests, {@link ManifestWriter#existing} is
   *       contractually required to preserve the original data sequence number, so a survivor
   *       stays findable at exactly the sequence number the scan designated.
   * </ul>
   *
   * <p>Nor does a long-running compaction widen the window: the repair is a separate
   * metadata-only commit that lands BEFORE the rewrite is planned, so the rewrite's own duration
   * is irrelevant here. Ordinary CAS contention on the rewrite's own commit is a separate,
   * pre-existing concern that {@code partial-progress.enabled} addresses. The repair does add one
   * more commit competing with the writer, but it is metadata-only, fast, and inherits the
   * table's normal {@code commit.retry.*} budget.
   *
   * <p>Only an operation that explicitly removes or replaces that exact entry can trigger this:
   * another compaction, a DELETE/MERGE/overwrite touching that file, a dedup or privacy-delete
   * job, or another repair. None of those run on the streaming writer's cadence.
   *
   * <p><b>Known, accepted tradeoff (raised in review on Affirm/iceberg#6,
   * pullrequestreview-5105776910):</b> this fails the ENTIRE batch's commit if even one
   * duplicated path's survivor goes missing, even when every other path in the batch is still
   * perfectly repairable. Deliberately not implemented as "drop the poisoned path and repair the
   * rest": that would mean silently mutating repair intent baked into an EARLIER scan under
   * exactly the CAS-conflict conditions this method exists to be paranoid about, which is a much
   * larger correctness surface than the fail-and-let-a-fresh-run-rescan behavior chosen here.
   *
   * <p>The whole-batch failure is safe, and self-healing PROVIDED the interfering operation is
   * not itself recurring: the next scheduled compaction run re-scans and repairs whatever is
   * still actually duplicated. If some job repeatedly removes the same survivor on a cadence
   * shorter than the repair takes to commit, repair would keep failing and the table would stay
   * duplicated -- detectable as the same table failing this check run after run, which is the
   * signal the follow-up repair-frequency metric should alert on. Revisit the partial-repair
   * design only if that is actually observed, not preemptively.
   */
  private void validateDuplicateRegistrationSurvivorsPresent() {
    if (duplicateRegistrationKeepSequence.isEmpty()) {
      return;
    }

    Set<String> missing =
        Sets.difference(
                duplicateRegistrationKeepSequence.keySet(), duplicateRegistrationSurvivorsSeen)
            .immutableCopy();
    ValidationException.check(
        missing.isEmpty(),
        "Cannot repair duplicate file registrations: the intended surviving registration was "
            + "not found live for %d path(s), most likely because a concurrent commit already "
            + "changed them since this repair's scan. Aborting this commit attempt rather than "
            + "risking dropping every live registration of an affected path. A retry will "
            + "re-evaluate against the latest state. Affected: %s",
        missing.size(),
        COMMA.join(missing));
  }

  // Use the current set of referenced manifests as a source of truth when it's a subset of all
  // manifests and all removals which were performed reference manifests.
  // If a manifest without live files is not in the trusted referenced set, this means that the
  // manifest has no deleted entries and does not need to be rewritten.
  private boolean canTrustManifestReferences(List<ManifestFile> manifests) {
    Set<String> manifestLocations =
        manifests.stream().map(ManifestFile::path).collect(Collectors.toSet());
    return allDeletesReferenceManifests
        && !manifestsWithDeletes.isEmpty()
        && manifestLocations.containsAll(manifestsWithDeletes);
  }

  /**
   * Creates a snapshot summary builder with the files deleted from the set of filtered manifests.
   *
   * @param manifests a set of filtered manifests
   */
  SnapshotSummary.Builder buildSummary(Iterable<ManifestFile> manifests) {
    SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();

    for (ManifestFile manifest : manifests) {
      PartitionSpec manifestSpec = specsById.get(manifest.partitionSpecId());
      Iterable<F> manifestDeletes = filteredManifestToDeletedFiles.get(manifest);
      if (manifestDeletes != null) {
        for (F file : manifestDeletes) {
          summaryBuilder.deletedFile(manifestSpec, file);
        }
      }
    }

    summaryBuilder.incrementDuplicateDeletes(duplicateDeleteCount);

    return summaryBuilder;
  }

  /**
   * Throws a {@link ValidationException} if any deleted file was not present in a filtered
   * manifest.
   *
   * @param manifests a set of filtered manifests
   */
  @SuppressWarnings("CollectionUndefinedEquality")
  private void validateRequiredDeletes(ManifestFile... manifests) {
    if (failMissingDeletePaths) {
      Set<F> deletedFiles = deletedFiles(manifests);
      ValidationException.check(
          deletedFiles.containsAll(deleteFiles),
          "Missing required files to delete: %s",
          COMMA.join(
              deleteFiles.stream()
                  .filter(f -> !deletedFiles.contains(f))
                  .map(ContentFile::location)
                  .collect(Collectors.toList())));

      CharSequenceSet deletedFilePaths =
          deletedFiles.stream()
              .map(ContentFile::location)
              .collect(Collectors.toCollection(CharSequenceSet::empty));

      ValidationException.check(
          deletedFilePaths.containsAll(deletePaths),
          "Missing required files to delete: %s",
          COMMA.join(Iterables.filter(deletePaths, path -> !deletedFilePaths.contains(path))));
    }
  }

  private Set<F> deletedFiles(ManifestFile[] manifests) {
    Set<F> deletedFiles = newFileSet();

    if (manifests != null) {
      for (ManifestFile manifest : manifests) {
        Iterable<F> manifestDeletes = filteredManifestToDeletedFiles.get(manifest);
        if (manifestDeletes != null) {
          for (F file : manifestDeletes) {
            deletedFiles.add(file);
          }
        }
      }
    }

    return deletedFiles;
  }

  /**
   * Returns the count of manifests that were replaced (rewritten) during filtering.
   *
   * <p>A manifest is considered replaced when a new manifest was created to replace the original
   * one (i.e., the original manifest != filtered manifest).
   *
   * @return the count of replaced manifests
   */
  int replacedManifestsCount() {
    return replacedManifestsCount.get();
  }

  /**
   * Deletes filtered manifests that were created by this class, but are not in the committed
   * manifest set.
   *
   * @param committed the set of manifest files that were committed
   */
  void cleanUncommitted(Set<ManifestFile> committed) {
    // iterate over a copy of entries to avoid concurrent modification
    List<Map.Entry<ManifestFile, ManifestFile>> filterEntries =
        Lists.newArrayList(filteredManifests.entrySet());

    for (Map.Entry<ManifestFile, ManifestFile> entry : filterEntries) {
      // remove any new filtered manifests that aren't in the committed list
      ManifestFile manifest = entry.getKey();
      ManifestFile filtered = entry.getValue();
      if (!committed.contains(filtered)) {
        // only delete if the filtered copy was created (manifest was replaced)
        if (!manifest.equals(filtered)) {
          deleteFile(filtered.path());
          replacedManifestsCount.decrementAndGet();
        }

        // remove the entry from the cache
        filteredManifests.remove(manifest);
      }
    }
  }

  private void invalidateFilteredCache() {
    cleanUncommitted(SnapshotProducer.EMPTY_SET);
    replacedManifestsCount.set(0);
  }

  /**
   * @return a ManifestReader that is a filtered version of the input manifest.
   */
  private ManifestFile filterManifest(
      Schema tableSchema, ManifestFile manifest, boolean trustManifestReferences) {
    ManifestFile cached = filteredManifests.get(manifest);
    if (cached != null) {
      return cached;
    }

    if (!canContainDeletedFiles(manifest, trustManifestReferences)) {
      filteredManifests.put(manifest, manifest);
      return manifest;
    }

    try (ManifestReader<F> reader = newManifestReader(manifest)) {
      PartitionSpec spec = reader.spec();
      PartitionAndMetricsEvaluator evaluator =
          new PartitionAndMetricsEvaluator(tableSchema, spec, deleteExpression);
      // this assumes that the manifest doesn't have files to remove and streams through the
      // manifest without copying data. if a manifest does have a file to remove, this will break
      // out of the loop and move on to filtering the manifest.
      if (manifestHasDeletedFiles(evaluator, manifest, reader)) {
        ManifestFile filtered = filterManifestWithDeletedFiles(evaluator, manifest, reader);
        replacedManifestsCount.incrementAndGet();
        return filtered;
      } else {
        filteredManifests.put(manifest, manifest);
        return manifest;
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close manifest: %s", manifest);
    }
  }

  private boolean canContainDeletedFiles(ManifestFile manifest, boolean trustManifestReferences) {
    if (hasNoLiveFiles(manifest)) {
      return false;
    }

    if (trustManifestReferences) {
      return manifestsWithDeletes.contains(manifest.path());
    }

    return canContainDroppedFiles(manifest)
        || canContainExpressionDeletes(manifest)
        || canContainDroppedPartitions(manifest);
  }

  private boolean hasNoLiveFiles(ManifestFile manifest) {
    return !manifest.hasAddedFiles() && !manifest.hasExistingFiles();
  }

  private boolean canContainExpressionDeletes(ManifestFile manifest) {
    if (deleteExpression != null && deleteExpression != Expressions.alwaysFalse()) {
      ManifestEvaluator manifestEvaluator =
          ManifestEvaluator.forRowFilter(
              deleteExpression, specsById.get(manifest.partitionSpecId()), caseSensitive);
      return manifestEvaluator.eval(manifest);
    }

    return false;
  }

  private boolean canContainDroppedPartitions(ManifestFile manifest) {
    if (!dropPartitions.isEmpty()) {
      return ManifestFileUtil.canContainAny(manifest, dropPartitions, specsById);
    }

    return false;
  }

  private boolean canContainDroppedFiles(ManifestFile manifest) {
    // AFFIRM: independent checks, not an else-if chain. deleteFiles/deletePaths,
    // duplicateRegistrationKeepSequence, and removedDataFilePaths can in principle all be
    // populated on the same ManifestFilterManager instance (they're independent predicates on
    // the same class); an else-if here would let a deleteFiles partition-overlap check
    // short-circuit past a real duplicate registration or dangling DV in a manifest outside that
    // partition set, silently under-scanning.
    if (!deletePaths.isEmpty()
        || !duplicateRegistrationKeepSequence.isEmpty()
        || !removedDataFilePaths.isEmpty()) {
      // AFFIRM: no cheap partition/path pre-filter is available for a duplicate registration or
      // a dangling DV's referenced data file -- either can be in any manifest of either content
      // type -- so every manifest must be opened. Both repair paths are rare by construction, so
      // the extra scan cost is acceptable.
      return true;
    }

    if (!deleteFiles.isEmpty()) {
      return ManifestFileUtil.canContainAny(manifest, deleteFilePartitions, specsById);
    }

    return false;
  }

  @SuppressWarnings({"CollectionUndefinedEquality", "checkstyle:CyclomaticComplexity"})
  private boolean manifestHasDeletedFiles(
      PartitionAndMetricsEvaluator evaluator, ManifestFile manifest, ManifestReader<F> reader) {
    if (manifestsWithDeletes.contains(manifest.path())) {
      return true;
    }

    boolean isDelete = reader.isDeleteManifestReader();

    for (ManifestEntry<F> entry : reader.liveEntries()) {
      F file = entry.file();
      // AFFIRM: evaluated unconditionally, not inline in the || chain below. This method's
      // "mark the survivor as seen" side effect only fires when this call is actually reached;
      // placing it after an earlier condition in a || chain would let Java's short-circuiting
      // skip it whenever that earlier condition already matched the same entry, wrongly leaving
      // a live survivor unrecorded.
      boolean isDuplicateRegistrationDrop = isDuplicateRegistrationToDrop(file, entry);
      boolean markedForDelete =
          deletePaths.contains(file.location())
              || deleteFiles.contains(file)
              || dropPartitions.contains(file.specId(), file.partition())
              || isDuplicateRegistrationDrop
              || (isDelete
                  && entry.isLive()
                  && entry.dataSequenceNumber() > 0
                  && entry.dataSequenceNumber() < minSequenceNumber)
              || (isDelete && isDanglingDV((DeleteFile) file));

      if (markedForDelete || evaluator.rowsMightMatch(file)) {
        boolean allRowsMatch = markedForDelete || evaluator.rowsMustMatch(file);
        ValidationException.check(
            allRowsMatch
                || isDelete, // ignore delete files where some records may not match the expression
            "Cannot delete file where some, but not all, rows match filter %s: %s",
            this.deleteExpression,
            file.location());

        if (allRowsMatch) {
          if (failAnyDelete) {
            throw new DeleteException(reader.spec().partitionToPath(file.partition()));
          }

          // as soon as a deleted file is detected, stop scanning
          return true;
        }
      }
    }

    return false;
  }

  private boolean isDanglingDV(DeleteFile file) {
    return ContentFileUtil.isDV(file) && removedDataFilePaths.contains(file.referencedDataFile());
  }

  @SuppressWarnings({"CollectionUndefinedEquality", "checkstyle:CyclomaticComplexity"})
  private ManifestFile filterManifestWithDeletedFiles(
      PartitionAndMetricsEvaluator evaluator, ManifestFile manifest, ManifestReader<F> reader) {
    boolean isDelete = reader.isDeleteManifestReader();
    // when this point is reached, there is at least one file that will be deleted in the
    // manifest. produce a copy of the manifest with all deleted files removed.
    Set<F> deletedFiles = newFileSet();

    try {
      ManifestWriter<F> writer = newManifestWriter(reader.spec());
      try {
        reader
            .liveEntries()
            .forEach(
                entry -> {
                  F file = entry.file();
                  boolean isDanglingDV = isDelete && isDanglingDV((DeleteFile) file);
                  boolean isDuplicateRegistrationDrop = isDuplicateRegistrationToDrop(file, entry);
                  boolean markedForDelete =
                      isDanglingDV
                          || deletePaths.contains(file.location())
                          || deleteFiles.contains(file)
                          || dropPartitions.contains(file.specId(), file.partition())
                          || isDuplicateRegistrationDrop
                          || (isDelete
                              && entry.isLive()
                              && entry.dataSequenceNumber() > 0
                              && entry.dataSequenceNumber() < minSequenceNumber);
                  if (markedForDelete || evaluator.rowsMightMatch(file)) {
                    boolean allRowsMatch = markedForDelete || evaluator.rowsMustMatch(file);
                    ValidationException.check(
                        allRowsMatch
                            || isDelete, // ignore delete files where some records may not match
                        // the expression
                        "Cannot delete file where some, but not all, rows match filter %s: %s",
                        this.deleteExpression,
                        file.location());

                    if (allRowsMatch) {
                      writer.delete(entry);
                      F fileCopy = file.copyWithoutStats();
                      if (!isDuplicateRegistrationDrop) {
                        // AFFIRM: skip for a duplicate-registration drop -- the path itself is
                        // NOT gone, a sibling entry at a different sequence number still lives
                        // at this exact location. deleteFiles has two consumers and adding the
                        // path here would corrupt both:
                        //
                        //  1. filesToBeDeleted() feeds
                        //     DeleteFileFilterManager#removeDanglingDeletesFor, which would then
                        //     drop a DV that still legitimately covers the surviving entry --
                        //     reproducing this same class of defect.
                        //  2. validateRequiredDeletes() asserts
                        //     deletedFiles.containsAll(deleteFiles) when failMissingDeletePaths
                        //     is set. Since deleteFiles/DeleteFileSet key identity on
                        //     (location, contentOffset, contentSizeInBytes) -- identical across
                        //     both registrations of one physical file -- an entry added here
                        //     also changes what that assertion demands.
                        //
                        // Note this runs during the filtering loop, i.e. AFTER
                        // validateNoContradictoryDuplicateRegistrationIntent() has already read
                        // deleteFiles; that check deliberately runs before this loop so it sees
                        // only caller-supplied entries, not ones synthesised here.
                        deleteFiles.add(fileCopy);
                      }

                      if (deletedFiles.contains(file)) {
                        LOG.warn(
                            "Deleting a duplicate path from manifest {}: {}",
                            manifest.path(),
                            file.location());
                        duplicateDeleteCount += 1;
                      } else {
                        // only add the file to deletes if it is a new delete
                        // this keeps the snapshot summary accurate for non-duplicate data
                        deletedFiles.add(fileCopy);
                      }
                    } else {
                      writer.existing(entry);
                    }

                  } else {
                    writer.existing(entry);
                  }
                });
      } finally {
        writer.close();
      }

      // return the filtered manifest as a reader
      ManifestFile filtered = writer.toManifestFile();

      // update caches
      filteredManifests.put(manifest, filtered);
      filteredManifestToDeletedFiles.put(filtered, deletedFiles);

      return filtered;

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close manifest writer");
    }
  }

  // an evaluator that checks whether rows in a file may/must match a given expression
  // this class first partially evaluates the provided expression using the partition tuple
  // and then checks the remaining part of the expression using metrics evaluators
  private class PartitionAndMetricsEvaluator {
    private final Schema tableSchema;
    private final ResidualEvaluator residualEvaluator;
    private final StructLikeMap<Pair<InclusiveMetricsEvaluator, StrictMetricsEvaluator>>
        metricsEvaluators;

    PartitionAndMetricsEvaluator(Schema tableSchema, PartitionSpec spec, Expression expr) {
      this.tableSchema = tableSchema;
      this.residualEvaluator = ResidualEvaluator.of(spec, expr, caseSensitive);
      this.metricsEvaluators = StructLikeMap.create(spec.partitionType());
    }

    boolean rowsMightMatch(F file) {
      Pair<InclusiveMetricsEvaluator, StrictMetricsEvaluator> evaluators = metricsEvaluators(file);
      InclusiveMetricsEvaluator inclusiveMetricsEvaluator = evaluators.first();
      return inclusiveMetricsEvaluator.eval(file);
    }

    boolean rowsMustMatch(F file) {
      Pair<InclusiveMetricsEvaluator, StrictMetricsEvaluator> evaluators = metricsEvaluators(file);
      StrictMetricsEvaluator strictMetricsEvaluator = evaluators.second();
      return strictMetricsEvaluator.eval(file);
    }

    private Pair<InclusiveMetricsEvaluator, StrictMetricsEvaluator> metricsEvaluators(F file) {
      // ResidualEvaluator removes predicates in the expression using strict/inclusive projections
      // if strict projection returns true -> the pred would return true -> replace the pred with
      // true
      // if inclusive projection returns false -> the pred would return false -> replace the pred
      // with false
      // otherwise, keep the original predicate and proceed to other predicates in the expression
      // in other words, ResidualEvaluator returns a part of the expression that needs to be
      // evaluated
      // for rows in the given partition using metrics
      PartitionData partition = (PartitionData) file.partition();
      if (!metricsEvaluators.containsKey(partition)) {
        Expression residual = residualEvaluator.residualFor(partition);
        InclusiveMetricsEvaluator inclusive =
            new InclusiveMetricsEvaluator(tableSchema, residual, caseSensitive);
        StrictMetricsEvaluator strict =
            new StrictMetricsEvaluator(tableSchema, residual, caseSensitive);

        metricsEvaluators.put(
            partition.copy(), // The partition may be a re-used container so a copy is required
            Pair.of(inclusive, strict));
      }
      return metricsEvaluators.get(partition);
    }
  }
}
