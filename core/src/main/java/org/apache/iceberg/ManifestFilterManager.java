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
  // AFFIRM: path -> the one data sequence number at that path which must survive; every other
  // live registration of that same path is a duplicate to be dropped. See
  // #dropDuplicateRegistrations for why the lowest sequence number is always the one kept.
  private final Map<String, Long> duplicateRegistrationKeepSequence = Maps.newHashMap();
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
   */
  private void validateDuplicateRegistrationSurvivorsPresent() {
    if (duplicateRegistrationKeepSequence.isEmpty()) {
      return;
    }

    Set<String> missing =
        Sets.difference(
                duplicateRegistrationKeepSequence.keySet(), duplicateRegistrationSurvivorsSeen)
            .immutableCopy();
    if (!missing.isEmpty()) {
      throw new ValidationException(
          "Cannot repair duplicate file registrations: the intended surviving registration was "
              + "not found live for %d path(s), most likely because a concurrent commit already "
              + "changed them since this repair's scan. Aborting this commit attempt rather than "
              + "risking dropping every live registration of an affected path. A retry will "
              + "re-evaluate against the latest state. Affected: %s",
          missing.size(),
          COMMA.join(missing));
    }
  }

  // Use the current set of referenced manifests as a source of truth when it's a subset of all
  // manifests and all removals which were performed reference manifests.
  // If a manifest without live files is not in the trusted referenced set, this means that the
  // manifest has no deleted entries and does not need to be rewritten.
  private boolean canTrustManifestReferences(List<ManifestFile> manifests) {
    Set<String> manifestLocations =
        manifests.stream().map(ManifestFile::path).collect(Collectors.toSet());
    return allDeletesReferenceManifests && manifestLocations.containsAll(manifestsWithDeletes);
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
        // only delete if the filtered copy was created
        if (!manifest.equals(filtered)) {
          deleteFile(filtered.path());
        }

        // remove the entry from the cache
        filteredManifests.remove(manifest);
      }
    }
  }

  private void invalidateFilteredCache() {
    cleanUncommitted(SnapshotProducer.EMPTY_SET);
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
        return filterManifestWithDeletedFiles(evaluator, manifest, reader);
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
    // AFFIRM: independent checks, not an else-if chain. deleteFiles/deletePaths and
    // duplicateRegistrationKeepSequence can in principle both be populated on the same
    // ManifestFilterManager instance (they're independent predicates on the same class); an
    // else-if here would let a deleteFiles partition-overlap check short-circuit past a real
    // duplicate registration in a manifest outside that partition set, silently under-scanning.
    if (!deletePaths.isEmpty() || !duplicateRegistrationKeepSequence.isEmpty()) {
      // AFFIRM: no cheap partition/path pre-filter is available for a duplicate registration --
      // it can be in any manifest of either content type -- so every manifest must be opened.
      // This repair path is rare by construction, so the extra scan cost is acceptable.
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
      boolean markedForDelete =
          deletePaths.contains(file.location())
              || deleteFiles.contains(file)
              || dropPartitions.contains(file.specId(), file.partition())
              || isDuplicateRegistrationToDrop(file, entry)
              || (isDelete
                  && entry.isLive()
                  && entry.dataSequenceNumber() > 0
                  && entry.dataSequenceNumber() < minSequenceNumber);

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
                  boolean markedForDelete =
                      deletePaths.contains(file.location())
                          || deleteFiles.contains(file)
                          || dropPartitions.contains(file.specId(), file.partition())
                          || isDuplicateRegistrationToDrop(file, entry)
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

                      if (deletedFiles.contains(file)) {
                        LOG.warn(
                            "Deleting a duplicate path from manifest {}: {}",
                            manifest.path(),
                            file.location());
                        duplicateDeleteCount += 1;
                      } else {
                        // only add the file to deletes if it is a new delete
                        // this keeps the snapshot summary accurate for non-duplicate data
                        deletedFiles.add(file.copyWithoutStats());
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
