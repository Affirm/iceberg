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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * AFFIRM: repairs a table where a data or delete file was registered more than once at different
 * data sequence numbers -- the state {@link org.apache.iceberg.actions.RewriteDataFiles}'
 * {@code validate-duplicate-file-registrations} check refuses to compact.
 *
 * <p>A single physical file registered twice is not something a healthy writer produces. It
 * arises when a commit is retried after its outcome became unknown (for example {@link
 * org.apache.iceberg.exceptions.CommitStateUnknownException}) and the retry re-registers a
 * WriteResult that had in fact already been applied. Left alone, this corrupts the next
 * compaction: file identity in the rewrite path is keyed on location alone, so the two
 * registrations are indistinguishable there -- the data side collapses them into one manifest
 * reference and removes only one, while the delete side has no sequence number to key on and
 * removes both, leaving the surviving data registration with no delete coverage and resurrecting
 * rows that were correctly suppressed.
 *
 * <p>For every duplicated path, callers must keep the LOWEST live data sequence number
 * registered for that path -- not an arbitrary choice. See {@link
 * ManifestFilterManager#dropDuplicateRegistrations} for why: any delete file committed between
 * two duplicate registrations has a sequence number greater than the lower one, so it continues
 * to cover a registration kept at the lower sequence number. Keeping the higher one instead would
 * silently drop that coverage and reproduce the exact defect this repairs.
 *
 * <p>Callers are expected to have already identified the duplicated paths and their live
 * sequence numbers (for example via a scan of the {@code entries} metadata table), and to supply
 * only the sequence number to KEEP per path -- every other live registration of that path is
 * dropped from the new snapshot.
 */
public class DuplicateRegistrationRepair
    extends MergingSnapshotProducer<DuplicateRegistrationRepair> {

  private final Map<String, Long> keepDataFileSequenceNumberByPath = Maps.newHashMap();
  private final Map<String, Long> keepDeleteFileSequenceNumberByPath = Maps.newHashMap();

  public DuplicateRegistrationRepair(String tableName, TableOperations ops) {
    super(tableName, ops);
  }

  @Override
  protected DuplicateRegistrationRepair self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.DELETE;
  }

  /**
   * Marks {@code sequenceNumber} as the live data sequence number to keep for a duplicated data
   * file path. Every other live registration of that same path is dropped.
   *
   * <p><b>Not validated by this method:</b> the caller alone is responsible for {@code
   * sequenceNumber} actually being the lowest live one for {@code path}. This class has no cheap
   * way to verify that without itself scanning the manifests it exists to avoid opening
   * unnecessarily. Passing anything other than the true minimum silently reproduces the defect
   * this class repairs.
   *
   * @param path the duplicated data file's location
   * @param sequenceNumber the lowest live data sequence number registered for that path
   * @return this for method chaining
   */
  public DuplicateRegistrationRepair keepDataFile(String path, long sequenceNumber) {
    checkKeepArgs(path, sequenceNumber);
    keepDataFileSequenceNumberByPath.put(path, sequenceNumber);
    return this;
  }

  /**
   * Marks {@code sequenceNumber} as the live data sequence number to keep for a duplicated
   * delete file path. Every other live registration of that same path is dropped.
   *
   * <p><b>Not validated by this method:</b> the caller alone is responsible for {@code
   * sequenceNumber} actually being the lowest live one for {@code path}. This class has no cheap
   * way to verify that without itself scanning the manifests it exists to avoid opening
   * unnecessarily. Passing anything other than the true minimum silently reproduces the defect
   * this class repairs.
   *
   * @param path the duplicated delete file's location
   * @param sequenceNumber the lowest live data sequence number registered for that path
   * @return this for method chaining
   */
  public DuplicateRegistrationRepair keepDeleteFile(String path, long sequenceNumber) {
    checkKeepArgs(path, sequenceNumber);
    keepDeleteFileSequenceNumberByPath.put(path, sequenceNumber);
    return this;
  }

  private static void checkKeepArgs(String path, long sequenceNumber) {
    Preconditions.checkNotNull(path, "path cannot be null");
    Preconditions.checkArgument(
        sequenceNumber >= 0, "sequenceNumber must not be negative: %s", sequenceNumber);
  }

  @Override
  protected void validate(TableMetadata base, Snapshot parent) {
    dropDuplicateRegistrations(
        keepDataFileSequenceNumberByPath, keepDeleteFileSequenceNumberByPath);
  }
}
