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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.SinkUtil;
import org.apache.iceberg.io.BaseTaskWriter.InsertedRowTracker;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg writer implementation for the {@link SinkWriter} interface. Used by the
 * DynamicIcebergSink. Writes out the data to the final place, and emits {@link DynamicWriteResult}
 * for every unique {@link WriteTarget} at checkpoint time.
 */
class DynamicWriter implements CommittingSinkWriter<DynamicRecordInternal, DynamicWriteResult> {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicWriter.class);

  private final Map<WriteTarget, RowDataTaskWriterFactory> taskWriterFactories;
  private final Map<WriteTarget, TaskWriter<RowData>> writers;
  // Inserted-row trackers shared by the writers of one table (key: WriteTarget without schema ID)
  // within the current checkpoint. A schema evolution mid-checkpoint opens another writer for the
  // same table; without a shared tracker its re-writes of keys already written by the earlier
  // writer would produce equality deletes at the same sequence number as their targets, which
  // never apply. Writers whose key types cannot share a tracker go to separate groups.
  private final Map<WriteTarget, List<TrackerGroup>> insertedRowTrackers;
  private final Configuration flinkConfig;
  private final Map<String, String> commonWriteProperties;
  private final DynamicWriterMetrics metrics;
  private final int subTaskId;
  private final int attemptId;
  private final Catalog catalog;

  DynamicWriter(
      Catalog catalog,
      Map<String, String> commonWriteProperties,
      Configuration flinkConfig,
      int cacheMaximumSize,
      DynamicWriterMetrics metrics,
      int subTaskId,
      int attemptId) {
    this.catalog = catalog;
    this.commonWriteProperties = commonWriteProperties;
    this.flinkConfig = flinkConfig;
    this.metrics = metrics;
    this.subTaskId = subTaskId;
    this.attemptId = attemptId;
    this.taskWriterFactories = new LRUCache<>(cacheMaximumSize);
    this.writers = Maps.newHashMap();
    this.insertedRowTrackers = Maps.newHashMap();

    LOG.debug("DynamicIcebergSinkWriter created for subtask {} attemptId {}", subTaskId, attemptId);
  }

  @Override
  public void write(DynamicRecordInternal element, Context context)
      throws IOException, InterruptedException {
    WriteTarget writeTarget =
        new WriteTarget(
            element.tableName(),
            element.branch(),
            element.schema().schemaId(),
            element.spec().specId(),
            element.upsertMode(),
            element.equalityFields());

    TaskWriter<RowData> writer = writers.get(writeTarget);
    if (writer == null) {
      writer = createWriter(writeTarget, element);
      writers.put(writeTarget, writer);
    }

    writer.write(element.rowData());
    metrics.mainMetricsGroup().getNumRecordsSendCounter().inc();
  }

  private TaskWriter<RowData> createWriter(WriteTarget writerKey, DynamicRecordInternal element) {
    RowDataTaskWriterFactory taskWriterFactory =
        taskWriterFactories.computeIfAbsent(
            writerKey,
            factoryKey -> {
              Table table = catalog.loadTable(TableIdentifier.parse(factoryKey.tableName()));

              Set<Integer> equalityFieldIds = getEqualityFields(table, element.equalityFields());
              if (element.upsertMode()) {
                Preconditions.checkState(
                    !equalityFieldIds.isEmpty(),
                    "Equality field columns shouldn't be empty when configuring to use UPSERT data.");

                if (!table.spec().isUnpartitioned()) {
                  for (PartitionField partitionField : table.spec().fields()) {
                    Preconditions.checkState(
                        equalityFieldIds.contains(partitionField.sourceId()),
                        "In UPSERT mode, partition field '%s' should be included in equality fields: '%s'",
                        partitionField,
                        equalityFieldIds);
                  }
                }
              }

              FlinkWriteConf flinkWriteConf =
                  new FlinkWriteConf(table, commonWriteProperties, flinkConfig);
              Map<String, String> tableWriteProperties =
                  SinkUtil.writeProperties(flinkWriteConf.dataFileFormat(), flinkWriteConf, table);

              WriteTarget trackerScope = factoryKey.withoutSchemaId();
              LOG.debug("Creating new writer factory for table '{}'", table.name());
              return new RowDataTaskWriterFactory(
                  () -> table,
                  FlinkSchemaUtil.convert(element.schema()),
                  flinkWriteConf.targetDataFileSize(),
                  flinkWriteConf.dataFileFormat(),
                  tableWriteProperties,
                  Lists.newArrayList(equalityFieldIds),
                  element.upsertMode(),
                  element.schema(),
                  element.spec(),
                  (partition, keyType) ->
                      insertedRowTracker(factoryKey, trackerScope, partition, keyType));
            });

    taskWriterFactory.initialize(subTaskId, attemptId);
    return taskWriterFactory.create();
  }

  private InsertedRowTracker insertedRowTracker(
      WriteTarget writeTarget,
      WriteTarget trackerScope,
      StructLike partition,
      Types.StructType keyType) {
    List<TrackerGroup> groups =
        insertedRowTrackers.computeIfAbsent(trackerScope, scope -> Lists.newArrayList());
    return trackerGroup(groups, writeTarget, keyType).forPartition(writeTarget, partition);
  }

  // Returns the group of the writer for the target, joining or opening one on its first lookup
  private TrackerGroup trackerGroup(
      List<TrackerGroup> groups, WriteTarget writeTarget, Types.StructType keyType) {
    Integer schemaId = writeTarget.schemaId();
    for (TrackerGroup group : groups) {
      if (group.schemaIds.contains(schemaId)) {
        return group;
      }
    }

    for (TrackerGroup group : groups) {
      if (group.canShareWith(keyType)) {
        LOG.warn(
            "Opening another writer for table {} branch {} within one checkpoint: schema IDs {} "
                + "-> {}, spec ID {}, subtask {}, attempt {}. The writers share an inserted-row "
                + "tracker, so re-writes of a key across the schema change are retired with "
                + "position deletes",
            writeTarget.tableName(),
            writeTarget.branch(),
            group.schemaIds,
            schemaId,
            writeTarget.specId(),
            subTaskId,
            attemptId);
        group.schemaIds.add(schemaId);
        return group;
      }
    }

    if (!groups.isEmpty()) {
      LOG.warn(
          "Not sharing inserted-row tracker for table {} branch {}: key type {} of the new writer "
              + "for schema ID {} cannot share the trackers of the earlier writers {} (an equality "
              + "field's optionality or position changed, or its type changed in a way a shared "
              + "tracker cannot convert). Re-writes of a key across these schema versions within "
              + "one checkpoint will produce equality deletes that cannot apply to data written in "
              + "the same commit",
          writeTarget.tableName(),
          writeTarget.branch(),
          keyType,
          schemaId,
          groups);
    }

    TrackerGroup group = new TrackerGroup(keyType, schemaId);
    groups.add(group);
    return group;
  }

  /**
   * Inserted-row trackers of one table within one checkpoint, one per partition, shared by the
   * writers whose key types can share them. The trackers are keyed by the key type of the first
   * writer; writers with a promoted key type convert their keys.
   */
  private static class TrackerGroup {
    private final Types.StructType keyType;
    private final Set<Integer> schemaIds = Sets.newLinkedHashSet();
    private final Map<StructLike, InsertedRowTracker> byPartition = Maps.newHashMap();
    // The first partition seen with each partition's values widened to long and double
    private final Map<List<Object>, StructLike> partitionsByWidenedValues = Maps.newHashMap();
    private boolean warnedPartitionTypes = false;

    private TrackerGroup(Types.StructType keyType, Integer schemaId) {
      this.keyType = keyType;
      schemaIds.add(schemaId);
    }

    private boolean canShareWith(Types.StructType otherKeyType) {
      // a group gets its first tracker as soon as it is opened
      return byPartition.values().iterator().next().canShareWith(otherKeyType);
    }

    private InsertedRowTracker forPartition(WriteTarget writeTarget, StructLike partition) {
      InsertedRowTracker tracker = byPartition.get(partition);
      if (tracker == null) {
        // Promoting the source column of an identity or truncate partition field turns its
        // values from Integer to Long, or Float to Double, so one partition gets two keys. Files
        // of both cannot be committed together, so sharing a tracker across them would not help.
        StructLike earlier =
            partitionsByWidenedValues.putIfAbsent(widenedValues(partition), partition);
        if (earlier != null && !warnedPartitionTypes) {
          LOG.warn(
              "Not sharing inserted-row tracker for partition {} of table {} branch {} with "
                  + "partition {} of an earlier writer: the partition values differ in type "
                  + "because a partition source column was promoted. Re-writes of a key across "
                  + "these schema versions within one checkpoint will produce equality deletes "
                  + "that cannot apply to data written in the same commit",
              partition,
              writeTarget.tableName(),
              writeTarget.branch(),
              earlier);
          this.warnedPartitionTypes = true;
        }

        tracker = InsertedRowTracker.create(keyType);
        byPartition.put(partition, tracker);
      }

      return tracker;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("keyType", keyType)
          .add("schemaIds", schemaIds)
          .toString();
    }
  }

  // Returns the partition's values with Integer and Float values widened to Long and Double
  private static List<Object> widenedValues(StructLike partition) {
    if (partition == null) {
      return Collections.emptyList();
    }

    List<Object> values = Lists.newArrayListWithCapacity(partition.size());
    for (int pos = 0; pos < partition.size(); pos += 1) {
      Object value = partition.get(pos, Object.class);
      if (value instanceof Integer) {
        values.add(((Integer) value).longValue());
      } else if (value instanceof Float) {
        values.add(((Float) value).doubleValue());
      } else {
        values.add(value);
      }
    }

    return values;
  }

  @Override
  public void flush(boolean endOfInput) {
    // flush is used to handle flush/endOfInput, so no action is taken here.
  }

  @Override
  public void close() throws Exception {
    try {
      for (TaskWriter<RowData> writer : writers.values()) {
        writer.close();
      }
    } finally {
      insertedRowTrackers.clear();
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("subtaskId", subTaskId)
        .add("attemptId", attemptId)
        .add("writeProperties", commonWriteProperties)
        .toString();
  }

  @Override
  public Collection<DynamicWriteResult> prepareCommit() throws IOException {
    List<DynamicWriteResult> result = Lists.newArrayList();
    for (Map.Entry<WriteTarget, TaskWriter<RowData>> entry : writers.entrySet()) {
      long startNano = System.nanoTime();
      WriteResult writeResult = entry.getValue().complete();
      WriteTarget writeTarget = entry.getKey();
      metrics.updateFlushResult(writeTarget.tableName(), writeResult);
      metrics.flushDuration(
          writeTarget.tableName(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));
      LOG.debug(
          "Iceberg writer for table {} subtask {} attempt {} flushed {} data files and {} delete files",
          writeTarget.tableName(),
          subTaskId,
          attemptId,
          writeResult.dataFiles().length,
          writeResult.deleteFiles().length);

      result.add(
          new DynamicWriteResult(
              new TableKey(writeTarget.tableName(), writeTarget.branch()),
              writeTarget.specId(),
              writeResult));
    }

    writers.clear();
    insertedRowTrackers.clear();

    return result;
  }

  private static Set<Integer> getEqualityFields(Table table, Set<Integer> equalityFieldIds) {
    if (equalityFieldIds != null && !equalityFieldIds.isEmpty()) {
      return equalityFieldIds;
    }
    Set<Integer> identifierFieldIds = table.schema().identifierFieldIds();
    if (identifierFieldIds != null && !identifierFieldIds.isEmpty()) {
      return identifierFieldIds;
    }
    return Collections.emptySet();
  }

  @VisibleForTesting
  DynamicWriterMetrics getMetrics() {
    return metrics;
  }

  @VisibleForTesting
  Map<WriteTarget, RowDataTaskWriterFactory> getTaskWriterFactories() {
    return taskWriterFactories;
  }
}
