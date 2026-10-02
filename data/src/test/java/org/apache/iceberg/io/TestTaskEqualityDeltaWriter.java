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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.BaseTaskWriter.InsertedRowTracker;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestTaskEqualityDeltaWriter extends TestBase {
  private static final int FORMAT_V2 = 2;
  private static final long TARGET_FILE_SIZE = 128L;
  private final GenericRecord gRecord = GenericRecord.create(SCHEMA);
  private final GenericRecord posRecord = GenericRecord.create(DeleteSchemaUtil.pathPosSchema());

  private OutputFileFactory fileFactory = null;
  private int idFieldId;
  private int dataFieldId;

  @Parameter(index = 1)
  protected FileFormat format;

  @Parameters(name = "formatVersion = {0}, FileFormat = {1}")
  protected static List<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format :
        new FileFormat[] {FileFormat.AVRO, FileFormat.ORC, FileFormat.PARQUET}) {
      for (int version : TestHelpers.V2_AND_ABOVE) {
        parameters.add(new Object[] {version, format});
      }
    }

    return parameters;
  }

  @Override
  @BeforeEach
  public void setupTable() throws IOException {
    this.metadataDir = new File(tableDir, "metadata");

    this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(format).build();

    this.idFieldId = table.schema().findField("id").fieldId();
    this.dataFieldId = table.schema().findField("data").fieldId();

    table.updateProperties().defaultFormat(format).commit();
  }

  private Record createRecord(Integer id, String data) {
    return gRecord.copy("id", id, "data", data);
  }

  @TestTemplate
  public void testPureInsert() throws IOException {
    List<Integer> eqDeleteFieldIds = Lists.newArrayList(idFieldId, dataFieldId);
    Schema eqDeleteRowSchema = table.schema();

    GenericTaskDeltaWriter deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);
    List<Record> expected = Lists.newArrayList();
    for (int i = 0; i < 20; i++) {
      Record record = createRecord(i, String.format("val-%d", i));
      expected.add(record);

      deltaWriter.write(record);
    }

    WriteResult result = deltaWriter.complete();
    assertThat(result.dataFiles()).as("Should only have a data file.").hasSize(1);
    assertThat(result.deleteFiles()).as("Should have no delete file").hasSize(0);
    commitTransaction(result);
    assertThat(expectedRowSet(expected))
        .as("Should have expected records")
        .isEqualTo(actualRowSet("*"));

    deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);
    for (int i = 20; i < 30; i++) {
      Record record = createRecord(i, String.format("val-%d", i));
      expected.add(record);

      deltaWriter.write(record);
    }
    result = deltaWriter.complete();
    assertThat(result.dataFiles()).as("Should only have a data file.").hasSize(1);
    assertThat(result.deleteFiles()).as("Should have no delete file").hasSize(0);
    commitTransaction(result);
    assertThat(actualRowSet("*"))
        .as("Should have expected records")
        .isEqualTo(expectedRowSet(expected));
  }

  @TestTemplate
  public void testInsertDuplicatedKey() throws IOException {
    List<Integer> equalityFieldIds = Lists.newArrayList(idFieldId);
    Schema eqDeleteRowSchema = table.schema();

    GenericTaskDeltaWriter deltaWriter =
        createTaskWriter(equalityFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);
    deltaWriter.write(createRecord(1, "aaa"));
    deltaWriter.write(createRecord(2, "bbb"));
    deltaWriter.write(createRecord(3, "ccc"));
    deltaWriter.write(createRecord(4, "ddd"));
    deltaWriter.write(createRecord(4, "eee"));
    deltaWriter.write(createRecord(3, "fff"));
    deltaWriter.write(createRecord(2, "ggg"));
    deltaWriter.write(createRecord(1, "hhh"));

    WriteResult result = deltaWriter.complete();
    commitTransaction(result);

    assertThat(result.dataFiles()).as("Should have a data file.").hasSize(1);
    assertThat(result.deleteFiles()).as("Should have a pos-delete file").hasSize(1);
    DeleteFile posDeleteFile = result.deleteFiles()[0];
    assertThat(posDeleteFile.content())
        .as("Should be a pos-delete file")
        .isEqualTo(FileContent.POSITION_DELETES);
    assertThat(result.referencedDataFiles()).hasSize(1);
    assertThat(actualRowSet("*"))
        .as("Should have expected records")
        .isEqualTo(
            expectedRowSet(
                ImmutableList.of(
                    createRecord(4, "eee"),
                    createRecord(3, "fff"),
                    createRecord(2, "ggg"),
                    createRecord(1, "hhh"))));

    // Check records in the data file.
    DataFile dataFile = result.dataFiles()[0];
    assertThat(readRecordsAsList(table.schema(), dataFile.location()))
        .isEqualTo(
            ImmutableList.of(
                createRecord(1, "aaa"),
                createRecord(2, "bbb"),
                createRecord(3, "ccc"),
                createRecord(4, "ddd"),
                createRecord(4, "eee"),
                createRecord(3, "fff"),
                createRecord(2, "ggg"),
                createRecord(1, "hhh")));

    if (formatVersion == FORMAT_V2) {
      // Check records in the pos-delete file.
      Schema posDeleteSchema = DeleteSchemaUtil.pathPosSchema();
      assertThat(readRecordsAsList(posDeleteSchema, posDeleteFile.location()))
          .isEqualTo(
              ImmutableList.of(
                  posRecord.copy("file_path", dataFile.location(), "pos", 0L),
                  posRecord.copy("file_path", dataFile.location(), "pos", 1L),
                  posRecord.copy("file_path", dataFile.location(), "pos", 2L),
                  posRecord.copy("file_path", dataFile.location(), "pos", 3L)));
    } else {
      assertThat(posDeleteFile.format()).isEqualTo(FileFormat.PUFFIN);
      PositionDeleteIndex positionDeleteIndex = readDVFile(table, posDeleteFile);
      assertThat(positionDeleteIndex.cardinality()).isEqualTo(4);
      assertThat(positionDeleteIndex.isDeleted(0L)).isTrue();
      assertThat(positionDeleteIndex.isDeleted(1L)).isTrue();
      assertThat(positionDeleteIndex.isDeleted(2L)).isTrue();
      assertThat(positionDeleteIndex.isDeleted(3L)).isTrue();
    }
  }

  @TestTemplate
  public void testUpsertSameRow() throws IOException {
    List<Integer> eqDeleteFieldIds = Lists.newArrayList(idFieldId, dataFieldId);
    Schema eqDeleteRowSchema = table.schema();

    GenericTaskDeltaWriter deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);

    Record record = createRecord(1, "aaa");
    deltaWriter.write(record);

    // UPSERT <1, 'aaa'> to <1, 'aaa'>
    deltaWriter.delete(record);
    deltaWriter.write(record);

    WriteResult result = deltaWriter.complete();
    assertThat(result.dataFiles()).as("Should have a data file.").hasSize(1);
    assertThat(result.deleteFiles()).as("Should have a pos-delete file").hasSize(1);
    commitTransaction(result);
    assertThat(actualRowSet("*"))
        .as("Should have an expected record")
        .isEqualTo(expectedRowSet(ImmutableList.of(record)));

    // Check records in the data file.
    DataFile dataFile = result.dataFiles()[0];
    assertThat(readRecordsAsList(table.schema(), dataFile.location()))
        .isEqualTo(ImmutableList.of(record, record));

    DeleteFile posDeleteFile = result.deleteFiles()[0];
    if (formatVersion == FORMAT_V2) {
      // Check records in the pos-delete file.
      assertThat(readRecordsAsList(DeleteSchemaUtil.pathPosSchema(), posDeleteFile.location()))
          .isEqualTo(ImmutableList.of(posRecord.copy("file_path", dataFile.location(), "pos", 0L)));
    } else {
      assertThat(posDeleteFile.format()).isEqualTo(FileFormat.PUFFIN);
      PositionDeleteIndex positionDeleteIndex = readDVFile(table, posDeleteFile);
      assertThat(positionDeleteIndex.cardinality()).isEqualTo(1);
      assertThat(positionDeleteIndex.isDeleted(0L)).isTrue();
    }

    deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);
    deltaWriter.delete(record);
    result = deltaWriter.complete();
    assertThat(result.dataFiles()).as("Should have 0 data file.").hasSize(0);
    assertThat(result.deleteFiles()).as("Should have 1 eq-delete file").hasSize(1);
    commitTransaction(result);
    assertThat(actualRowSet("*"))
        .as("Should have no record")
        .isEqualTo(expectedRowSet(ImmutableList.of()));
  }

  @TestTemplate
  public void testUpsertData() throws IOException {
    List<Integer> eqDeleteFieldIds = Lists.newArrayList(dataFieldId);
    Schema eqDeleteRowSchema = table.schema().select("data");

    GenericTaskDeltaWriter deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);
    deltaWriter.write(createRecord(1, "aaa"));
    deltaWriter.write(createRecord(2, "bbb"));
    deltaWriter.write(createRecord(3, "aaa"));
    deltaWriter.write(createRecord(3, "ccc"));
    deltaWriter.write(createRecord(4, "ccc"));

    // Commit the 1th transaction.
    WriteResult result = deltaWriter.complete();
    assertThat(result.dataFiles()).as("Should have a data file").hasSize(1);
    assertThat(result.deleteFiles())
        .as("Should have a pos-delete file for deduplication purpose")
        .hasSize(1);
    assertThat(result.deleteFiles()[0].content())
        .as("Should be pos-delete file")
        .isEqualTo(FileContent.POSITION_DELETES);
    assertThat(result.referencedDataFiles()).hasSize(1);
    commitTransaction(result);

    assertThat(actualRowSet("*"))
        .as("Should have expected records")
        .isEqualTo(
            expectedRowSet(
                ImmutableList.of(
                    createRecord(2, "bbb"), createRecord(3, "aaa"), createRecord(4, "ccc"))));

    // Start the 2nd transaction.
    deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);
    GenericRecord keyRecord = GenericRecord.create(eqDeleteRowSchema);
    Function<String, Record> keyFunc = data -> keyRecord.copy("data", data);

    // UPSERT <3,'aaa'> to <5,'aaa'> - (by delete the key)
    deltaWriter.deleteKey(keyFunc.apply("aaa"));
    deltaWriter.write(createRecord(5, "aaa"));

    // UPSERT <5,'aaa'> to <6,'aaa'> - (by delete the key)
    deltaWriter.deleteKey(keyFunc.apply("aaa"));
    deltaWriter.write(createRecord(6, "aaa"));

    // UPSERT <4,'ccc'> to <7,'ccc'> - (by delete the key)
    deltaWriter.deleteKey(keyFunc.apply("ccc"));
    deltaWriter.write(createRecord(7, "ccc"));

    // DELETE <2, 'bbb'> - (by delete the key)
    deltaWriter.deleteKey(keyFunc.apply("bbb"));

    // Commit the 2nd transaction.
    result = deltaWriter.complete();
    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(2);
    commitTransaction(result);

    assertThat(actualRowSet("*"))
        .as("Should have expected records")
        .isEqualTo(
            expectedRowSet(ImmutableList.of(createRecord(6, "aaa"), createRecord(7, "ccc"))));

    // Check records in the data file.
    DataFile dataFile = result.dataFiles()[0];
    assertThat(readRecordsAsList(table.schema(), dataFile.location()))
        .isEqualTo(
            ImmutableList.of(
                createRecord(5, "aaa"), createRecord(6, "aaa"), createRecord(7, "ccc")));

    // Check records in the eq-delete file.
    DeleteFile eqDeleteFile = result.deleteFiles()[0];
    assertThat(eqDeleteFile.content()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(readRecordsAsList(eqDeleteRowSchema, eqDeleteFile.location()))
        .isEqualTo(
            ImmutableList.of(keyFunc.apply("aaa"), keyFunc.apply("ccc"), keyFunc.apply("bbb")));

    // Check records in the pos-delete file.
    DeleteFile posDeleteFile = result.deleteFiles()[1];
    Schema posDeleteSchema = DeleteSchemaUtil.pathPosSchema();
    assertThat(posDeleteFile.content()).isEqualTo(FileContent.POSITION_DELETES);

    if (formatVersion == FORMAT_V2) {
      assertThat(readRecordsAsList(posDeleteSchema, posDeleteFile.location()))
          .isEqualTo(ImmutableList.of(posRecord.copy("file_path", dataFile.location(), "pos", 0L)));
    } else {
      assertThat(posDeleteFile.format()).isEqualTo(FileFormat.PUFFIN);
      PositionDeleteIndex positionDeleteIndex = readDVFile(table, posDeleteFile);
      assertThat(positionDeleteIndex.cardinality()).isEqualTo(1);
      assertThat(positionDeleteIndex.isDeleted(0L)).isTrue();
    }
  }

  @TestTemplate
  public void testUpsertDataWithFullRowSchema() throws IOException {
    List<Integer> eqDeleteFieldIds = Lists.newArrayList(dataFieldId);
    Schema eqDeleteRowSchema = table.schema();

    GenericTaskDeltaWriter deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);
    deltaWriter.write(createRecord(1, "aaa"));
    deltaWriter.write(createRecord(2, "bbb"));
    deltaWriter.write(createRecord(3, "aaa"));
    deltaWriter.write(createRecord(3, "ccc"));
    deltaWriter.write(createRecord(4, "ccc"));

    // Commit the 1th transaction.
    WriteResult result = deltaWriter.complete();
    assertThat(result.dataFiles()).as("Should have a data file").hasSize(1);
    assertThat(result.deleteFiles())
        .as("Should have a pos-delete file for deduplication purpose")
        .hasSize(1);
    assertThat(result.deleteFiles()[0].content())
        .as("Should be pos-delete file")
        .isEqualTo(FileContent.POSITION_DELETES);
    assertThat(result.referencedDataFiles()).hasSize(1);
    commitTransaction(result);

    assertThat(actualRowSet("*"))
        .as("Should have expected records")
        .isEqualTo(
            expectedRowSet(
                ImmutableList.of(
                    createRecord(2, "bbb"), createRecord(3, "aaa"), createRecord(4, "ccc"))));

    // Start the 2nd transaction.
    deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);

    // UPSERT <3,'aaa'> to <5,'aaa'> - (by delete the entire row)
    deltaWriter.delete(createRecord(3, "aaa"));
    deltaWriter.write(createRecord(5, "aaa"));

    // UPSERT <5,'aaa'> to <6,'aaa'> - (by delete the entire row)
    deltaWriter.delete(createRecord(5, "aaa"));
    deltaWriter.write(createRecord(6, "aaa"));

    // UPSERT <4,'ccc'> to <7,'ccc'> - (by delete the entire row)
    deltaWriter.delete(createRecord(4, "ccc"));
    deltaWriter.write(createRecord(7, "ccc"));

    // DELETE <2, 'bbb'> - (by delete the entire row)
    deltaWriter.delete(createRecord(2, "bbb"));

    // Commit the 2nd transaction.
    result = deltaWriter.complete();
    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(2);
    assertThat(result.referencedDataFiles()).hasSize(1);
    commitTransaction(result);

    assertThat(actualRowSet("*"))
        .as("Should have expected records")
        .isEqualTo(
            expectedRowSet(ImmutableList.of(createRecord(6, "aaa"), createRecord(7, "ccc"))));

    // Check records in the data file.
    DataFile dataFile = result.dataFiles()[0];
    assertThat(readRecordsAsList(table.schema(), dataFile.location()))
        .isEqualTo(
            ImmutableList.of(
                createRecord(5, "aaa"), createRecord(6, "aaa"), createRecord(7, "ccc")));

    // Check records in the eq-delete file.
    DeleteFile eqDeleteFile = result.deleteFiles()[0];
    assertThat(eqDeleteFile.content()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(readRecordsAsList(eqDeleteRowSchema, eqDeleteFile.location()))
        .isEqualTo(
            ImmutableList.of(
                createRecord(3, "aaa"), createRecord(4, "ccc"), createRecord(2, "bbb")));

    // Check records in the pos-delete file.
    DeleteFile posDeleteFile = result.deleteFiles()[1];
    Schema posDeleteSchema = DeleteSchemaUtil.pathPosSchema();
    assertThat(posDeleteFile.content()).isEqualTo(FileContent.POSITION_DELETES);
    if (formatVersion == FORMAT_V2) {
      assertThat(readRecordsAsList(posDeleteSchema, posDeleteFile.location()))
          .isEqualTo(ImmutableList.of(posRecord.copy("file_path", dataFile.location(), "pos", 0L)));
    } else {
      assertThat(posDeleteFile.format()).isEqualTo(FileFormat.PUFFIN);
      PositionDeleteIndex positionDeleteIndex = readDVFile(table, posDeleteFile);
      assertThat(positionDeleteIndex.cardinality()).isEqualTo(1);
      assertThat(positionDeleteIndex.isDeleted(0L)).isTrue();
    }
  }

  @TestTemplate
  public void testDeleteFileGranularity() throws IOException {
    withGranularity(DeleteGranularity.FILE);
  }

  @TestTemplate
  public void testDeletePartitionGranularity() throws IOException {
    withGranularity(DeleteGranularity.PARTITION);
  }

  private void withGranularity(DeleteGranularity granularity) throws IOException {
    List<Integer> eqDeleteFieldIds = Lists.newArrayList(idFieldId, dataFieldId);
    Schema eqDeleteRowSchema = table.schema();

    GenericTaskDeltaWriter deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, granularity);

    Map<Integer, Record> expected = Maps.newHashMapWithExpectedSize(2000);
    int expectedDeleteCount = 0;
    // Create enough records, so we have multiple files
    for (int i = 0; i < 2000; ++i) {
      Record record = createRecord(i, "aaa" + i);
      deltaWriter.write(record);
      if (i % 5 == 0) {
        deltaWriter.delete(record);
        ++expectedDeleteCount;
      } else {
        expected.put(i, record);
      }
    }

    // Add some deletes in the end
    for (int i = 0; i < 199; ++i) {
      int id = i * 10 + 1;
      Record record = createRecord(id, "aaa" + id);
      deltaWriter.delete(record);
      ++expectedDeleteCount;
      expected.remove(id);
    }

    WriteResult result = deltaWriter.complete();

    // Should have 2 files, as BaseRollingWriter checks the size on every 1000 rows (ROWS_DIVISOR)
    assertThat(result.dataFiles()).as("Should have 2 data files.").hasSize(2);
    if (formatVersion == FORMAT_V2) {
      assertThat(result.deleteFiles())
          .as("Should have correct number of pos-delete files")
          .hasSize(granularity.equals(DeleteGranularity.FILE) ? 2 : 1);
    } else {
      assertThat(result.deleteFiles())
          .as("Should have correct number of pos-delete files")
          .hasSize(2);
    }

    assertThat(Arrays.stream(result.deleteFiles()).mapToLong(delete -> delete.recordCount()).sum())
        .isEqualTo(expectedDeleteCount);

    commitTransaction(result);
    assertThat(actualRowSet("*"))
        .as("Should have expected record")
        .isEqualTo(expectedRowSet(expected.values()));
  }

  private void commitTransaction(WriteResult result) {
    RowDelta rowDelta = table.newRowDelta();
    Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
    Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);

    rowDelta
        .validateDeletedFiles()
        .validateDataFilesExist(Lists.newArrayList(result.referencedDataFiles()))
        .commit();
  }

  private StructLikeSet expectedRowSet(Iterable<Record> records) {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    records.forEach(set::add);
    return set;
  }

  private StructLikeSet actualRowSet(String... columns) throws IOException {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).select(columns).build()) {
      reader.forEach(set::add);
    }
    return set;
  }

  @TestTemplate
  public void testSharedInsertedRowTrackerAcrossWriters() throws IOException {
    List<Integer> equalityFieldIds = Lists.newArrayList(idFieldId);
    Schema deleteSchema = table.schema().select("id");
    InsertedRowTracker sharedInsertedRows = InsertedRowTracker.create(deleteSchema.asStruct());
    GenericRecord keyRecord = GenericRecord.create(deleteSchema);

    // Two writers of the same table within one commit, as a sink opens when the table schema
    // evolves mid-commit. Every file they produce lands at the same sequence number, so only
    // position deletes can retire rows they wrote for each other.
    GenericTaskDeltaWriter first =
        createTaskWriter(
            equalityFieldIds, deleteSchema, DeleteGranularity.FILE, sharedInsertedRows);
    GenericTaskDeltaWriter second =
        createTaskWriter(
            equalityFieldIds, deleteSchema, DeleteGranularity.FILE, sharedInsertedRows);

    first.write(createRecord(1, "aaa"));
    first.write(createRecord(2, "bbb"));

    // upsert of key 1 through the second writer retires the row the first writer wrote
    second.deleteKey(keyRecord.copy("id", 1));
    second.write(createRecord(1, "ccc"));

    // upsert of key 1 back through the first writer retires the second writer's row
    first.deleteKey(keyRecord.copy("id", 1));
    first.write(createRecord(1, "ddd"));

    WriteResult firstResult = first.complete();
    WriteResult secondResult = second.complete();

    assertThat(firstResult.dataFiles()).hasSize(1);
    assertThat(secondResult.dataFiles()).hasSize(1);
    DataFile firstDataFile = firstResult.dataFiles()[0];
    DataFile secondDataFile = secondResult.dataFiles()[0];

    assertThat(secondResult.deleteFiles())
        .as("Second writer should retire the first writer's row with a position delete")
        .hasSize(1)
        .allMatch(file -> file.content() == FileContent.POSITION_DELETES);
    assertThat(secondResult.referencedDataFiles()).containsExactly(firstDataFile.location());

    assertThat(firstResult.deleteFiles())
        .as("First writer should retire the second writer's row with a position delete")
        .hasSize(1)
        .allMatch(file -> file.content() == FileContent.POSITION_DELETES);
    assertThat(firstResult.referencedDataFiles()).containsExactly(secondDataFile.location());

    // closing a writer again is a no-op
    first.close();

    // closing a writer must not clear the shared tracker: a third writer still finds key 2
    GenericTaskDeltaWriter third =
        createTaskWriter(
            equalityFieldIds, deleteSchema, DeleteGranularity.FILE, sharedInsertedRows);
    third.deleteKey(keyRecord.copy("id", 2));
    WriteResult thirdResult = third.complete();
    assertThat(thirdResult.dataFiles()).isEmpty();
    assertThat(thirdResult.deleteFiles())
        .hasSize(1)
        .allMatch(file -> file.content() == FileContent.POSITION_DELETES);
    assertThat(thirdResult.referencedDataFiles()).containsExactly(firstDataFile.location());

    RowDelta rowDelta = table.newRowDelta();
    for (WriteResult result : ImmutableList.of(firstResult, secondResult, thirdResult)) {
      Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
      Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
    }

    rowDelta.commit();

    assertThat(actualRowSet("*"))
        .as("Only the last write of key 1 should survive; key 2 should be deleted")
        .isEqualTo(expectedRowSet(ImmutableList.of(createRecord(1, "ddd"))));
  }

  @TestTemplate
  public void testSharedInsertedRowTrackerAcceptsRenamedKeyField() throws IOException {
    List<Integer> equalityFieldIds = Lists.newArrayList(idFieldId);
    Schema deleteSchema = table.schema().select("id");
    Types.StructType renamedKeyType =
        Types.StructType.of(
            Types.NestedField.from(table.schema().findField(idFieldId))
                .withName("renamed_id")
                .withDoc("doc")
                .build());
    InsertedRowTracker sharedInsertedRows = InsertedRowTracker.create(renamedKeyType);

    assertThat(sharedInsertedRows.acceptsKeyType(deleteSchema.asStruct())).isTrue();

    GenericTaskDeltaWriter writer =
        createTaskWriter(
            equalityFieldIds, deleteSchema, DeleteGranularity.FILE, sharedInsertedRows);
    writer.write(createRecord(1, "aaa"));
    writer.write(createRecord(1, "bbb"));
    WriteResult result = writer.complete();

    assertThat(result.deleteFiles())
        .hasSize(1)
        .allMatch(file -> file.content() == FileContent.POSITION_DELETES);
  }

  @TestTemplate
  public void testSharedInsertedRowTrackerRejectsMismatchedKeyType() {
    List<Integer> equalityFieldIds = Lists.newArrayList(idFieldId);
    Schema deleteSchema = table.schema().select("id");
    InsertedRowTracker dataKeyedTracker =
        InsertedRowTracker.create(table.schema().select("data").asStruct());

    assertThatThrownBy(
            () ->
                createTaskWriter(
                    equalityFieldIds, deleteSchema, DeleteGranularity.FILE, dataKeyedTracker))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Shared inserted-row tracker key type")
        .hasMessageContaining("cannot be shared with equality-delete schema");
  }

  @TestTemplate
  public void testSharedInsertedRowTrackerRetiresRowDeletedThroughOtherWriter() throws IOException {
    List<Integer> equalityFieldIds = Lists.newArrayList(idFieldId);
    Schema deleteSchema = table.schema().select("id");
    InsertedRowTracker sharedInsertedRows = InsertedRowTracker.create(deleteSchema.asStruct());

    GenericTaskDeltaWriter first =
        createTaskWriter(
            equalityFieldIds, deleteSchema, DeleteGranularity.FILE, sharedInsertedRows);
    GenericTaskDeltaWriter second =
        createTaskWriter(
            equalityFieldIds, deleteSchema, DeleteGranularity.FILE, sharedInsertedRows);

    first.write(createRecord(1, "aaa"));
    first.write(createRecord(2, "bbb"));

    // a row delete (CDC UPDATE_BEFORE or DELETE) through the second writer retires the row the
    // first writer wrote with a position delete, instead of an equality delete
    second.delete(createRecord(1, "aaa"));
    second.write(createRecord(1, "ccc"));

    WriteResult firstResult = first.complete();
    WriteResult secondResult = second.complete();

    assertThat(firstResult.deleteFiles()).isEmpty();
    assertThat(secondResult.deleteFiles())
        .hasSize(1)
        .allMatch(file -> file.content() == FileContent.POSITION_DELETES);
    assertThat(secondResult.referencedDataFiles())
        .containsExactly(firstResult.dataFiles()[0].location());

    RowDelta rowDelta = table.newRowDelta();
    for (WriteResult result : ImmutableList.of(firstResult, secondResult)) {
      Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
      Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
    }

    rowDelta.commit();

    assertThat(actualRowSet("*"))
        .isEqualTo(
            expectedRowSet(ImmutableList.of(createRecord(1, "ccc"), createRecord(2, "bbb"))));
  }

  @TestTemplate
  public void testInsertedRowTrackerKeyTypeCompatibility() {
    Types.NestedField id = table.schema().findField(idFieldId);
    Types.StructType keyType = Types.StructType.of(id);
    InsertedRowTracker tracker = InsertedRowTracker.create(keyType);

    assertThat(tracker.keyType()).isEqualTo(keyType);
    assertThat(tracker.acceptsKeyType(keyType)).isTrue();
    assertThat(
            tracker.acceptsKeyType(
                Types.StructType.of(
                    Types.NestedField.from(id)
                        .withName("renamed")
                        .withDoc("doc")
                        .withWriteDefault(7)
                        .build())))
        .as("Names, docs and defaults do not affect how keys compare")
        .isTrue();
    assertThat(
            tracker.acceptsKeyType(
                Types.StructType.of(Types.NestedField.from(id).withId(id.fieldId() + 100).build())))
        .as("A different field ID describes a different equality field")
        .isFalse();
    assertThat(
            tracker.acceptsKeyType(
                Types.StructType.of(
                    Types.NestedField.from(id).isOptional(!id.isOptional()).build())))
        .as("Optionality must match")
        .isFalse();
    assertThat(
            tracker.acceptsKeyType(
                Types.StructType.of(
                    Types.NestedField.from(id).ofType(Types.LongType.get()).build())))
        .as("A promoted key type compares differently")
        .isFalse();
    assertThat(tracker.acceptsKeyType(Types.StructType.of(id, table.schema().findField("data"))))
        .as("The number of key fields must match")
        .isFalse();

    assertThatThrownBy(() -> InsertedRowTracker.create(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Inserted-row tracker key type cannot be null");
  }

  @TestTemplate
  void testSharedInsertedRowTrackerAcrossIntToLongKeyPromotion() throws Exception {
    List<Integer> equalityFieldIds = Lists.newArrayList(idFieldId);
    Schema narrowSchema = table.schema();
    Schema narrowKey = narrowSchema.select("id");
    InsertedRowTracker sharedInsertedRows = InsertedRowTracker.create(narrowKey.asStruct());
    GenericTaskDeltaWriter narrow =
        createTaskWriter(equalityFieldIds, narrowKey, DeleteGranularity.FILE, sharedInsertedRows);
    assertThat(insertedRows(narrow)).isSameAs(sharedInsertedRows);

    narrow.write(record(narrowSchema, 1, "aaa"));
    narrow.write(record(narrowSchema, 2, "bbb"));

    table.updateSchema().updateColumn("id", Types.LongType.get()).commit();
    Schema wideSchema = table.schema();
    Schema wideKey = wideSchema.select("id");
    GenericTaskDeltaWriter wide =
        createTaskWriter(equalityFieldIds, wideKey, DeleteGranularity.FILE, sharedInsertedRows);
    assertThat(insertedRows(wide))
        .as("A writer with a promoted key type converts its keys through a view")
        .isNotSameAs(sharedInsertedRows);

    // the long-keyed writer retires both rows of the int-keyed writer, by write and by deleteKey
    wide.write(record(wideSchema, 1L, "ccc"));
    wide.deleteKey(record(wideKey, 2L));
    // and the int-keyed writer finds the row the long-keyed writer wrote
    narrow.write(record(narrowSchema, 1, "ddd"));

    WriteResult narrowResult = narrow.complete();
    WriteResult wideResult = wide.complete();
    assertThat(wideResult.deleteFiles())
        .hasSize(1)
        .allMatch(file -> file.content() == FileContent.POSITION_DELETES);
    assertThat(wideResult.referencedDataFiles())
        .containsExactly(narrowResult.dataFiles()[0].location());
    assertThat(narrowResult.deleteFiles())
        .hasSize(1)
        .allMatch(file -> file.content() == FileContent.POSITION_DELETES);
    assertThat(narrowResult.referencedDataFiles())
        .containsExactly(wideResult.dataFiles()[0].location());

    commit(narrowResult, wideResult);
    assertThat(actualRowSet("*"))
        .isEqualTo(expectedRowSet(ImmutableList.of(record(table.schema(), 1L, "ddd"))));
  }

  @TestTemplate
  void testSharedInsertedRowTrackerKeyedByLongAcrossIntKeyWriter() throws Exception {
    List<Integer> equalityFieldIds = Lists.newArrayList(idFieldId);
    Schema narrowSchema = table.schema();
    Types.StructType wideKeyType =
        Types.StructType.of(Types.NestedField.required(idFieldId, "id", Types.LongType.get()));
    InsertedRowTracker sharedInsertedRows = InsertedRowTracker.create(wideKeyType);
    GenericTaskDeltaWriter narrow =
        createTaskWriter(
            equalityFieldIds,
            narrowSchema.select("id"),
            DeleteGranularity.FILE,
            sharedInsertedRows);
    assertThat(insertedRows(narrow)).isNotSameAs(sharedInsertedRows);

    table.updateSchema().updateColumn("id", Types.LongType.get()).commit();
    Schema wideSchema = table.schema();
    Schema wideKey = wideSchema.select("id");
    GenericTaskDeltaWriter wide =
        createTaskWriter(equalityFieldIds, wideKey, DeleteGranularity.FILE, sharedInsertedRows);
    assertThat(insertedRows(wide)).isSameAs(sharedInsertedRows);

    // the tracker is keyed by long, so here the int-keyed writer converts its keys up
    wide.write(record(wideSchema, 1L, "wide"));
    narrow.write(record(narrowSchema, 1, "narrow"));
    narrow.write(record(narrowSchema, 2, "other"));
    wide.deleteKey(record(wideKey, 2L));

    WriteResult wideResult = wide.complete();
    WriteResult narrowResult = narrow.complete();
    assertThat(narrowResult.referencedDataFiles())
        .containsExactly(wideResult.dataFiles()[0].location());
    assertThat(wideResult.referencedDataFiles())
        .containsExactly(narrowResult.dataFiles()[0].location());
    assertThat(
            Iterables.concat(
                Arrays.asList(wideResult.deleteFiles()), Arrays.asList(narrowResult.deleteFiles())))
        .hasSize(2)
        .allMatch(file -> file.content() == FileContent.POSITION_DELETES);

    commit(narrowResult, wideResult);
    assertThat(actualRowSet("*"))
        .isEqualTo(expectedRowSet(ImmutableList.of(record(table.schema(), 1L, "narrow"))));
  }

  @TestTemplate
  void testSharedInsertedRowTrackerKeyedByDoubleAcrossFloatKeyWriter() throws IOException {
    table.updateSchema().addColumn("f", Types.FloatType.get()).commit();
    int floatFieldId = table.schema().findField("f").fieldId();
    List<Integer> equalityFieldIds = Lists.newArrayList(floatFieldId);
    Schema narrowSchema = table.schema();
    InsertedRowTracker sharedInsertedRows =
        InsertedRowTracker.create(
            Types.StructType.of(
                Types.NestedField.optional(floatFieldId, "f", Types.DoubleType.get())));
    GenericTaskDeltaWriter narrow =
        createTaskWriter(
            equalityFieldIds, narrowSchema.select("f"), DeleteGranularity.FILE, sharedInsertedRows);

    table.updateSchema().updateColumn("f", Types.DoubleType.get()).commit();
    Schema wideSchema = table.schema();
    GenericTaskDeltaWriter wide =
        createTaskWriter(
            equalityFieldIds, wideSchema.select("f"), DeleteGranularity.FILE, sharedInsertedRows);

    // the tracker is keyed by double, so the float-keyed writer converts its keys up
    wide.write(record(wideSchema, 1, "wide", 1.5d));
    narrow.write(record(narrowSchema, 2, "narrow", 1.5f));

    WriteResult wideResult = wide.complete();
    WriteResult narrowResult = narrow.complete();
    assertThat(narrowResult.deleteFiles())
        .hasSize(1)
        .allMatch(file -> file.content() == FileContent.POSITION_DELETES);
    assertThat(narrowResult.referencedDataFiles())
        .containsExactly(wideResult.dataFiles()[0].location());

    commit(wideResult, narrowResult);
    assertThat(actualRowSet("*"))
        .isEqualTo(expectedRowSet(ImmutableList.of(record(table.schema(), 2, "narrow", 1.5d))));
  }

  @TestTemplate
  void testSharedInsertedRowTrackerKeepsKeysOutsideIntRangeApart() throws IOException {
    List<Integer> equalityFieldIds = Lists.newArrayList(idFieldId, dataFieldId);
    Schema narrowSchema = table.schema();
    Schema narrowKey = narrowSchema.select("id", "data");
    InsertedRowTracker sharedInsertedRows = InsertedRowTracker.create(narrowKey.asStruct());
    GenericTaskDeltaWriter narrow =
        createTaskWriter(equalityFieldIds, narrowKey, DeleteGranularity.FILE, sharedInsertedRows);

    table.updateSchema().updateColumn("id", Types.LongType.get()).commit();
    Schema wideSchema = table.schema();
    Schema wideKey = wideSchema.select("id", "data");
    GenericTaskDeltaWriter first =
        createTaskWriter(equalityFieldIds, wideKey, DeleteGranularity.FILE, sharedInsertedRows);
    GenericTaskDeltaWriter second =
        createTaskWriter(equalityFieldIds, wideKey, DeleteGranularity.FILE, sharedInsertedRows);
    long wideId = Integer.MAX_VALUE + 1L;

    // no long-keyed writer has written a key outside the int range yet
    first.deleteKey(record(wideKey, wideId + 1, "aaa"));
    first.write(record(wideSchema, wideId, "aaa"));
    // the same key through another long-keyed writer is retired with a position delete
    second.write(record(wideSchema, wideId, "aaa"));
    // keys outside the int range that no writer has written are not found
    second.deleteKey(record(wideKey, wideId + 1, "aaa"));
    // the int that the long narrows to is a different key
    narrow.deleteKey(record(narrowKey, (int) wideId, "aaa"));

    WriteResult firstResult = first.complete();
    WriteResult secondResult = second.complete();
    WriteResult narrowResult = narrow.complete();
    assertThat(firstResult.deleteFiles())
        .hasSize(1)
        .allMatch(file -> file.content() == FileContent.EQUALITY_DELETES);
    assertThat(secondResult.deleteFiles())
        .hasSize(2)
        .extracting(DeleteFile::content)
        .containsExactlyInAnyOrder(FileContent.POSITION_DELETES, FileContent.EQUALITY_DELETES);
    assertThat(secondResult.referencedDataFiles())
        .containsExactly(firstResult.dataFiles()[0].location());
    assertThat(narrowResult.deleteFiles())
        .hasSize(1)
        .allMatch(file -> file.content() == FileContent.EQUALITY_DELETES);

    commit(firstResult, secondResult);
    assertThat(actualRowSet("*"))
        .isEqualTo(expectedRowSet(ImmutableList.of(record(table.schema(), wideId, "aaa"))));
  }

  @TestTemplate
  void testSharedInsertedRowTrackerAcrossFloatToDoubleKeyPromotion() throws IOException {
    table.updateSchema().addColumn("f", Types.FloatType.get()).commit();
    List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().findField("f").fieldId());
    Schema narrowSchema = table.schema();
    Schema narrowKey = narrowSchema.select("f");
    InsertedRowTracker sharedInsertedRows = InsertedRowTracker.create(narrowKey.asStruct());
    GenericTaskDeltaWriter narrow =
        createTaskWriter(equalityFieldIds, narrowKey, DeleteGranularity.FILE, sharedInsertedRows);
    narrow.write(record(narrowSchema, 1, "exact", 1.5f));
    narrow.write(record(narrowSchema, 2, "nan", Float.NaN));
    narrow.write(record(narrowSchema, 3, "negative zero", -0.0f));
    narrow.write(record(narrowSchema, 4, "float tenth", 0.1f));

    table.updateSchema().updateColumn("f", Types.DoubleType.get()).commit();
    Schema wideSchema = table.schema();
    Schema wideKey = wideSchema.select("f");
    GenericTaskDeltaWriter wide =
        createTaskWriter(equalityFieldIds, wideKey, DeleteGranularity.FILE, sharedInsertedRows);
    GenericTaskDeltaWriter other =
        createTaskWriter(equalityFieldIds, wideKey, DeleteGranularity.FILE, sharedInsertedRows);

    // doubles with an exact float value, NaN included, find the float-keyed writer's rows
    wide.write(record(wideSchema, 5, "exact", 1.5d));
    wide.write(record(wideSchema, 6, "nan", Double.NaN));
    // positive and negative zero are different keys, as in equality deletes
    wide.write(record(wideSchema, 7, "positive zero", 0.0d));
    // 0.1 has no exact float value, so it is not the float closest to it
    wide.write(record(wideSchema, 8, "double tenth", 0.1d));
    other.write(record(wideSchema, 9, "double tenth", 0.1d));

    WriteResult narrowResult = narrow.complete();
    WriteResult wideResult = wide.complete();
    WriteResult otherResult = other.complete();
    assertThat(narrowResult.deleteFiles()).isEmpty();
    assertThat(wideResult.deleteFiles())
        .hasSize(1)
        .allMatch(file -> file.content() == FileContent.POSITION_DELETES)
        .allMatch(file -> file.recordCount() == 2);
    assertThat(wideResult.referencedDataFiles())
        .containsExactly(narrowResult.dataFiles()[0].location());
    assertThat(otherResult.referencedDataFiles())
        .containsExactly(wideResult.dataFiles()[0].location());

    commit(narrowResult, wideResult, otherResult);
    Schema schema = table.schema();
    assertThat(actualRowSet("*"))
        .isEqualTo(
            expectedRowSet(
                ImmutableList.of(
                    record(schema, 3, "negative zero", -0.0d),
                    record(schema, 4, "float tenth", (double) 0.1f),
                    record(schema, 5, "exact", 1.5d),
                    record(schema, 6, "nan", Double.NaN),
                    record(schema, 7, "positive zero", 0.0d),
                    record(schema, 9, "double tenth", 0.1d))));
  }

  @TestTemplate
  void testSharedInsertedRowTrackerAcrossDecimalPrecisionWidening() throws Exception {
    table.updateSchema().addColumn("amount", Types.DecimalType.of(10, 2)).commit();
    List<Integer> equalityFieldIds =
        Lists.newArrayList(table.schema().findField("amount").fieldId());
    Schema narrowSchema = table.schema();
    Schema narrowKey = narrowSchema.select("amount");
    InsertedRowTracker sharedInsertedRows = InsertedRowTracker.create(narrowKey.asStruct());
    GenericTaskDeltaWriter narrow =
        createTaskWriter(equalityFieldIds, narrowKey, DeleteGranularity.FILE, sharedInsertedRows);
    narrow.write(record(narrowSchema, 1, "aaa", new BigDecimal("12.34")));

    table.updateSchema().updateColumn("amount", Types.DecimalType.of(12, 2)).commit();
    Schema wideSchema = table.schema();
    Schema wideKey = wideSchema.select("amount");
    assertThat(sharedInsertedRows.acceptsKeyType(wideKey.asStruct())).isFalse();
    assertThat(sharedInsertedRows.canShareWith(wideKey.asStruct())).isTrue();
    GenericTaskDeltaWriter wide =
        createTaskWriter(equalityFieldIds, wideKey, DeleteGranularity.FILE, sharedInsertedRows);
    assertThat(insertedRows(wide))
        .as("Decimals of the same scale need no conversion")
        .isSameAs(sharedInsertedRows);
    wide.write(record(wideSchema, 2, "bbb", new BigDecimal("12.34")));

    WriteResult narrowResult = narrow.complete();
    WriteResult wideResult = wide.complete();
    assertThat(wideResult.deleteFiles())
        .hasSize(1)
        .allMatch(file -> file.content() == FileContent.POSITION_DELETES);

    commit(narrowResult, wideResult);
    assertThat(actualRowSet("*"))
        .isEqualTo(
            expectedRowSet(
                ImmutableList.of(record(table.schema(), 2, "bbb", new BigDecimal("12.34")))));
  }

  @TestTemplate
  void testSharedInsertedRowTrackerAcrossPromotionsOfTwoKeyFields() throws IOException {
    table
        .updateSchema()
        .addColumn("k", Types.IntegerType.get())
        .addColumn("f", Types.FloatType.get())
        .commit();
    List<Integer> equalityFieldIds =
        Lists.newArrayList(
            table.schema().findField("k").fieldId(), table.schema().findField("f").fieldId());
    Schema intFloatSchema = table.schema();
    InsertedRowTracker sharedInsertedRows =
        InsertedRowTracker.create(intFloatSchema.select("k", "f").asStruct());
    GenericTaskDeltaWriter intFloat =
        createTaskWriter(
            equalityFieldIds,
            intFloatSchema.select("k", "f"),
            DeleteGranularity.FILE,
            sharedInsertedRows);

    table.updateSchema().updateColumn("k", Types.LongType.get()).commit();
    Schema longFloatSchema = table.schema();
    GenericTaskDeltaWriter longFloat =
        createTaskWriter(
            equalityFieldIds,
            longFloatSchema.select("k", "f"),
            DeleteGranularity.FILE,
            sharedInsertedRows);

    table.updateSchema().updateColumn("f", Types.DoubleType.get()).commit();
    Schema longDoubleSchema = table.schema();
    GenericTaskDeltaWriter longDouble =
        createTaskWriter(
            equalityFieldIds,
            longDoubleSchema.select("k", "f"),
            DeleteGranularity.FILE,
            sharedInsertedRows);
    long wideKey = Integer.MAX_VALUE + 1L;

    // each key field converts on its own
    intFloat.write(record(intFloatSchema, 1, "aaa", 1, 1.5f));
    longDouble.write(record(longDoubleSchema, 2, "bbb", 1L, 1.5d));
    // a long outside the int range keeps the key apart, with the float widened to double
    longFloat.write(record(longFloatSchema, 3, "ccc", wideKey, 2.5f));
    longDouble.write(record(longDoubleSchema, 4, "ddd", wideKey, 2.5d));

    WriteResult intFloatResult = intFloat.complete();
    WriteResult longFloatResult = longFloat.complete();
    WriteResult longDoubleResult = longDouble.complete();
    assertThat(longDoubleResult.deleteFiles())
        .allMatch(file -> file.content() == FileContent.POSITION_DELETES);
    assertThat(longDoubleResult.referencedDataFiles())
        .containsExactlyInAnyOrder(
            intFloatResult.dataFiles()[0].location(), longFloatResult.dataFiles()[0].location());

    commit(intFloatResult, longFloatResult, longDoubleResult);
    assertThat(actualRowSet("*"))
        .isEqualTo(
            expectedRowSet(
                ImmutableList.of(
                    record(table.schema(), 2, "bbb", 1L, 1.5d),
                    record(table.schema(), 4, "ddd", wideKey, 2.5d))));
  }

  @TestTemplate
  void testSharedInsertedRowTrackerMatchesNullKeysThroughView() throws IOException {
    table
        .updateSchema()
        .addColumn("k", Types.IntegerType.get())
        .addColumn("f", Types.FloatType.get())
        .commit();
    List<Integer> equalityFieldIds =
        Lists.newArrayList(
            table.schema().findField("k").fieldId(), table.schema().findField("f").fieldId());
    Schema intFloatSchema = table.schema();
    InsertedRowTracker sharedInsertedRows =
        InsertedRowTracker.create(intFloatSchema.select("k", "f").asStruct());
    GenericTaskDeltaWriter intFloat =
        createTaskWriter(
            equalityFieldIds,
            intFloatSchema.select("k", "f"),
            DeleteGranularity.FILE,
            sharedInsertedRows);

    // promoted in the other order than above: the float first
    table.updateSchema().updateColumn("f", Types.DoubleType.get()).commit();
    Schema intDoubleSchema = table.schema();
    GenericTaskDeltaWriter intDouble =
        createTaskWriter(
            equalityFieldIds,
            intDoubleSchema.select("k", "f"),
            DeleteGranularity.FILE,
            sharedInsertedRows);

    table.updateSchema().updateColumn("k", Types.LongType.get()).commit();
    Schema longDoubleSchema = table.schema();
    GenericTaskDeltaWriter longDouble =
        createTaskWriter(
            equalityFieldIds,
            longDoubleSchema.select("k", "f"),
            DeleteGranularity.FILE,
            sharedInsertedRows);

    // null key values pass through the conversion and match null
    intFloat.write(record(intFloatSchema, 1, "aaa", null, null));
    intFloat.write(record(intFloatSchema, 2, "bbb", null, 1.5f));
    longDouble.write(record(longDoubleSchema, 3, "ccc", null, null));
    intDouble.write(record(intDoubleSchema, 4, "ddd", null, 1.5d));
    // keys without a float value, with the int widened to long or a null passed through
    intDouble.write(record(intDoubleSchema, 5, "eee", 3, 0.1d));
    intDouble.write(record(intDoubleSchema, 6, "fff", null, 0.3d));
    longDouble.write(record(longDoubleSchema, 7, "ggg", 3L, 0.1d));
    longDouble.write(record(longDoubleSchema, 8, "hhh", null, 0.3d));

    WriteResult intFloatResult = intFloat.complete();
    WriteResult intDoubleResult = intDouble.complete();
    WriteResult longDoubleResult = longDouble.complete();
    assertThat(intDoubleResult.referencedDataFiles())
        .containsExactly(intFloatResult.dataFiles()[0].location());
    assertThat(longDoubleResult.referencedDataFiles())
        .containsExactlyInAnyOrder(
            intFloatResult.dataFiles()[0].location(), intDoubleResult.dataFiles()[0].location());

    commit(intFloatResult, intDoubleResult, longDoubleResult);
    Schema schema = table.schema();
    assertThat(actualRowSet("*"))
        .isEqualTo(
            expectedRowSet(
                ImmutableList.of(
                    record(schema, 3, "ccc", null, null),
                    record(schema, 4, "ddd", null, 1.5d),
                    record(schema, 7, "ggg", 3L, 0.1d),
                    record(schema, 8, "hhh", null, 0.3d))));
  }

  @TestTemplate
  void testSharedInsertedRowTrackerRetiresRowsDeletedThroughPromotedWriter() throws IOException {
    List<Integer> equalityFieldIds = Lists.newArrayList(idFieldId);
    Schema narrowSchema = table.schema();
    InsertedRowTracker sharedInsertedRows =
        InsertedRowTracker.create(narrowSchema.select("id").asStruct());
    GenericTaskDeltaWriter narrow =
        createTaskWriter(
            equalityFieldIds,
            narrowSchema.select("id"),
            DeleteGranularity.FILE,
            sharedInsertedRows);
    narrow.write(record(narrowSchema, 1, "aaa"));
    narrow.write(record(narrowSchema, 2, "bbb"));

    table.updateSchema().updateColumn("id", Types.LongType.get()).commit();
    Schema wideSchema = table.schema();
    Schema wideKey = wideSchema.select("id");
    GenericTaskDeltaWriter wide =
        createTaskWriter(equalityFieldIds, wideKey, DeleteGranularity.FILE, sharedInsertedRows);
    // an upsert's key delete and a CDC row delete both look the key up through the view
    wide.deleteKey(record(wideKey, 1L));
    wide.delete(record(wideSchema, 2L, "bbb"));

    WriteResult narrowResult = narrow.complete();
    WriteResult wideResult = wide.complete();
    assertThat(wideResult.dataFiles()).isEmpty();
    assertThat(wideResult.deleteFiles())
        .hasSize(1)
        .allMatch(file -> file.content() == FileContent.POSITION_DELETES)
        .allMatch(file -> file.recordCount() == 2);

    commit(narrowResult, wideResult);
    assertThat(actualRowSet("*")).isEqualTo(expectedRowSet(ImmutableList.of()));
  }

  @TestTemplate
  void testInsertedRowTrackerCanShareWithPromotedKeyTypes() {
    Types.NestedField id = Types.NestedField.required(1, "id", Types.IntegerType.get());
    Types.NestedField data = Types.NestedField.required(2, "data", Types.StringType.get());
    InsertedRowTracker tracker = InsertedRowTracker.create(Types.StructType.of(id, data));

    assertThat(tracker.canShareWith(Types.StructType.of(id, data))).isTrue();
    assertThat(tracker.canShareWith(Types.StructType.of(withType(id, Types.LongType.get()), data)))
        .isTrue();
    assertThat(
            tracker.acceptsKeyType(Types.StructType.of(withType(id, Types.LongType.get()), data)))
        .as("acceptsKeyType stays strict")
        .isFalse();
    assertThat(
            tracker.canShareWith(
                Types.StructType.of(
                    Types.NestedField.required(1, "renamed_id", Types.LongType.get()), data)))
        .as("A rename together with a promotion")
        .isTrue();
    assertThat(
            tracker.canShareWith(
                Types.StructType.of(
                    Types.NestedField.optional(1, "id", Types.LongType.get()), data)))
        .as("Optionality must still match")
        .isFalse();
    assertThat(tracker.canShareWith(Types.StructType.of(data, id)))
        .as("Key field positions must still match")
        .isFalse();
    assertThat(
            tracker.canShareWith(
                Types.StructType.of(
                    Types.NestedField.required(3, "id", Types.LongType.get()), data)))
        .as("Field IDs must still match")
        .isFalse();
    assertThat(tracker.canShareWith(Types.StructType.of(withType(id, Types.LongType.get()))))
        .as("The number of key fields must still match")
        .isFalse();

    Types.NestedField amount = Types.NestedField.required(3, "amount", Types.DecimalType.of(10, 2));
    InsertedRowTracker decimalTracker = InsertedRowTracker.create(Types.StructType.of(amount));
    assertThat(
            decimalTracker.canShareWith(
                Types.StructType.of(withType(amount, Types.DecimalType.of(12, 2)))))
        .isTrue();
    assertThat(
            decimalTracker.canShareWith(
                Types.StructType.of(withType(amount, Types.DecimalType.of(12, 3)))))
        .as("A decimal's scale cannot change")
        .isFalse();
    assertThat(
            decimalTracker.canShareWith(
                Types.StructType.of(withType(amount, Types.IntegerType.get()))))
        .isFalse();

    Types.NestedField struct =
        Types.NestedField.required(
            4,
            "struct",
            Types.StructType.of(Types.NestedField.required(5, "inner", Types.IntegerType.get())));
    InsertedRowTracker nestedTracker = InsertedRowTracker.create(Types.StructType.of(struct, id));
    assertThat(nestedTracker.canShareWith(Types.StructType.of(struct, id))).isTrue();
    assertThat(
            nestedTracker.canShareWith(
                Types.StructType.of(struct, withType(id, Types.LongType.get()))))
        .as("A key with a nested field is only shared when its type matches exactly")
        .isFalse();
    assertThat(
            nestedTracker.canShareWith(
                Types.StructType.of(
                    withType(
                        struct,
                        Types.StructType.of(
                            Types.NestedField.required(5, "inner", Types.LongType.get()))),
                    id)))
        .isFalse();
  }

  @TestTemplate
  void testInsertedRowTrackerSharesAcrossExactlyTheAllowedTypePromotions() {
    List<Type.PrimitiveType> types =
        ImmutableList.of(
            Types.BooleanType.get(),
            Types.IntegerType.get(),
            Types.LongType.get(),
            Types.FloatType.get(),
            Types.DoubleType.get(),
            Types.DateType.get(),
            Types.TimeType.get(),
            Types.TimestampType.withoutZone(),
            Types.TimestampType.withZone(),
            Types.TimestampNanoType.withoutZone(),
            Types.StringType.get(),
            Types.UUIDType.get(),
            Types.BinaryType.get(),
            Types.FixedType.ofLength(4),
            Types.DecimalType.of(9, 2),
            Types.DecimalType.of(18, 2),
            Types.DecimalType.of(18, 4));

    for (Type.PrimitiveType type : types) {
      InsertedRowTracker tracker =
          InsertedRowTracker.create(
              Types.StructType.of(Types.NestedField.optional(1, "key", type)));
      for (Type.PrimitiveType otherType : types) {
        boolean promotion =
            TypeUtil.isPromotionAllowed(type, otherType)
                || TypeUtil.isPromotionAllowed(otherType, type);
        assertThat(
                tracker.canShareWith(
                    Types.StructType.of(Types.NestedField.optional(1, "key", otherType))))
            .as("%s and %s", type, otherType)
            .isEqualTo(promotion);
      }
    }
  }

  /**
   * Create a generic task equality delta writer.
   *
   * @param equalityFieldIds defines the equality field ids.
   * @param eqDeleteRowSchema defines the schema of rows that eq-delete writer will write, it could
   *     be the entire fields of the table schema.
   */
  private GenericTaskDeltaWriter createTaskWriter(
      List<Integer> equalityFieldIds,
      Schema eqDeleteRowSchema,
      DeleteGranularity deleteGranularity) {
    return createTaskWriter(equalityFieldIds, eqDeleteRowSchema, deleteGranularity, null);
  }

  private GenericTaskDeltaWriter createTaskWriter(
      List<Integer> equalityFieldIds,
      Schema eqDeleteRowSchema,
      DeleteGranularity deleteGranularity,
      InsertedRowTracker sharedInsertedRows) {
    FileWriterFactory<Record> fileWriterFactory =
        new GenericFileWriterFactory.Builder(table)
            .dataFileFormat(format)
            .equalityFieldIds(ArrayUtil.toIntArray(equalityFieldIds))
            .equalityDeleteRowSchema(eqDeleteRowSchema)
            .build();

    List<String> columns = Lists.newArrayList();
    for (Integer fieldId : equalityFieldIds) {
      columns.add(table.schema().findField(fieldId).name());
    }
    Schema deleteSchema = table.schema().select(columns);

    return new GenericTaskDeltaWriter(
        table.schema(),
        deleteSchema,
        table.spec(),
        format,
        fileWriterFactory,
        fileFactory,
        table.io(),
        TARGET_FILE_SIZE,
        deleteGranularity,
        formatVersion > 2,
        sharedInsertedRows);
  }

  private static class GenericTaskDeltaWriter extends BaseTaskWriter<Record> {
    private final GenericEqualityDeltaWriter deltaWriter;

    private GenericTaskDeltaWriter(
        Schema schema,
        Schema deleteSchema,
        PartitionSpec spec,
        FileFormat format,
        FileWriterFactory<Record> fileWriterFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        long targetFileSize,
        DeleteGranularity deleteGranularity,
        boolean useDv,
        InsertedRowTracker sharedInsertedRows) {
      super(spec, format, fileWriterFactory, fileFactory, io, targetFileSize, useDv);
      // without a shared tracker, use the constructor that existed before shared trackers
      this.deltaWriter =
          sharedInsertedRows == null
              ? new GenericEqualityDeltaWriter(
                  null, schema, deleteSchema, deleteGranularity, dvFileWriter())
              : new GenericEqualityDeltaWriter(
                  null,
                  schema,
                  deleteSchema,
                  deleteGranularity,
                  dvFileWriter(),
                  sharedInsertedRows);
    }

    @Override
    public void write(Record row) throws IOException {
      deltaWriter.write(row);
    }

    public void delete(Record row) throws IOException {
      deltaWriter.delete(row);
    }

    // The caller of this function is responsible for passing in a record with only the key fields
    public void deleteKey(Record key) throws IOException {
      deltaWriter.deleteKey(key);
    }

    @Override
    public void close() throws IOException {
      deltaWriter.close();
      super.close();
    }

    private class GenericEqualityDeltaWriter extends BaseEqualityDeltaWriter {
      private GenericEqualityDeltaWriter(
          PartitionKey partition,
          Schema schema,
          Schema eqDeleteSchema,
          DeleteGranularity deleteGranularity,
          PartitioningDVWriter<Record> dvWriter) {
        super(partition, schema, eqDeleteSchema, deleteGranularity, dvWriter);
      }

      private GenericEqualityDeltaWriter(
          PartitionKey partition,
          Schema schema,
          Schema eqDeleteSchema,
          DeleteGranularity deleteGranularity,
          PartitioningDVWriter<Record> dvWriter,
          InsertedRowTracker sharedInsertedRows) {
        super(partition, schema, eqDeleteSchema, deleteGranularity, dvWriter, sharedInsertedRows);
      }

      @Override
      protected StructLike asStructLike(Record row) {
        return row;
      }

      @Override
      protected StructLike asStructLikeKey(Record data) {
        return data;
      }
    }
  }

  private static Types.NestedField withType(Types.NestedField field, Type type) {
    return Types.NestedField.from(field).ofType(type).build();
  }

  private static Record record(Schema schema, Object... values) {
    Record record = GenericRecord.create(schema);
    for (int pos = 0; pos < values.length; pos += 1) {
      record.set(pos, values[pos]);
    }

    return record;
  }

  private static Object insertedRows(GenericTaskDeltaWriter writer)
      throws ReflectiveOperationException {
    Field field = BaseTaskWriter.BaseEqualityDeltaWriter.class.getDeclaredField("insertedRows");
    field.setAccessible(true);
    return field.get(writer.deltaWriter);
  }

  private void commit(WriteResult... results) {
    RowDelta rowDelta = table.newRowDelta();
    for (WriteResult result : results) {
      Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
      Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
    }

    rowDelta.commit();
  }

  private List<Record> readRecordsAsList(Schema schema, CharSequence path) throws IOException {
    CloseableIterable<Record> iterable;

    InputFile inputFile = Files.localInput(path.toString());
    switch (format) {
      case PARQUET:
        iterable =
            Parquet.read(inputFile)
                .project(schema)
                .createReaderFunc(
                    fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
                .build();
        break;

      case AVRO:
        iterable =
            Avro.read(inputFile)
                .project(schema)
                .createResolvingReader(PlannedDataReader::create)
                .build();
        break;

      case ORC:
        iterable =
            ORC.read(inputFile)
                .project(schema)
                .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
                .build();
        break;

      default:
        throw new UnsupportedOperationException("Unsupported file format: " + format);
    }

    try (CloseableIterable<Record> closeableIterable = iterable) {
      return Lists.newArrayList(closeableIterable);
    }
  }

  private PositionDeleteIndex readDVFile(Table table, DeleteFile dvFile) {
    BaseDeleteLoader deleteLoader =
        new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile.location()));
    PositionDeleteIndex index =
        deleteLoader.loadPositionDeletes(List.of(dvFile), dvFile.referencedDataFile());
    return index;
  }
}
