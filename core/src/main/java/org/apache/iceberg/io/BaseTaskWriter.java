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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.SortingPositionOnlyDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructLikeUtil;
import org.apache.iceberg.util.StructProjection;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseTaskWriter<T> implements TaskWriter<T> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseTaskWriter.class);

  private final List<DataFile> completedDataFiles = Lists.newArrayList();
  private final List<DeleteFile> completedDeleteFiles = Lists.newArrayList();
  private final CharSequenceSet referencedDataFiles = CharSequenceSet.empty();

  private final PartitionSpec spec;
  private final FileFormat format;
  private final FileAppenderFactory<T> appenderFactory;
  private final FileWriterFactory<T> writerFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private Throwable failure;
  private PartitioningDVWriter<T> dvFileWriter;

  protected BaseTaskWriter(
      PartitionSpec spec,
      FileFormat format,
      FileAppenderFactory<T> appenderFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize) {
    this.spec = spec;
    this.format = format;
    this.appenderFactory = appenderFactory;
    this.writerFactory = null;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
  }

  protected BaseTaskWriter(
      PartitionSpec spec,
      FileFormat format,
      FileWriterFactory<T> writerFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize) {
    this.spec = spec;
    this.format = format;
    this.appenderFactory = null;
    this.writerFactory = writerFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
  }

  protected BaseTaskWriter(
      PartitionSpec spec,
      FileFormat format,
      FileWriterFactory<T> writerFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      boolean useDv) {
    this.spec = spec;
    this.format = format;
    this.appenderFactory = null;
    this.writerFactory = writerFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
    if (useDv) {
      this.dvFileWriter = new PartitioningDVWriter<>(fileFactory, p -> null);
    }
  }

  protected PartitionSpec spec() {
    return spec;
  }

  protected void setFailure(Throwable throwable) {
    if (failure == null) {
      this.failure = throwable;
    }
  }

  protected PartitioningDVWriter<T> dvFileWriter() {
    return dvFileWriter;
  }

  @Override
  public void abort() throws IOException {
    close();

    // clean up files created by this writer
    Tasks.foreach(Iterables.concat(completedDataFiles, completedDeleteFiles))
        .executeWith(ThreadPools.getWorkerPool())
        .throwFailureWhenFinished()
        .noRetry()
        .run(file -> io.deleteFile(file.location()));
  }

  @Override
  public WriteResult complete() throws IOException {
    close();

    Preconditions.checkState(failure == null, "Cannot return results from failed writer", failure);

    return WriteResult.builder()
        .addDataFiles(completedDataFiles)
        .addDeleteFiles(completedDeleteFiles)
        .addReferencedDataFiles(referencedDataFiles)
        .build();
  }

  @Override
  public void close() throws IOException {
    try {
      if (dvFileWriter != null) {
        try {
          // complete will call close
          dvFileWriter.close();
          DeleteWriteResult result = dvFileWriter.result();
          completedDeleteFiles.addAll(result.deleteFiles());
          referencedDataFiles.addAll(result.referencedDataFiles());
        } finally {
          dvFileWriter = null;
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close dvFileWriter", e);
    }
  }

  /**
   * Tracks the file position of the latest row written for each equality key within one commit, so
   * that a later write or delete of the same key is retired with a position delete. An equality
   * delete cannot do that, because it only applies to data files with a strictly lower data
   * sequence number than its own, and every file in one commit shares a sequence number.
   *
   * <p>A tracker may be shared by several {@code BaseEqualityDeltaWriter}s that write the same
   * table within one commit, for example when a schema evolution makes a sink open a second writer
   * mid-commit. A shared tracker lives as long as the commit it belongs to; the sink that created
   * it drops it at commit time, and a writer only clears a tracker it created itself.
   *
   * <p>Writers whose key types differ by a type promotion can share a tracker too, see {@link
   * #canShareWith}. Such a writer converts its keys to this tracker's key type. A key that has no
   * exact value in that type, such as a long outside the int range, can only equal keys of other
   * promoted writers; it is kept apart, in the widest type of each key field.
   */
  public static class InsertedRowTracker {
    private final Types.StructType keyType;
    private final Map<StructLike, PathOffset> offsetsByKey;
    // Keys without an exact value in keyType, created when a promoted writer first writes one
    private Map<StructLike, PathOffset> widenedOffsetsByKey = null;

    private InsertedRowTracker(Types.StructType keyType) {
      this.keyType = keyType;
      this.offsetsByKey = StructLikeMap.create(keyType);
    }

    // Shares the key type and offsets of the given tracker, for a view of it
    private InsertedRowTracker(InsertedRowTracker tracker) {
      this.keyType = tracker.keyType;
      this.offsetsByKey = tracker.offsetsByKey;
    }

    /**
     * Creates a tracker keyed by the given equality-delete key struct type.
     *
     * @param keyType the struct type of the equality fields, i.e. the equality-delete schema
     */
    public static InsertedRowTracker create(Types.StructType keyType) {
      Preconditions.checkNotNull(keyType, "Inserted-row tracker key type cannot be null");
      return new InsertedRowTracker(keyType);
    }

    /** Returns the struct type of the equality fields this tracker is keyed by. */
    public Types.StructType keyType() {
      return keyType;
    }

    /**
     * Returns whether keys of the given struct type can be stored and looked up in this tracker.
     *
     * <p>Keys are compared by position, type and optionality, which is what {@link StructLikeMap}
     * uses; field names, docs and defaults may differ. Field IDs must match so that the two key
     * types describe the same equality fields.
     */
    public boolean acceptsKeyType(Types.StructType otherKeyType) {
      return matchesKeyType(otherKeyType, false);
    }

    /**
     * Returns whether writers whose equality-delete key has the given struct type can share this
     * tracker.
     *
     * <p>This is {@link #acceptsKeyType} that also allows a key field's type to differ by a type
     * promotion, in either direction: int and long, float and double, or decimals of the same
     * scale. Field IDs, optionality and positions must still match. A key with a nested field is
     * only shared when its type matches exactly.
     */
    public boolean canShareWith(Types.StructType otherKeyType) {
      return matchesKeyType(otherKeyType, true);
    }

    private boolean matchesKeyType(Types.StructType otherKeyType, boolean allowPromotion) {
      List<Types.NestedField> fields = keyType.fields();
      List<Types.NestedField> otherFields = otherKeyType.fields();
      if (fields.size() != otherFields.size()) {
        return false;
      }

      boolean promoted = false;
      boolean nested = false;
      for (int pos = 0; pos < fields.size(); pos += 1) {
        Types.NestedField field = fields.get(pos);
        Types.NestedField otherField = otherFields.get(pos);
        if (field.fieldId() != otherField.fieldId()
            || field.isOptional() != otherField.isOptional()) {
          return false;
        }

        if (!field.type().equals(otherField.type())) {
          if (!allowPromotion || !isPromotion(field.type(), otherField.type())) {
            return false;
          }

          promoted = true;
        }

        nested = nested || field.type().isNestedType();
      }

      return !promoted || !nested;
    }

    /**
     * Returns whether one of the two types promotes to the other. These are the promotions that
     * {@code TypeUtil.isPromotionAllowed} allows; they are listed here because a view must convert
     * the values of each, so one added there later is not shared until a view can convert it.
     */
    private static boolean isPromotion(Type type, Type otherType) {
      switch (type.typeId()) {
        case INTEGER:
          return otherType.typeId() == Type.TypeID.LONG;
        case LONG:
          return otherType.typeId() == Type.TypeID.INTEGER;
        case FLOAT:
          return otherType.typeId() == Type.TypeID.DOUBLE;
        case DOUBLE:
          return otherType.typeId() == Type.TypeID.FLOAT;
        case DECIMAL:
          return otherType.typeId() == Type.TypeID.DECIMAL
              && ((Types.DecimalType) type).scale() == ((Types.DecimalType) otherType).scale();
        default:
          return false;
      }
    }

    /**
     * Returns the tracker a writer with the given key type uses, which must be one this tracker
     * {@link #canShareWith}: this tracker when the writer's keys hold values of the same classes,
     * otherwise a view that converts them.
     */
    private InsertedRowTracker forKeyType(Types.StructType writerKeyType) {
      List<Types.NestedField> fields = keyType.fields();
      List<Types.NestedField> writerFields = writerKeyType.fields();
      for (int pos = 0; pos < fields.size(); pos += 1) {
        // decimals of the same scale compare and hash alike whatever their precision
        if (fields.get(pos).type().typeId() != writerFields.get(pos).type().typeId()) {
          return new PromotedKeyView(this, writerKeyType);
        }
      }

      return this;
    }

    // Keyed by the key type with int and float fields widened to long and double
    private Map<StructLike, PathOffset> widenedOffsetsByKey() {
      if (widenedOffsetsByKey == null) {
        List<Types.NestedField> widenedFields = Lists.newArrayList();
        for (Types.NestedField field : keyType.fields()) {
          widenedFields.add(
              Types.NestedField.of(
                  field.fieldId(), field.isOptional(), field.name(), widen(field.type())));
        }

        this.widenedOffsetsByKey = StructLikeMap.create(Types.StructType.of(widenedFields));
      }

      return widenedOffsetsByKey;
    }

    private static Type widen(Type type) {
      switch (type.typeId()) {
        case INTEGER:
          return Types.LongType.get();
        case FLOAT:
          return Types.DoubleType.get();
        default:
          return type;
      }
    }

    private void clear() {
      offsetsByKey.clear();
      this.widenedOffsetsByKey = null;
    }

    /** Records the position of the latest row written for the key, storing a copy of the key. */
    PathOffset put(StructLike key, PathOffset offset) {
      return offsetsByKey.put(StructLikeUtil.copy(key), offset);
    }

    PathOffset remove(StructLike key) {
      return offsetsByKey.remove(key);
    }
  }

  /**
   * A writer's view of a tracker whose key type differs from the writer's by type promotions. Keys
   * are converted to the tracker's key type when they have an exact value in it, otherwise to the
   * widest type of each key field. Views never own the tracker they view.
   */
  private static class PromotedKeyView extends InsertedRowTracker {
    private final InsertedRowTracker tracker;
    private final Type.TypeID[] writerTypes;
    private final Type.TypeID[] trackerTypes;
    private final KeyValues lookupKey;

    private PromotedKeyView(InsertedRowTracker tracker, Types.StructType writerKeyType) {
      super(tracker);
      List<Types.NestedField> trackerFields = tracker.keyType().fields();
      List<Types.NestedField> writerFields = writerKeyType.fields();
      this.tracker = tracker;
      this.writerTypes = new Type.TypeID[writerFields.size()];
      this.trackerTypes = new Type.TypeID[writerFields.size()];
      for (int pos = 0; pos < writerTypes.length; pos += 1) {
        writerTypes[pos] = writerFields.get(pos).type().typeId();
        trackerTypes[pos] = trackerFields.get(pos).type().typeId();
      }

      this.lookupKey = new KeyValues(writerTypes.length);
    }

    @Override
    PathOffset put(StructLike key, PathOffset offset) {
      KeyValues converted = new KeyValues(writerTypes.length);
      if (toTrackerTypes(key, converted)) {
        return tracker.offsetsByKey.put(converted, offset);
      }

      toWidenedTypes(key, converted);
      return tracker.widenedOffsetsByKey().put(converted, offset);
    }

    @Override
    PathOffset remove(StructLike key) {
      if (toTrackerTypes(key, lookupKey)) {
        return tracker.offsetsByKey.remove(lookupKey);
      }

      // only another promoted writer can have put a key without a value in the tracker's type
      toWidenedTypes(key, lookupKey);
      Map<StructLike, PathOffset> widenedOffsets = tracker.widenedOffsetsByKey;
      return widenedOffsets != null ? widenedOffsets.remove(lookupKey) : null;
    }

    // Sets the key in the tracker's types; false if a value has no exact equivalent in them
    private boolean toTrackerTypes(StructLike key, KeyValues converted) {
      for (int pos = 0; pos < converted.size(); pos += 1) {
        Object value = key.get(pos, Object.class);
        if (value == null || writerTypes[pos] == trackerTypes[pos]) {
          converted.set(pos, value);
        } else if (writerTypes[pos] == Type.TypeID.INTEGER) {
          converted.set(pos, ((Integer) value).longValue());
        } else if (writerTypes[pos] == Type.TypeID.FLOAT) {
          converted.set(pos, ((Float) value).doubleValue());
        } else if (writerTypes[pos] == Type.TypeID.LONG) {
          long longValue = (Long) value;
          if (longValue != (int) longValue) {
            return false;
          }

          converted.set(pos, (int) longValue);
        } else {
          double doubleValue = (Double) value;
          // Double.compare treats NaN as equal to itself and keeps the sign of zero
          if (Double.compare(doubleValue, (float) doubleValue) != 0) {
            return false;
          }

          converted.set(pos, (float) doubleValue);
        }
      }

      return true;
    }

    // Sets the key with int and float values widened to long and double
    private void toWidenedTypes(StructLike key, KeyValues converted) {
      for (int pos = 0; pos < converted.size(); pos += 1) {
        Object value = key.get(pos, Object.class);
        if (value != null && writerTypes[pos] == Type.TypeID.INTEGER) {
          converted.set(pos, ((Integer) value).longValue());
        } else if (value != null && writerTypes[pos] == Type.TypeID.FLOAT) {
          converted.set(pos, ((Float) value).doubleValue());
        } else {
          converted.set(pos, value);
        }
      }
    }
  }

  // The values of a key converted for the tracker's maps
  private static class KeyValues implements StructLike {
    private final Object[] values;

    private KeyValues(int size) {
      this.values = new Object[size];
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    public <V> V get(int pos, Class<V> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <V> void set(int pos, V value) {
      values[pos] = value;
    }
  }

  /** Base equality delta writer to write both insert records and equality-deletes. */
  protected abstract class BaseEqualityDeltaWriter implements Closeable {
    private final StructProjection structProjection;
    private final PositionDelete<T> positionDelete;
    private final StructLike partitionKey;
    private final boolean ownsInsertedRows;
    private RollingFileWriter dataWriter;
    private RollingEqDeleteWriter eqDeleteWriter;
    private PartitioningWriter<PositionDelete<T>, DeleteWriteResult> posDeleteWriter;
    private InsertedRowTracker insertedRows;
    private boolean closePosDeleteWriter;

    protected BaseEqualityDeltaWriter(StructLike partition, Schema schema, Schema deleteSchema) {
      this(partition, schema, deleteSchema, DeleteGranularity.PARTITION, null);
    }

    protected BaseEqualityDeltaWriter(
        StructLike partition,
        Schema schema,
        Schema deleteSchema,
        DeleteGranularity deleteGranularity) {
      this(partition, schema, deleteSchema, deleteGranularity, null);
    }

    protected BaseEqualityDeltaWriter(
        StructLike partition,
        Schema schema,
        Schema deleteSchema,
        DeleteGranularity deleteGranularity,
        PartitioningDVWriter<T> posDeleteWriter) {
      this(partition, schema, deleteSchema, deleteGranularity, posDeleteWriter, null);
    }

    /**
     * Creates an equality delta writer that records inserted rows in a shared tracker.
     *
     * @param sharedInsertedRows a tracker shared with other writers of the same table within the
     *     current commit, or null to use a private tracker. It must be shareable with the
     *     equality-delete schema's struct type, see {@link InsertedRowTracker#canShareWith}.
     */
    protected BaseEqualityDeltaWriter(
        StructLike partition,
        Schema schema,
        Schema deleteSchema,
        DeleteGranularity deleteGranularity,
        PartitioningDVWriter<T> posDeleteWriter,
        InsertedRowTracker sharedInsertedRows) {
      Preconditions.checkNotNull(schema, "Iceberg table schema cannot be null.");
      Preconditions.checkNotNull(deleteSchema, "Equality-delete schema cannot be null.");
      Preconditions.checkArgument(
          sharedInsertedRows == null || sharedInsertedRows.canShareWith(deleteSchema.asStruct()),
          "Shared inserted-row tracker key type %s cannot be shared with equality-delete schema %s",
          sharedInsertedRows != null ? sharedInsertedRows.keyType() : null,
          deleteSchema.asStruct());
      this.structProjection = StructProjection.create(schema, deleteSchema);
      this.positionDelete = PositionDelete.create();

      this.dataWriter = new RollingFileWriter(partition);
      this.eqDeleteWriter = new RollingEqDeleteWriter(partition);
      this.posDeleteWriter =
          posDeleteWriter != null
              ? posDeleteWriter
              : createPosDeleteWriter(partition, deleteGranularity);
      this.ownsInsertedRows = sharedInsertedRows == null;
      this.insertedRows =
          ownsInsertedRows
              ? InsertedRowTracker.create(deleteSchema.asStruct())
              : sharedInsertedRows.forKeyType(deleteSchema.asStruct());
      this.partitionKey = partition;
    }

    /** Wrap the data as a {@link StructLike}. */
    protected abstract StructLike asStructLike(T data);

    /** Wrap the passed in key of a row as a {@link StructLike} */
    protected abstract StructLike asStructLikeKey(T key);

    public void write(T row) throws IOException {
      PathOffset pathOffset = PathOffset.of(dataWriter.currentPath(), dataWriter.currentRows());

      // Adding a pos-delete to replace the old path-offset. The tracker stores a copy of the key.
      PathOffset previous = insertedRows.put(structProjection.wrap(asStructLike(row)), pathOffset);
      if (previous != null) {
        // TODO attach the previous row if has a positional-delete row schema in appender factory.
        writePosDelete(previous);
      }

      dataWriter.write(row);
    }

    private PartitioningWriter<PositionDelete<T>, DeleteWriteResult> createPosDeleteWriter(
        StructLike partition, DeleteGranularity deleteGranularity) {
      this.closePosDeleteWriter = true;
      return new WrappedPositionDeleteWriter<>(
          () ->
              writerFactory != null
                  ? writerFactory.newPositionDeleteWriter(newOutputFile(partition), spec, partition)
                  : appenderFactory.newPosDeleteWriter(newOutputFile(partition), format, partition),
          deleteGranularity);
    }

    private EncryptedOutputFile newOutputFile(StructLike partition) {
      if (spec.isUnpartitioned() || partition == null) {
        return fileFactory.newOutputFile();
      } else {
        return fileFactory.newOutputFile(spec, partition);
      }
    }

    private void writePosDelete(PathOffset pathOffset) {
      positionDelete.set(pathOffset.path, pathOffset.rowOffset, null);
      posDeleteWriter.write(positionDelete, spec, partitionKey);
    }

    /**
     * Write the pos-delete if there's an existing row matching the given key.
     *
     * @param key has the same columns with the equality fields.
     */
    private boolean internalPosDelete(StructLike key) {
      PathOffset previous = insertedRows.remove(key);

      if (previous != null) {
        // TODO attach the previous row if has a positional-delete row schema in appender factory.
        writePosDelete(previous);
        return true;
      }

      return false;
    }

    /**
     * Delete those rows whose equality fields has the same values with the given row. It will write
     * the entire row into the equality-delete file.
     *
     * @param row the given row to delete.
     */
    public void delete(T row) throws IOException {
      if (!internalPosDelete(structProjection.wrap(asStructLike(row)))) {
        eqDeleteWriter.write(row);
      }
    }

    /**
     * Delete those rows with the given key. It will only write the values of equality fields into
     * the equality-delete file.
     *
     * @param key is the projected data whose columns are the same as the equality fields.
     */
    public void deleteKey(T key) throws IOException {
      if (!internalPosDelete(asStructLikeKey(key))) {
        eqDeleteWriter.write(key);
      }
    }

    @Override
    public void close() throws IOException {
      try {
        // Close data writer and add completed data files.
        if (dataWriter != null) {
          try {
            dataWriter.close();
          } finally {
            dataWriter = null;
          }
        }

        // Close eq-delete writer and add completed equality-delete files.
        if (eqDeleteWriter != null) {
          try {
            eqDeleteWriter.close();
          } finally {
            eqDeleteWriter = null;
          }
        }

        if (insertedRows != null) {
          if (ownsInsertedRows) {
            insertedRows.clear();
          }

          insertedRows = null;
        }

        // Add the completed pos-delete files.
        if (closePosDeleteWriter && posDeleteWriter != null) {
          try {
            // complete will call close
            posDeleteWriter.close();
            DeleteWriteResult result = posDeleteWriter.result();
            completedDeleteFiles.addAll(result.deleteFiles());
            referencedDataFiles.addAll(result.referencedDataFiles());
          } finally {
            posDeleteWriter = null;
          }
        }
      } catch (IOException | RuntimeException e) {
        setFailure(e);
        throw e;
      }
    }
  }

  private static class PathOffset {
    private final CharSequence path;
    private final long rowOffset;

    private PathOffset(CharSequence path, long rowOffset) {
      this.path = path;
      this.rowOffset = rowOffset;
    }

    private static PathOffset of(CharSequence path, long rowOffset) {
      return new PathOffset(path, rowOffset);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("path", path)
          .add("row_offset", rowOffset)
          .toString();
    }
  }

  private abstract class BaseRollingWriter<W extends Closeable> implements Closeable {
    private static final int ROWS_DIVISOR = 1000;
    private final StructLike partitionKey;

    private EncryptedOutputFile currentFile = null;
    private W currentWriter = null;
    private long currentRows = 0;

    private BaseRollingWriter(StructLike partitionKey) {
      this.partitionKey = partitionKey;
      openCurrent();
    }

    abstract W newWriter(EncryptedOutputFile file, StructLike partition);

    abstract long length(W writer);

    abstract void write(W writer, T record);

    abstract void complete(W closedWriter);

    public void write(T record) throws IOException {
      write(currentWriter, record);
      this.currentRows++;

      if (shouldRollToNewFile()) {
        closeCurrent();
        openCurrent();
      }
    }

    public CharSequence currentPath() {
      Preconditions.checkNotNull(currentFile, "The currentFile shouldn't be null");
      return currentFile.encryptingOutputFile().location();
    }

    public long currentRows() {
      return currentRows;
    }

    private void openCurrent() {
      if (partitionKey == null) {
        // unpartitioned
        this.currentFile = fileFactory.newOutputFile();
      } else {
        // partitioned
        this.currentFile = fileFactory.newOutputFile(partitionKey);
      }
      this.currentWriter = newWriter(currentFile, partitionKey);
      this.currentRows = 0;
    }

    private boolean shouldRollToNewFile() {
      return currentRows % ROWS_DIVISOR == 0 && length(currentWriter) >= targetFileSize;
    }

    private void closeCurrent() throws IOException {
      if (currentWriter != null) {
        try {
          currentWriter.close();

          if (currentRows == 0L) {
            // the file may not have been created or cannot be deleted, and it isn't worth failing
            // the job to clean up, skip deleting
            Tasks.foreach(currentFile.encryptingOutputFile())
                .suppressFailureWhenFinished()
                .onFailure(
                    (file, exc) ->
                        LOG.warn(
                            "Failed to delete the uncommitted empty file during writer clean up: {}",
                            file,
                            exc))
                .run(io::deleteFile);
          } else {
            complete(currentWriter);
          }

        } catch (IOException | RuntimeException e) {
          setFailure(e);
          throw e;

        } finally {
          this.currentFile = null;
          this.currentWriter = null;
          this.currentRows = 0;
        }
      }
    }

    @Override
    public void close() throws IOException {
      closeCurrent();
    }
  }

  protected class RollingFileWriter extends BaseRollingWriter<DataWriter<T>> {
    public RollingFileWriter(StructLike partitionKey) {
      super(partitionKey);
    }

    @Override
    DataWriter<T> newWriter(EncryptedOutputFile file, StructLike partitionKey) {
      if (writerFactory != null) {
        return writerFactory.newDataWriter(file, spec, partitionKey);
      } else {
        return appenderFactory.newDataWriter(file, format, partitionKey);
      }
    }

    @Override
    long length(DataWriter<T> writer) {
      return writer.length();
    }

    @Override
    void write(DataWriter<T> writer, T record) {
      writer.write(record);
    }

    @Override
    void complete(DataWriter<T> closedWriter) {
      completedDataFiles.add(closedWriter.toDataFile());
    }
  }

  protected class RollingEqDeleteWriter extends BaseRollingWriter<EqualityDeleteWriter<T>> {
    RollingEqDeleteWriter(StructLike partitionKey) {
      super(partitionKey);
    }

    @Override
    EqualityDeleteWriter<T> newWriter(EncryptedOutputFile file, StructLike partitionKey) {
      if (writerFactory != null) {
        return writerFactory.newEqualityDeleteWriter(file, spec, partitionKey);
      } else {
        return appenderFactory.newEqDeleteWriter(file, format, partitionKey);
      }
    }

    @Override
    long length(EqualityDeleteWriter<T> writer) {
      return writer.length();
    }

    @Override
    void write(EqualityDeleteWriter<T> writer, T record) {
      writer.write(record);
    }

    @Override
    void complete(EqualityDeleteWriter<T> closedWriter) {
      completedDeleteFiles.add(closedWriter.toDeleteFile());
    }
  }

  private static class WrappedPositionDeleteWriter<T> extends SortingPositionOnlyDeleteWriter<T>
      implements PartitioningWriter<PositionDelete<T>, DeleteWriteResult> {

    WrappedPositionDeleteWriter(
        Supplier<FileWriter<PositionDelete<T>, DeleteWriteResult>> writers,
        DeleteGranularity granularity) {
      super(writers, granularity);
    }

    @Override
    public void write(PositionDelete<T> positionDelete, PartitionSpec spec, StructLike partition) {
      super.write(positionDelete);
    }
  }
}
