# Flink: Log committed offsets (checkpointIds) and snapshotId on commit

## Summary
Add INFO-level logging when an Iceberg snapshot/commit happens so that the **committed offsets** (Flink checkpoint IDs) and the resulting **snapshot ID** are recorded in the operator logs. This helps identify the source of data loss when it occurs by correlating Flink checkpoints with Iceberg table state.

## Motivation
When data loss is suspected, we need to answer:
- Which Flink checkpoint(s) were actually committed to the table?
- Which Iceberg snapshot corresponds to each commit?

Today this is only recoverable by scanning table metadata (snapshot summaries for `flink.max-committed-checkpoint-id`). Having the same information in the committer logs makes debugging and incident response faster.

## Data loss investigation steps (using these logs)

1. **Confirm the symptom**  
   Establish that data is missing (e.g. expected time range or key range is absent in the table).

2. **Find the last committed offset in logs**  
   Search committer logs for `Committed offsets (checkpointIds):` for the affected table/branch. The logged list is the set of Flink checkpoint IDs that were committed in each batch. The highest checkpoint ID you see there is the last offset the sink committed.

3. **Map to Iceberg snapshots**  
   Use the same log lines (or the follow-up "Committed ... snapshotId: ..." lines) to get the **snapshot ID** for each commit. That gives a direct mapping: checkpoint ID → snapshot ID.

4. **Compare with source/producer progress**  
   - If the source is Kafka (or another offset-based system), compare the last committed checkpoint ID from step 2 to the offsets that were read at that checkpoint (e.g. from Flink checkpoint metadata or source operator metrics).  
   - If the sink’s last committed checkpoint is behind the source’s progress, the gap is in the sink/commit path (e.g. slow commits, failures, or backpressure).  
   - If they align, the gap is likely upstream (source not reading, or data never produced).

5. **Inspect table state at that snapshot**  
   Read the table as of the snapshot ID from step 3 (e.g. `SELECT ... FROM table VERSION AS OF snapshot_id`) to see exactly what was committed and validate against expectations.

6. **Check for commit failures or skips**  
   Search logs for "Skip commit" or commit exceptions around the time of the suspected loss. Cross-check checkpoint IDs: a missing or skipped checkpoint in the "Committed offsets" logs indicates that batch was never committed.

## Changes
- **IcebergCommitter** (Sink V2): After committing a batch, log `Committed offsets (checkpointIds): [...]` and extend the existing commit log line to include `snapshotId` (via `table.refresh()` + `table.snapshot(branch)`).
- **IcebergFilesCommitter** (legacy sink): Same — log committed checkpoint IDs after the batch and add `snapshotId` to the commit log line.
- **DynamicCommitter** (dynamic tables): Same — log committed checkpoint IDs per table/branch and add `snapshotId` to each commit log line.

## Example log output
```
Committed offsets (checkpointIds): [42, 43, 44] for table: db.table, branch: main
Committed append to table: db.table, branch: main, checkpointId: 44, snapshotId: 123456789 in 150 ms
```

## Testing
- No new tests; existing committer behavior unchanged except for additional logging.
- Suggested: run a streaming job and confirm logs appear at commit time.

---
*Authored with Cursor agent.*
