# Affirm Iceberg patch versions

This file is Affirm-local (not part of upstream Apache Iceberg). It records what each
`<upstream-version>-affirm-patch.<N>` release of this fork contains, so a consumer can tell exactly
which Affirm-local patches are baked into a given jar without reconstructing it from branch history.

Format: one section per published version, listing its upstream base and every Affirm-local fork PR
carried on top, each linked to its upstream reference where one exists. The git tag for a version
is the source of truth for the commit published to Artifactory.

## 1.11.0-affirm-patch.2

- **Base:** `apache-iceberg-1.11.0`

| Fork PR | Upstream reference | Scope |
|---|---|---|
| [Affirm/iceberg#10](https://github.com/Affirm/iceberg/pull/10) | apache/iceberg#16011, apache/iceberg#16648 | Fix duplicate commits in `DynamicCommitter` when the Flink jobId changes on restart (e.g. stop-with-savepoint, autoscaler resubmit); backported to `flink/v2.1`, `flink/v2.0`, `flink/v1.20` |
| [Affirm/iceberg#11](https://github.com/Affirm/iceberg/pull/11) | apache/iceberg#18071 | Fix CI by pulling MinIO test containers from Quay instead of Docker Hub, avoiding Docker Hub pull failures |
| [Affirm/iceberg#12](https://github.com/Affirm/iceberg/pull/12) | apache/iceberg#17194, apache/iceberg#17212 | Fix microsecond-timestamp sub-ms-remainder crash in `AvroToRowDataConverters` (`all-the-things#222721` review, darthburrito); backported to `flink/v2.1`, `flink/v2.0`, `flink/v1.20` |
| [Affirm/iceberg#13](https://github.com/Affirm/iceberg/pull/13) | apache/iceberg#18101, apache/iceberg#18196 | Preserve `RowKind` in `DynamicIcebergSink`'s `DataConverter` instead of defaulting schema-mismatch row rebuilds to `INSERT` (`all-the-things#222721` review, dcagney); backported to `flink/v2.1`, `flink/v2.0`, `flink/v1.20` |
| [Affirm/iceberg#14](https://github.com/Affirm/iceberg/pull/14) | apache/iceberg#17437, apache/iceberg#17590 | Reuse loaded `Catalog` in `TableSerializerCache` instead of reloading (and leaking a REST HTTP client) on every schema-evolution cache miss (`all-the-things#222721` review, dcagney); backported to `flink/v2.1`, `flink/v2.0`, `flink/v1.20` |

**Not yet published — rebase before tagging.** This entry documents intended `.2` contents so consumers can see what's coming; #12/#13/#14 are open, not yet merged, as of this PR. Rebase this branch onto `1.11.0-affirm-patch` after all three merge, confirm no further gaps, then tag `1.11.0-affirm-patch.2` at the resulting commit and rebuild/republish per `RUNBOOK_ICEBERG_PATCH_PUBLISH.md`.

## 1.11.0-affirm-patch.1

- **Base:** `apache-iceberg-1.11.0`

| Fork PR | Upstream reference | Scope |
|---|---|---|
| [Affirm/iceberg#10](https://github.com/Affirm/iceberg/pull/10) | apache/iceberg#16011, apache/iceberg#16648 | Fix duplicate commits in `DynamicCommitter` when the Flink jobId changes on restart (e.g. stop-with-savepoint, autoscaler resubmit); backported to `flink/v2.1`, `flink/v2.0`, `flink/v1.20` |
| [Affirm/iceberg#11](https://github.com/Affirm/iceberg/pull/11) | apache/iceberg#18071 | Fix CI by pulling MinIO test containers from Quay instead of Docker Hub, avoiding Docker Hub pull failures |
