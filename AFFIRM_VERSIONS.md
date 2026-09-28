# Affirm Iceberg patch versions

This file is Affirm-local (not part of upstream Apache Iceberg). It records what each
`<upstream-version>-PATCH.<N>` release of the 1.8.1 line of this fork contains, so a consumer can tell
exactly which Affirm-local patches are baked into a given jar without reconstructing it from branch
history. The 1.11 line keeps its own `AFFIRM_VERSIONS.md` on `1.11.0-affirm-patch`.

Format: one section per released version, listing its upstream base and every Affirm-local fork PR
carried on top, each linked to its upstream reference where one exists. From `1.8.1-PATCH.4` on, the
git tag for a version is the source of truth for the commit that was built. `PATCH.1` and `PATCH.3`
were built from untagged commits with a hand-written `version.txt`; the shas below are recorded from
branch history.

The jar ships only inside the Affirm pyspark wheel (`Affirm/spark@affirm-3.5.4`, `pom.xml`
`${iceberg.version}-PATCH.N`). Keep the uppercase `-PATCH.N` spelling; that pom interpolates it.

## 1.8.1-PATCH.4

- **Base:** `1.8.1-PATCH.3`

| Fork PR | Upstream reference | Scope |
|---|---|---|
| [Affirm/iceberg#16](https://github.com/Affirm/iceberg/pull/16) | apache/iceberg#12855 (fixes apache/iceberg#11239) | Deep-copy `ByteBuffer` and nested `GenericRecord` values in `GenericRecord.copy()`, so equality deletes on a binary key are no longer collapsed to the last record of each delete file (LAKE-7013) |

## 1.8.1-PATCH.3

- **Base:** `1.8.1-PATCH.1`
- **Commit:** `d4406cd29194` (merge of #4). Shipped in pyspark `2815!3.5.4+affirm.6`.

| Fork PR | Upstream reference | Scope |
|---|---|---|
| [Affirm/iceberg#4](https://github.com/Affirm/iceberg/pull/4) | apache/iceberg#15727 | Spark 3.5: scan-based dangling delete removal; pin the scan to the snapshot; drop column stats from retained dangling delete files (LAKE-6732) |

`PATCH.2` was built during #4's development and never shipped.

## 1.8.1-PATCH.1

- **Base:** `apache-iceberg-1.8.1`
- **Commit:** `1d358d27778a` (merge of #1). First shipped in pyspark `2815!3.5.4+affirm.4`; also in `+affirm.5`.

| Fork PR | Upstream reference | Scope |
|---|---|---|
| [Affirm/iceberg#1](https://github.com/Affirm/iceberg/pull/1) | apache/iceberg#12818, apache/iceberg#13352 | REST: stop retrying non-idempotent requests on 502/503/504 and raise `CommitStateUnknownException` on 503; allow retries for idempotent requests |
