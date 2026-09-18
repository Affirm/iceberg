# Affirm Iceberg patch versions

This file is Affirm-local (not part of upstream Apache Iceberg). It records what each
`<upstream-version>-affirm-patch.<N>` release of this fork contains, so a consumer can tell exactly
which Affirm-local patches are baked into a given jar without reconstructing it from branch history.

Format: one section per published version, listing its upstream base and every Affirm-local fork PR
carried on top, each linked to its upstream reference where one exists. The git tag for a version
is the source of truth for the commit published to Artifactory.

## 1.11.0-affirm-patch.1

- **Base:** `apache-iceberg-1.11.0`

| Fork PR | Upstream reference | Scope |
|---|---|---|
| [Affirm/iceberg#10](https://github.com/Affirm/iceberg/pull/10) | apache/iceberg#16011, apache/iceberg#16648 | Fix duplicate commits in `DynamicCommitter` when the Flink jobId changes on restart (e.g. stop-with-savepoint, autoscaler resubmit); backported to `flink/v2.1`, `flink/v2.0`, `flink/v1.20` |
