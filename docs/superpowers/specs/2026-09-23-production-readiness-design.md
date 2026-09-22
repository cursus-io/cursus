# Production Readiness Design

## Status

Approved in chat on 2026-09-23.

## Goal

Make the broker's existing durability and transaction features predictable at
larger retained state and during long-running production operation. This work
reduces avoidable transaction-coordinator cost, makes validation triggers match
the code they protect, provides reproducible durable-storage evidence, and
turns storage, upgrade, and Kubernetes boundaries into explicit operator
contracts.

## Scope And Delivery

The work is delivered as four dependent, independently reviewable changes:

1. transaction journal compaction and transaction-state access regressions;
2. CI coverage, compatibility/compaction documentation, and contract tests;
3. opt-in durable-storage soak and recovery benchmark harnesses;
4. upgrade/recovery tooling and a production three-node Kubernetes chart.

The existing standalone Helm chart remains a one-replica deployment. Cluster
deployment is a separate chart; raising `replicaCount` on the standalone chart
must remain an error.

## Non-goals

- Supporting arbitrary mixed-version rolling upgrades across incompatible
  on-disk, Raft-snapshot, or internal-record formats.
- Relaxing `read_committed`, fencing, transaction recovery, acknowledgement, or
  quorum guarantees to improve a benchmark result.
- Claiming that a tmpfs benchmark represents physical-disk durability latency.
- Adding distributed log compaction or treating an empty payload as a tombstone.

## 1. Transaction State Cost

### Journal compaction

The standalone transaction journal currently decides to compact from total
physical record count or file size. After compaction, the file may legitimately
retain 256 or more distinct latest transactional IDs. A later append therefore
re-enters compaction even though no old record can be removed.

The journal will track the size and count of the authoritative latest snapshot
set in addition to its physical size and record count. The removable debt is:

```text
supersededRecords = physicalRecords - latestTransactionIDs
supersededBytes   = physicalJournalBytes - latestSnapshotBytes
```

Only removable debt triggers automatic compaction. Thresholds retain their
current operational meaning: at least 256 superseded records or 16 MiB of
superseded bytes. A journal with many unique, live IDs is not repeatedly
rewritten merely because its useful state is large. The state is reconstructed
while loading an existing journal, so a restart does not reintroduce the
problem.

An append updates the latest-set accounting under the existing journal mutex.
Compaction continues to write a same-directory temporary file, sync it, replace
the authoritative file atomically, and sync its directory. A compaction failure
does not acknowledge the pending transition or corrupt the previous journal.

### Scoped transaction snapshots

Request-path transaction rollback and persistence use `Manager.Snapshot(id)`,
which locks only the owning shard. `ExportState()` is reserved for whole-state
operations such as Raft snapshots and journal rewrites. Tests must prove a
single-ID snapshot can complete while an unrelated shard is held and benchmarks
must measure the operation against increasing retained-ID counts.

## 2. Validation And Public Contracts

### CI triggers

The E2E workflow must run when `sdk/**` or `internal/**` changes. Unit coverage
may exclude command packages from its percentage calculation, but a separate
race-enabled `go test ./cmd/...` job must execute command tests. Existing build,
lint, unit, and E2E jobs remain independent failure signals.

### Documentation contracts

Documentation will state that standalone keyed compaction preserves
transaction/control records, is not distributed compaction, has no tombstone
grace period, and requires backups to preserve each `.log`, matching
`.log.compacted-<size>` sidecar, and `.index` from one generation.

Transaction metric documentation will be reconciled with the implementation.
If state-count and oldest-active metrics are not exported, it must say so; if
they are exported in a later change, the collector and both reference documents
must change together and be covered by a collector test.

## 3. Durable Performance Evidence

The fast Docker benchmark remains a correctness/routing workload and keeps its
tmpfs limitation explicit. A separate opt-in harness accepts a host-provided
durable log directory, workload duration, retained-data target, and result path.
It emits machine-readable metadata: software revision, Go version, OS,
filesystem/storage identity supplied by the operator, broker settings, topology,
message and transaction counts, correctness counters, latency percentiles,
recovery time, journal growth, segment/file counts, and disk-space observations.

Required scenarios are:

- cold and warm cache with retained data exceeding memory;
- transaction commits while consumers rejoin and compactable topics age;
- replica interruption/catch-up and transaction coordinator recovery;
- restart/recovery after acknowledged writes with no missing or duplicate
  committed records or offsets.

This is an opt-in operator or scheduled validation workload, not a mandatory
pull-request job. It fails closed on incomplete correctness counters, missing
metadata, broker/consumer failure, or an unusable result path.

## 4. Upgrade, Recovery, And Kubernetes Operations

### Compatibility and recovery

The supported upgrade contract is a compatibility matrix covering broker release,
topic manifest, transaction journal, internal offset record, and Raft snapshot
versions. Before a format boundary, operators run a read-only preflight that
validates a backup generation and reports every incompatible artifact.

The supported procedure is a coordinated whole-cluster transition: drain client
traffic, verify a healthy quorum and ISR state, take and validate a consistent
backup generation, stop all members, upgrade all binaries/configuration, start
a quorum, then run readiness, metadata, offset, and transaction recovery
checks. Rollback across an unreadable format boundary restores the validated
backup with the previously compatible release; it is not a live binary
downgrade. The procedure is exercised against representative persisted data.

### Kubernetes cluster chart

A new cluster chart provides exactly three StatefulSet replicas by default and
rejects invalid even/quorum-breaking topology values. It supplies a headless
service and ordinal-derived stable broker, Raft, and internal addresses; one PVC
per member; required internal mTLS configuration; a PodDisruptionBudget; and a
termination sequence that removes readiness before graceful shutdown.

Templates validate required quorum, advertised DNS, storage, and TLS values at
render time. Tests render the chart and assert stable addresses, PVC claims,
headless discovery, PDB, and refusal of invalid topology. The runbook covers
initial bootstrap, replacement of one member, restore from a validated backup,
and ordered maintenance restart.

## Failure Handling And Invariants

- An acknowledged transaction transition remains durably recoverable.
- Journal compaction never drops the latest snapshot for any transactional ID.
- A failed journal replacement leaves the previous journal recoverable.
- CI path filtering never skips the relevant broker-boundary test for SDK or
  internal runtime changes.
- Benchmark output is not presented as a durability result unless its durable
  storage metadata and correctness counters are complete.
- Unsupported mixed-format deployments fail before serving traffic.
- Cluster deployment does not claim quorum safety when required TLS, stable
  discovery, persistent storage, or topology constraints are absent.

## Verification

1. Unit tests cover distinct-ID journals at and above both compaction thresholds,
   repeated-ID debt, restart accounting, failed replacement, and recovery.
2. Manager/controller tests prove scoped snapshots do not wait on unrelated
   shards; race tests and retained-state benchmarks cover the request path.
3. Workflow tests or static assertions cover SDK/internal triggers and command
   test execution; all changed Go packages run with the appropriate unit/E2E
   suite.
4. Durable harness integration tests validate metadata/result schema and failure
   cases; a documented physical-disk run demonstrates the required scenarios.
5. Storage preflight, compatibility-matrix, backup-validation, and recovery
   rehearsal tests cover accepted and rejected states.
6. Helm rendering and chart tests cover valid three-node output and invalid
   topology/configuration rejection.

## Acceptance Criteria

- No automatic journal compaction occurs solely because live distinct IDs or
  their useful bytes exceed a threshold.
- Superseded records remain compactable and restart recovery preserves each
  latest transaction snapshot.
- Relevant SDK/internal/cmd changes exercise their intended CI validation.
- Durable performance results are reproducible, explicit about storage, and
  include correctness plus recovery evidence.
- Operators have tested, versioned instructions for backup, upgrade, rollback,
  and recovery.
- A supported three-node Kubernetes deployment has stable identity, persistent
  storage, internal transport security, and quorum-aware maintenance behavior.
