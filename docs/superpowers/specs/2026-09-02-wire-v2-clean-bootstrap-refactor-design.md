# Wire v2 Clean-Bootstrap Refactor Design

## Status

Approved on 2026-09-02. This design combines the codebase-wide refactoring
audit with the clean-bootstrap HWM and ISR recovery design. The implementation
is intentionally breaking: no network, SDK, snapshot, Raft-log, topic-log,
transaction-journal, or consumer-offset compatibility is retained.

The detailed recovery rules remain normative in
`2026-09-02-hwm-isr-recovery-design.md`.

## Objectives

- Make the broker and Go client consume one protocol schema and codec.
- Preserve transaction and control-record semantics at every API boundary.
- Apply compression, authentication, TLS, deadlines, errors, and redirects
  consistently through one SDK transport.
- Make saga persistence transitions atomic and consumer lifecycle transitions
  race-free.
- Remove repeated scans, repeated segment mapping, and competing stream
  notifications from hot paths.
- Establish snapshot version 9 and fenced ISR recovery as the first supported
  recovery format.
- Split large handlers around typed decoding, validation, execution, and
  response encoding boundaries.

## Compatibility Boundary

The new binary accepts only Wire v2 and Storage v2. Legacy text envelopes,
prefix-based responses, duplicated SDK codecs, snapshots through version 8,
markerless topic commands, and legacy persisted state have no fallback path.

The broker never silently deletes persisted operator data. Unsupported state
causes a fail-closed startup error that names the clean-bootstrap requirement.
Operators must stop every broker and remove all Cursus persistence targets as
one verified unit before starting the new binary.

## Wire v2

`pkg/wire` is the sole owner of the network contract. Broker controllers and
the Go SDK depend on its exported request, response, record, error, framing,
compression, and negotiation types. No SDK-local or server-local copy of a
wire codec remains.

Every frame has a fixed header containing magic, protocol version, frame kind,
flags, command ID, status code, request ID, encoded payload length,
uncompressed payload length, and CRC32C. Encoded and decoded frames are both
limited to 64 MiB. Length and allocation checks occur before allocation or
decompression.

The initial negotiation request and response are uncompressed. Negotiation
selects one supported compression mode. Every later request and response sets
an explicit compression flag; a flag/negotiation mismatch is a protocol error.
CRC validation is applied to the transmitted payload.

Payloads use deterministic length-delimited binary encoding. Message records
include topic, partition, offset, timestamp, key, payload, event type, schema
version, aggregate version, metadata, transactional ID, transaction state,
transaction marker, and control-record status. Optional and required fields
are distinguished explicitly rather than inferred from zero values.

Responses use exact status and error codes. String-prefix and substring error
classification is removed. HELP and protocol documentation are generated or
tested against the registered command set so ghost commands cannot recur.

## SDK Transport

Producer, Consumer, EventStore, and Admin clients use one internal connection
and request path. It owns TLS, authentication, negotiation, compression,
deadlines, request IDs, response decoding, broker errors, leader redirects,
and bounded retry/failover. Configuration validation rejects nil or invalid
durations, incomplete credentials, unsupported modes, acknowledgements, and
compression settings before opening a connection.

Configuration fields must either affect runtime behavior and have a test or be
removed. Topic auto-creation obeys the configured switch and is never performed
implicitly when disabled.

## Transaction and Saga Semantics

Transactional data and commit/abort markers share the partition log.
`read_committed` returns committed application records only.
`read_uncommitted` returns pending and aborted records plus transaction metadata
and control markers.

The transaction visibility index is updated on append and marker application.
Resolved entries are pruned with the log-retention boundary. Reads do not clone
the full transaction map or repeatedly rescan an accumulated result set.

Saga state changes, Inbox completion, and Outbox insertion run in one storage
transaction. `ENQUEUED` means durable outbox registration; `SUCCEEDED` is
recorded only after delivery acknowledgement. Outbox delivery is at-least-once
with a stable idempotency key. Saga versions use compare-and-swap so concurrent
executions cannot overwrite one another.

## Consumer, Stream, and Disk Runtime

Consumer lifecycle is an explicit
`new -> running <-> rebalancing -> closing -> closed` state machine. A single
root context owns all workers and monitors, all goroutines are tracked, and an
assignment generation fences stale fetches and commits.

Partition notifications use broadcast generations rather than a shared channel
that lets streams steal wakeups from one another. Scheduling is manager-owned
instead of allocating independent ticker sets for every stream.

Segment descriptors and memory maps are reused through a bounded,
reference-counted cache. Retention cannot remove an in-use segment. The read
path avoids per-poll stat/open/map/close cycles and exposes benchmarks for
steady-state polling and multi-stream fan-out.

## Storage v2 and Cluster Recovery

Snapshot version 9 is the only accepted snapshot version. Every partition has
`committed_hwm_version: 1` and an explicit numeric committed HWM, including
zero. TOPIC Raft commands require the same marker. Legacy, missing, malformed,
and unknown versions fail before materialization or local truncation.

Authoritative HWM reconciliation truncates only a local tail above the
committed HWM and fails when local LEO is below it.

An out-of-ISR replica may submit a catch-up proof only when local LEO and local
HWM exactly equal the current authoritative HWM. The proof includes broker,
topic, partition, leader epoch, and topic lifecycle epoch. Authenticated
heartbeat carries proofs but never changes ISR directly. The Raft leader and
FSM independently validate the proof before applying an idempotent
`ISR_CATCHUP` command. ISR order follows replica-set order.

## Package Boundaries

- `pkg/wire`: schema, frame, codec, compression, errors, command registry.
- `sdk/internal/transport`: SDK connection and request lifecycle using
  `pkg/wire`.
- controller command packages: typed decode, validation, execution, encode.
- cluster recovery packages: snapshot v9, HWM reconciliation, catch-up proof,
  heartbeat submission, and Raft application.
- topic/disk packages: transaction visibility and cached segment reads.
- SDK runtime packages: consumer state machine and atomic saga persistence.

Existing large files are split only along these ownership boundaries. Pure
movement and behavior changes are kept in separate commits where practical.

## Error Handling and Observability

Protocol, validation, authorization, fencing, storage-version, and retryable
leadership failures have stable codes. Logs contain request IDs and structural
metadata but redact credentials, payloads, message keys, and authentication
material. Metrics cover protocol failures, decompression rejection, ISR proof
accept/reject reasons, stale consumer workers, segment-cache behavior, and
under-replicated partitions.

## Verification

- Unit and conformance tests share golden Wire v2 frames between broker and SDK.
- Fuzz tests cover frame lengths, decoding, decompression limits, and record
  optionals.
- Snapshot and Raft tests cover every version and HWM-marker failure mode.
- ISR tests cover identity, replica membership, HWM, leader epoch, lifecycle
  epoch, idempotence, and proof generation.
- Saga crash-point tests cover each atomic persistence boundary.
- Consumer tests cover concurrent close and rebalance generations.
- Benchmarks compare transaction reads, cached disk polling, and stream fan-out.
- Required gates are targeted non-cached tests, `go test ./...`, `go vet ./...`,
  configured lint, `git diff --check`, race tests where CGO is available, and an
  opt-in three-node clean-bootstrap/rolling-restart Docker E2E.

The E2E publishes fixture data once, restarts every broker sequentially without
another publish, waits after each restart for ISR size three, and requires zero
under-replicated partitions.

## Delivery

Implementation is delivered as multiple signed logical commits in one ready
pull request. Initial automated reviews are inspected and actionable feedback
is addressed before final handoff. The implementation does not merge the PR,
delete production data, change deployment configuration, pin an image, or
perform a rollout.
