# Cursus System Hardening TODO

This checklist tracks the repository-wide production hardening work discovered
after the standalone consumer metadata recovery incident. Changes are kept
on one branch and split into reviewable, signed work-unit commits.

## P0: Data integrity and credential safety

- [x] Prevent async append failures from reserving permanent offset gaps.
- [x] Return async publish enqueue failures instead of reporting false success.
- [x] Use one authoritative maximum record size across wire, serialization,
      append, read, compaction, and startup recovery paths.
- [x] Define and test the application-record durability boundary for `acks=1`.
- [x] Redact shared secrets, SASL tokens, and loaded TLS key material from startup
      configuration logs.

## P0: Cluster control plane and group consistency

- [x] Authenticate and optionally encrypt discovery membership commands.
- [x] Protect Raft transport or fail configuration validation when secure cluster
      transport cannot be guaranteed.
- [x] Construct cluster clients with non-zero timeouts and cover dynamic join.
- [x] Route `REGISTER_GROUP` through the authoritative cluster coordinator/Raft
      path and keep retries idempotent.
- [x] Close discovery, Raft, and Raft storage in deterministic shutdown order.
- [x] Separate distributed Raft-authoritative consumer state from strict local
      standalone metadata-log recovery.
- [x] Bound distributed `__consumer_offsets` growth without losing the latest
      group/offset state.

## P1: Network availability hardening

- [x] Enforce configurable connection limits and idle timeouts on client and
      internal listeners.
- [x] Bound discovery connections, request deadlines, and concurrent handlers.

## P1: Container and Kubernetes deployment

- [x] Make the default Helm chart render/install successfully, including the
      ServiceAccount and TLS Secret lifecycle.
- [x] Make `tls.enabled` actually enable broker TLS without embedding placeholder
      credentials.
- [x] Replace unsafe shared-PVC replica scaling with an explicit standalone
      deployment contract or a supported StatefulSet cluster layout.
- [x] Honor `CONFIG_PATH`, run the image as a non-root user, and avoid `/root`
      runtime paths.
- [x] Add a startup probe and deterministic termination grace behavior.
- [x] Fix ServiceMonitor label selection and avoid exposing health/metrics with
      the broker Service unintentionally.

## P1: Bounded state and observability

- [x] Compact/rewrite the standalone transaction journal so completed history
      does not grow without bound.
- [x] Export transaction state/recovery/age metrics and document alert signals.

## P1: Release validation

- [x] Add Helm lint/render validation and a non-publishing PR Docker build.
- [x] Run cluster chaos coverage in CI, including hard-kill recovery where the
      environment supports it.
- [x] Add regression tests for every item above, including read-only behavior and
      mixed-version/format compatibility where applicable.
