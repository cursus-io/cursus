# Production Readiness Implementation Plan

## Checkpoint 1 — Transaction journal compaction debt

Files: `pkg/transaction/journal.go`, `pkg/transaction/journal_compaction_test.go`.

1. Add private live-record count/byte accounting to `Journal`.
2. Reconstruct it in journal load/tail repair by replacing the prior latest
   record contribution when an ID repeats.
3. Update it only after a successful append sync, reset it after a successful
   compaction/rewrite, and base automatic compaction on superseded debt.
4. Add regression tests for 256+ distinct IDs, repeated IDs, restart accounting,
   and byte debt. Run `go test ./pkg/transaction/...` and `go test -race
   ./pkg/transaction/...`.

## Checkpoint 2 — Scoped snapshot evidence and CI triggers

Files: `pkg/transaction/manager_shard_test.go`,
`pkg/controller/transaction_*test.go`, `.github/workflows/e2e-tests.yml`,
`.github/workflows/unit-tests.yml`.

1. Add a concurrency regression proving an unrelated locked shard does not block
   `Snapshot(id)`.
2. Add retained-ID benchmark coverage for request-state capture.
3. Trigger E2E on `sdk/**` and `internal/**`; run command tests with race
   detection separately from the coverage denominator.
4. Run focused tests, `go test ./cmd/...`, and workflow YAML validation.

## Checkpoint 3 — Durable benchmark and public contracts

Files: `test/e2e-benchmark`, `docs/reference/benchmark.md`,
`docs/reference/performance.md`, compaction/transaction operation docs.

1. Define versioned JSON result metadata and validate required fields.
2. Add opt-in host-durable log-directory and duration configuration; preserve
   the tmpfs workload as an explicitly separate fast correctness workload.
3. Add transaction/recovery scenario result collection and documentation.
4. Test parser/metadata failures and run the existing opt-in benchmark only
   when its Docker prerequisites are available.

## Checkpoint 4 — Upgrade/recovery contract and Kubernetes chart

Files: storage CLI, operations docs, new cluster Helm chart and template tests.

1. Implement read-only backup-generation preflight and an explicit version
   compatibility manifest/table.
2. Write/verify coordinated upgrade, restore, rollback, and recovery runbooks.
3. Add a separate three-member StatefulSet chart with stable identity, PVCs,
   headless discovery, mTLS requirements, PDB, and topology validation.
4. Render/test valid and invalid chart values and test preflight/recovery
   fixtures.

## Final verification

Run the focused suites after every checkpoint, then the complete unit/race
suite, relevant E2E suites, storage preflight fixtures, and Helm rendering.
Document any checks that require Docker, a Kubernetes cluster, or a physical
durable volume instead of representing them as locally verified.
