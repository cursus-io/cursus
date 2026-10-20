# Bounded diagnostic observations

## Design and evidence

The October 20 incident artifacts identify shared I/O stalls and synchronous
diagnostic paths, not a proven deadlock or a regression in checkpoint suppression.
The starting revision is `fa218d884c02f43e927705748ab8b64a6a554d45`.
`HealthState.report` invokes callbacks synchronously; its context cannot interrupt
callbacks that ignore it. Disk readiness performs stat and statfs; disk runtime
collection also waits on handler locks held by fsync. Topic and transaction
snapshots can indirectly wait on I/O too.

The selected approach runs fixed, sequential diagnostic workers and publishes
immutable memory snapshots. A per-request timeout goroutine was rejected because
uncancellable calls would accumulate. Replacing every storage lock with atomics
was rejected because it expands the durability change surface unnecessarily.

### Kafka comparison (before implementation)

- [Completed KIP-112](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/67638402/KIP-112+Handle+disk+failure+for+JBOD)
  separates broker availability from failed log directories. Adopt explicit
  failure state; do not pretend a failed observation means zero usage or healthy
  storage. Its JBOD replica reassignment policy is not Cursus's health contract.
- [Kafka 4.1.0 LogDirFailureChannel](https://github.com/apache/kafka/blob/4.1.0/storage/src/main/java/org/apache/kafka/storage/internals/log/LogDirFailureChannel.java)
  keeps an in-memory failure map and a bounded, deduplicated notification queue.
  Adopt memory-only observation and bounded background work. Kafka's directory
  failures stay offline until restart; Cursus transient probe failures may recover
  after a timely successful probe, without clearing terminal write failures.
- [Kafka 4.1.0 LogManager](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/log/LogManager.scala)
  publishes offline-directory gauges from its in-memory directory set and retains
  flush/checkpoint ordering at shutdown. [Official monitoring documentation](https://kafka.apache.org/40/operations/monitoring/)
  exposes flush latency separately from offline status. Cursus additionally needs
  observation freshness and in-flight state; Kafka does not establish our HTTP
  timeout or snapshot-expiry policy.

These references describe implemented Kafka behavior. The expiry policy and
workers below are the Cursus design, not a claim of Kafka equivalence.

## Contract and regression criteria

- `/live` stays process liveness (200); storage failures do not request restart.
  `/ready` retains startup/dependency gating, GET/HEAD, JSON status/checks, and
  200/503. Initial, expired, over-budget and stopped observations fail closed.
- Readiness/process sampling defaults: one second between completed attempts,
  two-second execution budget (the previous health timeout), five-second maximum
  result age. Runtime metrics use a five-second interval and 15-second maximum
  age because their segment accounting costs more I/O. A late
  success is not accepted as healthy. New observations run immediately at startup.
  No existing YAML/env/CLI keys, disk thresholds or HTTP probe timeouts change.
- Request handlers read published data only. Metrics from unavailable observations
  are omitted, never synthesized as healthy zero. Age, completion time, failures,
  validity, timeout and in-progress state remain observable.
- Hold fake filesystem calls and actual I/O-shared locks; repeated concurrent
  HTTP probes/scrapes must finish while the holder is blocked. Verify initial
  state, expiry, failures, late success rejection and subsequent recovery. Count
  attempts/concurrency across many timeout intervals; require no accumulated work.
- Keep fsync, flush barriers, durable checkpoints, ACK and replication behavior
  unchanged. Run existing storage/recovery tests. Diagnostic recovery performs
  metadata/stat work only: no payload replay, copied recovery bytes or checkpoint
  writes. Re-observing disk usage is O(handlers + segments), with two path stats
  per distinct segment. A missing path allows one extra pass for that handler,
  after refreshing membership, to reconcile retention/truncate races. This stays
  O(handlers + segments), with no unbounded retries or request-driven work.

## Availability and lifecycle limits

There is no hard wall-clock guarantee if the process/OS cannot schedule threads
or the client cannot read the response. The guarantee is isolation from storage
I/O and application locks in diagnostic handlers. Context cancellation cannot
interrupt an OS stat/statfs or mutex wait. Stop must cancel future work without
joining such a call; at most the fixed worker count remains blocked. Normal broker
durable shutdown can still wait for fsync; this change does not bypass it.

Sampling introduces a bounded detection/recovery delay and freshness expiry can
reduce availability under load. This is intentional: an old healthy result cannot
certify current storage. There are three sequential workers per exporting broker:
readiness, runtime metrics and default process/counter gathering. Only the readiness worker runs
when the exporter is disabled. Each worker has at most one outstanding attempt. Prometheus may fan out its fixed set of
default collectors within that one attempt. All dynamic runtime metrics may be
omitted when their collection is blocked; diagnostic status and independently
sampled process/counter metrics remain available. The default registry is also
sampled to keep Linux procfs access out of the request path. Observation failures
count attempts, not HTTP scrapes.

## Snapshot consumption and compatibility

`NewHealthState` and `NewCollector` do not perform work. Their owners call `Start`
and defer `Close`; the ordinary broker and both recovery diagnostics paths do so.
External callers using the exported constructors must also adopt this lifecycle.
`metrics.Handler` callers outside the server helper must close its returned
handler; the server helper closes it on shutdown, listener failure and Serve exit.
Close never signals that durable storage shutdown has completed.

`cursus_observation_*{component="readiness|runtime|process"}` exports validity,
in-progress, deadline overrun, stale status, last completion, age, last failure,
and completed failure count. Timestamp/age/last-failure series are absent until
the first completion. A stuck first attempt exports valid=0, in_progress=1 and
(after two seconds) timed_out=1 without inventing a completed result. A storage
stat failure invalidates the runtime observation instead of exposing partial
usage/capacity as zero. Missing segment/index paths trigger one membership refresh
and resample: completed retention can remove the segment, and truncate recreates
its index before releasing the mutation lock. Persistent absence after that bounded
retry still invalidates the observation; it is never counted as measured zero. Successfully
measured capacity below the configured headroom threshold is different: it remains
a valid runtime observation with actual free/total bytes and headroom readiness
`0`, while broker readiness and write admission still fail. Both Helm charts retain
the disk-headroom alert and add `CursusObservationUnavailable` when any component's
observation stays invalid for one minute, including initial absence, failure,
expiry and deadline overrun. A successful HTTP scrape alone does not establish
healthy storage. Existing metric names and labels remain unchanged when
the observation is valid. Removed dynamic labels disappear on the next valid
sample, or when the previous observation expires.

Readiness adds an `observation` JSON object and keeps existing `status` and
`checks` fields. New check registration/replacement invalidates the earlier
result until a pass over that check set completes. The two-second budget now
covers the entire sequential health pass. Cluster topology, broker registration,
metadata recovery and other existing dependency checks retain their meaning.

## Local validation

- Linux Go 1.26.6: affected observation, observability, metrics, server and disk
  packages pass `go test -race` including real HTTP probes while both storage
  observation paths remain blocked. Windows targeted snapshot/endpoint tests
  also pass with normal local permissions; the sandbox denies loopback sockets.
- The full `go test -race ./pkg/... ./cmd/...` run passed, including topic
  checkpoint crash/restart, replication/FSM recovery and ACK regression suites.
  Affected packages were rerun after follow-up changes.
- `go build ./cmd/...` passes on Linux; changed-package `golangci-lint` reports
  zero issues. No workflow trigger exceptions or production workloads were used.
- Fault tests cover blocked statfs, actual headroom/manager/handler mutexes,
  failure, expiry, initial absence, late success, recovery and non-blocking stop.
  Eight concurrent clients issue 160 liveness/readiness requests during a stall;
  another integrated test issues 60 requests across all three endpoints with a
  one-second HTTP client deadline while only two storage observations remain
  outstanding. Runtime and process tests add 40 and 100 repeated scrapes.
- Headroom regression tests distinguish measured low capacity from failed statfs,
  preserve admission rejection, and verify recovery. Both rendered Helm charts
  pass the same [promtool rule tests](https://prometheus.io/docs/prometheus/latest/configuration/unit_testing_rules/)
  for disk-headroom alerts, failed/expired/blocked/initial observations, grace
  periods, namespace isolation and alert resolution. Run
  `bash test/monitoring/check-rules.sh STANDALONE_RENDER.yaml CLUSTER_RENDER.yaml`
  with monitoring enabled and namespace `brokers`; the deployment CI does so.
- Retention and truncate/index-recreation race tests verify one bounded resample,
  correct retained byte counts and continued failure for persistently missing
  files. The retry performs only metadata queries, not index rebuilding or writes.
- Recovery work remains the existing storage protocol: no added replay, payload
  bytes copied or checkpoint publication. Storage sampling uses metadata/stat
  operations; unchanged durable barrier code is covered by existing disk, topic
  and replication tests. This is not a production throughput or latency claim.
