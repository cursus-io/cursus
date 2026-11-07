# Acknowledged-write fault tests

## Contract and Kafka comparison

This first increment tests retained, nontransactional, uncompacted records with
three replicas, `min_insync_replicas=2`, `acks=all`, and topic/request idempotence.
It extends the existing cluster harness; it does not change broker semantics.

Kafka 4.0.0's [replication system test](https://github.com/apache/kafka/blob/4.0.0/tests/kafkatest/tests/core/replication_test.py)
checks acknowledged records after leader shutdowns and repeated hard kills.
Adopt that client-history oracle and distinct clean/crash failure models.
[Accepted KIP-101](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=177052956)
and the versioned [ReplicaFetcherThread](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/ReplicaFetcherThread.scala)
explain why matching watermarks alone cannot justify truncation. Cursus uses push
replication and Raft commit metadata, so test the response/commit boundary and
stale-leader fencing rather than copying Kafka's fetch protocol. Recovery must
preserve exact record contents, not merely record counts or a nonempty ISR.

Before implementation, the regression requirements are:

* Every acknowledged logical write survives with its original payload. A retry
  retains producer identity, epoch and sequence, and creates at most one record.
* A transport failure is unknown, never proof of rejection. Unknown writes may
  be present or absent until retried successfully. Broker errors are recorded
  separately without assuming they prove that no append occurred.
* The checker detects missing acknowledgements, duplicate IDs, changed payloads,
  unexpected records, offset collisions, order violations and replica divergence.
* A real successful publish response is consumed by a proxy and withheld from
  the client. The leader is then killed with SIGKILL, another leader accepts the
  same logical write, and the original broker rejoins using its retained volume.
* An old partition leader is isolated from both peers while its client port stays
  reachable. The majority elects a new leader and accepts writes. A fresh direct
  write to the old leader must not succeed. After healing, every replica and a
  public consumer must agree with the client history.
* Full ISR must remain stable for 65 seconds before final readback. A delayed
  group-join response is reconciled with the existing membership lookup helper,
  not retried as a fresh member or misclassified as rejection.
* Fault injection must be observed, not inferred from elapsed time. Required CI
  scenarios must actually run; skipped or absent scenarios fail the job.

Safety, availability and cost are reported separately. Failure to elect, resume
writes or restore full ISR fails availability; it does not count as a safety pass.
The test permits conservative rejection during uncertainty. It does not require
availability without a majority or certify all-replica power loss.

## Scope

The workload is deterministic and bounded. The broker-external JSONL journal
records request identities, payload hashes, invocation/completion times, target,
outcome and fault milestones. No transparent client retries hide attempts.
Independent checker mutation tests are runnable without Docker. Docker scenarios
use dedicated containers, ports, network and volumes, with firewall changes only
inside a test container's network namespace. The broker binary has no test hooks.

Recovery measurements include per-process logical read bytes, decoded frames,
canonical hash bytes and transferred bytes where existing metrics expose them.
These application counters do not measure physical device I/O.
Counter resets across a process restart are recorded as separate process intervals.
Samples before readback separate recovery from the catch-up reads performed by
the verifier; samples after readback also include that inspection work.
These small fixtures establish correctness, not a recovery amplification bound
or a production latency SLO. Full retained scans, bootstrap and cold/warm scaling
remain separate measurements; RPC counts alone cannot establish efficiency.

Transactions, retention, compaction, SDK auto-retry behavior, sustained concurrent
traffic, randomized schedules and disk/power faults are later increments. Their
different visibility/deletion contracts must not be inferred from this oracle.

## Running and interpreting evidence

From the repository root, run checker/proxy regressions without Docker:

```sh
go test -race -count=1 ./test/e2e-cluster -run '^Test(WriteHistoryOracle|AckFaultReplyProxy)$'
```

On a Docker host, run both fault scenarios (do not run two copies concurrently):

```sh
RUN_E2E_ACK_FAULTS=1 CURSUS_ACK_FAULT_RESULTS="$PWD/ack-fault-results" \
  go test -v -count=1 -timeout 25m ./test/e2e-cluster -run '^TestAcknowledgedWrites'
```

The reserved Compose project/network/container prefix is `cursus-ack-faults`;
host ports use a 23,000 offset from the base cluster fixture. The fixture deletes
only its dedicated volumes at setup/cleanup and preserves each volume during the
actual crash. The test broker runs unprivileged; root `iptables` commands execute
only inside the NET_ADMIN-enabled test container. No host firewall rules change.

CI runs the oracle, proxy and both scenarios, checks their JSON test events for
explicit passes, and uploads the JSONL client histories, fault milestones,
decoded record evidence and recovery-counter samples. Broker logs are included
in failed-test output. Timeline milestones expose time to new leadership and
full ISR, but the fixture's polling/verification overhead is included: these are
test recovery observations, not calibrated production latency measurements.
