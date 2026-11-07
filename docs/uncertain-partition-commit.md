# Uncertain partition commit recovery

## Design and regression contract

Kafka's accepted [KIP-101](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=177052956)
requires leader-history evidence instead of treating a stale local watermark as
permission to truncate. Kafka 4.0.0's [ReplicaFetcherThread](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/ReplicaFetcherThread.scala)
appends fetched records before advancing its watermark and truncates using the
leader's epoch response. Adopt the preservation rule, while keeping Cursus's push
replication and separate fenced Raft PARTITION_COMMIT command.

Implemented correction: once ISR replication has succeeded and a commit has been
attempted, retain the exact append and partition ownership while retrying that
same idempotent commit. Do not re-append, re-replicate, or roll back to a stale
local HWM on an uncertain commit reply. Recheck the leader/lifecycle fence before
retrying. On shutdown, fence change or unresolved bounded wait, preserve the
tail and mark reconciliation pending before releasing ownership. Never turn an
uncertain reply into a successful acknowledgement without commit evidence.

This trades temporary partition availability for retained history. Retries send
metadata commands, not data batches; they add no retained-log rewrite or
partition-record decoding, hashing or transfer. Raft metadata transport still
adds work. Repair after a changed fence still belongs to existing
verified recovery. The retry window is the existing five-second FSM allowance;
an in-flight commit RPC retains its own timeout. If the outcome is still unknown,
later local HWM application alone does not clear the reconciliation fence. This
is a conservative fallback, not a new automatic repair mechanism for arbitrary
outages. No wire or storage format changes are needed.

Before implementation, regressions must demonstrate: an uncertain reply followed
by delayed commit retains the original offset/payload, holds write ownership,
replicates data once and eventually commits; shutdown and fence change retain
bytes and block new writes; normal accepted commits and permanent replication
rejections keep their existing contracts. Then rerun interrupted mixed-load
recovery with its original zero-error/progress checks and full post-heal identity
checks, recording per-process transfer/read/write/hash/decode work. Large-replica
recovery delay is a separate finding until evidence connects it to this path.

## Validation and measured recovery work

The pre-fix deterministic regression fails: after the lost commit response, the
retained end falls from 1 to 0 or write ownership is released. The corrected
tests cover delayed success, changed epoch, shutdown and unresolved timeout.
A real FSM/commit-executor test loses the first Raft reply, retries the commit,
applies the original delayed entry, and checks unchanged retained log bytes.
The full controller package passes with the Linux race detector (359.510 seconds),
covering replication, consumer metadata, transactions and stream paths.
The changed-package static check reports zero issues. Initial corrected
mixed-load windows complete 123, 129 and 107 offset commits without foreground
errors, while exposing the post-heal verification sizing problems below.

Detailed recovery logs distinguish source admission backoff from divergence:
after source stalls are removed, the receiver advances from offset 64 through
3,151 with successful incremental batches. The old generic metadata
wait allowed only 60 seconds per partition. The >32 MiB payload alone takes
over 64 seconds at the configured 512 KiB/s per-source limit, before wire
overhead and outstanding source stalls/backoff. The post-heal check now uses one
shared deadline: 60 seconds for control/backoff plus twice the seeded payload
bytes divided by the default per-source rate. The journal records these inputs.
This corrects test sizing; it does not increase production quotas, relax the
50-second zero-error/progress contract, or skip post-heal content verification.
The full-history reader uses the same byte-sized allowance for the large topic;
its previous 30-second timeout was also below the transfer floor. Inspection of
the two-replica foreground topic now identifies a configured peer instead of
assuming that every topic has three replicas.

A cold-start attempt also exposed a fixture setup race before fault injection:
all three brokers were configured to bootstrap, and one received the initial
Raft entry before its own bootstrap call, then exited with `ErrCantBootstrap`.
The isolated fixture now follows `test/cluster` and enables bootstrap only on
broker 1; the same three-member static voter set and restart volumes are retained.
This does not change production bootstrap error handling or certify concurrent
bootstrap attempts by multiple nodes.

A separate two-node restart failure was not a slow data transfer: a surviving
Raft connection accepted a different broker's response after IP reuse. The
[peer-identity correction](raft-peer-response-identity.md) rejects that evidence
and reconnects. This is distinct from uncertain partition-commit ownership.

The corrected interrupted mixed-load E2E passes in 299.54 seconds. During its
50-second window all 120 produce, consume and offset-commit operations succeed;
there are no empty reads or foreground errors. Small-replica ISR recovery is
observed at 11,418 ms. After healing, full record identities match for 2, 129,
4,097 and 3 records in the four recovery topics, plus all 120 acknowledged live
records on both foreground replicas. The final committed consumer offset also
matches the consumed history. The shared post-heal allowance is 193 seconds for
34,686,641 payload bytes, independently of the unchanged foreground window.

Before oracle readback, the replacement receiver's process-lifetime counters are:

| Stage/unit | First corrected run | With Raft peer-ID checks |
| --- | ---: | ---: |
| Catch-up RPC transfer bytes | 36,154,875 | 36,187,579 |
| Lineage logical read bytes | 419,168,053 | 419,217,167 |
| Lineage logical write bytes | 15,024 | 15,392 |
| Lineage canonical hash bytes | 294,304,115 | 294,329,476 |
| Lineage decoded frames | 35,257 | 35,262 |
| Retained-page logical read bytes | 34,058,462 | 34,083,528 |
| Retained-page decoded frames | 4,099 | 4,103 |

The sweep with Raft peer-ID checks passes in 288.54 seconds: 123 successful
produce/consume/offset commits, zero foreground errors or empty reads, small ISR
recovery at 15,687 ms, and complete post-heal record/offset verification. The two
runs have different successful foreground counts and scheduling; these figures
are recovery-work evidence, not a controlled throughput comparison.

These are stage-specific totals since that receiver restarted, not physical
device I/O or total log-write bytes. They include failed/retried work and startup
recovery. The debug evidence uses legacy recovery protocol 1; the substantial
read/hash amplification is visible and is not a claim of constant-cost recovery.
No cross-process subtraction or sender-plus-receiver network sum is used.
