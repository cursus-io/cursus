# Stream append commit safety

Distributed `APPEND_STREAM` holds partition write and replication-mutation
ownership across append, replication, and commit. Different aggregate keys have
different stream locks but share the partition's offset space.

Previously this path released ownership after a forwarded `PARTITION_COMMIT`
reply without waiting for local Raft application. It also released ownership
after a replication or commit error. A transport error does not prove rejection:
replicas can already hold the record, and a submitted commit can still apply.
The next aggregate's write could reconcile against an older local HWM, truncate
the first record, and reuse its offset in the same leader epoch. A later commit
could then cover different records on different replicas.

## Implemented behavior

The stream path now uses the existing partition replication executor's commit
barrier. Before returning success and releasing ownership, it waits for local
Raft metadata with the matching leader, leader epoch, lifecycle epoch, and HWM,
then applies and flushes the local watermark. This wait is bounded by the existing
FSM apply timeout.

If replication or commit fails after append, the partition is marked as requiring
reconciliation before ownership is released. Its retained tail is preserved;
subsequent writes cannot truncate or reuse it. Committed reads also fail closed
while this existing partition recovery fence is set. Ordinary version conflicts
do not set the fence. This deliberately trades partition availability for data
preservation after an uncertain result, including failures that may ultimately
prove harmless.

The fence uses the existing in-memory reconciliation state. It is not a durable
recovery journal or automatic recovery service. Delayed Raft application alone
does not clear it. Recovery must establish authoritative history through the
existing fenced reconciliation mechanisms; restarting is not a data repair.
This change neither repairs pre-existing divergence nor selects a winning replica
by majority or by equal HWM. Those require retained-record evidence and the
separate replica recovery workflow.

## Kafka comparison

Kafka's accepted [KIP-101](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=177052956)
explains why an HWM alone is insufficient evidence for safe truncation around
leadership changes. Kafka 3.9.0's
[ReplicaFetcherThread](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/server/ReplicaFetcherThread.scala)
checks fetch offsets against the replica log end and uses leader-epoch recovery.
We adopt the safety requirement that an assigned offset must not be silently
reused while replication/commit outcome is unknown. Cursus uses push replication
and Raft-managed commit metadata rather than Kafka's follower fetch protocol, so
its local metadata barrier and conservative reconciliation fence are specific to
this architecture. An epoch match alone does not prove equal record contents.

## Validation and work bounds

Real-disk fault tests cover a lost commit reply followed by a different aggregate,
delayed application after a successful forwarded reply, failed replication,
normal successful appends, and ordinary version conflicts. The lost-reply test
fails on the previous implementation because offset zero changes from the first
aggregate to the second. It now checks both decoded records and exact retained
log bytes before/after the blocked write, then applies the delayed commit and
verifies that it still refers to the original record. Rewritten retained-log
bytes for the blocked write: zero. No recovery batch is transferred by this fix.

The added success barrier polls local metadata and applies a watermark; it does
not scan or transfer retained history. Failure handling sets a partition fence
instead of performing speculative rollback/replay. Existing stream-index
recovery can still read committed records; this is not a claim that the entire
stream append path has constant I/O cost. Actual replica repair cost belongs to
the separate recovery implementation and is not measured by these tests.
