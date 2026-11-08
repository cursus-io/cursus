# Recovering an uncertain partition commit

## Observed failure

The compound-fault run on 2026-11-07 completed nine rounds, then stopped during round ten. The partition leader retained its replicated tail when a Raft HWM commit could not find a leader. Raft elected a leader about two seconds later, but the partition's untyped reconciliation error continued rejecting every publish until the test deadline. This attempt did not complete 24 hours. It was independent of the earlier external container deletion.

## Kafka comparison and intended behavior

Kafka 4.1.0 checks leader ownership, the high watermark and minimum ISR before completing a delayed produce. Expiration returns the unresolved request outcome; it does not itself prove that an appended record must be removed. See [DelayedProduce](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/server/DelayedProduce.scala) and [Partition.checkEnoughReplicasReachOffset](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/cluster/Partition.scala#L1001-L1032).

Cursus should preserve that separation between an unknown response and authoritative commitment. Unlike Kafka's data-replication watermark, Cursus persists partition HWM through Raft. Recovering Raft alone therefore does not clear a local unknown-commit fence.

The repair retains the exact original command, partition leader/lifecycle fence and target HWM when the existing bounded commit retry expires. A later publish, under partition write and replication-mutation ownership, may resume only that typed outcome. It validates the retained records and current authority, replicates the same records to the current ISR, rechecks the fence and ISR, and submits the original HWM and consumer route. Only successful local application can release the exact marker. No append, offset reuse, truncation or history replay is permitted during reconciliation.

Unknown storage/quarantine errors, changed leader/lifecycle/replica assignments, missing or conflicting bytes, insufficient ISR and stale consumer routes remain fenced. ISR membership may change between attempts: the retained batch must satisfy the current ISR before commitment. Recovery is request-driven and bounded to one replication/commit attempt per incoming request. A restart loses the in-memory command proof and continues using existing startup/leadership recovery; this change does not invent a persisted recovery protocol.

The same pending outcome also appeared in the cooperative consumer fault test. Committed reads must continue to fail closed while it is pending, but report the existing retryable `recovery_busy` protocol code instead of terminating consumers with `command_failed`. This classification applies only to a typed commit outcome with no accompanying storage/index/bootstrap/rollback failure. It must not make generic internal errors retryable.

The safety cost is continued unavailability when proof is missing. With valid proof, a recovered metadata quorum can resume writes without a broker restart. Recovery work is bounded by the original batch: read and compare its records, retransmit it to current remote ISR members, and apply one metadata commit. Local retained log bytes rewritten or truncated must be zero. No committed-prefix scan is added.

## Checks defined before implementation

- Hold Raft commits unavailable beyond the bounded retry; restore Raft and retry the same producer identity. Require original offset, one retained copy, committed visibility and a successful next append.
- Compare retained log files before and after reconciliation; report batch payload bytes and records, not only request counts.
- Reject changed leader/lifecycle/replicas, insufficient ISR, stale consumer route, replaced marker, inconsistent HWM and conflicting retained bytes; preserve the original tail and fence.
- Confirm a delayed original Raft command remains idempotent after recovery, cancellation cannot grant an ACK, and unrelated storage/index/bootstrap failures cannot be cleared.
- Run the compound-fault schedule with the repair before restarting a fresh 24-hour campaign. Do not aggregate interrupted attempts into a pass.

## Validation results

The unchanged baseline fails the producer retry after a five-second Raft outage. The repaired native controller/topic checks pass (20.159 s / 1.290 s), and Linux race checks pass (16.807 s / 1.102 s). The recovered single record contains 8 payload bytes; retained log files are byte-identical before and after recovery (zero local log bytes rewritten). A two-record proof checks contiguous batch recovery and rejection of changed authority, replica assignment, payload, sequence, route and target. Independent transaction/producer index failures remain fenced. Compound-cluster validation is still pending; no 24-hour pass is claimed.

The cooperative rolling-restart regression passed three consecutive runs after this repair (126.90 s, 58.99 s and 44.30 s), with zero duplicate deliveries under an unchanged grant. The middle run replayed two 133-byte records during a member handoff, within the existing oracle bounds. Focused checks in an isolated source copy containing all five reliability changes also passed: controller 7.213 s, topic 0.716 s, SDK 0.544 s and E2E helpers 1.131 s. This is separate from the ongoing 12-round compound campaign.
