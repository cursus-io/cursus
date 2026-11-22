# Transaction Raft apply outcomes

This note records the design comparison made before changing distributed
transaction rollback behavior.

## Kafka comparison

Kafka's transactional producer keeps the same producer identity and
transactional operation across retryable failures. KIP-98 defines producer
epochs and transaction coordinator state as the fencing and recovery boundary,
and Kafka's `TransactionManager` retains in-flight transactional state rather
than assuming that a timed-out coordinator request did not execute.

Primary sources:

- [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98%2B-%2BExactly%2BOnce%2BDelivery%2Band%2BTransactional%2BMessaging)
- [Kafka producer transaction API](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
- [Kafka 4.1.0 TransactionManager source](https://github.com/apache/kafka/blob/4.1.0/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java)

Cursus adopts the same safety rule: an acknowledgement failure after a
transaction-coordinator mutation may be an unknown outcome, so the broker must
preserve the exact producer epoch, transaction revision, request assignment,
and decision being retried. A client may retry the same operation; it must not
infer that the mutation was rejected.

Cursus deliberately differs in implementation. Kafka uses its transaction
coordinator log and producer request state machine. Cursus stores coordinator
snapshots in its existing Raft FSM and exposes textual internal routing. This
change therefore classifies each Raft attempt as `not_accepted`, `rejected`,
`applied`, or `unknown` instead of importing Kafka's request queue or wire
protocol.

## Implemented behavior

- `not_accepted` and deterministic FSM rejection may roll back a speculative
  local mutation.
- `unknown` preserves the candidate snapshot because Raft or a forwarded leader
  may already have applied it.
- Rollback is conditional on the current snapshot still matching the complete
  attempted snapshot and on no Raft apply having crossed the transaction's
  shard since the local mutation began. A newer or exact replicated apply is
  never overwritten by a late failure.
- Retried `BEGIN_TXN` for the same producer and epoch reuses the existing open
  snapshot without changing its deadline or revision.
- Retried legacy publishes reuse an identical producer sequence and reject a
  changed payload instead of consuming capacity or staging a duplicate.
- In standalone mode, a journal record is treated as applied after its fsync
  boundary even if the manifest or a later compaction rewrite fails.

This classification improves safety at a small availability cost: an ambiguous
failure can leave the transaction open until the same request is retried or
normal timeout recovery aborts it. The unknown-outcome path adds no recovery
records or extra transaction-payload copy. The retry may repeat one `TXN_SYNC`
request and its snapshot bytes, while idempotent revision checks prevent
duplicate state or record application.

Regression coverage distinguishes a pre-accept failure from a durable remote
apply with a lost response, checks same-identity retry, verifies local/remote
snapshot convergence, and proves that a stale rollback cannot replace a newer
snapshot. Recovery efficiency remains bounded by the existing snapshot size:
one ambiguous retry can resend at most one transaction snapshot and performs
one guarded comparison plus the normal Raft/FSM work.
