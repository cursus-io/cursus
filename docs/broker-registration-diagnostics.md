# Broker registration diagnostics

## Baseline and Kafka comparison

The reported deployment revision `9139bf3` and main `14f7a68` share the same
registration path. A follower first tries local Raft REGISTER, then forwards
RAFT_APPLY REGISTER to the leader. Previously the expected local rejection was
an ERROR on every retry, while the final forwarding failure was DEBUG only.
Replicated metadata agreement alone does not prove internal TLS or broker
liveness: a broker can be inactive even when its Raft log is current.

Accepted [KIP-631](https://cwiki.apache.org/confluence/display/KAFKA/KIP-631%3A+The+Quorum-based+Kafka+Controller)
separates broker registration and heartbeat leases. Kafka 4.1.0's
[BrokerLifecycleManager](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/server/BrokerLifecycleManager.scala)
sends registration to the controller, reports authentication failures and retries
communication. Cursus adopts visible final outcomes and retry continuity. It
retains local Raft apply followed by leader forwarding, process incarnation,
capability/readiness publication and its existing one-second retry schedule.
It does not introduce Kafka's broker epoch protocol or startup timeout policy.

## Log contract

The discovery REGISTER method returns failures without logging them: callers
own the final outcome. The periodic worker emits WARN for its first failed
attempt, changed final cause/leader, and at most once per minute for an unchanged
cause. Logs retain the leader, final TLS/network/protocol error, local error,
failed-attempt count, elapsed time and suppressed count. Certificate validity
errors are grouped by certificate and validity interval, excluding the changing
current-time text; network errors exclude ephemeral local ports. Arbitrary
different error messages are distinct causes.

An INFO recovery event is emitted once the worker observes the current active
process registration in its local FSM. An RPC success alone is not recovery.
After another outage the first failure is logged immediately again. Cancellation
does not create a failure event. Other callers, including leader-election setup,
retain their own error handling.

## Safety, availability and regression checks

TLS verification, heartbeat expiry and registration payloads are unchanged.
Rate limiting affects logs only. Retrying through election/transport failure and
re-publishing capabilities after a successful attempt remain required. There is
no new recovery log, checkpoint, replay or data scan: added recovery bytes and
replay records are zero. Each attempt still submits at most the same REGISTER
payload through the existing paths; the log state is constant-size per worker.
This is a diagnostics fix, not a recovery-throughput improvement.

Checks defined before implementation: follower forwarding success without an
ERROR; a real TLS handshake rejecting an expired synthetic certificate with its
final cause preserved; rejection/unknown leader; stable repeated expiry despite
changing clock text; first/change/periodic/recovery log transitions; automatic
retry after leader change and renewed registration after inactivity; and the
existing capability update during an in-flight successful registration.

Expired CA/server certificates still require renewal and rollout in deployment
configuration. This code cannot repair them. No operational certificate, private
key, offset, PVC or cluster membership is modified by these tests.
