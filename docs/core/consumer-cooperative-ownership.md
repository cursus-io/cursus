# Cooperative ownership state

This layer implements the deterministic ownership transition model beneath the
cooperative protocol. It does not advertise a capability, create cooperative
groups through the wire protocol, or relax existing generation checks. A state
returned by the model is a candidate: it becomes authoritative only after the
coordinator durably publishes it under its admission and ownership fences.

## Kafka comparison and Cursus contract

[KIP-429](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/103090108/KIP-429+Kafka+Consumer+Incremental+Rebalance+Protocol)
and [KIP-848](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217387038/KIP-848+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
separate target assignment from current ownership. The implemented
[Kafka 4.1.0 CurrentAssignmentBuilder](https://github.com/apache/kafka/blob/4.1.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/modern/consumer/CurrentAssignmentBuilder.java)
waits for revocation and withholds partitions still owned by another member.
Cursus adopts those ordering rules and retains its server-side sticky assignor.

Cursus uses a partition grant epoch rather than changing every retained worker's
group generation. A token identifies the group registration, member session,
topic lifecycle, partition and grant epoch. The target revision is an instruction
version, not an authorization token. Retained grants survive unrelated target
changes; a revoked grant never becomes valid again, including if the partition
returns to the same member.

## Transition rules

- Joining a member creates a monotonically newer session identity. A caller must
  not reuse an active member ID; reconnects use the existing session. Expiration
  and leave require the exact session so an old request cannot remove a new one.
- Target assignment is deterministic and sticky. Pending targets are retained
  as the next assignment input to avoid oscillation while revocations are pending.
- When a target differs, record the original revocation revision on the current
  grant. Keep it until acknowledgement or durable member/topic fencing. Later
  target changes do not retract that revocation or alter its acknowledgement key.
- Acknowledgements identify the partition lifecycle, grant epoch and revocation
  revision. Duplicate or old acknowledgements are harmless; malformed batches
  fail atomically. They cannot revoke a newer grant or a different session.
- A new grant is possible only after the previous current grant is gone. The
  caller supplies the partitions blocked by unresolved transaction reservations
  from the same locked authoritative state. Blocked partitions stay unassigned;
  retained owners can finish their existing transaction before revocation.
- A topic lifecycle change durably fences grants from the old lifecycle. Reusing
  the topic name or partition number does not reuse its token.
- All transitions return independent candidate values. Failed persistence must
  leave the installed input unchanged. An unchanged acknowledgement/reconcile
  returns an unchanged revision and needs no new durable record.

Revision and session counters reject overflow. Recovery rejects inconsistent
membership, duplicate partition ownership, invalid tokens, non-balanced targets,
and unknown fields rather than manufacturing a new assignment. Canonical JSON
round-trips preserve pending revocations and grant identities without assigning
new epochs during recovery.

The model sorts partition/member slices for canonical output, and its indexes
and encoded state grow with the number of partitions and members. Acknowledgement
processing scans the current grants once after indexing the acknowledgement
batch; it does not rescan all grants for each acknowledgement.

## Verification before activation

Tests cover delayed/duplicate acknowledgements, target reversal, same-ID member
return, batch expiration, topic recreation, unresolved transaction barriers,
counter overflow, candidate isolation, malformed recovery and deterministic
encoding. A transition trace measures serialized recovery bytes and recovered
grants/revocations, not just metadata request counts.

The 120-partition four-member transition fixture replays 11 states containing
297,909 JSON bytes, 1,200 grant entries and 180 pending-revocation entries in
total. On Linux with Go 1.26.2 and two CPUs, decoding/validating a pending handoff
used about 0.65 ms and 160 KB allocated at 120 partitions, and 82 ms and 21.4 MB
allocated at 12,000 partitions. These are model recovery costs, not measurements
of broker failover latency or application replay. Runtime integration must still
measure processing gaps, duplicate records and replay bytes.

The durable coordinator integration must serialize validation, persistence and
publication with commits/reservations and reject eager commands for cooperative
groups. Every snapshot, log replay, cleanup and admission-refresh path must retain
the ownership state. No SDK may use this model as authorization before those
paths and fetch/stream/commit validation are integrated.

New metadata versions are an activation barrier: all serving brokers must support
them before enabling the protocol. Draining a group does not erase old-version
incompatible log records. Binary downgrade is therefore forbidden while such
records or snapshots remain; returning clients to eager mode on supporting
binaries is distinct from a binary downgrade. A downgrade procedure needs a
verified compatible checkpoint and physical history replacement, not just an
empty group or a configuration toggle.
