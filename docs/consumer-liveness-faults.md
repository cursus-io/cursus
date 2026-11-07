# Consumer liveness during fault setup and durable membership changes

## Evidence and scope

On 2026-11-07 the native Windows cleanup driver failed its second snapshot run
at the initial LEAVE. Broker logs explicitly expired the member after the
30-second session limit; setup had restarted a follower and waited for health
and ISR without heartbeats. Other cleanup tests already use a separate
same-member heartbeat connection during setup. Extend that existing helper to
both old and replacement members in delayed-follower recovery. Preserve the
session limit, registration, generation, fault milestones and final assertions.

The separate cooperative driver failed after repeated Wire timeouts, with
`cooperative_fenced` during crashed-member reassignment. That log does not prove
the initiating cause. Code inspection identifies another candidate: a pending
durable membership update makes even surviving members wait on `lifecycleMu`.
The eventual publication also installs member clocks copied before the append.
A controlled blocked-writer regression must distinguish this path from the
already-fixed offset-append heartbeat convoy. Do not attribute all observed
network stalls to it or automatically rejoin a fenced SDK session.

## Kafka comparison and contract defined before implementation

[Accepted KIP-848](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217387038/KIP-848+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
separates incremental assignment reconciliation from member liveness.
[Kafka 4.1.0 GroupMetadataManager](https://github.com/apache/kafka/blob/4.1.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java)
reschedules the member session timer while processing a consumer heartbeat.
[AbstractMembershipManager at the same version](https://github.com/apache/kafka/blob/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractMembershipManager.java)
quiesces lost assignments before rejoining a fenced member.

Adopt independent volatile liveness for an already-validated session. Cursus
retains its Raft-backed complete lifecycle snapshots, explicit registration,
session/grant tokens, pending-transition fences and terminal SDK fencing.
During a pending transition, refresh only a member with the same registration
and session in both the committed and candidate assignments. Return a retryable
availability response without publishing candidate grants or acknowledging a
revocation. A member selected for removal cannot refresh itself back into that
candidate. On successful publication, preserve newer volatile clocks only for
matching surviving sessions. A failed or ambiguous append must retain the
existing recovery fence. No persisted format or broker session timeout changes.

This improves availability under slow lifecycle storage without granting stale
authority. It adds no metadata records, log scan or replay bytes. Failed append
recovery may still require replay; its work must not be described as zero.

## Regression and recovery checks

* Hold another member's expiration append. A surviving heartbeat must finish
  before releasing storage, refresh its clock and expose no uncommitted grants.
* Repeat with pending revocation acknowledgements; they must remain pending.
  Reject wrong registration/session and preserve the removed member's fence.
* Publish successfully and verify the fresh clock survives the candidate copy;
  fail the append and require the recovery/admission fence to remain closed.
* Keep existing retained-grant commits, stale-grant rejection, failed offset
  append and strict replay tests. Report lifecycle record count and payload
  bytes; liveness-only updates must not add either.
* Repeat cleanup replay and snapshot modes with a deliberate setup wait longer
  than 30 seconds, without increasing broker deadlines or changing member IDs.
* Rerun native cooperative fault recovery alone, recording expiration timing and
  storage duration. A passing rerun does not by itself explain earlier Wire
  timeouts; retain those failures and distinguish confirmed regressions from
  residual host/transport hypotheses.

## Results and follow-up evidence

The blocked-expiration regression failed before the change for both successful
and ambiguous append outcomes: the live heartbeat waited until storage was
released. The corrected focused suite passed natively and passed three Linux
race repetitions, including pending revocation ACKs and stale-session rejection.
The member-clock change adds no lifecycle records or replay payload bytes.

The isolated native cooperative run passed in 134.24 seconds, drained all
partitions and reached their committed ends. It delivered 252 records with one
allowed member-join replay (133 bytes), zero same-grant duplicates, and 12 parent
worker starts. Wire timeouts still occurred. The captured crashed-member expiry
selected a 13,036 ms-old heartbeat against the unchanged 12,000 ms limit and
published in 105 ms. This run did not reproduce a slow expiry append; it does not
prove that the original transport failure had the same cause as the controlled
regression.

With a 35-second setup hold, three of four cleanup paths passed. The last
snapshot repetition failed reading the initial LEAVE response, with a Windows
connection-aborted error, not `member_not_found`. The keeper had succeeded.
The keeper owns a separate socket, so it does not prevent the original management
socket from crossing the broker's 60-second idle timeout during setup. Reopen
that transport before sending LEAVE once with the original member/generation.
Do not classify an ambiguous mutation as rejected or retry it automatically.
A follow-up 65-second hold checks both the 30-second session and 60-second idle
connection limits; results remain pending until that run finishes. The Windows
socket error alone is not proof that IPv6 or the host proxy caused it.

The real-Wire closed-management-connection regression passed natively and with
the Linux race detector. It requires one LEAVE on fresh transport with the
original member and generation; no rejoin or mutation retry is accepted.

The full local coordinator race sweep hit its 12-minute package limit during
the existing `TestCleanupReservedRecordBoundaries/67108864` case, after 79 seconds
in that subtest. The container was limited to two CPUs and 1,500 MiB. The stack
was in metadata decoding, not the changed heartbeat path; no data-race report or
assertion failure preceded the timeout. This is an incomplete full-suite run,
not a pass and not evidence that increasing a broker deadline would help.
