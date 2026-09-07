# Replication acknowledgement durability

## Problem

An `acks=all` append used to return the first transient ISR replication error to
the producer while its partition lane continued retrying and could later commit
the same append. An idempotent retry then checked only the leader fence, not the
committed high-water mark (HWM). The producer therefore received an ambiguous
failure and a duplicate retry could be acknowledged before the original offset
was consumer-visible.

This is independent from distributed log compaction. Distributed compaction is
supported by the current broker and is guarded by cluster-safety eligibility;
older releases scheduled an unsupported pass and must be upgraded rather than
having their error logs suppressed.

## Contract

- `acks=all` succeeds only after every replica in the captured ISR has accepted
  the append and the leader has durably advanced the HWM.
- Transient replication errors are retried in the ordered partition lane with
  exponential backoff. They are observable but are not terminal producer
  acknowledgements.
- A leader or lifecycle fence is terminal for the current request.
- An idempotent duplicate is acknowledged only when its original offset is
  below the committed HWM under the same replication fence.
- Client cancellation may stop waiting for an acknowledgement, but it does not
  abandon an already durable local append. A later idempotent retry resolves the
  ambiguous result without acknowledging uncommitted data.

## Verification

Fault tests cover a transient follower error followed by recovery, leader epoch
fencing, the `acks=all` commit boundary, and duplicate acknowledgement waiting
for the original offset to enter the HWM. Existing distributed compaction tests
cover safety-gate skips and successful distributed passes.
