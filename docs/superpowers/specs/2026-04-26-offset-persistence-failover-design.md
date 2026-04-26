# Offset Persistence + Failover Improvements

**Date:** 2026-04-26
**Status:** Draft

## Issue #5: Consumer Offset Not Persisted in FSM Snapshot

**Problem:** `BrokerFSMState` does not include consumer group offsets. When Raft restores from a snapshot (broker restart, new leader), all committed offsets are lost. Consumers restart from offset 0.

**Root cause:** `OFFSET_SYNC` FSM command calls `Coordinator.CommitOffset` (in-memory), but `Snapshot()` only serializes `Brokers`, `PartitionMetadata`, `Logs`, `ProducerState`.

**Fix:** Add consumer group state (offsets + memberships) to `BrokerFSMState` and restore it on `Restore()`.

### Design

Add to `BrokerFSMState`:
```go
type BrokerFSMState struct {
    // ... existing fields
    GroupState map[string]*GroupStateSnapshot `json:"groupState,omitempty"`
}

type GroupStateSnapshot struct {
    TopicName   string                        `json:"topic"`
    Generation  int                           `json:"generation"`
    Members     map[string][]int              `json:"members"`     // memberID -> partitions
    Offsets     map[string]map[int]uint64     `json:"offsets"`     // topic -> partition -> offset
}
```

In `Snapshot()`: serialize Coordinator group state.
In `Restore()`: replay group state into Coordinator.

## Issue #2: Partition Leader Failover Gaps

**Problem:** ISRManager detects leader failure and elects new leader via FSM. But:
1. `METADATA` returns stale leader until FSM propagates the failover
2. SDK only refreshes on NOT_LEADER or periodic timer (30s default)
3. No LeaderEpoch validation — SDK may send to stale leader during transition

**Fix (scoped):**
1. `METADATA` should include `leader_epoch` per partition so SDK can detect stale metadata
2. CONSUME/PRODUCE responses should include `leader_epoch` — if SDK's epoch < broker's epoch, refresh metadata

This is an incremental improvement, not a full fix. Full fix requires SDK-side epoch tracking.

## Issue #1: Dual Replication Path (Document Only)

**Problem:** Messages use TCP REPLICATE_MESSAGE (not Raft), while metadata uses Raft. This creates:
- Ordering gaps between Raft metadata and TCP message delivery
- No single source of truth for message durability

**Long-term fix:** Migrate to Kafka-style ISR log replication where partition leaders maintain their own log and followers fetch from the leader (FETCH replica protocol). Raft is used only for controller operations (metadata, leader election, ISR updates).

**This is out of scope for this plan.** Documented for future reference.

## Files Changed (#5 only — #2 is documentation)

- `pkg/cluster/replication/fsm/fsm.go` — Add GroupState to BrokerFSMState, update Snapshot/Restore
- `pkg/cluster/replication/fsm/fsm_command.go` — Add GroupStateSnapshot type
- `pkg/coordinator/coordinator.go` — Add ExportState/ImportState methods
