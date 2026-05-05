# Replication Consistency Fixes

**Date:** 2026-04-26
**Status:** Draft

## Issues

### #4 (Critical): Follower Offset Double-Assignment

**Current flow:**
```
Leader: EnqueueBatch → AppendMessage → offset=42 assigned
Leader: ReplicateToFollowers → REPLICATE_MESSAGE(msgs with offset=42)
Follower: handleReplicateMessage → EnqueueBatch → AppendMessage → offset=0 assigned (OVERWRITE!)
```

`AppendMessage` always calls `atomic.AddUint64(&d.AbsoluteOffset, 1)` to assign a new offset. The leader's offset is lost on the follower.

**Fix:** Add `EnqueueBatchWithOffsets` (or `ReplicaAppend`) to Partition that preserves pre-assigned offsets. The follower uses this instead of `EnqueueBatch`.

### #3 (Critical): HWM Race — Consumer Reads Unreplicated Messages

**Current flow:**
```
EnqueueBatch → HWM = offset+1 (IMMEDIATELY)
ReplicateToFollowers → may fail
p.SetHWM(prevHWM) → rollback, but consumer may have already read
```

Between HWM update and replication failure, `ReadCommitted` can return messages that no follower has.

**Fix:** Don't update HWM in EnqueueBatch for cluster mode. Instead, update HWM after replication succeeds. This requires splitting LEO (local end offset) from HWM (replicated end offset).

Currently LEO and HWM are updated together in EnqueueBatch. After the fix:
- `EnqueueBatch` updates LEO only
- After successful replication, explicitly set `HWM = LEO`
- `ReadCommitted` already uses HWM, so it naturally only reads replicated data

### #6 (Critical): Flush() vs flushLoop Race

**Current:** Both `Flush()` and `flushLoop()` read from the same `writeCh` channel concurrently. Messages can be split between them, breaking ordering.

**Fix:** Remove direct channel reading from `Flush()`. Instead, `Flush()` sends a signal to `flushLoop` and waits for it to drain the channel.

## Design

### D1: ReplicaAppend for Follower

Add to `Partition`:
```go
func (p *Partition) ReplicaAppend(msgs []types.Message) error {
    // Write messages with their pre-assigned offsets (no new offset allocation)
    // Update LEO to max(msg.Offset)+1
    // Update HWM = LEO (follower trusts leader's offset assignments)
}
```

Add to `DiskHandler`:
```go
func (d *DiskHandler) AppendMessageWithOffset(topic string, partition int, msg *types.Message) error {
    // Write to disk WITHOUT calling atomic.AddUint64(&d.AbsoluteOffset, 1)
    // Use msg.Offset as-is
    // Update AbsoluteOffset if msg.Offset >= current
}
```

Update `handleReplicateMessage`:
```go
// Before: p.EnqueueBatch(msgCmd.Messages)
// After:  p.ReplicaAppend(msgCmd.Messages)
```

### D2: Deferred HWM for Cluster Produce

In produce_handler.go (distributed path):
```go
// Before:
p.EnqueueBatch(msgs)  // updates both LEO and HWM

// After:
p.EnqueueBatchNoHWM(msgs)  // updates LEO only, HWM unchanged
ReplicateToFollowers()
p.FlushDisk()
p.SetHWM(p.LEO())  // update HWM only after replication succeeds
```

Add `EnqueueBatchNoHWM` to Partition:
```go
func (p *Partition) EnqueueBatchNoHWM(msgs []types.Message) error {
    // Same as EnqueueBatch but skip HWM update
    // Only update LEO
}
```

On replication failure, don't need to rollback HWM since it was never advanced.

### D3: Signal-Based Flush

Replace `Flush()` channel reading with signal:

```go
type DiskHandler struct {
    // ... existing fields
    flushSignal chan chan struct{} // send done-channel, flushLoop signals back
}

func (d *DiskHandler) Flush() {
    done := make(chan struct{})
    d.flushSignal <- done
    <-done // wait for flushLoop to process
}

func (d *DiskHandler) flushLoop() {
    for {
        select {
        case msg := <-d.writeCh:
            batch = append(batch, msg)
            // ... batch logic
        case done := <-d.flushSignal:
            // Drain remaining messages from writeCh
            // WriteBatch
            // Flush bufio + sync
            close(done) // signal completion
        case <-ticker.C:
            // ... existing timer flush
        }
    }
}
```

## Files Changed

- `pkg/disk/handler.go` — `AppendMessageWithOffset`, flushSignal field
- `pkg/disk/flush_common.go` — signal-based Flush, flushLoop update
- `pkg/topic/partition.go` — `ReplicaAppend`, `EnqueueBatchNoHWM`
- `pkg/controller/produce_handler.go` — deferred HWM in cluster produce
- `pkg/controller/produce_handler.go` — `handleReplicateMessage` uses ReplicaAppend
- `pkg/types/storage.go` — add new methods to StorageHandler interface
