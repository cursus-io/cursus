# Replication Consistency Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix three critical data consistency bugs: follower offset double-assignment (#4), HWM race where consumers read unreplicated messages (#3), and Flush/flushLoop channel race condition (#6).

**Architecture:** Add `AppendMessageWithOffset` for followers to preserve leader-assigned offsets. Split LEO/HWM updates so HWM only advances after replication. Replace Flush's direct channel read with a signal to flushLoop.

**Tech Stack:** Go

**Spec:** `docs/superpowers/specs/2026-04-26-replication-consistency-design.md`

---

### Task 1: #6 — Signal-based Flush (fix race condition)

**Files:**
- Modify: `pkg/disk/handler.go` (add flushSignal channel)
- Modify: `pkg/disk/flush_common.go` (rewrite Flush + update flushLoop)

This must be done first because Tasks 2-3 depend on Flush working correctly.

- [ ] **Step 1: Add flushSignal channel to DiskHandler**

In `pkg/disk/handler.go`, add to the `DiskHandler` struct:

```go
	flushSignal chan chan struct{}
```

In `NewDiskHandler`, initialize it (after `done: make(chan struct{})`):

```go
		flushSignal: make(chan chan struct{}, 1),
```

- [ ] **Step 2: Rewrite Flush() to use signal**

In `pkg/disk/flush_common.go`, replace the entire `Flush()` method:

```go
// Flush forces all pending data to be written and synced to disk.
// It signals the flushLoop to drain the write channel, avoiding a race condition
// where both Flush and flushLoop read from writeCh concurrently.
func (d *DiskHandler) Flush() {
	done := make(chan struct{})
	select {
	case d.flushSignal <- done:
		<-done // wait for flushLoop to process
	case <-d.done:
		return // shutting down
	}
}
```

- [ ] **Step 3: Update flushLoop to handle flushSignal**

In `pkg/disk/flush_common.go`, add a case in the flushLoop select:

```go
	for {
		select {
		case msg, ok := <-d.writeCh:
			if !ok {
				d.drainAndShutdown(batch)
				return
			}

			batch = append(batch, msg)
			if len(batch) >= d.batchSize {
				util.Debug("Batch size threshold reached, flushing %d messages", len(batch))
				if err := d.WriteBatch(batch); err != nil {
					util.Error("WriteBatch failed: %v", err)
				}
				batch = batch[:0]
			}

		case done := <-d.flushSignal:
			// Drain any remaining messages from writeCh
			draining := true
			for draining {
				select {
				case msg, ok := <-d.writeCh:
					if !ok {
						draining = false
					} else {
						batch = append(batch, msg)
					}
				default:
					draining = false
				}
			}
			if len(batch) > 0 {
				if err := d.WriteBatch(batch); err != nil {
					util.Error("WriteBatch failed during flush: %v", err)
				}
				batch = batch[:0]
			}
			// Sync to disk
			d.ioMu.Lock()
			if d.writer != nil {
				_ = d.writer.Flush()
			}
			if d.file != nil {
				_ = d.file.Sync()
			}
			d.ioMu.Unlock()
			close(done)

		case <-ticker.C:
			// ... existing timer flush (unchanged)
```

- [ ] **Step 4: Build and test**

```bash
go build ./...
go test ./pkg/disk/... -count=1 -timeout 60s
```

- [ ] **Step 5: Do NOT commit** — user controls git

---

### Task 2: #4 — AppendMessageWithOffset for Follower Replication

**Files:**
- Modify: `pkg/disk/handler.go` (add AppendMessageWithOffset)
- Modify: `pkg/disk/flush_common.go` (add WriteDirectWithOffset)
- Modify: `pkg/types/storage.go` (add to interface)
- Modify: `pkg/topic/partition.go` (add ReplicaAppend)
- Modify: `pkg/controller/produce_handler.go` (handleReplicateMessage uses ReplicaAppend)

- [ ] **Step 1: Add AppendMessageWithOffset to DiskHandler**

In `pkg/disk/handler.go`, add after `AppendMessageSync`:

```go
// AppendMessageWithOffset writes a message with a pre-assigned offset (for follower replication).
// Unlike AppendMessage/AppendMessageSync, it does NOT allocate a new offset.
func (d *DiskHandler) AppendMessageWithOffset(topic string, partition int, msg *types.Message) error {
	if err := d.WriteDirect(topic, partition, *msg); err != nil {
		return fmt.Errorf("WriteDirect failed: %w", err)
	}
	// Update AbsoluteOffset if this message extends beyond current
	for {
		current := atomic.LoadUint64(&d.AbsoluteOffset)
		newOffset := msg.Offset + 1
		if newOffset <= current {
			break
		}
		if atomic.CompareAndSwapUint64(&d.AbsoluteOffset, current, newOffset) {
			break
		}
	}
	return nil
}
```

- [ ] **Step 2: Add to StorageHandler interface**

In `pkg/types/storage.go`, add:

```go
	AppendMessageWithOffset(topic string, partition int, msg *Message) error
```

- [ ] **Step 3: Add AppendMessageWithOffset to all mock StorageHandlers**

Search for all test files that implement `StorageHandler` and add the method. Files:
- `pkg/controller/handler_coverage_test.go` — `testMockStorage`
- `pkg/controller/handler_real_test.go` — `mockStorage`
- `pkg/eventsource/handler_test.go` — `fakeStorageHandler`
- `pkg/topic/manager_test.go` — `MockStorageHandler`
- `pkg/cluster/replication/isr_manager_test.go` — `MockStorageHandler`
- `pkg/cluster/replication/fsm/fsm_test.go` — `MockStorageHandler`

For simple mocks, add:
```go
func (m *<Type>) AppendMessageWithOffset(topic string, partition int, msg *types.Message) error {
	return nil
}
```

For testify mocks (`MockStorageHandler` in manager_test.go):
```go
func (m *MockStorageHandler) AppendMessageWithOffset(topic string, partition int, msg *types.Message) error {
	args := m.Called(topic, partition, msg)
	return args.Error(0)
}
```

- [ ] **Step 4: Add ReplicaAppend to Partition**

In `pkg/topic/partition.go`, add after `EnqueueBatch`:

```go
// ReplicaAppend writes messages with pre-assigned offsets from the leader (follower replication).
// It preserves the leader's offset assignments and updates LEO/HWM accordingly.
func (p *Partition) ReplicaAppend(msgs []types.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}

	for i := range msgs {
		if err := p.dh.AppendMessageWithOffset(p.topic, p.id, &msgs[i]); err != nil {
			return fmt.Errorf("replica append failed at index %d: %w", i, err)
		}

		newLEO := msgs[i].Offset + 1
		if currentLEO := p.LEO.Load(); newLEO > currentLEO {
			p.LEO.Store(newLEO)
		}
		if p.HWM < newLEO {
			p.HWM = newLEO
		}
	}
	p.NotifyNewMessage()
	return nil
}
```

- [ ] **Step 5: Update handleReplicateMessage**

In `pkg/controller/produce_handler.go`, in `handleReplicateMessage`, replace:

```go
	if err := p.EnqueueBatch(msgCmd.Messages); err != nil {
		return fmt.Sprintf("ERROR: enqueue failed: %v", err)
	}
```

With:

```go
	if err := p.ReplicaAppend(msgCmd.Messages); err != nil {
		return fmt.Sprintf("ERROR: replica append failed: %v", err)
	}
```

- [ ] **Step 6: Build and test**

```bash
go build ./...
go test ./pkg/disk/... ./pkg/topic/... ./pkg/controller/... -count=1 -timeout 60s
```

- [ ] **Step 7: Do NOT commit**

---

### Task 3: #3 — Deferred HWM for Cluster Produce

**Files:**
- Modify: `pkg/topic/partition.go` (add EnqueueBatchLeader)
- Modify: `pkg/controller/produce_handler.go` (use EnqueueBatchLeader + explicit HWM update)

- [ ] **Step 1: Add EnqueueBatchLeader to Partition**

In `pkg/topic/partition.go`, add after `EnqueueBatch`:

```go
// EnqueueBatchLeader appends messages and updates LEO, but does NOT update HWM.
// Used by the partition leader in cluster mode. HWM is updated separately after
// successful replication, ensuring consumers never read unreplicated messages.
func (p *Partition) EnqueueBatchLeader(msgs []types.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}

	for i := range msgs {
		if p.isDuplicate(&msgs[i]) {
			continue
		}

		offset, err := p.dh.AppendMessage(p.topic, p.id, &msgs[i])
		if err != nil {
			p.NotifyNewMessage()
			return fmt.Errorf("batch enqueue failed at index %d: %w", i, err)
		}

		p.updateProducerState(&msgs[i])
		msgs[i].Offset = offset
		p.LEO.Store(offset + 1)
		// NOTE: HWM is NOT updated here — caller updates after replication
	}
	p.NotifyNewMessage()
	return nil
}

// AdvanceHWM sets HWM to the current LEO. Called after successful replication.
func (p *Partition) AdvanceHWM() {
	p.mu.Lock()
	defer p.mu.Unlock()
	leo := p.LEO.Load()
	if leo > p.HWM {
		p.HWM = leo
	}
}
```

- [ ] **Step 2: Update single PUBLISH (distributed path)**

In `pkg/controller/produce_handler.go`, find the distributed single PUBLISH block. Replace:

```go
		// Save HWM before enqueue so we can roll back on replication failure
		prevHWM := p.GetHWM()

		// 1. Append locally (async batch write)
		if err := p.EnqueueBatch(messageData.Messages); err != nil {
			return ch.errorResponse(fmt.Sprintf("failed to append locally: %v", err))
		}
```

With:

```go
		// 1. Append locally (LEO advances, HWM stays until replication succeeds)
		if err := p.EnqueueBatchLeader(messageData.Messages); err != nil {
			return ch.errorResponse(fmt.Sprintf("failed to append locally: %v", err))
		}
```

And replace the replication failure handler:

```go
			err = ch.Cluster.ReplicateToFollowers(topicName, partition, messageData, minISR)
			if err != nil {
				// Roll back HWM so consumers don't see unreplicated messages
				p.SetHWM(prevHWM)
				return ch.errorResponse(...)
			}
```

With:

```go
			err = ch.Cluster.ReplicateToFollowers(topicName, partition, messageData, minISR)
			if err != nil {
				// HWM was never advanced, so no rollback needed
				return ch.errorResponse(fmt.Sprintf("replication failed (offset=%d): %v", assignedOffset, err))
			}
```

And before the FlushDisk + ackResp, add HWM advance:

```go
		// Ensure data is on disk before ACK
		p.FlushDisk()

		// Advance HWM now that replication and flush are complete
		p.AdvanceHWM()

		ackResp = types.AckResponse{
```

- [ ] **Step 3: Update BATCH PUBLISH (distributed path)**

Same pattern. Find the distributed BATCH block. Replace `EnqueueBatch` with `EnqueueBatchLeader`, remove `prevHWM` save/rollback, add `p.AdvanceHWM()` after FlushDisk.

Replace:
```go
		prevHWM := p.GetHWM()

		// 1. Append locally (async batch write)
		if err := p.EnqueueBatch(batch.Messages); err != nil {
```

With:
```go
		// 1. Append locally (LEO advances, HWM stays until replication succeeds)
		if err := p.EnqueueBatchLeader(batch.Messages); err != nil {
```

Remove/update the replication failure HWM rollback (no longer needed since HWM wasn't advanced).

Add before respAck:
```go
		p.FlushDisk()
		p.AdvanceHWM()

		respAck = types.AckResponse{
```

- [ ] **Step 4: Build and test**

```bash
go build ./...
go test ./pkg/controller/... ./pkg/topic/... -count=1 -timeout 60s
```

- [ ] **Step 5: Do NOT commit**

---

### Task 4: Full Test Suite + Integration

- [ ] **Step 1: Run all Go tests**

```bash
go test ./pkg/... ./sdk/... ./util/... -count=1 -timeout 120s
```

All must pass.

- [ ] **Step 2: Rebuild cluster and verify**

```bash
cd test/cluster && docker compose down && docker compose up -d --build broker-1 broker-2 broker-3
sleep 20
```

Test produce + consume:

```bash
docker run --rm --network cluster_network python:3.11-slim python3 -c "
import socket, struct, time
def send(addr, port, cmd):
    s = socket.socket(); s.connect((addr, port)); s.settimeout(10)
    msg = struct.pack('>H', 0) + cmd.encode()
    s.send(struct.pack('>I', len(msg)) + msg)
    rl = s.recv(4); rlen = struct.unpack('>I', rl)[0]
    resp = b''
    while len(resp) < rlen: resp += s.recv(rlen - len(resp))
    s.close(); return resp

def sendt(a, p, c): return send(a, p, c).decode()

sendt('broker-1', 9000, 'CREATE topic=rep-test partitions=2')
time.sleep(1)
# Produce 5 messages
for i in range(5):
    ack = sendt('broker-1', 9000, f'PUBLISH topic=rep-test acks=1 producerId=p1 message=msg-{i}')
    print(f'ACK {i}: {ack[:50]}')

time.sleep(0.5)
# Consume immediately
resp = send('broker-1', 9000, 'CONSUME topic=rep-test partition=0 offset=0 member=m1 group=g1 batch=10')
print(f'CONSUME: {len(resp)} bytes')

# Health
for b in ['broker-1', 'broker-2', 'broker-3']:
    try: sendt(b, 9000, 'LIST'); print(f'{b}: alive')
    except: print(f'{b}: DEAD')
"
```

Expected: All ACKs succeed, CONSUME returns data, all brokers alive.
