# ISR Pull-Based Replication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace push-based REPLICATE_MESSAGE with pull-based FETCH_REPLICA, where followers actively fetch from the partition leader's log — matching Kafka's ISR replication model.

**Architecture:** Followers run a fetch loop per assigned partition, pulling messages from the leader's log. The leader tracks each follower's LEO and computes HWM = min(ISR LEOs). Produce ACK waits for HWM to reach the produced offset. ISR membership is determined by fetch lag instead of heartbeat.

**Tech Stack:** Go

**Spec:** `docs/superpowers/specs/2026-04-26-isr-pull-replication-design.md`

---

### Task 1: ISRManager — Replica LEO Tracking

**Files:**
- Modify: `pkg/cluster/replication/isr_manager.go`
- Modify: `pkg/cluster/replication/manager.go` (ISRManagerInterface)

Add replica LEO tracking so the leader knows how far each follower has fetched.

- [ ] **Step 1: Add replicaLEOs map to ISRManager**

In `pkg/cluster/replication/isr_manager.go`, add to the `ISRManager` struct:

```go
	replicaLEOs map[string]map[string]uint64 // brokerID -> "topic-partition" -> LEO
```

Initialize in `NewISRManager`:

```go
		replicaLEOs: make(map[string]map[string]uint64),
```

- [ ] **Step 2: Add UpdateReplicaLEO and GetReplicaLEO methods**

```go
// UpdateReplicaLEO records a follower's latest fetched offset for a partition.
func (i *ISRManager) UpdateReplicaLEO(brokerID, topic string, partition int, leo uint64) {
	key := fmt.Sprintf("%s-%d", topic, partition)
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.replicaLEOs[brokerID] == nil {
		i.replicaLEOs[brokerID] = make(map[string]uint64)
	}
	i.replicaLEOs[brokerID][key] = leo
	i.lastSeen[brokerID] = time.Now()
}

// GetReplicaLEO returns a follower's last known LEO for a partition.
func (i *ISRManager) GetReplicaLEO(brokerID, topic string, partition int) uint64 {
	key := fmt.Sprintf("%s-%d", topic, partition)
	i.mu.RLock()
	defer i.mu.RUnlock()
	if leos, ok := i.replicaLEOs[brokerID]; ok {
		return leos[key]
	}
	return 0
}
```

- [ ] **Step 3: Add to ISRManagerInterface**

In `pkg/cluster/replication/manager.go`, add to `ISRManagerInterface`:

```go
	UpdateReplicaLEO(brokerID, topic string, partition int, leo uint64)
	GetReplicaLEO(brokerID, topic string, partition int) uint64
```

- [ ] **Step 4: Update mock ISRManagers in tests**

Add the methods to all ISRManager mocks in test files:
- `pkg/cluster/replication/manager_test.go` — `MockISRManager`

```go
func (m *MockISRManager) UpdateReplicaLEO(brokerID, topic string, partition int, leo uint64) {}
func (m *MockISRManager) GetReplicaLEO(brokerID, topic string, partition int) uint64 { return 0 }
```

- [ ] **Step 5: Build and test**

```bash
go build ./...
go test ./pkg/cluster/replication/... -count=1 -timeout 60s
```

---

### Task 2: Partition — ReadBeyondHWM for Replica Fetch

**Files:**
- Modify: `pkg/topic/partition.go`

Replicas need to read up to LEO (not just HWM) to catch up.

- [ ] **Step 1: Add ReadReplica method**

```go
// ReadReplica reads messages up to LEO (not limited by HWM).
// Used by follower fetch — followers need to read beyond HWM to catch up.
func (p *Partition) ReadReplica(offset uint64, max int) ([]types.Message, error) {
	leo := p.LEO.Load()
	if offset >= leo {
		return nil, nil
	}

	canRead := int(leo - offset)
	if max > canRead {
		max = canRead
	}

	return p.ReadMessages(offset, max)
}
```

- [ ] **Step 2: Build and test**

```bash
go build ./...
go test ./pkg/topic/... -count=1 -timeout 60s
```

---

### Task 3: FETCH_REPLICA Command Handler

**Files:**
- Create: `pkg/controller/replication_handler.go`
- Modify: `pkg/controller/handler.go` (register command)
- Modify: `pkg/server/main.go` (isCommand)

- [ ] **Step 1: Create replication_handler.go**

```go
package controller

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/util"
)

// HandleFetchReplica processes FETCH_REPLICA from follower brokers.
// Response: binary batch (same format as CONSUME).
func (ch *CommandHandler) HandleFetchReplica(conn net.Conn, rawCmd string) {
	args := parseKeyValueArgs(rawCmd[15:]) // len("FETCH_REPLICA ") = 15... actually let's be safe
	argsStr := rawCmd
	if idx := strings.Index(rawCmd, " "); idx != -1 {
		argsStr = rawCmd[idx+1:]
	}
	args := parseKeyValueArgs(argsStr)

	topicName := args["topic"]
	partitionStr := args["partition"]
	offsetStr := args["offset"]
	brokerID := args["broker_id"]

	if topicName == "" || partitionStr == "" || offsetStr == "" || brokerID == "" {
		util.WriteWithLength(conn, []byte("ERROR: FETCH_REPLICA requires topic, partition, offset, broker_id"))
		return
	}

	partitionID, err := strconv.Atoi(partitionStr)
	if err != nil {
		util.WriteWithLength(conn, []byte("ERROR: invalid partition"))
		return
	}

	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		util.WriteWithLength(conn, []byte("ERROR: invalid offset"))
		return
	}

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		util.WriteWithLength(conn, []byte(fmt.Sprintf("ERROR: topic '%s' not found", topicName)))
		return
	}

	p, err := t.GetPartition(partitionID)
	if err != nil {
		util.WriteWithLength(conn, []byte(fmt.Sprintf("ERROR: %v", err)))
		return
	}

	// Read up to LEO (not HWM) for replica catch-up
	maxBatch := 1000
	messages, err := p.ReadReplica(offset, maxBatch)
	if err != nil {
		util.WriteWithLength(conn, []byte(fmt.Sprintf("ERROR: read failed: %v", err)))
		return
	}

	// Update follower's LEO in ISRManager
	newLEO := offset
	if len(messages) > 0 {
		newLEO = messages[len(messages)-1].Offset + 1
	}
	if ch.isDistributed() {
		if isrMgr := ch.Cluster.RaftManager.GetISRManager(); isrMgr != nil {
			isrMgr.UpdateReplicaLEO(brokerID, topicName, partitionID, newLEO)
		}
	}

	// Send as binary batch
	batchData, err := util.EncodeBatchMessages(topicName, partitionID, "1", false, messages)
	if err != nil {
		util.WriteWithLength(conn, []byte(fmt.Sprintf("ERROR: encode failed: %v", err)))
		return
	}
	_ = util.WriteWithLength(conn, batchData)
}
```

Note: fix the double `args :=` — use the second pattern only:

```go
func (ch *CommandHandler) HandleFetchReplica(conn net.Conn, rawCmd string) {
	idx := strings.Index(rawCmd, " ")
	if idx == -1 {
		_ = util.WriteWithLength(conn, []byte("ERROR: invalid FETCH_REPLICA syntax"))
		return
	}
	args := parseKeyValueArgs(rawCmd[idx+1:])
	// ... rest as above
```

- [ ] **Step 2: Register in handler.go**

Add to `ch.commands`:

```go
{prefix: "FETCH_REPLICA ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return STREAM_DATA_SIGNAL }},
```

And in `handleCommandMessage` in `pkg/server/main.go`, add before the STREAM check:

```go
	if strings.HasPrefix(strings.ToUpper(payload), "FETCH_REPLICA ") {
		cmdHandler.HandleFetchReplica(conn, payload)
		return false, nil
	}
```

- [ ] **Step 3: Add to isCommand in main.go**

Add `"FETCH_REPLICA"` to the keywords list.

- [ ] **Step 4: Build and test**

```bash
go build ./...
go test ./pkg/controller/... -count=1 -timeout 60s
```

---

### Task 4: ReplicaFetcher — Follower Fetch Loop

**Files:**
- Create: `pkg/cluster/replication/replica_fetcher.go`

- [ ] **Step 1: Create replica_fetcher.go**

```go
package replication

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// ReplicaFetcher pulls messages from the partition leader for a single partition.
type ReplicaFetcher struct {
	topic       string
	partition   int
	brokerID    string
	leaderAddr  string
	tm          *topic.TopicManager
	localOffset uint64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewReplicaFetcher(ctx context.Context, brokerID, topic string, partition int, leaderAddr string, tm *topic.TopicManager) *ReplicaFetcher {
	childCtx, cancel := context.WithCancel(ctx)

	// Determine starting offset from local partition
	var startOffset uint64
	if t := tm.GetTopic(topic); t != nil {
		if p, err := t.GetPartition(partition); err == nil {
			startOffset = p.LEO.Load()
		}
	}

	return &ReplicaFetcher{
		topic:       topic,
		partition:   partition,
		brokerID:    brokerID,
		leaderAddr:  leaderAddr,
		tm:          tm,
		localOffset: startOffset,
		ctx:         childCtx,
		cancel:      cancel,
	}
}

func (rf *ReplicaFetcher) Start() {
	rf.wg.Add(1)
	go func() {
		defer rf.wg.Done()
		rf.fetchLoop()
	}()
}

func (rf *ReplicaFetcher) Stop() {
	rf.cancel()
	rf.wg.Wait()
}

func (rf *ReplicaFetcher) fetchLoop() {
	backoff := 100 * time.Millisecond

	for {
		select {
		case <-rf.ctx.Done():
			return
		default:
		}

		messages, err := rf.fetchFromLeader()
		if err != nil {
			util.Debug("ReplicaFetcher %s-%d: fetch error: %v", rf.topic, rf.partition, err)
			select {
			case <-rf.ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 5*time.Second {
				backoff *= 2
			}
			continue
		}

		backoff = 100 * time.Millisecond

		if len(messages) == 0 {
			select {
			case <-rf.ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}

		// Append to local partition
		t := rf.tm.GetTopic(rf.topic)
		if t == nil {
			continue
		}
		p, err := t.GetPartition(rf.partition)
		if err != nil {
			continue
		}

		if err := p.ReplicaAppend(messages); err != nil {
			util.Error("ReplicaFetcher %s-%d: append error: %v", rf.topic, rf.partition, err)
			continue
		}

		rf.localOffset = messages[len(messages)-1].Offset + 1
	}
}

func (rf *ReplicaFetcher) fetchFromLeader() ([]types.Message, error) {
	conn, err := net.DialTimeout("tcp", rf.leaderAddr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial leader %s: %w", rf.leaderAddr, err)
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	cmd := fmt.Sprintf("FETCH_REPLICA topic=%s partition=%d offset=%d broker_id=%s",
		rf.topic, rf.partition, rf.localOffset, rf.brokerID)

	payload := make([]byte, 2+len(cmd))
	binary.BigEndian.PutUint16(payload[:2], 0)
	copy(payload[2:], cmd)

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))
	if _, err := conn.Write(lenBuf); err != nil {
		return nil, err
	}
	if _, err := conn.Write(payload); err != nil {
		return nil, err
	}

	// Read response
	respLenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, respLenBuf); err != nil {
		return nil, err
	}
	respLen := binary.BigEndian.Uint32(respLenBuf)
	if respLen == 0 {
		return nil, nil
	}

	respBuf := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		return nil, err
	}

	// Check for error response
	if len(respBuf) > 5 && strings.HasPrefix(string(respBuf[:6]), "ERROR") {
		return nil, fmt.Errorf("leader error: %s", string(respBuf))
	}

	// Decode batch
	batch, err := util.DecodeBatchMessages(respBuf)
	if err != nil {
		return nil, fmt.Errorf("decode batch: %w", err)
	}

	return batch.Messages, nil
}
```

- [ ] **Step 2: Build and test**

```bash
go build ./...
go test ./pkg/cluster/replication/... -count=1 -timeout 60s
```

---

### Task 5: Fetcher Lifecycle Management

**Files:**
- Modify: `pkg/cluster/controller/controller.go` (add fetcher management)

- [ ] **Step 1: Add fetcher registry to ClusterController**

```go
type ClusterController struct {
	// ... existing fields
	fetchers   map[string]*replication.ReplicaFetcher // "topic-partition" -> fetcher
	fetchersMu sync.Mutex
	tm         *topic.TopicManager
}
```

- [ ] **Step 2: Add StartFetcher/StopFetcher methods**

```go
func (cc *ClusterController) StartFetcher(ctx context.Context, topicName string, partitionID int, leaderAddr string) {
	key := fmt.Sprintf("%s-%d", topicName, partitionID)
	cc.fetchersMu.Lock()
	defer cc.fetchersMu.Unlock()

	if existing, ok := cc.fetchers[key]; ok {
		existing.Stop()
	}

	fetcher := replication.NewReplicaFetcher(ctx, cc.brokerID, topicName, partitionID, leaderAddr, cc.tm)
	cc.fetchers[key] = fetcher
	fetcher.Start()
	util.Info("Started replica fetcher for %s", key)
}

func (cc *ClusterController) StopFetcher(topicName string, partitionID int) {
	key := fmt.Sprintf("%s-%d", topicName, partitionID)
	cc.fetchersMu.Lock()
	defer cc.fetchersMu.Unlock()

	if fetcher, ok := cc.fetchers[key]; ok {
		fetcher.Stop()
		delete(cc.fetchers, key)
		util.Info("Stopped replica fetcher for %s", key)
	}
}

func (cc *ClusterController) StopAllFetchers() {
	cc.fetchersMu.Lock()
	defer cc.fetchersMu.Unlock()
	for key, fetcher := range cc.fetchers {
		fetcher.Stop()
		delete(cc.fetchers, key)
	}
}
```

- [ ] **Step 3: Initialize fetchers map in NewClusterController**

Add `fetchers: make(map[string]*replication.ReplicaFetcher)` and accept `tm` parameter.

- [ ] **Step 4: Start fetchers on topic creation**

In the FSM topic creation callback or in the leader election monitor, when a broker discovers it's a follower for a partition, start a fetcher. This requires hooking into the FSM apply for TOPIC commands.

For now, add a `SyncFetchers` method that reads FSM partition metadata and starts/stops fetchers:

```go
func (cc *ClusterController) SyncFetchers(ctx context.Context) {
	fsm := cc.RaftManager.GetFSM()
	if fsm == nil {
		return
	}

	keys := fsm.GetAllPartitionKeys()
	activeFetchers := make(map[string]bool)

	for _, key := range keys {
		meta := fsm.GetPartitionMetadata(key)
		if meta == nil {
			continue
		}

		// If we are a replica but NOT the leader, start fetcher
		isReplica := false
		for _, r := range meta.Replicas {
			if r == cc.brokerID {
				isReplica = true
				break
			}
		}

		if isReplica && meta.Leader != cc.brokerID {
			leaderBroker := fsm.GetBroker(meta.Leader)
			if leaderBroker == nil {
				continue
			}
			leaderAddr := leaderBroker.ClientAddr
			if leaderAddr == "" {
				leaderAddr = leaderBroker.Addr
			}

			// Parse topic and partition from key "topic-N"
			idx := strings.LastIndex(key, "-")
			if idx == -1 {
				continue
			}
			topicName := key[:idx]
			partitionID, err := strconv.Atoi(key[idx+1:])
			if err != nil {
				continue
			}

			cc.StartFetcher(ctx, topicName, partitionID, leaderAddr)
			activeFetchers[key] = true
		}
	}

	// Stop fetchers for partitions we no longer follow
	cc.fetchersMu.Lock()
	for key, fetcher := range cc.fetchers {
		if !activeFetchers[key] {
			fetcher.Stop()
			delete(cc.fetchers, key)
		}
	}
	cc.fetchersMu.Unlock()
}
```

Call `SyncFetchers` periodically (e.g., in the reconciler loop or on leader change).

- [ ] **Step 5: Build and test**

```bash
go build ./...
go test ./pkg/cluster/... -count=1 -timeout 60s
```

---

### Task 6: Produce Handler — HWM-Wait ACK

**Files:**
- Modify: `pkg/controller/produce_handler.go`
- Modify: `pkg/cluster/controller/controller.go` (add ComputeHWM)

Replace push replication + AdvanceHWM with HWM-wait.

- [ ] **Step 1: Add ComputeHWM to ClusterController**

```go
// ComputeHWM returns the minimum LEO across all ISR members for a partition.
func (cc *ClusterController) ComputeHWM(topicName string, partitionID int) uint64 {
	fsm := cc.RaftManager.GetFSM()
	if fsm == nil {
		return 0
	}

	key := fmt.Sprintf("%s-%d", topicName, partitionID)
	meta := fsm.GetPartitionMetadata(key)
	if meta == nil {
		return 0
	}

	isrMgr := cc.RaftManager.GetISRManager()
	if isrMgr == nil {
		return 0
	}

	// Start with local LEO
	minLEO := uint64(0)
	if t := cc.tm.GetTopic(topicName); t != nil {
		if p, err := t.GetPartition(partitionID); err == nil {
			minLEO = p.LEO.Load()
		}
	}

	for _, replica := range meta.ISR {
		if replica == cc.brokerID {
			continue
		}
		replicaLEO := isrMgr.GetReplicaLEO(replica, topicName, partitionID)
		if replicaLEO < minLEO {
			minLEO = replicaLEO
		}
	}

	return minLEO
}
```

- [ ] **Step 2: Update single PUBLISH distributed path**

Replace the `ReplicateToFollowers` + `AdvanceHWM` block:

```go
		// 1. Append locally
		if err := p.EnqueueBatchLeader(messageData.Messages); err != nil {
			return ch.errorResponse(fmt.Sprintf("failed to append locally: %v", err))
		}

		assignedOffset := messageData.Messages[0].Offset
		p.FlushDisk()

		// 2. Wait for followers to fetch (HWM-wait)
		if acksLower != "0" {
			minISR := 1
			if acksLower == "-1" || acksLower == "all" {
				minISR = ch.Config.MinInSyncReplicas
			}

			targetOffset := assignedOffset + 1
			deadline := time.Now().Add(time.Duration(ch.Config.AckTimeoutMS) * time.Millisecond)
			if ch.Config.AckTimeoutMS == 0 {
				deadline = time.Now().Add(5 * time.Second)
			}

			for time.Now().Before(deadline) {
				hwm := ch.Cluster.ComputeHWM(topicName, partition)
				if hwm >= targetOffset {
					p.SetHWM(hwm)
					break
				}
				time.Sleep(5 * time.Millisecond)
			}

			// Check ISR quorum
			if isrMgr := ch.Cluster.RaftManager.GetISRManager(); isrMgr != nil {
				if !isrMgr.HasQuorum(topicName, partition, minISR) {
					return ch.errorResponse(fmt.Sprintf("insufficient ISR for acks=%s", acks))
				}
			}
		}

		ackResp = types.AckResponse{...}
```

- [ ] **Step 3: Same for BATCH PUBLISH**

Apply identical pattern to the batch handler.

- [ ] **Step 4: Build and test**

```bash
go build ./...
go test ./pkg/controller/... -count=1 -timeout 60s
```

---

### Task 7: Remove Push Replication

**Files:**
- Modify: `pkg/cluster/controller/controller.go` (delete ReplicateToFollowers)
- Modify: `pkg/controller/produce_handler.go` (delete handleReplicateMessage)
- Modify: `pkg/controller/handler.go` (remove REPLICATE_MESSAGE command)
- Modify: `pkg/server/main.go` (remove from isCommand if present)

- [ ] **Step 1: Delete ReplicateToFollowers method**

Remove the entire `ReplicateToFollowers` function from `pkg/cluster/controller/controller.go`.

- [ ] **Step 2: Delete handleReplicateMessage**

Remove the entire `handleReplicateMessage` function from `pkg/controller/produce_handler.go`.

- [ ] **Step 3: Remove REPLICATE_MESSAGE from command registry**

In `pkg/controller/handler.go`, remove the command entry:
```go
{prefix: "REPLICATE_MESSAGE ", exact: false, handler: ...},
```

- [ ] **Step 4: Build and test**

```bash
go build ./...
go test ./pkg/controller/... ./pkg/cluster/... -count=1 -timeout 60s
```

---

### Task 8: ISR — Fetch-Lag Based

**Files:**
- Modify: `pkg/cluster/replication/isr_manager.go`

Replace heartbeat-only ISR with fetch-lag based.

- [ ] **Step 1: Update ComputeISR to use replica LEOs**

In `ComputeISR`, after the heartbeat-based alive check, add fetch-lag check:

```go
	// Determine alive replicas by fetch lag (pull-based) or heartbeat (fallback)
	i.mu.RLock()
	for _, replica := range metadata.Replicas {
		if broker := i.fsm.GetBroker(replica); broker != nil && broker.Status != "active" {
			continue
		}

		if replica == i.brokerID {
			currentISR = append(currentISR, replica)
			continue
		}

		// Check by fetch lag: replica LEO should be within acceptable range
		key := fmt.Sprintf("%s-%d", topic, partition)
		if leos, ok := i.replicaLEOs[replica]; ok {
			if replicaLEO, ok := leos[key]; ok {
				// Replica has fetched — check if recently active
				if lastSeen, ok := i.lastSeen[replica]; ok {
					if time.Since(lastSeen) < i.heartbeatTimeout {
						currentISR = append(currentISR, replica)
						continue
					}
				}
			}
		}

		// Fallback: heartbeat-only check (for backward compat during migration)
		if lastSeen, ok := i.lastSeen[replica]; ok {
			if time.Since(lastSeen) < i.heartbeatTimeout {
				currentISR = append(currentISR, replica)
			}
		}
	}
	i.mu.RUnlock()
```

This replaces the existing alive-check loop in ComputeISR.

- [ ] **Step 2: Build and test**

```bash
go build ./...
go test ./pkg/cluster/replication/... -count=1 -timeout 60s
```

---

### Task 9: Config — Add Replication Settings

**Files:**
- Modify: `pkg/config/properties.go`

- [ ] **Step 1: Add config fields**

```go
	AckTimeoutMS          int `yaml:"ack_timeout_ms" json:"ack_timeout_ms"`
	ReplicaFetchBatchSize int `yaml:"replica_fetch_batch_size" json:"replica_fetch_batch_size"`
```

Set defaults:
```go
	AckTimeoutMS:          5000,
	ReplicaFetchBatchSize: 1000,
```

- [ ] **Step 2: Build and test**

```bash
go build ./...
go test ./pkg/config/... -count=1 -timeout 60s
```

---

### Task 10: Integration Test — Cluster E2E with Pull Replication

- [ ] **Step 1: Rebuild cluster**

```bash
cd test/cluster && docker compose down && docker compose up -d --build broker-1 broker-2 broker-3
sleep 20
```

- [ ] **Step 2: Test produce + consume with replication**

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

sendt('broker-1', 9000, 'CREATE topic=pull-test partitions=2')
time.sleep(2)

# Produce
for i in range(10):
    ack = sendt('broker-1', 9000, f'PUBLISH topic=pull-test acks=1 producerId=p1 message=msg-{i}')
    print(f'ACK {i}: {ack[:50]}')

time.sleep(1)

# Consume from partition leader
meta = sendt('broker-1', 9000, 'METADATA topic=pull-test')
print(f'METADATA: {meta}')

resp = send('broker-1', 9000, 'CONSUME topic=pull-test partition=0 offset=0 member=m1 group=g1 batch=20')
print(f'CONSUME: {len(resp)} bytes')

# Health
for b in ['broker-1', 'broker-2', 'broker-3']:
    try: sendt(b, 9000, 'LIST'); print(f'{b}: alive')
    except: print(f'{b}: DEAD')
"
```

Expected: All ACKs succeed, CONSUME returns data, all brokers alive.

- [ ] **Step 3: Verify follower has data (read from non-leader)**

```bash
# Find which broker is NOT the leader for partition 0
# Consume from a follower — should also have data (replicated via fetch)
```
