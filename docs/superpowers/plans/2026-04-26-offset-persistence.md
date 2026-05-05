# Offset Persistence in FSM Snapshot — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist consumer group offsets and membership in Raft FSM snapshots so they survive broker restarts.

**Architecture:** Add `ExportState()` to Coordinator that serializes group state. Add `ImportState()` to restore it. Include this in `BrokerFSMState`, `BrokerFSMSnapshot.Persist`, and `BrokerFSM.Restore`. Bump snapshot version to 2 with backward compatibility.

**Tech Stack:** Go

---

### Task 1: Coordinator ExportState / ImportState

**Files:**
- Modify: `pkg/coordinator/coordinator.go`

- [ ] **Step 1: Define GroupStateSnapshot type**

Add to `pkg/coordinator/coordinator.go`:

```go
// GroupStateSnapshot is a serializable snapshot of a consumer group's state.
type GroupStateSnapshot struct {
	TopicName  string                    `json:"topic"`
	Generation int                       `json:"generation"`
	Members    map[string][]int          `json:"members"`  // memberID -> assigned partitions
	Offsets    map[string]map[int]uint64 `json:"offsets"`   // topic -> partition -> offset
}
```

- [ ] **Step 2: Add ExportState method**

```go
// ExportState returns a serializable snapshot of all consumer groups.
func (c *Coordinator) ExportState() map[string]*GroupStateSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]*GroupStateSnapshot, len(c.groups))
	for name, group := range c.groups {
		group.mu.RLock()
		snap := &GroupStateSnapshot{
			TopicName:  group.TopicName,
			Generation: group.Generation,
			Members:    make(map[string][]int, len(group.Members)),
			Offsets:    make(map[string]map[int]uint64),
		}
		for mid, member := range group.Members {
			assignments := make([]int, len(member.Assignments))
			copy(assignments, member.Assignments)
			snap.Members[mid] = assignments
		}
		for topic, partitions := range group.Offsets {
			snap.Offsets[topic] = make(map[int]uint64, len(partitions))
			for pid, offset := range partitions {
				snap.Offsets[topic][pid] = offset
			}
		}
		group.mu.RUnlock()
		result[name] = snap
	}
	return result
}
```

- [ ] **Step 3: Add ImportState method**

```go
// ImportState restores consumer group state from a snapshot.
func (c *Coordinator) ImportState(state map[string]*GroupStateSnapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for name, snap := range state {
		group := &GroupMetadata{
			TopicName:  snap.TopicName,
			Generation: snap.Generation,
			Members:    make(map[string]*MemberMetadata, len(snap.Members)),
			Partitions: make([]int, 0),
			Offsets:    make(map[string]map[int]uint64),
		}

		for mid, assignments := range snap.Members {
			group.Members[mid] = &MemberMetadata{
				ID:            mid,
				LastHeartbeat: time.Now(),
				Assignments:   assignments,
			}
		}

		for topic, partitions := range snap.Offsets {
			group.Offsets[topic] = make(map[int]uint64, len(partitions))
			for pid, offset := range partitions {
				group.Offsets[topic][pid] = offset
			}
		}

		// Derive partition list from offsets
		if topicOffsets, ok := snap.Offsets[snap.TopicName]; ok {
			for pid := range topicOffsets {
				group.Partitions = append(group.Partitions, pid)
			}
		}

		c.groups[name] = group
	}
}
```

- [ ] **Step 4: Build and test**

```bash
go build ./...
go test ./pkg/coordinator/... -count=1 -timeout 60s
```

- [ ] **Step 5: Do NOT commit**

---

### Task 2: FSM Snapshot — Include Group State

**Files:**
- Modify: `pkg/cluster/replication/fsm/fsm.go` (BrokerFSMState, Snapshot, Restore)
- Modify: `pkg/cluster/replication/fsm/fsm_snapshot.go` (BrokerFSMSnapshot, Persist)

- [ ] **Step 1: Add GroupState to BrokerFSMState**

In `pkg/cluster/replication/fsm/fsm.go`, update `BrokerFSMState`:

```go
type BrokerFSMState struct {
	Version           int                                 `json:"version"`
	Applied           uint64                              `json:"applied"`
	Logs              map[uint64]*ReplicationEntry        `json:"logs"`
	Brokers           map[string]*BrokerInfo              `json:"brokers"`
	PartitionMetadata map[string]*PartitionMetadata       `json:"partitionMetadata"`
	ProducerState     map[string]map[int]map[string]int64 `json:"producerState"`
	GroupState        map[string]*coordinator.GroupStateSnapshot `json:"groupState,omitempty"`
}
```

Add import: `"github.com/cursus-io/cursus/pkg/coordinator"`

- [ ] **Step 2: Update BrokerFSMSnapshot to include group state**

In `pkg/cluster/replication/fsm/fsm_snapshot.go`, add field:

```go
type BrokerFSMSnapshot struct {
	applied           uint64
	logs              map[uint64]*ReplicationEntry
	brokers           map[string]*BrokerInfo
	partitionMetadata map[string]*PartitionMetadata
	producerState     map[string]map[int]map[string]int64
	groupState        map[string]*coordinator.GroupStateSnapshot
}
```

Update `Persist`:

```go
func (s *BrokerFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	state := BrokerFSMState{
		Version:           2,
		Applied:           s.applied,
		Logs:              s.logs,
		Brokers:           s.brokers,
		PartitionMetadata: s.partitionMetadata,
		ProducerState:     s.producerState,
		GroupState:        s.groupState,
	}
	// ... rest unchanged
```

Add import: `"github.com/cursus-io/cursus/pkg/coordinator"`

- [ ] **Step 3: Update Snapshot() to export group state**

In `pkg/cluster/replication/fsm/fsm.go`, in the `Snapshot()` method, after copying producerState, add:

```go
	var groupState map[string]*coordinator.GroupStateSnapshot
	if f.cd != nil {
		groupState = f.cd.ExportState()
	}
```

And include it in the return:

```go
	return &BrokerFSMSnapshot{
		applied:           f.applied,
		logs:              logsCopy,
		brokers:           brokersCopy,
		partitionMetadata: metadataCopy,
		producerState:     producerStateCopy,
		groupState:        groupState,
	}, nil
```

- [ ] **Step 4: Update Restore() to import group state**

In `pkg/cluster/replication/fsm/fsm.go`, in `Restore()`, after restoring existing fields, add:

```go
	// Restore consumer group state (version 2+)
	if state.GroupState != nil && f.cd != nil {
		f.cd.ImportState(state.GroupState)
		util.Info("FSM Restore: Restored %d consumer groups from snapshot", len(state.GroupState))
	}
```

- [ ] **Step 5: Build and test**

```bash
go build ./...
go test ./pkg/cluster/replication/fsm/... -count=1 -timeout 60s
```

- [ ] **Step 6: Do NOT commit**

---

### Task 3: Test — Snapshot Restore preserves offsets

**Files:**
- Modify: `pkg/cluster/replication/fsm/fsm_test.go`

- [ ] **Step 1: Add test**

```go
func TestBrokerFSM_Snapshot_Restore_GroupState(t *testing.T) {
	cfg := &config.Config{LogDir: t.TempDir()}
	tm := topic.NewTopicManager(cfg, nil, nil)
	cd := coordinator.NewCoordinator(context.Background(), cfg, tm)

	fsm := NewBrokerFSM(tm, cd)

	// Create a group with offsets
	_ = cd.RegisterGroup("test-topic", "test-group", 4)
	_, _ = cd.AddConsumer("test-group", "member-1")
	cd.CommitOffset("test-group", "test-topic", 0, 42)
	cd.CommitOffset("test-group", "test-topic", 1, 100)

	// Snapshot
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)

	buf := new(bytes.Buffer)
	sink := &MockSnapshotSink{Writer: buf}
	require.NoError(t, snapshot.Persist(sink))

	// Restore into new FSM with fresh coordinator
	newCd := coordinator.NewCoordinator(context.Background(), cfg, tm)
	newFSM := NewBrokerFSM(tm, newCd)
	rc := io.NopCloser(bytes.NewReader(buf.Bytes()))
	require.NoError(t, newFSM.Restore(rc))

	// Verify offsets restored
	offset, found := newCd.GetOffset("test-group", "test-topic", 0)
	assert.True(t, found)
	assert.Equal(t, uint64(42), offset)

	offset2, found2 := newCd.GetOffset("test-group", "test-topic", 1)
	assert.True(t, found2)
	assert.Equal(t, uint64(100), offset2)
}
```

Note: Add required imports (`context`, `coordinator`, `config`, `topic`, `assert`, `require`).

- [ ] **Step 2: Run test**

```bash
go test ./pkg/cluster/replication/fsm/ -run TestBrokerFSM_Snapshot_Restore_GroupState -v
```

- [ ] **Step 3: Do NOT commit**

---

### Task 4: Build + Lint + Full Test

- [ ] **Step 1: Full build and lint**

```bash
go build ./...
go vet ./...
golangci-lint run ./... --timeout 120s
```

- [ ] **Step 2: Full test suite**

```bash
go test ./pkg/... ./sdk/... ./util/... -count=1 -timeout 120s
```

- [ ] **Step 3: Do NOT commit**
