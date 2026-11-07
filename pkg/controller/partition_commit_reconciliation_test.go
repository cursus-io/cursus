package controller

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// unavailableCommitRaft retains a delayed metadata command while simulating an
// election longer than the queue's bounded uncertain-commit retry.
type unavailableCommitRaft struct {
	*MockRaftManagerForForward
	unavailable atomic.Bool
	pending     []byte
}

// ApplyCommand fails HWM commits until the test restores metadata availability.
func (m *unavailableCommitRaft) ApplyCommand(prefix string, data []byte) error {
	if prefix == "PARTITION_COMMIT" && m.unavailable.Load() {
		m.pending = append([]byte(prefix+":"), data...)
		return errors.New("leader unknown, cannot process command")
	}
	return m.MockRaftManagerForForward.ApplyCommand(prefix, data)
}

// TestPartitionCommitRecoveryAfterRaftOutage requires a producer retry to
// recover the original offset without rewriting its retained log bytes.
func TestPartitionCommitRecoveryAfterRaftOutage(t *testing.T) {
	h, manager, _ := newDistributedAckTestHandler(t, 1)
	require.NoError(t, manager.CreateTopic("orders", 1, true, false))
	installPartitionMetadata(t, h, "orders", []string{"broker-1"})
	rm := &unavailableCommitRaft{MockRaftManagerForForward: h.Cluster.RaftManager.(*MockRaftManagerForForward)}
	h.Cluster.RaftManager, h.Cluster.Router = rm, nil
	h.replication.close()
	h.replication = newPartitionReplicationCoordinator(2, clusterPartitionReplicationExecutor{handler: h})
	p, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	command := "PUBLISH topic=orders partition=0 acks=all producerId=p1 isIdempotent=true seqNum=1 epoch=7 message=original"
	rm.unavailable.Store(true)
	require.Contains(t, h.HandleCommand(command, NewClientContext("", 0)), "ERROR")
	require.Error(t, p.ReconciliationError())
	uncommitted, _, _, readErr := readPartitionPage(p, 0, 10, 1<<20, true, ReadIsolationCommitted)
	require.Empty(t, uncommitted)
	require.ErrorContains(t, readErr, "ERROR: recovery_busy")
	require.Equal(t, uint64(1), p.NextOffset())
	require.Zero(t, p.GetHWM())
	files, err := filepath.Glob(filepath.Join(h.Config.LogDir, "orders", "*.log"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	before := map[string][]byte{}
	for _, path := range files {
		before[path], err = os.ReadFile(path)
		require.NoError(t, err)
	}
	rm.unavailable.Store(false)
	response := h.HandleCommand(command, NewClientContext("", 0))
	var ack struct {
		Status string `json:"status"`
		Offset uint64 `json:"last_offset"`
	}
	require.NoError(t, json.Unmarshal([]byte(response), &ack), response)
	require.Equal(t, "OK", ack.Status)
	require.Zero(t, ack.Offset)
	require.NoError(t, p.RecoveryError())
	visible, _, _, readErr := readPartitionPage(p, 0, 10, 1<<20, true, ReadIsolationCommitted)
	require.NoError(t, readErr)
	require.Len(t, visible, 1)
	require.Equal(t, uint64(1), p.NextOffset())
	require.Equal(t, uint64(1), p.GetHWM())
	for path, original := range before {
		after, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		require.Equal(t, original, after, "reconciliation must rewrite zero retained log bytes")
	}
	require.NotEmpty(t, rm.pending)
	require.Nil(t, rm.state.Apply(&raft.Log{Data: rm.pending}))
	records, err := p.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, "original", records[0].Payload)
	t.Logf("recovered_records=%d payload_bytes=%d local_log_bytes_rewritten=0", len(records), len(records[0].Payload))
	next := strings.ReplaceAll(strings.ReplaceAll(command, "seqNum=1", "seqNum=2"), "message=original", "message=next")
	require.NoError(t, json.Unmarshal([]byte(h.HandleCommand(next, NewClientContext("", 0))), &ack))
	require.Equal(t, "OK", ack.Status)
	require.Equal(t, uint64(1), ack.Offset)
}

// TestPartitionCommitRecoveryProof rejects unsafe recovery while permitting a
// contiguous batch to resume under the unchanged leader and replica assignment.
func TestPartitionCommitRecoveryProof(t *testing.T) {
	for _, kind := range []string{"success", "changed_isr", "leader", "lifecycle", "replicas", "insufficient_isr", "route", "record", "target", "unknown", "cancel", "record_epoch", "sequence"} {
		t.Run(kind, func(t *testing.T) {
			h, manager, _ := newDistributedAckTestHandler(t, 1)
			require.NoError(t, manager.CreateTopic("orders", 1, false, false))
			installPartitionMetadata(t, h, "orders", []string{"broker-1"})
			p, err := manager.GetTopic("orders").GetPartition(0)
			require.NoError(t, err)
			messages := []types.Message{{Offset: 0, LeaderEpoch: 7, Payload: "first"}, {Offset: 1, LeaderEpoch: 7, Payload: "second"}}
			require.NoError(t, p.ReplicaAppend(messages))
			snapshot, err := h.Cluster.GetPartitionReplicationSnapshot("orders", 0)
			require.NoError(t, err)
			pending := retainPartitionCommitOutcome(partitionReplicationTask{command: types.MessageCommand{Topic: "orders", Partition: 0, Messages: messages, LifecycleEpoch: snapshot.LifecycleEpoch}, snapshot: snapshot, commitHWM: 2, requiredISR: 1}, errors.New("Raft unavailable"))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch kind {
			case "changed_isr":
				pending.snapshot.ISR = []string{"broker-1", "broker-2"}
			case "leader":
				pending.snapshot.LeaderEpoch--
			case "lifecycle":
				pending.snapshot.LifecycleEpoch++
			case "replicas":
				pending.snapshot.Replicas = []string{"broker-1", "other"}
			case "insufficient_isr":
				pending.requiredISR = 2
			case "route":
				pending.command.ConsumerRoute = &types.ConsumerMetadataRoute{Group: "stale", Owner: "other", Partition: 0}
			case "record":
				pending.command.Messages[0].Payload = "replacement"
			case "target":
				pending.target = 3
			case "cancel":
				cancel()
			case "record_epoch":
				pending.command.Messages[0].LeaderEpoch--
			case "sequence":
				pending.command.Messages[1].Offset = 0
			}
			p.MarkReconciliationPending(pending)
			if kind == "unknown" {
				p.MarkReconciliationPending(errors.New("quarantine decision unavailable"))
			}
			marker := p.ReconciliationError()
			releaseWrite, releaseMutation, _, err := h.preparePartitionLeaderSnapshotContext(ctx, "orders", 0, p, 1)
			if kind == "success" || kind == "changed_isr" {
				require.NoError(t, err)
				releaseMutation()
				releaseWrite()
				require.NoError(t, p.RecoveryError())
				require.Equal(t, uint64(2), p.GetHWM())
			} else {
				require.Error(t, err)
				require.Same(t, marker, p.ReconciliationError())
				require.Zero(t, p.GetHWM())
			}
			retained, err := p.ReadMessages(0, 10)
			require.NoError(t, err)
			require.Equal(t, messages, retained)
		})
	}
}
