package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type MockRaftManagerForForward struct {
	isLeader      bool
	leaderAddress atomic.Value
	state         *fsm.BrokerFSM
	applyState    *fsm.BrokerFSM
	preApplyErr   error
	applyErr      error
	raftStatus    replication.RaftStatus
	raftStatusErr error
}

func requireTransactionSnapshotsEqual(t *testing.T, expected, actual *transaction.Snapshot) {
	t.Helper()
	expectedJSON, err := json.Marshal(expected)
	require.NoError(t, err)
	actualJSON, err := json.Marshal(actual)
	require.NoError(t, err)
	require.JSONEq(t, string(expectedJSON), string(actualJSON))
}

func (m *MockRaftManagerForForward) IsLeader() bool { return m.isLeader }
func (m *MockRaftManagerForForward) GetLeaderAddress() string {
	addr := m.leaderAddress.Load()
	if addr == nil {
		return ""
	}
	return addr.(string)
}
func (m *MockRaftManagerForForward) ApplyCommand(prefix string, data []byte) error {
	if m.preApplyErr != nil {
		return m.preApplyErr
	}
	state := m.state
	if m.applyState != nil {
		state = m.applyState
	}
	if state == nil {
		return nil
	}
	result := state.Apply(&raft.Log{Data: append([]byte(prefix+":"), data...)})
	if err, ok := result.(error); ok {
		return err
	}
	return m.applyErr
}
func (m *MockRaftManagerForForward) LeaderCh() <-chan bool  { return nil }
func (m *MockRaftManagerForForward) GetFSM() *fsm.BrokerFSM { return m.state }
func (m *MockRaftManagerForForward) GetRaftStatus() (replication.RaftStatus, error) {
	return m.raftStatus, m.raftStatusErr
}
func (m *MockRaftManagerForForward) GetConfiguration() raft.ConfigurationFuture { return nil }
func (m *MockRaftManagerForForward) ReplicateWithQuorum(t string, p int, msg types.Message, i int, idemp bool, scope string) (types.AckResponse, error) {
	return types.AckResponse{}, nil
}
func (m *MockRaftManagerForForward) ReplicateBatchWithQuorum(t string, p int, msgs []types.Message, i int, a string, idemp bool, scope string) (types.AckResponse, error) {
	return types.AckResponse{}, nil
}
func (m *MockRaftManagerForForward) ApplyResponse(p string, d []byte, t time.Duration) (types.AckResponse, error) {
	return types.AckResponse{}, nil
}
func (m *MockRaftManagerForForward) AddVoter(id string, addr string) error          { return nil }
func (m *MockRaftManagerForForward) RemoveServer(id string) error                   { return nil }
func (m *MockRaftManagerForForward) GetISRManager() replication.ISRManagerInterface { return nil }

func TestCommandHandler_isLeaderAndForward_WaitRetry(t *testing.T) {
	tm := topic.NewTopicManager(&config.Config{}, nil, nil)
	cfg := &config.Config{EnabledDistribution: true}
	rm := &MockRaftManagerForForward{isLeader: false}
	cc := &controller.ClusterController{
		RaftManager: rm,
	}

	ch := NewCommandHandler(tm, cfg, nil, nil, cc)

	ready := make(chan struct{})
	go func() {
		rm.leaderAddress.Store("localhost:7001")
		close(ready)
	}()

	<-ready
	resp, forwarded, err := ch.isLeaderAndForward("LIST")

	if !forwarded {
		t.Error("Expected forwarded to be true")
	}
	if !strings.Contains(resp, "router_not_available") {
		t.Errorf("Expected 'router_not_available' error, got %s", resp)
	}
	if err != nil {
		t.Errorf("Expected nil error (handled via response string), got %v", err)
	}
}

func TestWireErrorCodeRequiresExactErrorToken(t *testing.T) {
	if got := wireErrorCode("ERROR: PARTITION_LEADER_FENCED current=node2"); got != "PARTITION_LEADER_FENCED" {
		t.Fatalf("wireErrorCode() = %q", got)
	}
	if got := wireErrorCode("ERROR: raft_apply_failed reason=PARTITION_LEADER_FENCED"); got != "RAFT_APPLY_FAILED" {
		t.Fatalf("wireErrorCode() matched an embedded reason: %q", got)
	}
}

func TestApplyAndWaitClassifiesDurableApplyWithLostResponseAsUnknown(t *testing.T) {
	localFSM := fsm.NewBrokerFSM(nil, nil)
	localFSM.SetTransactionManager(transaction.NewManager())
	remoteManager := transaction.NewManager()
	remoteFSM := fsm.NewBrokerFSM(nil, nil)
	remoteFSM.SetTransactionManager(remoteManager)
	raftManager := &MockRaftManagerForForward{
		state:      localFSM,
		applyState: remoteFSM,
		applyErr:   errors.New("lost raft response"),
	}
	ch := NewCommandHandler(nil, &config.Config{EnabledDistribution: true}, nil, nil, &controller.ClusterController{RaftManager: raftManager})
	t.Cleanup(func() { _ = ch.Close() })

	producer, epoch, err := ch.TxnManager.InitProducer("lost-response")
	require.NoError(t, err)
	require.NoError(t, ch.TxnManager.Begin("lost-response", producer, epoch))
	snapshot, ok := ch.TxnManager.Snapshot("lost-response")
	require.True(t, ok)
	payload, err := ch.transactionSyncPayload(snapshot)
	require.NoError(t, err)

	_, outcome, err := ch.applyViaLeaderWithOutcome("TXN_SYNC", payload)
	require.ErrorContains(t, err, "lost raft response")
	require.Equal(t, raftApplyUnknown, outcome)
	remoteSnapshot, ok := remoteManager.Snapshot("lost-response")
	require.True(t, ok)
	requireTransactionSnapshotsEqual(t, snapshot, remoteSnapshot)
}

func TestApplyAndWaitClassifiesFSMRejection(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	state.SetTransactionManager(transaction.NewManager())
	raftManager := &MockRaftManagerForForward{state: state}
	ch := NewCommandHandler(nil, &config.Config{EnabledDistribution: true}, nil, nil, &controller.ClusterController{RaftManager: raftManager})
	t.Cleanup(func() { _ = ch.Close() })

	producer, epoch, err := ch.TxnManager.InitProducer("rejected")
	require.NoError(t, err)
	stale, ok := ch.TxnManager.Snapshot("rejected")
	require.True(t, ok)
	require.NoError(t, ch.TxnManager.Begin("rejected", producer, epoch))
	payload, err := ch.transactionSyncPayload(stale)
	require.NoError(t, err)

	_, outcome, err := ch.applyViaLeaderWithOutcome("TXN_SYNC", payload)
	require.ErrorContains(t, err, "stale transaction snapshot")
	require.Equal(t, raftApplyRejected, outcome)
}

func TestTransactionSyncRollbackDependsOnRaftOutcome(t *testing.T) {
	t.Run("not accepted restores previous snapshot", func(t *testing.T) {
		localFSM := fsm.NewBrokerFSM(nil, nil)
		localFSM.SetTransactionManager(transaction.NewManager())
		raftManager := &MockRaftManagerForForward{state: localFSM, preApplyErr: raft.ErrNotLeader}
		ch := NewCommandHandler(nil, &config.Config{EnabledDistribution: true}, nil, nil, &controller.ClusterController{RaftManager: raftManager})
		t.Cleanup(func() { _ = ch.Close() })

		producer, epoch, err := ch.TxnManager.InitProducer("not-accepted")
		require.NoError(t, err)
		generation := ch.TxnManager.ReplicatedApplyGeneration("not-accepted")
		previous, ok := ch.TxnManager.Snapshot("not-accepted")
		require.True(t, ok)
		require.NoError(t, ch.TxnManager.Begin("not-accepted", producer, epoch))

		err = ch.syncTransactionMutation("not-accepted", previous, true, generation)
		require.ErrorIs(t, err, raft.ErrNotLeader)
		current, ok := ch.TxnManager.Snapshot("not-accepted")
		require.True(t, ok)
		require.Equal(t, previous, current)
	})

	t.Run("durable apply with lost response preserves retryable snapshot", func(t *testing.T) {
		localFSM := fsm.NewBrokerFSM(nil, nil)
		localFSM.SetTransactionManager(transaction.NewManager())
		remoteManager := transaction.NewManager()
		remoteFSM := fsm.NewBrokerFSM(nil, nil)
		remoteFSM.SetTransactionManager(remoteManager)
		raftManager := &MockRaftManagerForForward{
			state:      localFSM,
			applyState: remoteFSM,
			applyErr:   errors.New("lost raft response"),
		}
		ch := NewCommandHandler(nil, &config.Config{EnabledDistribution: true}, nil, nil, &controller.ClusterController{RaftManager: raftManager})
		t.Cleanup(func() { _ = ch.Close() })

		producer, epoch, err := ch.TxnManager.InitProducer("unknown")
		require.NoError(t, err)
		generation := ch.TxnManager.ReplicatedApplyGeneration("unknown")
		previous, ok := ch.TxnManager.Snapshot("unknown")
		require.True(t, ok)
		require.NoError(t, ch.TxnManager.Begin("unknown", producer, epoch))
		candidate, ok := ch.TxnManager.Snapshot("unknown")
		require.True(t, ok)

		err = ch.syncTransactionMutation("unknown", previous, true, generation)
		require.ErrorContains(t, err, "lost raft response")
		current, ok := ch.TxnManager.Snapshot("unknown")
		require.True(t, ok)
		require.Equal(t, candidate, current)
		remote, ok := remoteManager.Snapshot("unknown")
		require.True(t, ok)
		requireTransactionSnapshotsEqual(t, candidate, remote)

		// Re-syncing the same client operation is idempotent and reuses the
		// exact durable open state.
		ch.Config.EnabledDistribution = false
		response := ch.handleBeginTxn(fmt.Sprintf("BEGIN_TXN transactional_id=unknown producerId=%s epoch=%d", producer, epoch))
		require.Contains(t, response, "OK ")
		retried, ok := ch.TxnManager.Snapshot("unknown")
		require.True(t, ok)
		require.Equal(t, candidate, retried)
	})
}
