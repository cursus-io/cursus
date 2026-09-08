package fsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func applyBrokerMembership(t *testing.T, brokerFSM *BrokerFSM, index uint64, prefix, id string) {
	t.Helper()
	payload := fmt.Sprintf(`{"id":%q,"addr":%q,"status":"active"}`, id, "127.0.0.1:7000")
	result := brokerFSM.Apply(&raft.Log{Index: index, Data: []byte(prefix + ":" + payload)})
	require.Nil(t, result)
}

func applyTransactionSnapshot(t *testing.T, brokerFSM *BrokerFSM, index uint64, snap *transaction.Snapshot, owner string, epoch int64) interface{} {
	t.Helper()
	payload, err := json.Marshal(map[string]interface{}{
		"transaction":       snap,
		"coordinator_owner": owner,
		"coordinator_epoch": epoch,
	})
	require.NoError(t, err)
	return brokerFSM.Apply(&raft.Log{Index: index, Data: append([]byte("TXN_SYNC:"), payload...)})
}

func TestTransactionCoordinatorShardsDistributeAndFencePreviousOwner(t *testing.T) {
	brokerFSM := NewBrokerFSM(nil, nil)
	manager := transaction.NewManager()
	brokerFSM.SetTransactionManager(manager)

	applyBrokerMembership(t, brokerFSM, 1, "REGISTER", "node-a")
	applyBrokerMembership(t, brokerFSM, 2, "REGISTER", "node-b")

	transactionalID := ""
	for i := 0; i < 10_000; i++ {
		candidate := fmt.Sprintf("txn-%d", i)
		ownership, ok := brokerFSM.GetTransactionCoordinator(candidate)
		if ok && ownership.Owner == "node-b" {
			transactionalID = candidate
			break
		}
	}
	require.NotEmpty(t, transactionalID)

	applyBrokerMembership(t, brokerFSM, 3, "DEREGISTER", "node-b")
	oldOwnership, ok := brokerFSM.GetTransactionCoordinator(transactionalID)
	require.True(t, ok)
	require.Equal(t, "node-a", oldOwnership.Owner)

	producerID, producerEpoch, err := manager.InitProducerWithMode(transactionalID, transaction.ModeProcessingV1)
	require.NoError(t, err)
	require.NoError(t, manager.SetCoordinatorEpoch(transactionalID, oldOwnership.Epoch))
	require.NoError(t, manager.Begin(transactionalID, producerID, producerEpoch))
	beforeMove := manager.ExportState()[transactionalID]
	require.NoError(t, resultError(applyTransactionSnapshot(t, brokerFSM, 4, beforeMove, oldOwnership.Owner, oldOwnership.Epoch)))

	applyBrokerMembership(t, brokerFSM, 5, "REGISTER", "node-b")
	newOwnership, ok := brokerFSM.GetTransactionCoordinator(transactionalID)
	require.True(t, ok)
	require.Equal(t, "node-b", newOwnership.Owner)
	require.Greater(t, newOwnership.Epoch, oldOwnership.Epoch)

	stale := *beforeMove
	stale.Revision++
	result := applyTransactionSnapshot(t, brokerFSM, 6, &stale, oldOwnership.Owner, oldOwnership.Epoch)
	require.ErrorContains(t, resultError(result), "transaction coordinator fenced")

	current := manager.ExportState()[transactionalID]
	require.Equal(t, newOwnership.Epoch, current.CoordinatorEpoch)
	require.NoError(t, resultError(applyTransactionSnapshot(t, brokerFSM, 7, current, newOwnership.Owner, newOwnership.Epoch)))
}

func TestTransactionCoordinatorOwnershipChangeSignalsRecovery(t *testing.T) {
	brokerFSM := NewBrokerFSM(nil, nil)
	changes := brokerFSM.TransactionCoordinatorChanges()

	applyBrokerMembership(t, brokerFSM, 1, "REGISTER", "node-a")
	select {
	case changed := <-changes:
		require.Len(t, changed, transaction.DefaultCoordinatorShardCount)
	default:
		t.Fatal("expected transaction coordinator ownership notification")
	}
}

func TestTransactionCoordinatorShardsSurviveSnapshotRestore(t *testing.T) {
	brokerFSM := NewBrokerFSM(nil, nil)
	applyBrokerMembership(t, brokerFSM, 1, "REGISTER", "node-a")
	applyBrokerMembership(t, brokerFSM, 2, "REGISTER", "node-b")
	want, ok := brokerFSM.GetTransactionCoordinator("snapshot-transaction")
	require.True(t, ok)

	snapshot, err := brokerFSM.Snapshot()
	require.NoError(t, err)
	buf := &bytes.Buffer{}
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: buf}))

	restored := NewBrokerFSM(nil, nil)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))))
	got, ok := restored.GetTransactionCoordinator("snapshot-transaction")
	require.True(t, ok)
	require.Equal(t, want, got)
}

func TestTransactionCoordinatorShardCountIsPersistedAndImmutable(t *testing.T) {
	brokerFSM := NewBrokerFSMWithTransactionCoordinatorShards(nil, nil, 7)
	payload := `{"id":"node-a","addr":"127.0.0.1:7000","status":"active","transaction_coordinator_shards":7}`
	require.Nil(t, brokerFSM.Apply(&raft.Log{Index: 1, Data: []byte("REGISTER:" + payload)}))
	require.Equal(t, 7, brokerFSM.TransactionCoordinatorShardCount())

	seen := make(map[int]struct{})
	for i := 0; i < 1_000; i++ {
		shard := transaction.CoordinatorShardForCount(fmt.Sprintf("txn-%d", i), 7)
		require.Less(t, shard, 7)
		seen[shard] = struct{}{}
	}
	require.Len(t, seen, 7)

	snapshot, err := brokerFSM.Snapshot()
	require.NoError(t, err)
	buf := &bytes.Buffer{}
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: buf}))

	restored := NewBrokerFSMWithTransactionCoordinatorShards(nil, nil, 7)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))))
	require.Equal(t, 7, restored.TransactionCoordinatorShardCount())

	mismatched := NewBrokerFSMWithTransactionCoordinatorShards(nil, nil, 8)
	err = mismatched.Restore(io.NopCloser(bytes.NewReader(buf.Bytes())))
	require.ErrorContains(t, err, "configured=8 persisted=7")
}

func TestTransactionCoordinatorShardCountRejectsMismatchedBroker(t *testing.T) {
	brokerFSM := NewBrokerFSMWithTransactionCoordinatorShards(nil, nil, 7)
	require.Nil(t, brokerFSM.Apply(&raft.Log{
		Index: 1,
		Data:  []byte(`REGISTER:{"id":"node-a","addr":"127.0.0.1:7000","status":"active","transaction_coordinator_shards":7}`),
	}))
	result := brokerFSM.Apply(&raft.Log{
		Index: 2,
		Data:  []byte(`REGISTER:{"id":"node-b","addr":"127.0.0.1:7001","status":"active","transaction_coordinator_shards":8}`),
	})
	require.ErrorContains(t, resultError(result), "configured=8 cluster=7")
	require.Len(t, brokerFSM.GetBrokers(), 1)
}

func TestTransactionCoordinatorShardCountRejectsMismatchedFirstBroker(t *testing.T) {
	brokerFSM := NewBrokerFSMWithTransactionCoordinatorShards(nil, nil, 7)
	result := brokerFSM.Apply(&raft.Log{
		Index: 1,
		Data:  []byte(`REGISTER:{"id":"node-a","addr":"127.0.0.1:7000","status":"active","transaction_coordinator_shards":8}`),
	})
	require.ErrorContains(t, resultError(result), "configured=8 cluster=7")
	require.Empty(t, brokerFSM.GetBrokers())
}

func TestTransactionCoordinatorShardCountLegacyRegistrationUsesConfiguredCount(t *testing.T) {
	brokerFSM := NewBrokerFSMWithTransactionCoordinatorShards(nil, nil, 7)
	require.Nil(t, brokerFSM.Apply(&raft.Log{
		Index: 1,
		Data:  []byte(`REGISTER:{"id":"node-a","addr":"127.0.0.1:7000","status":"active"}`),
	}))
	require.Equal(t, 7, brokerFSM.TransactionCoordinatorShardCount())
}

func TestRestoreRejectsLegacyTransactionCoordinatorSnapshot(t *testing.T) {
	state := BrokerFSMState{Version: 7}
	data, err := json.Marshal(state)
	require.NoError(t, err)

	brokerFSM := NewBrokerFSM(nil, nil)
	err = brokerFSM.Restore(io.NopCloser(bytes.NewReader(data)))
	require.ErrorContains(t, err, "unsupported recovery protocol")
}

func TestRestoreRejectsTransactionCoordinatorShardOutsidePersistedCount(t *testing.T) {
	state := BrokerFSMState{
		Version:                          SnapshotVersionCurrent,
		TransactionCoordinatorShardCount: 7,
		TransactionCoordinatorShards: map[int]TransactionCoordinatorShard{
			7: {Owner: "node-a", Epoch: 1},
		},
	}
	data, err := json.Marshal(state)
	require.NoError(t, err)

	brokerFSM := NewBrokerFSMWithTransactionCoordinatorShards(nil, nil, 7)
	err = brokerFSM.Restore(io.NopCloser(bytes.NewReader(data)))
	require.ErrorContains(t, err, "shard 7 outside configured count 7")
}

func resultError(result interface{}) error {
	if result == nil {
		return nil
	}
	err, _ := result.(error)
	return err
}
