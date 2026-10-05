package fsm

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/stretchr/testify/require"
)

func testProducerEpochWatermark() *uint64 {
	value := uint64(0)
	return &value
}

func TestProducerEpochSurvivesRaftSnapshotAfterAllTransactionsExpire(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		t.Run(map[bool]string{false: "attached", true: "deferred"}[deferred], func(t *testing.T) {
			m := transaction.NewManagerWithExpiration(time.Hour)
			producer, epoch, err := m.InitProducer("retired")
			require.NoError(t, err)
			// Reproduce retention on a replica that learns epochs through TXN_SYNC.
			f := NewBrokerFSM(nil, nil)
			peer := transaction.NewManagerWithExpiration(time.Hour)
			f.SetTransactionManager(peer)
			require.Nil(t, applyTransactionSnapshot(t, f, 1, m.ExportState()["retired"], "", 0))
			now := time.Now()
			require.Equal(t, 1, peer.PruneExpired(now.Add(2*time.Hour)))
			require.Equal(t, 1, peer.PruneExpired(now.Add(4*time.Hour)))
			require.Empty(t, peer.ExportState())
			snap, err := f.Snapshot()
			require.NoError(t, err)
			buf := new(bytes.Buffer)
			require.NoError(t, snap.Persist(&MockSnapshotSink{Writer: buf}))
			restored := NewBrokerFSM(nil, nil)
			manager := transaction.NewManager()
			if !deferred {
				restored.SetTransactionManager(manager)
			}
			require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))))
			if deferred {
				// A second snapshot before manager attachment must retain the floor.
				snap, err = restored.Snapshot()
				require.NoError(t, err)
				buf.Reset()
				require.NoError(t, snap.Persist(&MockSnapshotSink{Writer: buf}))
				restored = NewBrokerFSM(nil, nil)
				require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))))
				restored.SetTransactionManager(manager)
			}
			newProducer, next, err := manager.InitProducer("retired")
			require.NoError(t, err)
			require.NotEqual(t, producer, newProducer, "a fully retired producer identity must not be reused")
			require.Greater(t, next, epoch)
			var state BrokerFSMState
			require.NoError(t, json.Unmarshal(buf.Bytes(), &state))
			invalidEpoch := uint64(math.MaxUint64)
			state.NextProducerEpoch = &invalidEpoch
			invalid, err := json.Marshal(state)
			require.NoError(t, err)
			require.ErrorContains(t, restored.Restore(io.NopCloser(bytes.NewReader(invalid))), "producer epoch")
		})
	}
}

func TestVersionNineSnapshotMigratesProducerEpochAfterTransactionGC(t *testing.T) {
	legacy := BrokerFSMState{
		Version: SnapshotVersionLegacyEpoch,
		ProducerState: map[string]map[int]map[string]ProducerSequence{
			"retained-output": {0: {"txn-old-identity": {Epoch: 41, Seq: 1}}},
		},
	}
	data, err := json.Marshal(legacy)
	require.NoError(t, err)
	m := transaction.NewManager()
	f := NewBrokerFSM(nil, nil)
	f.SetTransactionManager(m)
	require.NoError(t, f.Restore(io.NopCloser(bytes.NewReader(data))))
	producer, epoch, err := m.InitProducer("retired")
	require.NoError(t, err)
	require.NotEqual(t, "txn-old-identity", producer)
	require.Equal(t, int64(42), epoch)

	current := legacy
	current.Version = SnapshotVersionCurrent
	data, err = json.Marshal(current)
	require.NoError(t, err)
	require.ErrorContains(t, NewBrokerFSM(nil, nil).Restore(io.NopCloser(bytes.NewReader(data))), "missing producer epoch watermark")
}
