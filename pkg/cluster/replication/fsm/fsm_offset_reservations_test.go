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

func TestOffsetReservationActivationRequiresUpgradeAndSurvivesRestore(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	f.SetTransactionManager(transaction.NewManager())
	register := func(f *BrokerFSM, index uint64, id, status string, version int) interface{} {
		return f.Apply(&raft.Log{Index: index, Data: []byte(fmt.Sprintf(`REGISTER:{"id":%q,"addr":"127.0.0.1:7000","status":%q,"lifecycle_protocol":%d}`, id, status, version))})
	}
	require.Nil(t, register(f, 1, "new", "active", OffsetReservationsProtocolVersion))
	require.Nil(t, register(f, 2, "old", "inactive", OffsetReservationsProtocolVersion-1))
	payload, err := json.Marshal(testTopicCommand("input", 1, 1))
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Index: 3, Data: append([]byte("TOPIC:"), payload...)}))
	staged := transaction.NewManager()
	const id = "reserved-input"
	producer, epoch, err := staged.InitProducerWithMode(id, transaction.ModeProcessingV1)
	require.NoError(t, err)
	owner, ok := f.GetTransactionCoordinator(id)
	require.True(t, ok)
	require.NoError(t, staged.SetCoordinatorEpoch(id, owner.Epoch))
	require.NoError(t, staged.Begin(id, producer, epoch))
	require.NoError(t, staged.AddOffsets(id, producer, epoch, []transaction.OffsetOperation{{Topic: "input", Group: "workers", Member: "worker", Generation: 1, RegistrationEpoch: 1, Offset: 9}}))
	_, err = staged.BeginOffsetReservations(id, producer, epoch)
	require.NoError(t, err)
	snap := staged.ExportState()[id]
	require.ErrorContains(t, resultError(applyTransactionSnapshot(t, f, 4, snap, owner.Owner, owner.Epoch)), "every registered broker")
	require.False(t, f.OffsetReservationsEnabled())
	_, err = f.txn.Status(id)
	require.Error(t, err, "rejected activation must not apply transaction state")
	require.Nil(t, register(f, 5, "old", "inactive", OffsetReservationsProtocolVersion))
	require.Nil(t, applyTransactionSnapshot(t, f, 6, snap, owner.Owner, owner.Epoch))
	require.True(t, f.OffsetReservationsEnabled())
	snapshot, err := f.Snapshot()
	require.NoError(t, err)
	buf := new(bytes.Buffer)
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: buf}))
	restored := NewBrokerFSM(nil, nil)
	restored.SetTransactionManager(transaction.NewManager())
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))))
	require.True(t, restored.OffsetReservationsEnabled())
	require.Error(t, resultError(register(restored, 7, "old", "active", OffsetReservationsProtocolVersion-1)))
	require.Error(t, resultError(register(restored, 8, "another-old", "active", OffsetReservationsProtocolVersion-1)))
	require.Len(t, restored.GetBrokers(), 2)
	var state BrokerFSMState
	require.NoError(t, json.Unmarshal(buf.Bytes(), &state))
	state.OffsetReservationsActivated = false
	invalid, err := json.Marshal(state)
	require.NoError(t, err)
	require.Error(t, NewBrokerFSM(nil, nil).Restore(io.NopCloser(bytes.NewReader(invalid))))
	state.OffsetReservationsActivated = true
	state.Brokers["old"].LifecycleProtocol = OffsetReservationsProtocolVersion - 1
	invalid, err = json.Marshal(state)
	require.NoError(t, err)
	require.ErrorContains(t, NewBrokerFSM(nil, nil).Restore(io.NopCloser(bytes.NewReader(invalid))), "activated offset reservations")
}
