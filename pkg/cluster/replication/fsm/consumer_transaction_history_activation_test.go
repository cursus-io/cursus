package fsm

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestConsumerTransactionHistoryActivationAndRestoreGate(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	registerConsumerHistoryBroker(t, f, "new", "active", 2)
	registerConsumerHistoryBroker(t, f, "offline", "inactive", 2)
	activate := func(voters string) interface{} {
		return f.Apply(&raft.Log{Data: []byte(`CONSUMER_TRANSACTION_HISTORY_ACTIVATE:{"voters":` + voters + `}`)})
	}
	require.ErrorContains(t, resultError(activate(`["new"]`)), "consumer history activation required")
	require.Nil(t, f.Apply(&raft.Log{Data: []byte(`CONSUMER_HISTORY_ACTIVATE:{"voters":["new"]}`)}))
	f.brokers["new"].LifecycleProtocol = ConsumerTransactionHistoryProtocolVersion
	require.ErrorContains(t, resultError(activate(`["new"]`)), "complete recovery capability")
	require.False(t, f.ConsumerTransactionHistoryEnabled())
	f.brokers["offline"].LifecycleProtocol = ConsumerTransactionHistoryProtocolVersion
	for _, voters := range []string{`[]`, `["new","new"]`, `["missing"]`} {
		require.Error(t, resultError(activate(voters)))
	}
	require.Nil(t, activate(`["new"]`))
	require.Nil(t, activate(`["new"]`))
	snapshot, err := f.Snapshot()
	require.NoError(t, err)
	defer snapshot.Release()
	var data bytes.Buffer
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: &data}))
	var state BrokerFSMState
	require.NoError(t, json.Unmarshal(data.Bytes(), &state))
	require.Equal(t, SnapshotVersionTransactionHistory, state.Version)
	restored := NewBrokerFSM(nil, nil)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(data.Bytes()))))
	require.True(t, restored.ConsumerTransactionHistoryEnabled())
	require.ErrorContains(t, resultError(restored.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"old","incarnation_id":"old-process","lifecycle_protocol":10,"recovery_protocol":2}`)})), "consumer_transaction_history_protocol")
	require.Nil(t, restored.GetBroker("old"))
	for _, mutation := range []string{"version", "base gate", "offline broker"} {
		t.Run(mutation, func(t *testing.T) {
			var invalid BrokerFSMState
			require.NoError(t, json.Unmarshal(data.Bytes(), &invalid))
			switch mutation {
			case "version":
				invalid.Version = SnapshotVersionCooperative
			case "base gate":
				invalid.ConsumerHistoryActivated = false
			case "offline broker":
				invalid.Brokers["offline"].LifecycleProtocol = ConsumerHistoryProtocolVersion
			}
			encoded, err := json.Marshal(invalid)
			require.NoError(t, err)
			require.Error(t, NewBrokerFSM(nil, nil).Restore(io.NopCloser(bytes.NewReader(encoded))))
		})
	}
}
