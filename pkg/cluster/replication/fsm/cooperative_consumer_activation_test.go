package fsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func registerCooperativeConsumerBroker(t *testing.T, f *BrokerFSM, id, status string, version int) {
	t.Helper()
	require.Nil(t, f.Apply(&raft.Log{Data: []byte(fmt.Sprintf(`REGISTER:{"id":%q,"addr":"localhost:7000","status":%q,"incarnation_id":%q,"lifecycle_protocol":10,"recovery_protocol":%d}`, id, status, id+"-process", version))}))
}

func TestCooperativeConsumerActivationRequiresAllBrokersAndPersists(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	registerCooperativeConsumerBroker(t, f, "new", "active", 2)
	registerCooperativeConsumerBroker(t, f, "old", "inactive", 0)
	activate := func(voters string) interface{} {
		return f.Apply(&raft.Log{Data: []byte(`COOPERATIVE_CONSUMER_ACTIVATE:{"voters":` + voters + `}`)})
	}
	require.Error(t, resultError(activate(`["new"]`)))
	require.False(t, f.CooperativeConsumerEnabled())
	registerCooperativeConsumerBroker(t, f, "old", "inactive", 2)
	require.Error(t, resultError(activate(`["new","missing"]`)))
	require.Error(t, resultError(activate(`["new","new"]`)))
	require.Nil(t, activate(`["new"]`))
	require.Nil(t, activate(`["new"]`))
	require.True(t, f.CooperativeConsumerEnabled())
	snapshot, err := f.Snapshot()
	require.NoError(t, err)
	buffer := new(bytes.Buffer)
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: buffer}))
	restored := NewBrokerFSM(nil, nil)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buffer.Bytes()))))
	require.True(t, restored.CooperativeConsumerEnabled())
	require.Error(t, resultError(restored.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"late","incarnation_id":"late-process","lifecycle_protocol":10,"recovery_protocol":0}`)})))
	require.Nil(t, restored.GetBroker("late"))
	var state BrokerFSMState
	require.NoError(t, json.Unmarshal(buffer.Bytes(), &state))
	require.Equal(t, SnapshotVersionCooperativeRuntime, state.Version)
	state.Version = SnapshotVersionCooperative
	wrongVersion, err := json.Marshal(state)
	require.NoError(t, err)
	require.ErrorContains(t, NewBrokerFSM(nil, nil).Restore(io.NopCloser(bytes.NewReader(wrongVersion))), "requires snapshot version 13")
	state.Version = SnapshotVersionCooperativeRuntime
	require.Error(t, resultError(restored.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"old-lifecycle","incarnation_id":"old-process","lifecycle_protocol":9,"recovery_protocol":2}`)})))
	state.Brokers["old"].RecoveryProtocol = 0
	invalid, err := json.Marshal(state)
	require.NoError(t, err)
	require.ErrorContains(t, NewBrokerFSM(nil, nil).Restore(io.NopCloser(bytes.NewReader(invalid))), "complete recovery capability")
}
