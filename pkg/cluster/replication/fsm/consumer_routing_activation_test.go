package fsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func registerConsumerRoutingBroker(t *testing.T, f *BrokerFSM, id, status string, version int) {
	t.Helper()
	require.Nil(t, f.Apply(&raft.Log{Data: []byte(fmt.Sprintf(`REGISTER:{"id":%q,"addr":"localhost:7000","status":%q,"incarnation_id":%q,"lifecycle_protocol":6,"recovery_protocol":%d}`, id, status, id+"-process", version))}))
}

func TestConsumerRoutingActivationRequiresAllBrokersAndPersists(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	registerConsumerRoutingBroker(t, f, "new", "active", 2)
	registerConsumerRoutingBroker(t, f, "old", "inactive", 0)
	activate := func(voters string) interface{} {
		return f.Apply(&raft.Log{Data: []byte(`CONSUMER_ROUTING_ACTIVATE:{"voters":` + voters + `}`)})
	}
	require.Error(t, resultError(activate(`["new"]`)))
	require.False(t, f.ConsumerMetadataRoutingEnabled())
	registerConsumerRoutingBroker(t, f, "old", "inactive", 2)
	require.Error(t, resultError(activate(`["new","missing"]`)))
	require.Error(t, resultError(activate(`["new","new"]`)))
	require.Nil(t, activate(`["new"]`))
	require.Nil(t, activate(`["new"]`))
	require.True(t, f.ConsumerMetadataRoutingEnabled())
	snapshot, err := f.Snapshot()
	require.NoError(t, err)
	buffer := new(bytes.Buffer)
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: buffer}))
	restored := NewBrokerFSM(nil, nil)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buffer.Bytes()))))
	require.True(t, restored.ConsumerMetadataRoutingEnabled())
	require.Error(t, resultError(restored.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"late","incarnation_id":"late-process","lifecycle_protocol":6,"recovery_protocol":0}`)})))
	require.Nil(t, restored.GetBroker("late"))
	var state BrokerFSMState
	require.NoError(t, json.Unmarshal(buffer.Bytes(), &state))
	state.Brokers["old"].RecoveryProtocol = 0
	invalid, err := json.Marshal(state)
	require.NoError(t, err)
	require.ErrorContains(t, NewBrokerFSM(nil, nil).Restore(io.NopCloser(bytes.NewReader(invalid))), "complete recovery capability")
}

func TestActivatedConsumerRoutingRejectsUnprotectedCommands(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	require.NoError(t, f.ValidateConsumerMetadataRoute(config.ConsumerOffsetsTopicName, 0, nil))
	registerConsumerRoutingBroker(t, f, "new", "active", 2)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte(`CONSUMER_ROUTING_ACTIVATE:{"voters":["new"]}`)}))
	f.partitionMetadata[config.ConsumerOffsetsTopicName+"-0"] = &PartitionMetadata{Leader: "new", LeaderEpoch: 1, LifecycleEpoch: 1, PartitionCount: 1, CommittedHWMKnown: true}
	for _, command := range []string{"MESSAGE", "BATCH"} {
		payload, err := json.Marshal(types.MessageCommand{Topic: config.ConsumerOffsetsTopicName, Partition: 0, Messages: []types.Message{{Payload: "unprotected"}}})
		require.NoError(t, err)
		response := f.Apply(&raft.Log{Data: append([]byte(command+":"), payload...)})
		require.Equal(t, "ERROR", response.(types.AckResponse).Status)
	}
	payload, err := json.Marshal(partitionCommitCommand{Topic: config.ConsumerOffsetsTopicName, Leader: "new", LeaderEpoch: 1, LifecycleEpoch: 1, HWM: 1})
	require.NoError(t, err)
	require.ErrorIs(t, resultError(f.Apply(&raft.Log{Data: append([]byte("PARTITION_COMMIT:"), payload...)})), ErrPartitionCommitFenced)
	require.Zero(t, f.partitionMetadata[config.ConsumerOffsetsTopicName+"-0"].CommittedHWM)
}
