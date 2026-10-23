package fsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func registerConsumerHistoryBroker(t *testing.T, f *BrokerFSM, id, status string, version int) {
	t.Helper()
	require.Nil(t, f.Apply(&raft.Log{Data: []byte(fmt.Sprintf(`REGISTER:{"id":%q,"addr":"localhost:7000","status":%q,"incarnation_id":%q,"lifecycle_protocol":10,"recovery_protocol":%d}`, id, status, id+"-process", version))}))
}

func TestConsumerHistoryRejectsCleanupOnlyProtocol(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	registerConsumerHistoryBroker(t, f, "new", "active", BootstrapRecoveryProtocolVersion)
	registerConsumerHistoryBroker(t, f, "cleanup-only", "inactive", BootstrapRecoveryProtocolVersion)
	f.brokers["cleanup-only"].LifecycleProtocol = RetentionTimeProtocolVersion
	require.ErrorContains(t, resultError(f.Apply(&raft.Log{Data: []byte(`CONSUMER_HISTORY_ACTIVATE:{"voters":["new"]}`)})), "complete recovery capability")
	require.False(t, f.ConsumerHistoryEnabled())
	f.brokers["cleanup-only"].LifecycleProtocol = ConsumerHistoryProtocolVersion
	require.Nil(t, f.Apply(&raft.Log{Data: []byte(`CONSUMER_HISTORY_ACTIVATE:{"voters":["new"]}`)}))
	require.True(t, f.ConsumerHistoryEnabled())
	require.Error(t, resultError(f.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"old","incarnation_id":"old-process","lifecycle_protocol":9,"recovery_protocol":2}`)})))
	require.Nil(t, f.GetBroker("old"))
}

func TestConsumerHistorySnapshotPreservesTruncateCleanupBoundary(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	registerConsumerHistoryBroker(t, f, "new", "active", BootstrapRecoveryProtocolVersion)
	d := topic.DefaultDefinition("orders", nil)
	d.Partitions, d.LifecycleEpoch, d.Revision = 1, 99, 2
	f.topicState[d.Name] = &d
	f.partitionMetadata[d.Name+"-0"] = authoritativePartitionMetadata(1)
	f.partitionMetadata[d.Name+"-0"].LifecycleEpoch = d.LifecycleEpoch
	f.captureTopicCleanupBoundaryLocked(d)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte(`CONSUMER_HISTORY_ACTIVATE:{"voters":["new"]}`)}))
	snapshot, err := f.Snapshot()
	require.NoError(t, err)
	var buffer bytes.Buffer
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: &buffer}))
	restored := NewBrokerFSM(nil, nil)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buffer.Bytes()))))
	require.True(t, restored.ConsumerHistoryEnabled())
	require.NotEmpty(t, restored.topicCleanupBoundaries)
	require.Equal(t, f.topicCleanupBoundaries, restored.topicCleanupBoundaries)
}

func TestConsumerHistoryActivationRequiresAllBrokersAndPersists(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	registerConsumerHistoryBroker(t, f, "new", "active", 2)
	registerConsumerHistoryBroker(t, f, "old", "inactive", 0)
	activate := func(voters string) interface{} {
		return f.Apply(&raft.Log{Data: []byte(`CONSUMER_HISTORY_ACTIVATE:{"voters":` + voters + `}`)})
	}
	require.Error(t, resultError(activate(`["new"]`)))
	require.False(t, f.ConsumerHistoryEnabled())
	registerConsumerHistoryBroker(t, f, "old", "inactive", 2)
	require.Error(t, resultError(activate(`["new","missing"]`)))
	require.Error(t, resultError(activate(`["new","new"]`)))
	require.Nil(t, activate(`["new"]`))
	require.Nil(t, activate(`["new"]`))
	require.True(t, f.ConsumerHistoryEnabled())
	snapshot, err := f.Snapshot()
	require.NoError(t, err)
	buffer := new(bytes.Buffer)
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: buffer}))
	restored := NewBrokerFSM(nil, nil)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buffer.Bytes()))))
	require.True(t, restored.ConsumerHistoryEnabled())
	require.Error(t, resultError(restored.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"late","incarnation_id":"late-process","lifecycle_protocol":10,"recovery_protocol":0}`)})))
	require.Nil(t, restored.GetBroker("late"))
	var state BrokerFSMState
	require.NoError(t, json.Unmarshal(buffer.Bytes(), &state))
	state.Brokers["old"].RecoveryProtocol = 0
	invalid, err := json.Marshal(state)
	require.NoError(t, err)
	require.ErrorContains(t, NewBrokerFSM(nil, nil).Restore(io.NopCloser(bytes.NewReader(invalid))), "complete recovery capability")
}
