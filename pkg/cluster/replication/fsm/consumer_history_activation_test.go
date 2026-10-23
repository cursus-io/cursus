package fsm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestConsumerHistoryActivationSurvivesCooperativeSnapshot(t *testing.T) {
	ownership, err := coordinator.NewCooperativeAssignment(19, []coordinator.CooperativePartition{{Topic: "orders", Partition: 0, LifecycleEpoch: 7}})
	require.NoError(t, err)
	ownership, err = ownership.Join("member", nil)
	require.NoError(t, err)
	c, err := coordinator.NewCoordinatorWithRecovery(context.Background(), config.DefaultConfig(), &registrationActivityReader{})
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	require.NoError(t, c.ImportState(map[string]*coordinator.GroupStateSnapshot{"workers": {
		TopicName: "orders", Partitions: []int{0}, RegistrationEpoch: 19,
		Members: map[string][]int{"member": {0}}, LastActivity: time.Unix(100, 0).UTC(),
		Offsets: map[string]map[int]uint64{}, Cooperative: ownership,
	}}))
	f := NewBrokerFSM(nil, c)
	registerConsumerHistoryBroker(t, f, "new", "active", BootstrapRecoveryProtocolVersion)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte(`CONSUMER_HISTORY_ACTIVATE:{"voters":["new"]}`)}))
	snapshot, err := f.Snapshot()
	require.NoError(t, err)
	defer snapshot.Release()
	var buffer bytes.Buffer
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: &buffer}))
	var state BrokerFSMState
	require.NoError(t, json.Unmarshal(buffer.Bytes(), &state))
	require.Equal(t, SnapshotVersionCooperative, state.Version)
	require.True(t, state.ConsumerHistoryActivated)
	restored, err := coordinator.NewCoordinatorWithRecovery(context.Background(), config.DefaultConfig(), &registrationActivityReader{})
	require.NoError(t, err)
	t.Cleanup(restored.Stop)
	restoredFSM := NewBrokerFSM(nil, restored)
	require.NoError(t, restoredFSM.Restore(io.NopCloser(bytes.NewReader(buffer.Bytes()))))
	require.True(t, restoredFSM.ConsumerHistoryEnabled())
	require.Equal(t, c.ExportState(), restored.ExportState())
	require.Error(t, resultError(restoredFSM.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"old","incarnation_id":"old-process","lifecycle_protocol":9,"recovery_protocol":2}`)})))
	require.Nil(t, restoredFSM.GetBroker("old"))
}

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
