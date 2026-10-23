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

func registerRetentionTimeBroker(t *testing.T, f *BrokerFSM, id, status string, version int) {
	t.Helper()
	require.Nil(t, f.Apply(&raft.Log{Data: []byte(fmt.Sprintf(`REGISTER:{"id":%q,"addr":"localhost:7000","status":%q,"incarnation_id":%q,"lifecycle_protocol":9,"recovery_protocol":%d}`, id, status, id+"-process", version))}))
}

func TestRetentionTimeRejectsCleanupOnlyProtocol(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	registerRetentionTimeBroker(t, f, "new", "active", BootstrapRecoveryProtocolVersion)
	registerRetentionTimeBroker(t, f, "cleanup-only", "inactive", BootstrapRecoveryProtocolVersion)
	f.brokers["cleanup-only"].LifecycleProtocol = TruncateCleanupBoundaryProtocolVersion
	require.ErrorContains(t, resultError(f.Apply(&raft.Log{Data: []byte(`RETENTION_TIME_ACTIVATE:{"voters":["new"]}`)})), "complete recovery capability")
	require.False(t, f.RetentionTimeEnabled())
	f.brokers["cleanup-only"].LifecycleProtocol = RetentionTimeProtocolVersion
	require.Nil(t, f.Apply(&raft.Log{Data: []byte(`RETENTION_TIME_ACTIVATE:{"voters":["new"]}`)}))
	require.True(t, f.RetentionTimeEnabled())
	require.Error(t, resultError(f.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"old","incarnation_id":"old-process","lifecycle_protocol":8,"recovery_protocol":2}`)})))
	require.Nil(t, f.GetBroker("old"))
}

func TestRetentionTimeSnapshotPreservesTruncateCleanupBoundary(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	registerRetentionTimeBroker(t, f, "new", "active", BootstrapRecoveryProtocolVersion)
	d := topic.DefaultDefinition("orders", nil)
	d.Partitions, d.LifecycleEpoch, d.Revision = 1, 99, 2
	f.topicState[d.Name] = &d
	f.partitionMetadata[d.Name+"-0"] = authoritativePartitionMetadata(1)
	f.partitionMetadata[d.Name+"-0"].LifecycleEpoch = d.LifecycleEpoch
	f.captureTopicCleanupBoundaryLocked(d)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte(`RETENTION_TIME_ACTIVATE:{"voters":["new"]}`)}))
	snapshot, err := f.Snapshot()
	require.NoError(t, err)
	var buffer bytes.Buffer
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: &buffer}))
	restored := NewBrokerFSM(nil, nil)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buffer.Bytes()))))
	require.True(t, restored.RetentionTimeEnabled())
	require.NotEmpty(t, restored.topicCleanupBoundaries)
	require.Equal(t, f.topicCleanupBoundaries, restored.topicCleanupBoundaries)
}

func TestRetentionTimeActivationRequiresAllBrokersAndPersists(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	registerRetentionTimeBroker(t, f, "new", "active", 2)
	registerRetentionTimeBroker(t, f, "old", "inactive", 0)
	activate := func(voters string) interface{} {
		return f.Apply(&raft.Log{Data: []byte(`RETENTION_TIME_ACTIVATE:{"voters":` + voters + `}`)})
	}
	require.Error(t, resultError(activate(`["new"]`)))
	require.False(t, f.RetentionTimeEnabled())
	registerRetentionTimeBroker(t, f, "old", "inactive", 2)
	require.Error(t, resultError(activate(`["new","missing"]`)))
	require.Error(t, resultError(activate(`["new","new"]`)))
	require.Nil(t, activate(`["new"]`))
	require.Nil(t, activate(`["new"]`))
	require.True(t, f.RetentionTimeEnabled())
	snapshot, err := f.Snapshot()
	require.NoError(t, err)
	buffer := new(bytes.Buffer)
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: buffer}))
	restored := NewBrokerFSM(nil, nil)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buffer.Bytes()))))
	require.True(t, restored.RetentionTimeEnabled())
	require.Error(t, resultError(restored.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"late","incarnation_id":"late-process","lifecycle_protocol":9,"recovery_protocol":0}`)})))
	require.Nil(t, restored.GetBroker("late"))
	var state BrokerFSMState
	require.NoError(t, json.Unmarshal(buffer.Bytes(), &state))
	state.Brokers["old"].RecoveryProtocol = 0
	invalid, err := json.Marshal(state)
	require.NoError(t, err)
	require.ErrorContains(t, NewBrokerFSM(nil, nil).Restore(io.NopCloser(bytes.NewReader(invalid))), "complete recovery capability")
}
