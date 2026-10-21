package fsm

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestTruncateInstanceReceiptReplayAndSnapshot(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution, cfg.LogDir = true, t.TempDir()
	manager := topic.NewTopicManager(cfg, &MockHandlerProvider{}, nil)
	t.Cleanup(manager.Stop)
	f := NewBrokerFSM(manager, nil)
	registerLifecycleBroker(t, f, "broker-1", TruncateReceiptProtocolVersion)
	create := testTopicCommand("orders", 1, 1)
	create.Definition.InstanceID = "instance-original-1234"
	encoded, err := json.Marshal(create)
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: append([]byte("TOPIC:"), encoded...), Index: 2}))
	command := []byte(`TOPIC_TRUNCATE:{"topic":"orders","expected_revision":1,"expected_instance":"instance-original-1234"}`)
	result := f.Apply(&raft.Log{Data: command, Index: 3}).(topic.TruncateResult)
	require.False(t, result.CleanupPending)
	require.False(t, result.Replayed)
	manager.SetDeleteHook(func(string) error { t.Error("receipt replay attempted physical cleanup"); return nil })
	result = f.Apply(&raft.Log{Data: command, Index: 4}).(topic.TruncateResult)
	require.True(t, result.Replayed)
	require.Equal(t, uint64(2), result.Definition.Revision)
	snapshot, err := f.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: &buf}))
	t.Logf("receipt snapshot serialized bytes=%d", buf.Len())
	cfg2 := *cfg
	cfg2.LogDir = t.TempDir()
	manager2 := topic.NewTopicManager(&cfg2, &MockHandlerProvider{}, nil)
	t.Cleanup(manager2.Stop)
	restored := NewBrokerFSM(manager2, nil)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))))
	result = restored.Apply(&raft.Log{Data: command, Index: 5}).(topic.TruncateResult)
	require.True(t, result.Replayed)
	require.Equal(t, "instance-original-1234", result.Definition.InstanceID)
	stale := restored.Apply(&raft.Log{Data: []byte(`TOPIC_TRUNCATE:{"topic":"orders","expected_revision":2,"expected_instance":"instance-recreated-5678"}`), Index: 6})
	require.ErrorIs(t, stale.(error), topic.ErrTopicRevisionConflict)
}

func TestTruncateInstanceRequiresAllBrokerCapabilities(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	registerLifecycleBroker(t, f, "old-broker", ReplicaGapRecoveryProtocolVersion)
	require.False(t, f.SupportsTruncateReceipts())
	result := f.Apply(&raft.Log{Data: []byte(`TOPIC_TRUNCATE:{"topic":"orders","expected_revision":1,"expected_instance":"instance-original-1234"}`), Index: 2})
	require.ErrorContains(t, result.(error), "requires broker protocol 7")
}
