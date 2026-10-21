package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestTruncateReceiptSurvivesRestartAndPreservesNewRecords(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	m := NewTopicManager(cfg, dm, nil)
	d := DefaultDefinition("orders", cfg)
	d.Partitions, d.Idempotent, d.InstanceID = 1, true, "instance-original-1234"
	_, err := m.CreateTopicWithPatch(d, DefinitionPatch{})
	require.NoError(t, err)
	require.NoError(t, m.PublishToPartitionWithAck("orders", 0, &types.Message{Payload: "old", ProducerID: "p", Epoch: 1, SeqNum: 1}))
	_, err = m.TruncateTopicInstanceDurable("orders", 1, d.InstanceID)
	require.NoError(t, err)
	require.NoError(t, m.CompleteTruncation("orders"))
	require.NoError(t, m.PublishToPartitionWithAck("orders", 0, &types.Message{Payload: "new", ProducerID: "p", Epoch: 1, SeqNum: 1}))
	m.Stop()
	dm.CloseAllHandlers()

	dm = disk.NewDiskManager(cfg)
	m = NewTopicManager(cfg, dm, nil)
	t.Cleanup(func() { m.Stop(); dm.CloseAllHandlers() })
	require.NoError(t, m.RestoreTopics())
	result, err := m.TruncateTopicInstanceDurable("orders", 1, d.InstanceID)
	require.NoError(t, err)
	require.True(t, result.Replayed)
	require.False(t, result.CleanupPending)
	messages, err := m.ReadTopicPartition("orders", 0, 0, 10)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "new", messages[0].Payload)
	_, err = m.TruncateTopicDurable("orders", 1)
	require.ErrorIs(t, err, ErrTopicRevisionConflict, "legacy callers retain their conflict contract")
	_, err = m.TruncateTopicInstanceDurable("orders", 2, "instance-recreated-5678")
	require.ErrorIs(t, err, ErrTopicRevisionConflict)
	retention := 25
	_, err = m.CreateTopicWithPatch(d, DefinitionPatch{RetentionHours: &retention})
	require.NoError(t, err)
	_, err = m.TruncateTopicInstanceDurable("orders", 1, d.InstanceID)
	require.ErrorIs(t, err, ErrTopicRevisionConflict, "a later definition update invalidates the receipt")

	_, err = m.DeleteTopicDurable("orders")
	require.NoError(t, err)
	d.InstanceID = "instance-recreated-5678"
	_, err = m.CreateTopicWithPatch(d, DefinitionPatch{})
	require.NoError(t, err)
	_, err = m.TruncateTopicInstanceDurable("orders", 1, "instance-original-1234")
	require.ErrorIs(t, err, ErrTopicRevisionConflict, "revision reuse must not defeat the instance fence")
	require.Equal(t, uint64(1), m.GetTopic("orders").LifecycleEpoch)
}

func TestTruncateReceiptValidationAndLegacyBinding(t *testing.T) {
	d := DefaultDefinition("orders", nil)
	bound, err := BindInstance(d, "instance-original-1234", d.Revision)
	require.NoError(t, err)
	require.Equal(t, uint64(2), bound.Revision)
	again, err := BindInstance(bound, "instance-recreated-5678", bound.Revision)
	require.NoError(t, err)
	require.Equal(t, bound, again)
	bound.TruncateRevision, bound.TruncateEpoch = 2, 2
	_, err = bound.Normalize()
	require.ErrorContains(t, err, "invalid truncate receipt")
}
