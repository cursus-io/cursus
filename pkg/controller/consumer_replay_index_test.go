package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestConsumerReplayIndexRealDiskStartupAndSuffix(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.ConsumerRecoveryIndexEnabled = true
	cfg.LogDir = t.TempDir()
	// This fixture writes a few KiB on hosts whose free-space ratio is below
	// the production default. Keep the absolute free-space guard enabled.
	cfg.DiskMinFreePercent = 0
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	tm := topic.NewTopicManager(cfg, dm, nil)
	t.Cleanup(tm.Stop)
	c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, tm)
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	state := fsm.NewBrokerFSM(tm, c)
	require.Nil(t, state.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"local","addr":"127.0.0.1:7000","status":"active","lifecycle_protocol":7}`)}))
	definition := topic.DefaultDefinition(config.ConsumerOffsetsTopicName, cfg)
	definition.Partitions = 4
	definition.ReplicationFactor = 1
	definition.Policy = topic.ConsumerMetadataPolicy()
	// Match BootstrapConsumerOffsetsTopic: internal topics have no InstanceID.
	require.Empty(t, definition.InstanceID)
	payload, err := json.Marshal(fsm.TopicCommand{Definition: &definition, LeaderID: "local"})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Index: 2, Data: append([]byte("TOPIC:"), payload...)}))
	h := newCoordinatorRoutingHandler("local", state, c)
	h.TopicManager = tm
	h.Config = cfg
	t.Cleanup(func() { require.NoError(t, h.Close()) })
	for _, p := range tm.GetTopic(config.ConsumerOffsetsTopicName).Partitions {
		p.SetHWM(0)
	}
	p, err := tm.GetTopic(config.ConsumerOffsetsTopicName).GetPartition(0)
	require.NoError(t, err)
	registration := coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordRegistration, Group: "g", Topic: "orders", PartitionCount: 1, Epoch: 1, Timestamp: time.Unix(1, 0)}
	metadata := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
	var messages []types.Message
	messages = append(messages, consumerMetadataMessage(t, registration, 0))
	for i := 1; i <= 100; i++ {
		record := coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordOffsetSnapshot, Group: "g", Topic: "orders", Epoch: 1, Revision: uint64(i), Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: uint64(i)}}}
		messages = append(messages, consumerMetadataMessage(t, record, uint64(i)))
	}
	require.NoError(t, p.ReplicaAppend(messages))
	require.NoError(t, p.ApplyReplicaHWM(101))
	p.FlushDisk()
	metadata.CommittedHWM = 101
	applyProjectionMetadata(t, state, 0, metadata)
	require.NoError(t, h.RecoverDistributedConsumerMetadata(context.Background()))
	first := c.RecoverySnapshot()
	require.False(t, first.IndexHit)
	require.Empty(t, first.IndexFailure)
	require.Positive(t, first.IndexWriteBytes)
	// A second complete runtime recovery uses the durable local projection.
	require.NoError(t, h.recoverDistributedConsumerMetadata(context.Background(), true))
	require.True(t, c.RecoverySnapshot().IndexHit)
	require.Zero(t, c.RecoverySnapshot().IndexSourceRecords)
	next := coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordOffsetSnapshot, Group: "g", Topic: "orders", Epoch: 1, Revision: 101, Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: 101}}}
	require.NoError(t, p.ReplicaAppend([]types.Message{consumerMetadataMessage(t, next, 101)}))
	require.NoError(t, p.ApplyReplicaHWM(102))
	p.FlushDisk()
	metadata.CommittedHWM = 102
	applyProjectionMetadata(t, state, 0, metadata)
	require.NoError(t, h.recoverDistributedConsumerMetadata(context.Background(), true))
	require.True(t, c.RecoverySnapshot().IndexHit)
	require.Equal(t, 2, c.RecoverySnapshot().IndexSourceRecords, "one raw certification plus one committed suffix record")
	value, ok := c.GetOffset("g", "orders", 0)
	require.True(t, ok)
	require.Equal(t, uint64(101), value)
	// A disposable index is never needed for authoritative recovery.
	require.NoError(t, os.Remove(filepath.Join(cfg.LogDir, ".consumer-replay-index.json")))
	require.NoError(t, h.recoverDistributedConsumerMetadata(context.Background(), true))
	require.False(t, c.RecoverySnapshot().IndexHit)
	value, ok = c.GetOffset("g", "orders", 0)
	require.True(t, ok)
	require.Equal(t, uint64(101), value)
	// Restore the real FSM snapshot and finish its normal disk recovery fence.
	snapshot, err := state.Snapshot()
	require.NoError(t, err)
	sink := &consumerRecoverySnapshotSink{}
	require.NoError(t, snapshot.Persist(sink))
	snapshot.Release()
	require.NoError(t, state.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))))
	require.NoError(t, state.ReconcileTopicMaterializations())
	require.NoError(t, state.FinalizeRecoveredPartitions())
	require.NoError(t, h.RecoverDistributedConsumerMetadata(context.Background()))
	value, ok = c.GetOffset("g", "orders", 0)
	require.True(t, ok)
	require.Equal(t, uint64(101), value)
}
