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

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestConsumerHistoryControllerPublishesAndRestores(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution, cfg.ConsumerRecoveryIndexEnabled, cfg.ConsumerHistoryCompactionEnabled = true, true, true
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreePercent = 0
	cfg.SegmentSize = 2048
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	tm := topic.NewTopicManager(cfg, dm, nil)
	t.Cleanup(tm.Stop)
	c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, tm)
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	state := fsm.NewBrokerFSM(tm, c)
	require.Nil(t, state.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"local","addr":"127.0.0.1:7000","status":"active","incarnation_id":"local-process","lifecycle_protocol":10,"recovery_protocol":2}`)}))
	definition := topic.DefaultDefinition(config.ConsumerOffsetsTopicName, cfg)
	definition.Partitions, definition.ReplicationFactor = 4, 1
	definition.Policy = topic.ConsumerMetadataPolicy()
	require.Empty(t, definition.InstanceID, "match auto-created internal topic")
	payload, err := json.Marshal(fsm.TopicCommand{Definition: &definition, LeaderID: "local"})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Index: 2, Data: append([]byte("TOPIC:"), payload...)}))
	h := newCoordinatorRoutingHandler("local", state, c)
	h.TopicManager, h.Config = tm, cfg
	h.Cluster = clusterController.NewClusterController(context.Background(), cfg, h.Cluster.RaftManager, nil, "local", "127.0.0.1:7000")
	t.Cleanup(func() { require.NoError(t, h.Close()) })
	for _, p := range tm.GetTopic(config.ConsumerOffsetsTopicName).Partitions {
		p.SetHWM(0)
	}
	p, err := tm.GetTopic(config.ConsumerOffsetsTopicName).GetPartition(0)
	require.NoError(t, err)
	for i := 0; i <= 100; i++ {
		record := coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordOffsetSnapshot, Group: "g", Topic: "orders", Epoch: 1, Revision: uint64(i), Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: uint64(i)}}}
		if i == 0 {
			record.Type, record.PartitionCount, record.Offsets = coordinator.ConsumerMetadataRecordRegistration, 1, nil
			record.Timestamp = time.Unix(1, 0)
		}
		message := consumerMetadataMessage(t, record, uint64(i))
		message.ProducerID = "consumer-offset-coordinator"
		require.NoError(t, p.ReplicaAppend([]types.Message{message}))
		p.FlushDisk()
	}
	require.NoError(t, p.ApplyReplicaHWM(101))
	metadata := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
	metadata.CommittedHWM = 101
	applyProjectionMetadata(t, state, 0, metadata)
	require.NoError(t, h.RecoverDistributedConsumerMetadata(context.Background()))
	cursor := consumerHistoryCursor{}
	result, err := h.compactConsumerHistory(context.Background(), &cursor)
	require.NoError(t, err)
	require.Equal(t, "activation_pending", result.SkippedReason)
	require.Nil(t, state.Apply(&raft.Log{Data: []byte(`CONSUMER_ROUTING_ACTIVATE:{"voters":["local"]}`)}))
	require.Nil(t, state.Apply(&raft.Log{Data: []byte(`CONSUMER_HISTORY_ACTIVATE:{"voters":["local"]}`)}))
	before := c.ExportState()
	var removed int
	for i := 0; i < 40; i++ {
		result, err = h.compactConsumerHistory(context.Background(), &cursor)
		require.NoError(t, err)
		removed += result.RecordsRemoved
	}
	require.Greater(t, removed, 80)
	require.Equal(t, before, c.ExportState(), "maintenance must not replace live groups")
	files, err := filepath.Glob(filepath.Join(cfg.LogDir, config.ConsumerOffsetsTopicName, "partition_0_segment_*.log"))
	require.NoError(t, err)
	require.LessOrEqual(t, len(files), 3, "anchor, latest state segment, and active tail")
	require.NoError(t, os.Remove(filepath.Join(cfg.LogDir, ".consumer-replay-index.json")))
	require.NoError(t, h.recoverDistributedConsumerMetadata(context.Background(), true))
	require.False(t, c.RecoverySnapshot().IndexHit)
	value, ok := c.GetOffset("g", "orders", 0)
	require.True(t, ok)
	require.Equal(t, uint64(100), value)
	snapshot, err := state.Snapshot()
	require.NoError(t, err)
	sink := &consumerRecoverySnapshotSink{}
	require.NoError(t, snapshot.Persist(sink))
	snapshot.Release()
	require.NoError(t, state.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))))
	require.True(t, state.ConsumerHistoryEnabled())
	require.NoError(t, state.ReconcileTopicMaterializations())
	require.NoError(t, state.FinalizeRecoveredPartitions())
	require.NoError(t, h.RecoverDistributedConsumerMetadata(context.Background()))
	value, ok = c.GetOffset("g", "orders", 0)
	require.True(t, ok)
	require.Equal(t, uint64(100), value)
}
