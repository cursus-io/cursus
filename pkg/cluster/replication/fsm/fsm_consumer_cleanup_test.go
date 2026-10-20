package fsm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func snapshotMetricValue(t *testing.T, metric prometheus.Metric) float64 {
	t.Helper()
	var value dto.Metric
	require.NoError(t, metric.Write(&value))
	return value.GetCounter().GetValue() + value.GetGauge().GetValue()
}

func TestBrokerFSMSnapshotDoesNotWaitForConsumerOffsetWriter(t *testing.T) {
	busyBefore := snapshotMetricValue(t, metrics.ConsumerSnapshotBusy)
	metrics.ConsumerSnapshotBusyConsecutive.Set(0)
	c := authorityCoordinator(t)
	c.SetRoutedOffsetRecordWriter(func(record coordinator.ConsumerMetadataRecord) error {
		c.RecordConsumerMetadataSource(record.Group, 0)
		return nil
	})
	require.NoError(t, c.RegisterGroup("orders", "workers", 1))
	entered, finish := make(chan struct{}), make(chan struct{})
	c.SetRoutedOffsetRecordWriter(func(record coordinator.ConsumerMetadataRecord) error {
		c.RecordConsumerMetadataSource(record.Group, 0)
		close(entered)
		<-finish // Model a durable append awaiting the same Raft FSM goroutine.
		return nil
	})
	committed := make(chan error, 1)
	go func() { committed <- c.CommitOffset("workers", "orders", 0, 7) }()
	<-entered
	f := NewBrokerFSM(nil, c)
	snapshotDone := make(chan error, 1)
	go func() {
		for range 3 {
			snapshot, err := f.Snapshot()
			if snapshot != nil {
				snapshot.Release()
			}
			if !errors.Is(err, coordinator.ErrConsumerSnapshotBusy) {
				snapshotDone <- err
				return
			}
		}
		snapshotDone <- coordinator.ErrConsumerSnapshotBusy
	}()
	blocked := false
	select {
	case err := <-snapshotDone:
		if !errors.Is(err, coordinator.ErrConsumerSnapshotBusy) {
			t.Errorf("snapshot must refuse an in-flight writer: %v", err)
		}
	case <-time.After(time.Second):
		blocked = true
		t.Error("snapshot blocked the Raft loop while an offset write awaited it")
	}
	close(finish)
	require.NoError(t, <-committed)
	if blocked {
		<-snapshotDone
	}
	require.Equal(t, busyBefore+3, snapshotMetricValue(t, metrics.ConsumerSnapshotBusy))
	require.Equal(t, float64(3), snapshotMetricValue(t, metrics.ConsumerSnapshotBusyConsecutive))
	raw := persistAuthorityFSM(t, f)
	require.Zero(t, snapshotMetricValue(t, metrics.ConsumerSnapshotBusyConsecutive))
	restored := authorityCoordinator(t)
	require.NoError(t, NewBrokerFSM(nil, restored).Restore(io.NopCloser(bytes.NewReader(raw))))
	require.Equal(t, uint64(7), restored.ExportState()["workers"].Offsets["orders"][0])
}

// These existing FSM tests model an owning coordinator with an acknowledged
// writer. Their MockStorageHandler intentionally does not retain record bodies.
// Follower replay is exercised with real disk storage below and in the E2E.
func configureLifecycleCleanupOwner(t *testing.T, manager *topic.TopicManager, c *coordinator.Coordinator) {
	t.Helper()
	for _, p := range manager.GetTopic(config.ConsumerOffsetsTopicName).Partitions {
		require.NoError(t, p.ReconcileCommittedHWM(0))
	}
	c.SetGroupSessionCallbacks(func(string) bool { return true }, nil)
	c.SetOffsetRecordWriter(func(coordinator.ConsumerMetadataRecord) error { return nil })
}

type cleanupStartupReader struct{ *topic.TopicManager }

func (r cleanupStartupReader) EndTopicOffset(name string, partition int) (uint64, error) {
	p, err := r.GetTopic(name).GetPartition(partition)
	if err != nil {
		return 0, err
	}
	return p.GetHWM(), nil
}

// No writers or replication run during fixture startup, so the manager's
// committed bounds remain fixed throughout this exclusive recovery.
func recoverCleanupCoordinator(t *testing.T, manager *topic.TopicManager, c *coordinator.Coordinator) {
	t.Helper()
	release, err := c.BeginConsumerMetadataRecovery(context.Background())
	require.NoError(t, err)
	defer release()
	prepared, err := c.PrepareFullConsumerMetadataReplay(cleanupStartupReader{manager})
	require.NoError(t, err)
	require.NoError(t, c.ApplyFullConsumerMetadataReplayAndComplete(prepared))
}

func TestBrokerFSMOwnerCleanupNeverAppendsDuringRaftApply(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	manager := topic.NewTopicManager(cfg, &MockHandlerProvider{}, nil)
	c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, manager)
	require.NoError(t, err)
	t.Cleanup(func() { c.Stop(); manager.Stop() })
	configureLifecycleCleanupOwner(t, manager, c)
	recoverCleanupCoordinator(t, manager, c)
	f := NewBrokerFSM(manager, c)
	registerLifecycleBroker(t, f, "broker-1", TopicLifecycleProtocolVersion)
	create, err := json.Marshal(testTopicCommand("orders", 1, 1))
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 1}))
	require.NoError(t, c.RegisterGroup("orders", "workers", 1))
	writes := 0
	c.SetOffsetRecordWriter(func(coordinator.ConsumerMetadataRecord) error {
		if !f.materializationMu.TryLock() {
			return errors.New("metadata writer is holding the materialization lock")
		}
		f.materializationMu.Unlock()
		writes++
		return nil
	})
	result := f.Apply(&raft.Log{Data: []byte(`TOPIC_TRUNCATE:{"topic":"orders","expected_revision":1}`), Index: 2})
	require.True(t, result.(topic.TruncateResult).CleanupPending)
	require.Zero(t, writes, "a new append can require this same Raft apply loop to commit")
	require.NoError(t, f.ReconcileTopicMaterializations())
	require.Equal(t, 1, writes)
	require.Empty(t, f.TopicMaterializationIssues())
}

func TestBrokerFSMFollowerCleanupRequiresCommittedTombstone(t *testing.T) {
	for _, operation := range []string{"delete", "truncate"} {
		t.Run(operation, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution = true
			cfg.LogDir = t.TempDir()
			dm := disk.NewDiskManager(cfg)
			manager := topic.NewTopicManager(cfg, dm, nil)
			c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, manager)
			require.NoError(t, err)
			t.Cleanup(func() { c.Stop(); manager.Stop(); dm.CloseAllHandlers() })
			for _, p := range manager.GetTopic(config.ConsumerOffsetsTopicName).Partitions {
				require.NoError(t, p.ReconcileCommittedHWM(0))
			}
			recoverCleanupCoordinator(t, manager, c)
			f := NewBrokerFSM(manager, c)
			registerLifecycleBroker(t, f, "broker-1", TopicLifecycleProtocolVersion)
			create, err := json.Marshal(testTopicCommand("orders", 1, 1))
			require.NoError(t, err)
			require.Nil(t, f.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 1}))
			require.NoError(t, c.RegisterGroup("orders", "workers", 1))
			_, err = c.AddConsumer("workers", "stale-member")
			require.NoError(t, err)
			c.SetOffsetRecordWriter(func(coordinator.ConsumerMetadataRecord) error {
				t.Error("follower attempted a metadata write")
				return nil
			})
			command := `TOPIC_DELETE:{"topic":"orders"}`
			if operation == "truncate" {
				command = `TOPIC_TRUNCATE:{"topic":"orders","expected_revision":1}`
			}
			f.Apply(&raft.Log{Data: []byte(command), Index: 2})
			require.Error(t, f.TopicMaterializationReadinessError())
			require.NotNil(t, c.GetGroup("workers"))
			require.NotNil(t, f.TopicMaterializationIssues()[0].ConsumerCleanup)
			payload, key, err := coordinator.EncodeConsumerMetadataRecord(coordinator.ConsumerMetadataRecord{
				Version: 1, Type: coordinator.ConsumerMetadataRecordTombstone,
				Group: "workers", Topic: "orders", Epoch: 2,
			})
			require.NoError(t, err)
			p := manager.GetTopic(config.ConsumerOffsetsTopicName).Partitions[0]
			require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Payload: string(payload), Key: key}}))
			require.Error(t, f.ReconcileTopicMaterializations(), "an uncommitted record is not deletion authority")
			require.NoError(t, p.ApplyReplicaHWM(1))
			require.NoError(t, f.ReconcileTopicMaterializations())
			require.Nil(t, c.GetGroup("workers"))
			require.Empty(t, f.TopicMaterializationIssues())
			if operation == "truncate" {
				require.NotNil(t, manager.GetTopic("orders"))
				require.Equal(t, uint64(2), manager.GetTopic("orders").LifecycleEpoch)
			} else {
				require.Nil(t, manager.GetTopic("orders"))
			}
		})
	}
}
