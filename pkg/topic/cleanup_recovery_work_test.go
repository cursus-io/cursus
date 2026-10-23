package topic_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type measuredCleanupReader struct {
	*topic.TopicManager
	reads, records, bytes, decoded, maxRecords, maxBytes int
	failAt                                               int
}

type measuredCleanupView struct {
	coordinator.ConsumerCleanupView
	reader *measuredCleanupReader
}

func (r *measuredCleanupReader) WithConsumerCleanupView(fn func(coordinator.ConsumerCleanupView) error) error {
	return r.TopicManager.WithConsumerCleanupView(func(v coordinator.ConsumerCleanupView) error {
		return fn(&measuredCleanupView{ConsumerCleanupView: v, reader: r})
	})
}

func (r *measuredCleanupReader) WithConsumerCleanupViewContext(ctx context.Context, fn func(coordinator.ConsumerCleanupView) error) error {
	return r.TopicManager.WithConsumerCleanupViewContext(ctx, func(v coordinator.ConsumerCleanupView) error {
		return fn(&measuredCleanupView{ConsumerCleanupView: v, reader: r})
	})
}

func (v *measuredCleanupView) Read(partition int, offset uint64, cursor types.ReplayCursor, maxRecords, maxBytes int) (coordinator.ConsumerCleanupPage, error) {
	r := v.reader
	r.reads++
	if r.reads == r.failAt {
		return coordinator.ConsumerCleanupPage{}, fmt.Errorf("injected transient read failure")
	}
	page, err := v.ConsumerCleanupView.Read(partition, offset, cursor, maxRecords, maxBytes)
	r.records += page.Records
	r.bytes += page.Bytes
	r.maxRecords = max(r.maxRecords, page.Records)
	r.maxBytes = max(r.maxBytes, page.Bytes)
	for _, m := range page.Messages {
		r.decoded += len(m.Payload) + len(m.Key)
	}
	return page, err
}

func TestConsumerCleanupDiskRecoveryWork(t *testing.T) {
	for _, count := range []int{1000, 10000, 100000} {
		t.Run(fmt.Sprintf("records_%d", count), func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.LogDir = t.TempDir()
			cfg.EnabledDistribution = true
			dm := disk.NewDiskManager(cfg)
			manager := topic.NewTopicManager(cfg, dm, nil)
			t.Cleanup(func() { manager.Stop(); dm.CloseAllHandlers() })
			require.NoError(t, manager.CreateTopic(config.ConsumerOffsetsTopicName, 4, false, false))
			for _, partition := range manager.GetTopic(config.ConsumerOffsetsTopicName).Partitions {
				require.NoError(t, partition.ApplyReplicaHWM(0))
			}
			reader := &measuredCleanupReader{TopicManager: manager, failAt: 3}
			groups, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, reader)
			require.NoError(t, err)
			t.Cleanup(groups.Stop)
			p := manager.GetTopic(config.ConsumerOffsetsTopicName).Partitions[0]
			for start := 0; start < count; start += 500 {
				batch := make([]types.Message, 0, min(500, count-start))
				for i := start; i < min(start+500, count); i++ {
					record := coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordOffsetSnapshot,
						Group: "unrelated", Topic: "audit", Epoch: 1, Revision: uint64(i + 1),
						Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: uint64(i)}}, Timestamp: time.Unix(1, 0)}
					if i == count-1 {
						record = coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordTombstone,
							Group: "workers", Topic: "orders", Epoch: 2}
					}
					payload, key, err := coordinator.EncodeConsumerMetadataRecord(record)
					require.NoError(t, err)
					batch = append(batch, types.Message{Offset: uint64(i), Key: key, Payload: string(payload)})
				}
				require.NoError(t, p.ReplicaAppend(batch))
			}
			require.NoError(t, p.ApplyReplicaHWM(uint64(count)))
			require.NoError(t, groups.ImportState(map[string]*coordinator.GroupStateSnapshot{
				"workers": {TopicName: "orders", Partitions: []int{0}, RegistrationEpoch: 1, LastActivity: time.Now(),
					Members: map[string][]int{"stale": {0}}, Offsets: map[string]map[int]uint64{"orders": {0: 17}}},
			}))
			writes := 0
			groups.SetOffsetRecordWriter(func(coordinator.ConsumerMetadataRecord) error {
				writes++
				return fmt.Errorf("unexpected follower write")
			})
			started := time.Now()
			attempts := 0
			var lastErr error
			for !groups.ExportState()["workers"].Deleted && attempts < count/128+20 {
				attempts++
				_, lastErr = groups.ReconcileTopicGroups("orders")
			}
			require.True(t, groups.ExportState()["workers"].Deleted, "bounded retries must converge after the transient error: %v", lastErr)
			require.Zero(t, writes, "replay must apply committed proof without new metadata writes")
			require.Equal(t, count, reader.records, "successful pages must not be replayed after the transient error")
			require.LessOrEqual(t, reader.maxRecords, 128)
			require.LessOrEqual(t, reader.maxBytes, 1<<20)
			reads := reader.reads
			for range 10 {
				_, err := groups.ReconcileTopicGroups("orders")
				require.NoError(t, err)
			}
			require.Equal(t, reads, reader.reads, "completed cleanup must not scan again")
			t.Logf("history=%d attempts=%d reads=%d decoded_payload_key_bytes=%d physical_page_bytes=%d replay_records=%d max_page_records=%d max_page_bytes=%d metadata_writes=%d elapsed=%s",
				count, attempts, reader.reads, reader.decoded, reader.bytes, reader.records, reader.maxRecords, reader.maxBytes, writes, time.Since(started))
		})
	}
}

func (v *measuredCleanupView) PrepareCleanupCheckpoint() error {
	if view, ok := v.ConsumerCleanupView.(interface{ PrepareCleanupCheckpoint() error }); ok {
		return view.PrepareCleanupCheckpoint()
	}
	return nil
}
