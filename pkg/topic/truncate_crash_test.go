package topic_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestStandaloneTruncateProcessCrash(t *testing.T) {
	if dir := os.Getenv("CURSUS_TRUNCATE_CRASH_DIR"); dir != "" {
		m, h, groups := crashHandler(t, dir)
		ctx := controller.NewClientContext("", 0)
		require.Contains(t, h.HandleCommand("CREATE topic=events partitions=1 idempotent=true event_sourcing=true", ctx), "OK ")
		require.Contains(t, h.HandleCommand("APPEND_STREAM topic=events key=aggregate version=1 message=old-event", ctx), "OK")
		require.Contains(t, h.HandleCommand(`SAVE_SNAPSHOT topic=events key=aggregate version=1 message={"old":true}`, ctx), "OK")
		require.NoError(t, m.PublishToPartitionWithAck("events", 0, &types.Message{Payload: "old-sequence", ProducerID: "p", Epoch: 1, SeqNum: 1}))
		require.NoError(t, groups.RegisterGroup("events", "workers", 1))
		require.NoError(t, groups.CommitOffset("workers", "events", 0, 17))
		snapshot := &transaction.Snapshot{ID: "tx", Producer: "p", Revision: 1, State: transaction.StateCommitted,
			CreatedAt: time.Now(), UpdatedAt: time.Now(), Messages: []transaction.MessageOperation{{Topic: "events"}, {Topic: "audit"}}}
		journal, err := transaction.OpenJournal(filepath.Join(dir, "transactions.journal"))
		require.NoError(t, err)
		require.NoError(t, journal.Append(snapshot))
		require.NoError(t, h.ConfigureTransactionJournal(filepath.Join(dir, "transactions.journal")))
		m.GetTopic("events").Partitions[0].FlushDisk()
		id := m.GetTopic("events").Definition().InstanceID
		topic.InstallTruncateCrashObserver(m, func(phase string) {
			if phase == os.Getenv("CURSUS_TRUNCATE_CRASH_PHASE") {
				os.Exit(86) // bypass every Close/defer, as a killed broker would
			}
		})
		_ = h.HandleCommand("TRUNCATE_INSTANCE topic=events expected_revision=1 expected_instance="+id, ctx)
		t.Fatal("crash boundary was not reached")
	}
	for _, phase := range []string{"manifest_synced", "manifest_renamed", "manifest_directory_synced", "storage_removed", "storage_created", "watermark_persisted", "group_record_1_synced", "group_record_2_synced", "marker_synced", "marker_renamed", "marker_directory_synced"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestStandaloneTruncateProcessCrash$")
			cmd.Env = append(os.Environ(), "CURSUS_TRUNCATE_CRASH_DIR="+dir, "CURSUS_TRUNCATE_CRASH_PHASE="+phase)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, string(out))
			require.Equal(t, 86, exit.ExitCode(), string(out))
			m, h, groups := crashHandler(t, dir)
			require.NoError(t, h.ConfigureTransactionJournal(filepath.Join(dir, "transactions.journal")))
			if phase == "manifest_synced" {
				require.Equal(t, uint64(1), m.GetTopic("events").Revision, "uncommitted manifest keeps the original generation")
				return
			}
			require.NoError(t, h.RecoverPendingTruncations())
			current := m.GetTopic("events")
			require.NotNil(t, current)
			require.Equal(t, uint64(2), current.Revision)
			require.Zero(t, current.Partitions[0].NextOffset())
			require.Zero(t, current.Partitions[0].GetHWM())
			require.Nil(t, groups.GetGroup("workers"))
			require.Equal(t, []transaction.MessageOperation{{Topic: "audit"}}, h.TxnManager.ExportState()["tx"].Messages)
			ctx := controller.NewClientContext("", 0)
			read := h.HandleCommand("READ_SNAPSHOT topic=events key=aggregate", ctx)
			require.NotContains(t, read, `"old":true`)
			version := h.HandleCommand("STREAM_VERSION topic=events key=aggregate", ctx)
			require.Contains(t, version, "version=0")
			// A fresh version-one event proves the old index is gone and the topic is usable.
			require.Contains(t, h.HandleCommand("APPEND_STREAM topic=events key=aggregate version=1 message=new-event", ctx), "OK")
			require.NoError(t, m.PublishToPartitionWithAck("events", 0, &types.Message{Payload: "new-sequence", ProducerID: "p", Epoch: 1, SeqNum: 1}))
			retry := h.HandleCommand("TRUNCATE_INSTANCE topic=events expected_revision=1 expected_instance="+current.InstanceID, ctx)
			require.Contains(t, retry, "replayed=true")
			messages, err := m.ReadTopicPartition("events", 0, 0, 10)
			require.NoError(t, err)
			require.Len(t, messages, 2)
			require.NotContains(t, messages[0].Payload, "old-event")
			require.Equal(t, "new-sequence", messages[1].Payload, "pre-reset producer sequence must not suppress a fresh record")
		})
	}
}

type crashMetadataReader struct {
	*topic.TopicManager
	records, bytes, reads, cleanupWrites int
}

func (r *crashMetadataReader) PublishWithAck(name string, message *types.Message) error {
	err := r.TopicManager.PublishWithAck(name, message)
	if err == nil && name == config.ConsumerOffsetsTopicName && r.IsTruncationPending("events") {
		r.cleanupWrites++
		if os.Getenv("CURSUS_TRUNCATE_CRASH_PHASE") == fmt.Sprintf("group_record_%d_synced", r.cleanupWrites) {
			os.Exit(86)
		}
	}
	return err
}

func (r *crashMetadataReader) ReadTopicPartition(name string, partition int, offset uint64, max int) ([]types.Message, error) {
	rows, err := r.TopicManager.ReadTopicPartition(name, partition, offset, max)
	r.reads++
	for _, row := range rows {
		data, _ := json.Marshal(row)
		r.records++
		r.bytes += len(data)
	}
	return rows, err
}

func crashHandler(t *testing.T, dir string) (*topic.TopicManager, *controller.CommandHandler, *coordinator.Coordinator) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = dir
	dm := disk.NewDiskManager(cfg)
	m := topic.NewTopicManager(cfg, dm, nil)
	require.NoError(t, m.RestoreTopics())
	reader := &crashMetadataReader{TopicManager: m}
	groups, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, reader)
	require.NoError(t, err)
	h := controller.NewCommandHandler(m, cfg, groups, nil, nil)
	t.Cleanup(func() { _ = h.Close(); groups.Stop(); m.Stop(); dm.CloseAllHandlers() })
	if !strings.Contains(t.Name(), "/") { // child output is only retained on failure
		return m, h, groups
	}
	t.Logf("consumer metadata reconstruction: calls=%d records=%d serialized_message_bytes=%d (not physical I/O)", reader.reads, reader.records, reader.bytes)
	return m, h, groups
}
