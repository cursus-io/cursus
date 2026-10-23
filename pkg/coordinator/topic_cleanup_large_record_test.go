package coordinator

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

type cleanupDiskView struct {
	DummyPublisher
	handler               *disk.DiskHandler
	end                   uint64
	calls, decoded, bytes int
	fail                  bool
	afterRead             func()
}

func TestCleanupReservedRecordBoundaries(t *testing.T) {
	for _, size := range []int{topicCleanupBytes - 1, topicCleanupBytes, topicCleanupBytes + 1, 8<<20 + 1, util.MaxMessageSize} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			message := cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordTombstone, Group: "other", Topic: "retired", Epoch: 2})
			// JSON whitespace is valid metadata and lets this test hit the exact
			// serialized storage boundary without relying on encoder estimates.
			base := util.EstimateDiskMessageSize(types.DiskMessage{Topic: config.ConsumerOffsetsTopicName, Payload: message.Payload, Key: message.Key})
			message.Payload += strings.Repeat(" ", size-base)
			view := newCleanupDiskView(t, message)
			scan := &topicCleanupScan{}
			require.NoError(t, scan.read(view))
			require.Equal(t, uint64(1), scan.next[0])
			require.Equal(t, 1, view.decoded)
			require.Equal(t, size, view.bytes)
			probes := 0
			if size > topicCleanupBytes {
				probes = 1
			}
			require.EqualValues(t, size+4+4*probes, scan.status.ReadBytes)
		})
	}
}

func TestCleanupLargeInvalidRecordsFailClosed(t *testing.T) {
	for _, fault := range []string{"over-maximum", "checksum", "truncated", "schema", "key"} {
		t.Run(fault, func(t *testing.T) {
			message := cleanupMessage(t, largeCleanupRecord(32, 1024))
			if fault == "schema" {
				message.Payload = strings.Replace(message.Payload, `"version":4`, `"version":99`, 1)
			}
			if fault == "key" {
				message.Key = "wrong-key"
			}
			view := newCleanupDiskView(t, message, cleanupMessage(t, cleanupDeletion()))
			path := view.handler.GetSegmentPath(0)
			if fault == "over-maximum" || fault == "checksum" {
				file, err := os.OpenFile(path, os.O_WRONLY, 0)
				require.NoError(t, err)
				var data [4]byte
				position := int64(0)
				if fault == "over-maximum" {
					binary.BigEndian.PutUint32(data[:], uint32(util.MaxMessageSize+1))
				} else {
					position = 4
				}
				_, err = file.WriteAt(data[:], position)
				require.NoError(t, err)
				require.NoError(t, file.Close())
			}
			if fault == "truncated" {
				require.NoError(t, os.Truncate(path, int64(len(message.Payload))))
			}
			scan := &topicCleanupScan{targets: map[string]*cleanupTarget{"workers": {epoch: 1}}, status: TopicCleanupPending{Topic: "orders"}}
			for range 2 {
				require.Error(t, scan.read(view))
				require.Zero(t, scan.next[0])
				require.False(t, scan.cursors[0].Initialized)
				require.Zero(t, scan.targets["workers"].evidence.epoch)
			}
		})
	}
}

func TestCleanupReplayReservationReleased(t *testing.T) {
	cleanupReplaySlot <- struct{}{}
	view := &byteBoundaryView{oversized: true}
	scan := &topicCleanupScan{}
	err := scan.read(view)
	<-cleanupReplaySlot
	var pending *TopicCleanupPending
	require.ErrorAs(t, err, &pending)
	require.Equal(t, CleanupWait, pending.State)
	require.Zero(t, view.calls)
	require.ErrorIs(t, scan.read(view), errCleanupBudget)
	require.NoError(t, scan.read(view))
	require.Empty(t, cleanupReplaySlot)
}

func (v *cleanupDiskView) Partitions() []ConsumerCleanupPartition {
	return []ConsumerCleanupPartition{{Identity: v.handler, DurableGeneration: "test-generation", End: v.end}}
}
func (v *cleanupDiskView) WithConsumerCleanupView(fn func(ConsumerCleanupView) error) error {
	return fn(v)
}
func (v *cleanupDiskView) PrepareCleanupCheckpoint() error { return nil }
func (v *cleanupDiskView) Read(_ int, offset uint64, cursor types.ReplayCursor, records, bytes int) (ConsumerCleanupPage, error) {
	v.calls++
	if v.fail {
		return ConsumerCleanupPage{Next: offset, Cursor: cursor}, fmt.Errorf("injected transient read")
	}
	page, err := v.handler.ReadReplayPage(offset, v.end, cursor, records, bytes)
	v.decoded += page.Records
	v.bytes += page.Bytes
	if v.afterRead != nil {
		v.afterRead()
	}
	return page, err
}

func largeCleanupRecord(topics, partitions int) ConsumerMetadataRecord {
	var names []string
	var assignments []TopicPartition
	for topic := 0; topic < topics; topic++ {
		name := fmt.Sprintf("audit-topic-%02d", topic)
		names = append(names, name)
		for partition := 0; partition < partitions; partition++ {
			assignments = append(assignments, TopicPartition{Topic: name, Partition: partition})
		}
	}
	return ConsumerMetadataRecord{Version: ConsumerMetadataRecordVersionLifecycle, Type: ConsumerMetadataRecordLifecycleSnapshot,
		Group: "audit-group", Epoch: 1, Revision: 1, Lifecycle: &GroupLifecycleSnapshot{Topics: names, TopicPartitions: assignments, Generation: 1,
			Members: []GroupLifecycleMember{{ID: "audit-member", TopicAssignments: assignments}}}}
}

func newCleanupDiskView(t *testing.T, messages ...types.Message) *cleanupDiskView {
	t.Helper()
	cfg := *config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreePercent = 0 // Isolated fixture; retain the absolute headroom guard.
	handler, err := disk.NewDiskHandler(&cfg, config.ConsumerOffsetsTopicName, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, handler.Close()) })
	for i := range messages {
		_, err := handler.AppendMessageSync(config.ConsumerOffsetsTopicName, 0, &messages[i])
		require.NoError(t, err)
	}
	return &cleanupDiskView{handler: handler, end: uint64(len(messages))}
}

func TestCleanupLargeCommittedRecordProgress(t *testing.T) {
	for _, topics := range []int{32, 128} {
		t.Run(fmt.Sprint(topics), func(t *testing.T) {
			large := cleanupMessage(t, largeCleanupRecord(topics, 1024))
			require.Greater(t, len(large.Payload), topicCleanupBytes)
			if topics == 128 {
				require.Greater(t, len(large.Payload), 8<<20)
			}
			view := newCleanupDiskView(t, large, cleanupMessage(t, cleanupDeletion()))
			// Preserve the strict disk API: the coordinator must explicitly reserve
			// the exception, not silently relax every storage caller's allocation cap.
			for range 2 {
				page, err := view.handler.ReadReplayPage(0, view.end, types.ReplayCursor{}, 128, topicCleanupBytes)
				var budget *types.ReplayByteBudget
				require.ErrorAs(t, err, &budget)
				require.Zero(t, page.Next)
				require.Zero(t, page.Records)
			}
			c, _ := newCleanupFollower(t)
			c.cfg.LogDir = t.TempDir()
			c.topicHandler = view
			c.standalone = true
			c.SetOffsetRecordWriter(nil)
			require.NoError(t, c.RegisterGroup("payments", "unrelated-live", 1))
			_, err := c.AddConsumer("unrelated-live", "member")
			require.NoError(t, err)
			require.NoError(t, c.CommitOffset("unrelated-live", "payments", 0, 29))
			unrelated := c.GetGroup("unrelated-live")
			before := c.ExportState()["unrelated-live"]
			heartbeat := unrelated.Members["member"].LastHeartbeat
			c.standalone = false
			_, err = c.ReconcileTopicGroups("orders")
			var pending *TopicCleanupPending
			require.ErrorAs(t, err, &pending)
			require.Equal(t, CleanupContinue, pending.State, "%v", err)
			require.Equal(t, uint64(1), c.cleanupProgress["orders"].next[0])
			require.FileExists(t, c.cleanupCheckpointPath("orders"))
			require.NotNil(t, c.GetGroup("workers"))
			require.Same(t, unrelated, c.GetGroup("unrelated-live"))
			require.Equal(t, before, c.ExportState()["unrelated-live"])
			require.Equal(t, heartbeat, unrelated.Members["member"].LastHeartbeat)
			// A transient failure after checkpoint must not restart the large decode.
			view.fail = true
			_, err = c.ReconcileTopicGroups("orders")
			require.ErrorContains(t, err, "injected transient read")
			view.fail = false
			fresh, _ := newCleanupFollower(t)
			fresh.cfg.LogDir, fresh.topicHandler = c.cfg.LogDir, view
			deleted, err := fresh.ReconcileTopicGroups("orders")
			require.NoError(t, err)
			require.Equal(t, []string{"workers"}, deleted)
			require.Equal(t, 2, view.decoded, "restart must not re-decode the checkpointed large record")
			t.Logf("metadata=%d decoded_records=%d serialized_bytes=%d reads=%d", len(large.Payload), view.decoded, view.bytes, view.calls)
		})
	}
}

func TestCleanupLargeRecordCancellation(t *testing.T) {
	view := newCleanupDiskView(t, cleanupMessage(t, largeCleanupRecord(32, 1024)))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scan := &topicCleanupScan{}
	require.ErrorIs(t, scan.readContext(ctx, view, time.Now), context.Canceled)
	require.Zero(t, view.calls)
}

func TestCleanupLargeRecordCancellationReleasesReservation(t *testing.T) {
	for _, after := range []int{1, 2} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			view := newCleanupDiskView(t, cleanupMessage(t, largeCleanupRecord(32, 1024)))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			view.afterRead = func() {
				if view.calls == after {
					cancel()
				}
			}
			scan := &topicCleanupScan{}
			start := time.Now()
			require.ErrorIs(t, scan.readContext(ctx, view, time.Now), context.Canceled)
			t.Logf("cancel_after_read=%d response_time=%s decoded=%d", after, time.Since(start), view.decoded)
			require.Zero(t, scan.next[0])
			require.Empty(t, cleanupReplaySlot)
			view.afterRead = nil
			require.NoError(t, scan.read(view))
		})
	}
}

func TestCleanupMixedSmallAndLargeTurns(t *testing.T) {
	small := cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordTombstone, Group: "other", Topic: "retired", Epoch: 2})
	large := cleanupMessage(t, largeCleanupRecord(32, 1024))
	view := newCleanupDiskView(t, small, large, small)
	scan := &topicCleanupScan{}
	require.ErrorIs(t, scan.read(view), errCleanupBudget)
	require.EqualValues(t, 1, scan.next[0], "partial turns yield before the large allocation")
	require.Equal(t, 1, view.decoded)
	require.ErrorIs(t, scan.read(view), errCleanupBudget)
	require.EqualValues(t, 2, scan.next[0])
	require.Equal(t, 2, view.decoded)
	require.NoError(t, scan.read(view))
	require.Equal(t, 3, view.decoded)
}
