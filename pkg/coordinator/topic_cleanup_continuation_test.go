package coordinator

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type durableCleanupTestLog struct {
	*cleanupTestLog
	token string
}

func (l *durableCleanupTestLog) Partitions() []ConsumerCleanupPartition {
	p := l.cleanupTestLog.Partitions()
	p[0].DurableGeneration = l.token
	return p
}
func (l *durableCleanupTestLog) WithConsumerCleanupView(fn func(ConsumerCleanupView) error) error {
	return fn(l)
}
func (l *durableCleanupTestLog) Read(p int, offset uint64, cursor types.ReplayCursor, records, bytes int) (ConsumerCleanupPage, error) {
	page, err := l.cleanupTestLog.Read(p, offset, cursor, records, bytes)
	page.Cursor = types.ReplayCursor{Initialized: true, Position: page.Next * 100, HasLast: page.Next > 0, LastOffset: page.Next - 1}
	return page, err
}

func TestCleanupCheckpointRestartAndInvalidation(t *testing.T) {
	for _, mutation := range []string{"restart", "new_commit", "generation", "retention", "rollback", "target", "torn"} {
		t.Run(mutation, func(t *testing.T) {
			c, log := newCleanupFollower(t)
			c.cfg.LogDir = t.TempDir()
			reader := &durableCleanupTestLog{log, "generation-a"}
			c.topicHandler = reader
			log.append(cleanupMessage(t, cleanupDeletion()))
			for range 300 {
				log.append(types.Message{Payload: `{"group":"unrelated"}`})
			}
			_, err := c.ReconcileTopicGroups("orders")
			var pending *TopicCleanupPending
			require.ErrorAs(t, err, &pending)
			require.Equal(t, CleanupContinue, pending.State)
			require.FileExists(t, c.cleanupCheckpointPath("orders"))
			saved := c.cleanupProgress["orders"].next[0]
			// A new coordinator/partition identity must bind the persisted proof
			// to current targets and durable content, never process pointers.
			fresh, _ := newCleanupFollower(t)
			fresh.cfg.LogDir, fresh.topicHandler = c.cfg.LogDir, reader
			log.identity++
			switch mutation {
			case "new_commit":
				log.append(types.Message{Payload: `{"group":"unrelated"}`})
			case "generation":
				reader.token = "generation-b"
			case "retention":
				log.start = 1
			case "rollback":
				log.end = saved - 1
			case "target":
				fresh.groups["workers"].Generation++
			case "torn":
				require.NoError(t, os.WriteFile(c.cleanupCheckpointPath("orders"), []byte(`{"Payload":`), 0o600))
			}
			before := len(log.reads)
			_, _ = fresh.ReconcileTopicGroups("orders")
			if mutation == "restart" || mutation == "new_commit" {
				require.Equal(t, saved, log.reads[before])
				for i := 0; fresh.GetGroup("workers") != nil && i < 10; i++ {
					_, _ = fresh.ReconcileTopicGroups("orders")
				}
				require.Nil(t, fresh.GetGroup("workers"), "tombstone evidence before the checkpoint must survive restart")
			} else {
				require.Equal(t, log.start, log.reads[before])
			}
		})
	}
}

func TestCleanupTimeBudgetAndCancellation(t *testing.T) {
	view := &cleanupCursorView{partitions: []ConsumerCleanupPartition{{Identity: 1, End: 1000}}}
	scan := &topicCleanupScan{}
	now := time.Unix(0, 0)
	err := scan.readContext(context.Background(), view, func() time.Time { now = now.Add(topicCleanupDuration); return now })
	require.ErrorIs(t, err, errCleanupBudget)
	require.Equal(t, uint64(1), scan.next[0])
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, scan.readContext(ctx, view, time.Now), context.Canceled)
	c, _ := newCleanupFollower(t)
	c.cleanupMu.Lock()
	_, err = c.ReconcileTopicGroups("orders")
	c.cleanupMu.Unlock()
	var pending *TopicCleanupPending
	require.ErrorAs(t, err, &pending)
	require.Equal(t, CleanupWait, pending.State)
}

type byteBoundaryView struct {
	calls     int
	oversized bool
}

func (v *byteBoundaryView) Partitions() []ConsumerCleanupPartition {
	return []ConsumerCleanupPartition{{Identity: 1, End: 2}}
}
func (v *byteBoundaryView) Read(_ int, offset uint64, _ types.ReplayCursor, _, budget int) (ConsumerCleanupPage, error) {
	v.calls++
	size := 700000
	if v.oversized {
		size = topicCleanupBytes + 1
	}
	if size > budget {
		return ConsumerCleanupPage{}, &types.ReplayByteBudget{Size: size, Budget: budget}
	}
	return ConsumerCleanupPage{Next: offset + 1, Records: 1, Bytes: size}, nil
}
func TestCleanupByteBoundaryYieldsButOversizedRecordFails(t *testing.T) {
	view := &byteBoundaryView{}
	scan := &topicCleanupScan{}
	require.ErrorIs(t, scan.read(view), errCleanupBudget)
	require.Equal(t, uint64(1), scan.next[0])
	require.NoError(t, scan.read(view))
	view = &byteBoundaryView{oversized: true}
	scan = &topicCleanupScan{}
	err := scan.read(view)
	var budget *types.ReplayByteBudget
	require.ErrorAs(t, err, &budget)
	require.False(t, errors.Is(err, errCleanupBudget))
	require.Equal(t, 1, view.calls)
}

func (l *durableCleanupTestLog) PrepareCleanupCheckpoint() error { return nil }

type stationaryCleanupView struct{}

func (stationaryCleanupView) Partitions() []ConsumerCleanupPartition {
	return []ConsumerCleanupPartition{{Identity: 1, End: 1}}
}
func (stationaryCleanupView) Read(_ int, offset uint64, cursor types.ReplayCursor, _, _ int) (ConsumerCleanupPage, error) {
	return ConsumerCleanupPage{Next: offset, Cursor: cursor}, nil
}
func TestCleanupNoProgressIsAnErrorNotAContinuation(t *testing.T) {
	scan := &topicCleanupScan{}
	err := scan.read(stationaryCleanupView{})
	require.ErrorContains(t, err, "invalid bounded metadata page")
	require.False(t, errors.Is(err, errCleanupBudget))
	require.Zero(t, scan.next[0])
}
