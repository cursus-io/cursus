package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type cleanupTestLog struct {
	DummyPublisher
	messages []types.Message
	end      uint64
	start    uint64
	identity int
	fail     bool
	before   func()
	onView   func(int)
	views    int
	reads    []uint64
	records  int
	bytes    int
}

func (log *cleanupTestLog) WithConsumerCleanupView(fn func(ConsumerCleanupView) error) error {
	log.views++
	if log.onView != nil {
		log.onView(log.views)
	}
	if log.before != nil {
		log.before()
		log.before = nil
	}
	return fn(log)
}

func TestTopicCleanupRevalidatesCommittedCutBeforeApplyingDeletion(t *testing.T) {
	c, log := newCleanupFollower(t)
	log.append(cleanupMessage(t, cleanupDeletion()))
	log.onView = func(view int) {
		if view == 2 {
			log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration,
				Group: "workers", Topic: "orders", Epoch: 3, PartitionCount: 1}))
		}
	}
	_, err := c.ReconcileTopicGroups("orders")
	require.ErrorContains(t, err, "view changed")
	require.NotNil(t, c.GetGroup("workers"))
	_, err = c.ReconcileTopicGroups("orders")
	require.Error(t, err)
	require.NotNil(t, c.GetGroup("workers"))
	require.Equal(t, []uint64{0, 1}, log.reads, "revalidation should tail new evidence without rescanning")
}

func (log *cleanupTestLog) Partitions() []ConsumerCleanupPartition {
	return []ConsumerCleanupPartition{{Identity: log.identity, Start: log.start, End: log.end}}
}

func (log *cleanupTestLog) Read(_ int, offset uint64, _ types.ReplayCursor, maxRecords, maxBytes int) (ConsumerCleanupPage, error) {
	log.reads = append(log.reads, offset)
	if log.fail {
		return ConsumerCleanupPage{}, errors.New("injected read failure")
	}
	page := ConsumerCleanupPage{Next: offset}
	for _, message := range log.messages {
		if message.Offset < offset || message.Offset >= log.end {
			continue
		}
		size := len(message.Payload) + len(message.Key) + 128
		if page.Records == maxRecords || page.Bytes+size > maxBytes {
			break
		}
		page.Messages = append(page.Messages, message)
		page.Next = message.Offset + 1
		page.Records++
		page.Bytes += size
	}
	if page.Next == offset {
		return page, errors.New("record exceeds byte budget")
	}
	log.records += page.Records
	log.bytes += page.Bytes
	return page, nil
}

func cleanupMessage(t *testing.T, record ConsumerMetadataRecord) types.Message {
	t.Helper()
	payload, key, err := EncodeConsumerMetadataRecord(record)
	require.NoError(t, err)
	return types.Message{Payload: string(payload), Key: key}
}

func cleanupDeletion() ConsumerMetadataRecord {
	return ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordTombstone, Group: "workers", Topic: "orders", Epoch: 2}
}

func TestTopicCleanupIgnoresValidIntentsWithoutTreatingThemAsDeletion(t *testing.T) {
	for _, mode := range []string{"intent_only", "deleted", "wrong_producer"} {
		t.Run(mode, func(t *testing.T) {
			c, log := newCleanupFollower(t)
			r := ConsumerMetadataRecord{Version: ConsumerMetadataRecordVersionTransactionHistory, Type: ConsumerMetadataRecordTransactionalOffsetSnapshot,
				Group: "workers", Topic: "orders", Epoch: 1, Revision: 1, Offsets: []OffsetItem{{Partition: 0, Offset: 37}}, TransactionalID: "tx", ProducerID: "producer", ProducerEpoch: 1, CoordinatorEpoch: 1}
			m := cleanupMessage(t, r)
			m.TransactionalID, m.ProducerID, m.Epoch, m.TransactionState = "tx", "producer", 1, types.TransactionStateOpen
			if mode == "wrong_producer" {
				m.ProducerID = "forged"
			}
			log.append(m)
			if mode != "intent_only" {
				log.append(cleanupMessage(t, cleanupDeletion()))
			}
			_, err := c.ReconcileTopicGroups("orders")
			if mode == "deleted" {
				require.NoError(t, err)
				require.Nil(t, c.GetGroup("workers"))
			} else {
				require.Error(t, err)
				require.NotNil(t, c.GetGroup("workers"))
			}
		})
	}
}

func newCleanupFollower(t *testing.T) (*Coordinator, *cleanupTestLog) {
	t.Helper()
	c := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	t.Cleanup(c.Stop)
	require.NoError(t, c.RegisterGroup("orders", "workers", 1))
	_, err := c.AddConsumer("workers", "stale-member")
	require.NoError(t, err)
	log := &cleanupTestLog{}
	c.standalone = false
	c.topicHandler = log
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error {
		t.Error("follower must not originate consumer metadata")
		return errors.New("unexpected follower write")
	})
	return c, log
}

func (log *cleanupTestLog) append(message types.Message) {
	message.Offset = uint64(len(log.messages))
	log.messages = append(log.messages, message)
	log.end = uint64(len(log.messages))
}

func TestTopicCleanupReplaysCommittedDeletionWithoutReplacingOtherGroups(t *testing.T) {
	c, log := newCleanupFollower(t)
	c.standalone = true
	c.SetOffsetRecordWriter(nil)
	require.NoError(t, c.RegisterGroup("payments", "unrelated", 1))
	_, err := c.AddConsumer("unrelated", "live-member")
	require.NoError(t, err)
	require.NoError(t, c.CommitOffset("unrelated", "payments", 0, 29))
	c.standalone = false
	other := c.GetGroup("unrelated")
	before := c.ExportState()["unrelated"]
	heartbeat := other.Members["live-member"].LastHeartbeat
	log.append(cleanupMessage(t, cleanupDeletion()))
	deleted, err := c.ReconcileTopicGroups("orders")
	require.NoError(t, err)
	require.Equal(t, []string{"workers"}, deleted)
	require.Nil(t, c.GetGroup("workers"))
	require.Same(t, other, c.GetGroup("unrelated"))
	require.Equal(t, before, c.ExportState()["unrelated"])
	require.Equal(t, heartbeat, other.Members["live-member"].LastHeartbeat)
	require.Equal(t, uint64(2), c.ExportState()["workers"].RegistrationEpoch)
	require.True(t, c.ExportState()["workers"].Deleted)
	restored := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	t.Cleanup(restored.Stop)
	require.NoError(t, restored.ImportState(c.ExportState()))
	require.NoError(t, restored.RegisterGroup("orders", "workers", 1))
	require.Equal(t, uint64(3), restored.GetGroup("workers").RegistrationEpoch)
	_, err = c.ReconcileTopicGroups("orders")
	require.NoError(t, err)
	require.Len(t, log.reads, 1)
}

func TestTopicCleanupRejectsUnprovenDeletion(t *testing.T) {
	tests := map[string]func(*testing.T, *cleanupTestLog){
		"missing": func(_ *testing.T, _ *cleanupTestLog) {},
		"uncommitted": func(t *testing.T, log *cleanupTestLog) {
			log.append(cleanupMessage(t, cleanupDeletion()))
			log.end = 0
		},
		"same epoch": func(t *testing.T, log *cleanupTestLog) {
			r := cleanupDeletion()
			r.Epoch = 1
			log.append(cleanupMessage(t, r))
		},
		"wrong topic": func(t *testing.T, log *cleanupTestLog) {
			r := cleanupDeletion()
			r.Topic = "payments"
			log.append(cleanupMessage(t, r))
		},
		"wrong key": func(t *testing.T, log *cleanupTestLog) {
			m := cleanupMessage(t, cleanupDeletion())
			m.Key = "other-key"
			log.append(m)
		},
		"wrong group under target key": func(t *testing.T, log *cleanupTestLog) {
			m := cleanupMessage(t, cleanupDeletion())
			m.Payload = strings.ReplaceAll(m.Payload, "workers", "stranger")
			log.append(m)
		},
		"malformed": func(_ *testing.T, log *cleanupTestLog) {
			log.append(types.Message{Payload: `{"group":"workers","version":`})
		},
		"transactional": func(t *testing.T, log *cleanupTestLog) {
			m := cleanupMessage(t, cleanupDeletion())
			m.TransactionalID = "uncommitted-transaction"
			log.append(m)
		},
		"new registration": func(t *testing.T, log *cleanupTestLog) {
			log.append(cleanupMessage(t, cleanupDeletion()))
			log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration,
				Group: "workers", Topic: "orders", Epoch: 3, PartitionCount: 1}))
		},
		"conflicting epoch": func(t *testing.T, log *cleanupTestLog) {
			log.append(cleanupMessage(t, cleanupDeletion()))
			log.append(cleanupMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordRegistration,
				Group: "workers", Topic: "orders", Epoch: 2, PartitionCount: 1}))
		},
		"extraneous scope": func(t *testing.T, log *cleanupTestLog) {
			m := cleanupMessage(t, cleanupDeletion())
			var payload map[string]any
			require.NoError(t, json.Unmarshal([]byte(m.Payload), &payload))
			payload["topics"] = []string{"payments"}
			b, err := json.Marshal(payload)
			require.NoError(t, err)
			m.Payload = string(b)
			log.append(m)
		},
	}
	for name, setup := range tests {
		t.Run(name, func(t *testing.T) {
			c, log := newCleanupFollower(t)
			setup(t, log)
			before := c.ExportState()
			for range 2 {
				deleted, err := c.ReconcileTopicGroups("orders")
				require.Error(t, err)
				require.Empty(t, deleted)
				require.Equal(t, before, c.ExportState())
			}
		})
	}
}

func TestTopicCleanupResumesBoundedPagesAndReadFailures(t *testing.T) {
	c, log := newCleanupFollower(t)
	for range 2*topicCleanupRecords + 1 {
		log.append(types.Message{Payload: `{"group":"unrelated","padding":"` + strings.Repeat("x", 10000) + `"}`})
	}
	log.append(cleanupMessage(t, cleanupDeletion()))
	totalBytes := 0
	for _, m := range log.messages {
		totalBytes += len(m.Payload) + len(m.Key) + 128
	}
	_, err := c.ReconcileTopicGroups("orders")
	require.ErrorContains(t, err, "budget")
	require.LessOrEqual(t, log.records, topicCleanupRecords)
	require.LessOrEqual(t, log.bytes, topicCleanupBytes)
	progress := c.cleanupProgress["orders"].next[0]
	require.Positive(t, progress)
	log.fail = true
	_, err = c.ReconcileTopicGroups("orders")
	require.ErrorContains(t, err, "injected read failure")
	require.Equal(t, progress, c.cleanupProgress["orders"].next[0])
	log.fail = false
	for range 8 {
		beforeRecords, beforeBytes := log.records, log.bytes
		_, err = c.ReconcileTopicGroups("orders")
		require.LessOrEqual(t, log.records-beforeRecords, topicCleanupRecords)
		require.LessOrEqual(t, log.bytes-beforeBytes, topicCleanupBytes)
		if err == nil {
			break
		}
	}
	require.NoError(t, err)
	require.Nil(t, c.GetGroup("workers"))
	require.Equal(t, len(log.messages), log.records, "successful pages must not be reread")
	require.Equal(t, totalBytes, log.bytes)
}

type cleanupCursorView struct {
	partitions []ConsumerCleanupPartition
	fail       bool
}

func (view *cleanupCursorView) Partitions() []ConsumerCleanupPartition { return view.partitions }

func (view *cleanupCursorView) Read(partition int, offset uint64, _ types.ReplayCursor, _, _ int) (ConsumerCleanupPage, error) {
	if view.fail {
		return ConsumerCleanupPage{}, errors.New("injected read failure")
	}
	return ConsumerCleanupPage{Next: offset + 1, Records: 1, Bytes: 100,
		Cursor: types.ReplayCursor{Initialized: true, Segment: uint64(partition + 1), Position: uint64((partition + 1) * 100)}}, nil
}

func TestTopicCleanupReadFailureReportsActivePartitionCursor(t *testing.T) {
	view := &cleanupCursorView{partitions: []ConsumerCleanupPartition{{Identity: 0, End: 1}, {Identity: 1, End: 1}}}
	scan := &topicCleanupScan{}
	require.NoError(t, scan.read(view))
	require.Equal(t, 1, scan.status.Partition)
	require.Equal(t, uint64(200), scan.status.Position)
	cursors := append([]types.ReplayCursor(nil), scan.cursors...)
	view.partitions[0].End = 2 // A late commit requires returning to an earlier partition.
	view.fail = true
	for range 2 {
		require.ErrorContains(t, scan.read(view), "injected read failure")
		require.Equal(t, 0, scan.status.Partition)
		require.Equal(t, uint64(1), scan.status.Next)
		require.Equal(t, cursors[0].Segment, scan.status.Segment)
		require.Equal(t, cursors[0].Position, scan.status.Position)
		require.Equal(t, uint64(2), scan.status.Records)
		require.Equal(t, uint64(200), scan.status.Bytes)
		require.Equal(t, cursors, scan.cursors)
	}
}

func TestTopicCleanupTailsNewCommitAndInvalidatesReplacedLog(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "append", true: "replacement"}[replace], func(t *testing.T) {
			c, log := newCleanupFollower(t)
			log.append(types.Message{Payload: `{"group":"unrelated"}`})
			_, err := c.ReconcileTopicGroups("orders")
			require.Error(t, err)
			_, err = c.ReconcileTopicGroups("orders")
			require.Error(t, err)
			require.Len(t, log.reads, 1, "unchanged prefix must not be reread")
			if replace {
				log.identity++
				log.messages = nil
			}
			log.append(cleanupMessage(t, cleanupDeletion()))
			_, err = c.ReconcileTopicGroups("orders")
			require.NoError(t, err)
			require.Equal(t, map[bool]uint64{false: 1, true: 0}[replace], log.reads[len(log.reads)-1])
		})
	}
}

func TestTopicCleanupSnapshotReplacementCannotUseOldTarget(t *testing.T) {
	c, log := newCleanupFollower(t)
	log.append(cleanupMessage(t, cleanupDeletion()))
	state := c.ExportState()
	state["workers"].RegistrationEpoch = 3
	log.before = func() { require.NoError(t, c.ImportState(state)) }
	_, err := c.ReconcileTopicGroups("orders")
	require.Error(t, err)
	require.Equal(t, uint64(3), c.GetGroup("workers").RegistrationEpoch)
	_, err = c.ReconcileTopicGroups("orders")
	require.Error(t, err)
	require.NotNil(t, c.GetGroup("workers"))
}

func TestTopicCleanupOwnerMustBeInactiveAndAppendDeletion(t *testing.T) {
	c, log := newCleanupFollower(t)
	c.SetGroupSessionCallbacks(func(string) bool { return true }, nil)
	writes := 0
	c.SetOffsetRecordWriter(func(r ConsumerMetadataRecord) error {
		writes++
		log.append(cleanupMessage(t, r))
		return nil
	})
	_, err := c.ReconcileTopicGroups("orders")
	require.ErrorContains(t, err, "active")
	require.Zero(t, writes)
	require.NoError(t, c.RemoveConsumer("workers", "stale-member"))
	writes = 0
	deleted, err := c.ReconcileTopicGroups("orders")
	require.NoError(t, err)
	require.Equal(t, []string{"workers"}, deleted)
	require.Equal(t, 1, writes)
}

func TestTopicCleanupLostOwnerAcknowledgementReplaysWithoutAnotherWrite(t *testing.T) {
	c, log := newCleanupFollower(t)
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { return nil })
	require.NoError(t, c.RemoveConsumer("workers", "stale-member"))
	c.SetGroupSessionCallbacks(func(string) bool { return true }, nil)
	writes := 0
	c.SetOffsetRecordWriter(func(r ConsumerMetadataRecord) error {
		writes++
		log.append(cleanupMessage(t, r))
		return errors.New("lost acknowledgement after commit")
	})
	_, err := c.ReconcileTopicGroups("orders")
	require.ErrorContains(t, err, "lost acknowledgement")
	require.NotNil(t, c.GetGroup("workers"))
	_, err = c.ReconcileTopicGroups("orders")
	require.NoError(t, err)
	require.Nil(t, c.GetGroup("workers"))
	require.Equal(t, 1, writes)
}

func TestTopicCleanupRollbackInvalidatesPreviouslyReadDeletion(t *testing.T) {
	c, log := newCleanupFollower(t)
	log.append(cleanupMessage(t, cleanupDeletion()))
	for range topicCleanupRecords {
		log.append(types.Message{Payload: `{"group":"other"}`})
	}
	_, err := c.ReconcileTopicGroups("orders")
	require.ErrorContains(t, err, "budget")
	// Replace the scanned deletion with a different committed prefix at the
	// same logical offsets, as in rollback followed by replica catch-up.
	log.identity++
	log.messages[0] = types.Message{Offset: 0, Payload: `{"group":"other"}`}
	for range 2 {
		_, err = c.ReconcileTopicGroups("orders")
		require.Error(t, err)
	}
	require.NotNil(t, c.GetGroup("workers"))
	require.Equal(t, uint64(0), log.reads[1])
}

func TestTopicCleanupApplyCheckNeverWaitsForCoordinatorWriter(t *testing.T) {
	c, _ := newCleanupFollower(t)
	c.mu.Lock()
	_, err := c.CheckTopicGroupCleanup("orders")
	c.mu.Unlock()
	require.ErrorContains(t, err, "outside Raft apply")
	require.NotNil(t, c.GetGroup("workers"))
}

func TestTopicCleanupSupersededIntentDoesNotApplyDeletion(t *testing.T) {
	c, log := newCleanupFollower(t)
	log.append(cleanupMessage(t, cleanupDeletion()))
	_, err := c.ReconcileTopicGroupsIf("orders", func() bool { return false })
	require.ErrorContains(t, err, "intent changed")
	require.NotNil(t, c.GetGroup("workers"))
	require.Empty(t, log.reads)
}

func TestTopicCleanupCapacityPreservesUnfinishedProgress(t *testing.T) {
	c, log := newCleanupFollower(t)
	c.cleanupProgress = make(map[string]*topicCleanupScan)
	for i := range topicCleanupTopics {
		c.cleanupProgress[strings.Repeat("x", i+1)] = &topicCleanupScan{
			partitions: []ConsumerCleanupPartition{{Identity: 0, End: 1}}, next: []uint64{0},
		}
	}
	_, err := c.ReconcileTopicGroups("orders")
	require.ErrorContains(t, err, "scan capacity")
	require.Len(t, c.cleanupProgress, topicCleanupTopics)
	require.Empty(t, log.reads)
	// A completed scan can yield its slot; unfinished scans retain their work.
	c.cleanupProgress["x"].next[0] = 1
	log.append(cleanupMessage(t, cleanupDeletion()))
	_, err = c.ReconcileTopicGroups("orders")
	require.NoError(t, err)
	require.NotContains(t, c.cleanupProgress, "x")
	require.Len(t, c.cleanupProgress, topicCleanupTopics-1)
}

func TestTopicCleanupCapacityReclaimsSupersededUnfinishedIntent(t *testing.T) {
	c, log := newCleanupFollower(t)
	current := true
	for range topicCleanupRecords + 1 {
		log.append(types.Message{Payload: `{"group":"other"}`})
	}
	_, err := c.ReconcileTopicGroupsIf("orders", func() bool { return current })
	require.ErrorContains(t, err, "budget")
	abandoned := c.cleanupProgress["orders"]
	require.NotNil(t, abandoned.current)
	// Model a later CREATE clearing the old materialization issue, so this
	// unfinished intent never receives another reconciliation call.
	delete(c.cleanupProgress, "orders")
	c.cleanupProgress["superseded"] = abandoned
	for i := range topicCleanupTopics - 1 {
		c.cleanupProgress[strings.Repeat("x", i+1)] = &topicCleanupScan{
			current:    func() bool { return true },
			partitions: []ConsumerCleanupPartition{{Identity: 0, End: 2}}, next: []uint64{1},
		}
	}
	current = false
	_, err = c.ReconcileTopicGroups("orders")
	require.ErrorContains(t, err, "budget")
	require.NotContains(t, c.cleanupProgress, "superseded")
	require.Len(t, c.cleanupProgress, topicCleanupTopics)
	for i := range topicCleanupTopics - 1 {
		require.Equal(t, []uint64{1}, c.cleanupProgress[strings.Repeat("x", i+1)].next)
	}
	require.NotNil(t, c.GetGroup("workers"))
	log.append(cleanupMessage(t, cleanupDeletion()))
	_, err = c.ReconcileTopicGroups("orders")
	require.NoError(t, err)
	require.Nil(t, c.GetGroup("workers"))
}
