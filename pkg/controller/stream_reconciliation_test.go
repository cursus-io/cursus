package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

const recoveryAppend = "APPEND_STREAM topic=events key=match-1 version=1 event_id=event-1 producerId=producer-1 seqNum=1 message=original"

// A failed append must not reset the committed historical index using the
// fail-closed stable offset (zero). Retry must preserve other aggregates too.
func TestStreamRecoveryPreservesHistoricalIndexWithoutRestart(t *testing.T) {
	h, p := newStreamRecoveryFixture(t)
	for _, cmd := range []string{
		"APPEND_STREAM topic=events key=history-a version=1 event_id=history-a-1 producerId=history-a seqNum=1 message=old-a",
		"APPEND_STREAM topic=events key=history-b version=1 event_id=history-b-1 producerId=history-b seqNum=1 message=old-b",
	} {
		require.True(t, strings.HasPrefix(h.handleAppendStream(cmd), "OK "))
	}
	loseStreamCommitReply(t, h)
	before, err := p.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, before, 3)
	require.Equal(t, "OK version=1 offset=2 partition=0", h.handleAppendStream(recoveryAppend))
	for _, key := range []string{"history-a", "history-b", "match-1"} {
		version, _, err := h.ESHandler.CurrentVersion("events", key)
		require.NoError(t, err)
		require.Equal(t, uint64(1), version, "historical aggregate %s", key)
	}
	after, err := p.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, "OK version=1 offset=2 partition=0", h.handleAppendStream(recoveryAppend))
	require.Contains(t, h.handleAppendStream("APPEND_STREAM topic=events key=history-a version=2 event_id=history-a-2 producerId=history-a seqNum=2 message=next"), "OK version=2 offset=3")
}

// newStreamRecoveryFixture uses real temporary log files and small explicit
// test headroom, independent of the operator disk's percentage of free space.
func newStreamRecoveryFixture(t *testing.T) (*CommandHandler, *topic.Partition) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	cfg.MinInSyncReplicas = 1
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.001
	h, p, _ := openStreamRecoveryFixture(t, cfg, 0)
	return h, p
}

// openStreamRecoveryFixture can reopen the same log directory with its known
// committed Raft boundary; it does not simulate Raft snapshot installation.
func openStreamRecoveryFixture(t *testing.T, cfg *config.Config, committed uint64) (*CommandHandler, *topic.Partition, func()) {
	t.Helper()
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	state := fsm.NewBrokerFSM(tm, nil)
	rm := &MockRaftManagerForForward{isLeader: true, state: state}
	rm.leaderAddress.Store("broker-1:9001")
	cluster := clusterController.NewClusterController(context.Background(), cfg, rm, nil, "broker-1", "broker-1:9001")
	h := NewCommandHandler(tm, cfg, nil, nil, cluster)
	enabled := true
	defaults := topic.DefaultDefinition("events", cfg)
	defaults.Partitions = 1
	_, err := tm.CreateTopicWithPatch(defaults, topic.DefinitionPatch{AggregateReplay: &enabled})
	require.NoError(t, err)
	installPartitionMetadata(t, h, "events", []string{"broker-1"})
	metadata := state.GetPartitionMetadata("events-0")
	metadata.CommittedHWM = committed
	applyPartitionMetadata(t, state, "events", 0, *metadata)
	p, err := tm.GetTopic("events").GetPartition(0)
	require.NoError(t, err)
	var once sync.Once
	stop := func() { once.Do(func() { _ = h.Close(); p.Close(); dm.CloseAllHandlers() }) }
	t.Cleanup(stop)
	return h, p, stop
}

// loseStreamCommitReply injects an accepted-but-unanswered Raft request without
// applying it locally, matching the uncertainty that must preserve the tail.
func loseStreamCommitReply(t *testing.T, h *CommandHandler) *uncertainStreamCommitRaft {
	t.Helper()
	rm := &uncertainStreamCommitRaft{MockRaftManagerForForward: h.Cluster.RaftManager.(*MockRaftManagerForForward)}
	h.Cluster.RaftManager = rm
	h.Cluster.Router = nil
	require.Contains(t, h.handleAppendStream(recoveryAppend), "append_stream_failed")
	return rm
}

// A retry can complete the original commit, and concurrent retries all receive
// the same offset without creating a second event or reusing its offset.
func TestStreamRecoveryLostCommitReplyDeduplicatesConcurrentRetries(t *testing.T) {
	h, p := newStreamRecoveryFixture(t)
	loseStreamCommitReply(t, h)
	before, err := p.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, before, 1)
	responses := make(chan string, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); responses <- h.handleAppendStream(recoveryAppend) }()
	}
	wg.Wait()
	close(responses)
	for response := range responses {
		require.Equal(t, "OK version=1 offset=0 partition=0", response)
	}
	after, err := p.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, uint64(1), p.GetHWM())
	require.NoError(t, p.RecoveryError())
	require.Contains(t, h.handleAppendStream(strings.Replace(recoveryAppend, "message=original", "message=conflict", 1)), "aggregate_identity_conflict")
	require.Equal(t, uint64(1), p.NextOffset())
}

// A different aggregate may progress only after the preserved event commits;
// the derived stream index is rebuilt before validating the next append.
func TestStreamRecoveryBeforeDifferentAggregate(t *testing.T) {
	h, p := newStreamRecoveryFixture(t)
	loseStreamCommitReply(t, h)
	response := h.handleAppendStream("APPEND_STREAM topic=events key=match-2 version=1 event_id=event-2 producerId=producer-2 seqNum=1 message=second")
	require.Contains(t, response, "OK version=1 offset=1")
	messages, err := p.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Len(t, messages, 2)
	require.Equal(t, "original", messages[0].Payload)
	require.Equal(t, "second", messages[1].Payload)
}

// The existing log, not a new database or an in-memory error string, retains
// the event identity. Reopening the disk preserves a committed prefix and a
// same-epoch unresolved tail; retries still return the original offset.
func TestStreamRecoveryDiskReopenPreservesPrefixAndTail(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	cfg.MinInSyncReplicas = 1
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.001
	h, p, stop := openStreamRecoveryFixture(t, cfg, 0)
	require.Equal(t, "OK version=1 offset=0 partition=0", h.handleAppendStream(recoveryAppend))
	second := "APPEND_STREAM topic=events key=match-1 version=2 event_id=event-2 producerId=producer-1 seqNum=2 message=second"
	rm := &uncertainStreamCommitRaft{MockRaftManagerForForward: h.Cluster.RaftManager.(*MockRaftManagerForForward)}
	h.Cluster.RaftManager = rm
	h.Cluster.Router = nil
	require.Contains(t, h.handleAppendStream(second), "append_stream_failed")
	before, err := p.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, before, 2)
	require.Equal(t, uint64(1), p.GetHWM())
	stop()
	reopened, partition, _ := openStreamRecoveryFixture(t, cfg, 1)
	require.Nil(t, partition.ReconciliationError(), "the in-memory outcome is intentionally absent")
	require.Equal(t, uint64(2), partition.NextOffset())
	require.Equal(t, "OK version=2 offset=1 partition=0", reopened.handleAppendStream(second))
	require.Equal(t, "OK version=2 offset=1 partition=0", reopened.handleAppendStream(second))
	after, err := partition.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, uint64(2), partition.NextOffset())
}

// Recovery never clears arbitrary storage fences, changes leader epochs, or
// substitutes the in-memory request for different bytes retained on disk.
func TestStreamRecoveryFailsClosedWithoutRewritingTail(t *testing.T) {
	for _, kind := range []string{"epoch", "lifecycle", "unknown_marker", "record_conflict", "replica_set", "unavailable"} {
		t.Run(kind, func(t *testing.T) {
			h, p := newStreamRecoveryFixture(t)
			rm := loseStreamCommitReply(t, h)
			before, err := p.ReadMessages(0, 10)
			require.NoError(t, err)
			switch kind {
			case "epoch":
				m := rm.state.GetPartitionMetadata("events-0")
				m.LeaderEpoch++
				applyPartitionMetadata(t, rm.state, "events", 0, *m)
			case "lifecycle":
				var pending *streamAppendOutcome
				require.ErrorAs(t, p.ReconciliationError(), &pending)
				pending.snapshot.LifecycleEpoch++
			case "replica_set":
				m := rm.state.GetPartitionMetadata("events-0")
				m.ISR = append(m.ISR, "broker-2")
				applyPartitionMetadata(t, rm.state, "events", 0, *m)
			case "unknown_marker":
				p.MarkReconciliationPending(errors.New("storage repair required"))
			case "record_conflict":
				var pending *streamAppendOutcome
				require.ErrorAs(t, p.ReconciliationError(), &pending)
				pending.message.Payload = "different"
			case "unavailable":
				h.Cluster.RaftManager = &alwaysFailStreamCommit{rm.MockRaftManagerForForward}
			}
			require.True(t, strings.HasPrefix(h.handleAppendStream(recoveryAppend), "ERROR"))
			after, err := p.ReadMessages(0, 10)
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Zero(t, p.GetHWM())
			require.Error(t, p.RecoveryError())
		})
	}
}

// A marker-free tail must prove uniqueness against committed history, not
// merely have plausible offsets. Failed proof never rewrites either record.
func TestStreamRecoveryRetainedTailRejectsInvalidIdentity(t *testing.T) {
	for _, kind := range []string{"duplicate_event", "bad_digest", "old_epoch", "multiple_records", "transactional"} {
		t.Run(kind, func(t *testing.T) {
			h, p := newStreamRecoveryFixture(t)
			require.Contains(t, h.handleAppendStream(recoveryAppend), "OK version=1")
			committed, err := p.ReadMessages(0, 1)
			require.NoError(t, err)
			message := committed[0]
			message.Offset = 1
			message.Key = "other-match"
			message.SeqNum = 2
			switch kind {
			case "bad_digest":
				message.EventID = "event-2"
				message.PayloadDigest = "invalid"
			case "old_epoch":
				message.LeaderEpoch--
			case "transactional":
				message.TransactionalID = "txn"
			}
			require.NoError(t, p.ReplicaAppend([]types.Message{message}))
			if kind == "multiple_records" {
				message.Offset = 2
				message.SeqNum = 3
				require.NoError(t, p.ReplicaAppend([]types.Message{message}))
			}
			before, err := p.ReadMessages(0, 10)
			require.NoError(t, err)
			for attempt := 0; attempt < 3; attempt++ {
				require.Contains(t, h.handleAppendStream(recoveryAppend), "ERROR")
				after, err := p.ReadMessages(0, 10)
				require.NoError(t, err)
				require.Equal(t, before, after)
				require.Equal(t, uint64(len(before)), p.NextOffset())
				require.Equal(t, uint64(1), p.GetHWM())
			}
		})
	}
}

type alwaysFailStreamCommit struct{ *MockRaftManagerForForward }

func (m *alwaysFailStreamCommit) ApplyCommand(prefix string, data []byte) error {
	if prefix == "PARTITION_COMMIT" {
		return errors.New("commit unavailable")
	}
	return m.MockRaftManagerForForward.ApplyCommand(prefix, data)
}

// A wire follower durably accepts the first record but drops its response. Its
// idempotent append path receives the exact same record during recovery.
func TestStreamRecoveryLostReplicaReplyPreservesBothCopies(t *testing.T) {
	h, p := newStreamRecoveryFixture(t)
	_, follower := newStreamRecoveryFixture(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	rm := h.Cluster.RaftManager.(*MockRaftManagerForForward)
	registerRoutingBroker(t, rm.state, "broker-2")
	installPartitionMetadata(t, h, "events", []string{"broker-1", "broker-2"})
	cfg := *h.Config
	cfg.BrokerPort = listener.Addr().(*net.TCPAddr).Port
	cfg.InternalBrokerPort = 0
	h.Cluster.Router = clusterController.NewClusterRouter("broker-1", "broker-1:9001", nil, rm, cfg.BrokerPort, "", &cfg)
	var attempts atomic.Int32
	served := make(chan error, 2)
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := listener.Accept()
			if err != nil {
				served <- err
				return
			}
			func() {
				defer func() { _ = conn.Close() }()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				wc, err := wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone})
				if err != nil {
					served <- err
					return
				}
				frame, err := wc.ReadFrame()
				if err != nil {
					served <- err
					return
				}
				payload, err := wire.DecodeCommandPayload(frame.Payload)
				if err != nil {
					served <- err
					return
				}
				var command types.MessageCommand
				if err = json.Unmarshal([]byte(payload.Fields["payload"]), &command); err != nil {
					served <- err
					return
				}
				if err = follower.ReplicaAppend(command.Messages); err != nil {
					served <- err
					return
				}
				attempts.Add(1)
				if i == 0 {
					served <- nil
					return
				}
				served <- wc.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: frame.Command, RequestID: frame.RequestID, Status: wire.StatusOK, Payload: []byte("OK")})
			}()
		}
	}()
	first := h.handleAppendStream(recoveryAppend)
	require.Contains(t, first, "append_stream_failed")
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatalf("replica did not receive append: %s", first)
	}
	require.Error(t, p.RecoveryError())
	require.Equal(t, uint64(1), follower.NextOffset())
	require.Equal(t, "OK version=1 offset=0 partition=0", h.handleAppendStream(recoveryAppend))
	require.NoError(t, <-served)
	require.Equal(t, int32(2), attempts.Load())
	leaderMessages, err := p.ReadMessages(0, 10)
	require.NoError(t, err)
	followerMessages, err := follower.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Equal(t, leaderMessages, followerMessages)
	require.Len(t, leaderMessages, 1)
}
