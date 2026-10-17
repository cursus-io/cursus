package controller

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// A Raft transport failure does not prove that the submitted entry was rejected.
type uncertainStreamCommitRaft struct {
	*MockRaftManagerForForward
	pending []byte
}

func (m *uncertainStreamCommitRaft) ApplyCommand(prefix string, data []byte) error {
	if prefix == "PARTITION_COMMIT" && m.pending == nil {
		m.pending = append([]byte(prefix+":"), data...)
		return errors.New("commit accepted but response lost")
	}
	return m.MockRaftManagerForForward.ApplyCommand(prefix, data)
}

func TestAppendStreamUncertainCommitPreservesOffset(t *testing.T) {
	handler, manager, _ := newDistributedAckTestHandler(t, 1)
	require.NoError(t, manager.CreateTopic("events", 1, false, true))
	installPartitionMetadata(t, handler, "events", []string{"broker-1"})
	rm := &uncertainStreamCommitRaft{MockRaftManagerForForward: handler.Cluster.RaftManager.(*MockRaftManagerForForward)}
	handler.Cluster.RaftManager = rm
	// Keep the injected transport failure from being retried through a router.
	handler.Cluster.Router = nil
	p, err := manager.GetTopic("events").GetPartition(0)
	require.NoError(t, err)
	first := handler.handleAppendStream("APPEND_STREAM topic=events key=aggregate-a version=1 message=original")
	require.Contains(t, first, "append_stream_failed")
	require.NotEmpty(t, rm.pending)
	before, err := p.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, before, 1)
	require.Zero(t, p.GetHWM())
	logFiles, err := filepath.Glob(filepath.Join(handler.Config.LogDir, "events", "*.log"))
	require.NoError(t, err)
	require.NotEmpty(t, logFiles)
	logBytes := make(map[string][]byte, len(logFiles))
	for _, path := range logFiles {
		logBytes[path], err = os.ReadFile(path)
		require.NoError(t, err)
	}

	// Another aggregate uses a different stream lock but shares this offset space.
	second := handler.handleAppendStream("APPEND_STREAM topic=events key=aggregate-b version=1 message=replacement")
	after, err := p.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Equal(t, before, after, "an uncertain commit must not permit offset reuse")
	require.True(t, strings.HasPrefix(second, "ERROR"), second)
	require.Error(t, p.RecoveryError())
	require.Equal(t, uint64(1), p.NextOffset())
	for path, original := range logBytes {
		current, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		require.Equal(t, original, current, "blocked append must rewrite zero retained log bytes")
	}

	// The delayed entry can still commit. It must refer to the original bytes.
	require.Nil(t, rm.state.Apply(&raft.Log{Data: rm.pending}))
	afterCommit, err := p.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Equal(t, before, afterCommit)
	require.Equal(t, uint64(1), rm.state.GetPartitionMetadata("events-0").CommittedHWM)
}

type streamCommitReply struct{ replied chan struct{} }

func (p streamCommitReply) ProcessCommand(string) string {
	close(p.replied)
	return "OK"
}

func TestAppendStreamForwardedCommitWaitsForLocalApply(t *testing.T) {
	handler, manager, _ := newDistributedAckTestHandler(t, 1)
	require.NoError(t, manager.CreateTopic("events", 1, false, true))
	installPartitionMetadata(t, handler, "events", []string{"broker-1"})
	rm := &uncertainStreamCommitRaft{MockRaftManagerForForward: handler.Cluster.RaftManager.(*MockRaftManagerForForward)}
	handler.Cluster.RaftManager = rm
	// Model a successful forwarded reply arriving before local Raft application.
	replied := make(chan struct{})
	handler.Cluster.SetLocalProcessor(streamCommitReply{replied: replied})
	done := make(chan string, 1)
	go func() {
		done <- handler.handleAppendStream("APPEND_STREAM topic=events key=aggregate-a version=1 message=original")
	}()
	select {
	case <-replied:
	case <-time.After(5 * time.Second):
		t.Fatal("commit was not forwarded")
	}
	select {
	case response := <-done:
		t.Fatalf("released append before local commit application: %s", response)
	case <-time.After(100 * time.Millisecond):
	}
	require.Nil(t, rm.state.Apply(&raft.Log{Data: rm.pending}))
	select {
	case response := <-done:
		require.True(t, strings.HasPrefix(response, "OK "), response)
	case <-time.After(5 * time.Second):
		t.Fatal("append did not finish after local commit application")
	}
	p, err := manager.GetTopic("events").GetPartition(0)
	require.NoError(t, err)
	require.Equal(t, uint64(1), p.GetHWM())
}

func TestAppendStreamSuccessAppliesLocalCommitBeforeNextAggregate(t *testing.T) {
	handler, manager, _ := newDistributedAckTestHandler(t, 1)
	require.NoError(t, manager.CreateTopic("events", 1, false, true))
	installPartitionMetadata(t, handler, "events", []string{"broker-1"})
	p, err := manager.GetTopic("events").GetPartition(0)
	require.NoError(t, err)
	for i, key := range []string{"aggregate-a", "aggregate-b"} {
		response := handler.handleAppendStream("APPEND_STREAM topic=events key=" + key + " version=1 message=event")
		require.True(t, strings.HasPrefix(response, "OK "), response)
		require.Equal(t, uint64(i+1), p.GetHWM())
	}
	messages, err := p.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Len(t, messages, 2)
	require.Equal(t, "aggregate-a", messages[0].Key)
	require.Equal(t, "aggregate-b", messages[1].Key)
	response := handler.handleAppendStream("APPEND_STREAM topic=events key=aggregate-a version=1 message=duplicate")
	require.Contains(t, response, "version_conflict")
	require.NoError(t, p.RecoveryError(), "validation failures must not fence a healthy partition")
}

func TestAppendStreamReplicationFailurePreservesTail(t *testing.T) {
	handler, manager, _ := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("events", 1, false, true))
	// Missing broker-2 registration causes replication to fail after local append.
	installPartitionMetadata(t, handler, "events", []string{"broker-1", "broker-2"})
	p, err := manager.GetTopic("events").GetPartition(0)
	require.NoError(t, err)
	response := handler.handleAppendStream("APPEND_STREAM topic=events key=aggregate-a version=1 message=original")
	require.Contains(t, response, "append_stream_failed")
	require.ErrorContains(t, p.RecoveryError(), "stream replication outcome")
	before, err := p.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, before, 1)
	response = handler.handleAppendStream("APPEND_STREAM topic=events key=aggregate-b version=1 message=replacement")
	require.Contains(t, response, "partition recovery pending")
	after, err := p.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Zero(t, p.GetHWM())
}
