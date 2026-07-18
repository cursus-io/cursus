package stream

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

var readFn = func(offset uint64, max int) ([]types.Message, error) {
	return nil, nil
}

var DefaultStreamCommitInterval = 5 * time.Second

func TestAddRemoveStream(t *testing.T) {
	sm := NewStreamManager(2, 500*time.Millisecond, 100*time.Millisecond)

	conn1, _ := net.Pipe()
	defer func() { _ = conn1.Close() }()
	stream1 := NewStreamConnection(conn1, "topic1", 0, "group1", 0)

	key1 := "topic1:0:group1"
	if err := sm.AddStream(key1, stream1, readFn, DefaultStreamCommitInterval); err != nil {
		t.Fatalf("failed to add stream: %v", err)
	}

	if len(sm.GetStreamsForPartition("topic1", 0)) != 1 {
		t.Fatalf("expected 1 stream for partition")
	}

	sm.RemoveStream(key1)
	if len(sm.GetStreamsForPartition("topic1", 0)) != 0 {
		t.Fatalf("expected 0 streams after removal")
	}
}

func TestAddStreamReplacesSameKeyWithoutOldMonitorDeletingReplacement(t *testing.T) {
	sm := NewStreamManager(2, time.Hour, 5*time.Millisecond)

	conn1, peer1 := net.Pipe()
	defer func() { _ = peer1.Close() }()
	stream1 := NewStreamConnection(conn1, "topic", 0, "group", 0)

	const key = "topic:0:group"
	if err := sm.AddStream(key, stream1, readFn, DefaultStreamCommitInterval); err != nil {
		t.Fatalf("failed to add initial stream: %v", err)
	}

	conn2, peer2 := net.Pipe()
	defer func() { _ = peer2.Close() }()
	stream2 := NewStreamConnection(conn2, "topic", 0, "group", 0)
	if err := sm.AddStream(key, stream2, readFn, DefaultStreamCommitInterval); err != nil {
		t.Fatalf("failed to replace stream: %v", err)
	}
	defer sm.RemoveStream(key)

	select {
	case <-stream1.stopCh:
	case <-time.After(time.Second):
		t.Fatal("replaced stream was not stopped")
	}

	deadline := time.Now().Add(time.Second)
	for stream1.Conn() != nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if stream1.Conn() != nil {
		t.Fatal("replaced stream connection was not closed")
	}

	streams := sm.GetStreamsForPartition("topic", 0)
	if len(streams) != 1 || streams[0] != stream2 {
		t.Fatalf("expected replacement stream to remain registered, got %v", streams)
	}
}

func TestMaxConnections(t *testing.T) {
	sm := NewStreamManager(1, time.Second, 100*time.Millisecond)

	conn1, _ := net.Pipe()
	defer func() { _ = conn1.Close() }()

	stream1 := NewStreamConnection(conn1, "topic", 0, "group1", 0)
	if err := sm.AddStream("key1", stream1, readFn, DefaultStreamCommitInterval); err != nil {
		t.Fatalf("failed to add stream: %v", err)
	}

	conn2, _ := net.Pipe()
	defer func() { _ = conn2.Close() }()
	stream2 := NewStreamConnection(conn2, "topic", 0, "group2", 0)
	if err := sm.AddStream("key2", stream2, readFn, DefaultStreamCommitInterval); err == nil {
		t.Fatalf("expected error when adding stream beyond maxConn")
	}
}

func TestGetStreamsForPartition(t *testing.T) {
	sm := NewStreamManager(5, time.Second, 100*time.Millisecond)

	var conns []net.Conn
	for i := 0; i < 3; i++ {
		c1, c2 := net.Pipe()
		conns = append(conns, c1, c2)
		s := NewStreamConnection(c1, "topicA", i, "group", uint64(i))
		if err := sm.AddStream("key"+strconv.Itoa(i), s, readFn, DefaultStreamCommitInterval); err != nil {
			t.Fatalf("failed to add stream: %v", err)
		}

	}

	streams := sm.GetStreamsForPartition("topicA", 1)
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream for partition 1, got %d", len(streams))
	}

	for _, c := range conns {
		if err := c.Close(); err != nil {
			util.Debug("failed to close connection: %v", err)
		}
	}
}

func TestStreamConnectionOffsetAndActive(t *testing.T) {
	conn, _ := net.Pipe()
	defer func() { _ = conn.Close() }()

	sc := NewStreamConnection(conn, "topic", 0, "group", 0)
	if sc.Offset() != 0 {
		t.Fatalf("expected initial offset 0")
	}

	sc.IncrementOffset()
	if sc.Offset() != 1 {
		t.Fatalf("expected offset 1 after increment")
	}

	now := time.Now()
	sc.SetLastActive(now)

	if !sc.LastActive().Equal(now) {
		t.Fatalf("expected lastActive to be updated")
	}
}
