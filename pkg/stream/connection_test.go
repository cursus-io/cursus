package stream

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/assert"
)

type countingOffsetCommitter struct {
	commits int
}

func (c *countingOffsetCommitter) CommitOffset(string, string, int, uint64) error {
	c.commits++
	return nil
}

func TestStreamConnection_Basic(t *testing.T) {
	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()

	sc := NewStreamConnection(c1, "test-topic", 0, "test-group", 100)
	assert.Equal(t, "test-topic", sc.Topic())
	assert.Equal(t, 0, sc.Partition())
	assert.Equal(t, "test-group", sc.Group())
	assert.Equal(t, uint64(100), sc.Offset())

	sc.SetBatchSize(5)
	sc.SetInterval(50 * time.Millisecond)
	sc.IncrementOffset()
	assert.Equal(t, uint64(101), sc.Offset())
}

func TestStreamConnection_Run(t *testing.T) {
	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()

	sc := NewStreamConnection(c1, "t1", 0, "g1", 0)
	sc.SetInterval(10 * time.Millisecond)

	readCalled := make(chan bool, 1)
	readFn := func(offset uint64, max int) ([]types.Message, error) {
		readCalled <- true
		return []types.Message{{Offset: offset, Payload: "msg"}}, nil
	}

	go sc.Run(readFn, 100*time.Millisecond)

	// Wait for read to be called
	select {
	case <-readCalled:
		// OK
	case <-time.After(1 * time.Second):
		t.Fatal("readFn not called")
	}

	sc.Stop()
}

func TestStreamConnection_Keepalive(t *testing.T) {
	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()

	sc := NewStreamConnection(c1, "t1", 0, "g1", 0)
	sc.SetInterval(10 * time.Millisecond)
	sc.SetKeepaliveInterval(100 * time.Millisecond)

	readFn := func(offset uint64, max int) ([]types.Message, error) {
		return []types.Message{}, nil // No messages -> should send keepalive
	}

	go sc.Run(readFn, 100*time.Millisecond)

	// Read keepalive from c2
	buf := make([]byte, 4)
	err := c2.SetReadDeadline(time.Now().Add(1 * time.Second))
	assert.NoError(t, err)
	_, err = io.ReadFull(c2, buf)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0, 0, 0, 0}, buf)

	sc.Stop()
}

func TestStreamConnection_StopSendsCloseControlFrame(t *testing.T) {
	c1, c2 := net.Pipe()
	defer func() { _ = c2.Close() }()

	sc := NewStreamConnection(c1, "t1", 0, "g1", 42)
	sc.SetInterval(10 * time.Second)
	sc.SetKeepaliveInterval(10 * time.Second)
	committer := &countingOffsetCommitter{}
	sc.SetCommitter(committer)

	readFn := func(offset uint64, max int) ([]types.Message, error) {
		return []types.Message{}, nil
	}

	done := make(chan struct{})
	go func() {
		sc.Run(readFn, 10*time.Second)
		close(done)
	}()

	sc.StopWithReason("test_stop")

	err := c2.SetReadDeadline(time.Now().Add(1 * time.Second))
	assert.NoError(t, err)
	data, err := util.ReadWithLength(c2)
	assert.NoError(t, err)

	frame := string(data)
	assert.True(t, strings.HasPrefix(frame, StreamControlPrefix))
	assert.Contains(t, frame, "type=CLOSE")
	assert.Contains(t, frame, "reason=test_stop")
	assert.Contains(t, frame, "offset=42")

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("stream did not stop")
	}
	assert.Zero(t, committer.commits, "delivery and close must not commit consumer offsets")
}
