package stream

import (
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestStreamConnection_Basic(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

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
	defer c1.Close()
	defer c2.Close()

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
	defer c1.Close()
	defer c2.Close()

	sc := NewStreamConnection(c1, "t1", 0, "g1", 0)
	sc.SetInterval(10 * time.Millisecond)

	readFn := func(offset uint64, max int) ([]types.Message, error) {
		return []types.Message{}, nil // No messages -> should send keepalive
	}

	go sc.Run(readFn, 100*time.Millisecond)

	// Read keepalive from c2
	buf := make([]byte, 4)
	c2.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, err := c2.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0, 0, 0, 0}, buf)

	sc.Stop()
}
