package stream

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testMessageGeneration struct {
	mu         sync.Mutex
	generation uint64
	ch         chan struct{}
}

func newTestMessageGeneration() *testMessageGeneration {
	return &testMessageGeneration{ch: make(chan struct{})}
}

func (g *testMessageGeneration) snapshot() (uint64, <-chan struct{}) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.generation, g.ch
}

func (g *testMessageGeneration) notify() {
	g.mu.Lock()
	previous := g.ch
	g.generation++
	g.ch = make(chan struct{})
	close(previous)
	g.mu.Unlock()
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

	go sc.Run(readFn)

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

	readCalled := make(chan bool, 1)
	readFn := func(offset uint64, max int) ([]types.Message, error) {
		select {
		case readCalled <- true:
		default:
		}
		return []types.Message{}, nil // No messages -> should send keepalive
	}

	go sc.Run(readFn)
	select {
	case <-readCalled:
	case <-time.After(time.Second):
		t.Fatal("initial stream read did not run")
	}
	sc.schedule(time.Now().Add(time.Second))

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
	readFn := func(offset uint64, max int) ([]types.Message, error) {
		return []types.Message{}, nil
	}

	done := make(chan struct{})
	go func() {
		sc.Run(readFn)
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
}

func TestMessageGenerationWakesEveryStream(t *testing.T) {
	generation := newTestMessageGeneration()
	firstConn, firstPeer := net.Pipe()
	secondConn, secondPeer := net.Pipe()
	defer func() { _ = firstPeer.Close() }()
	defer func() { _ = secondPeer.Close() }()

	first := NewStreamConnection(firstConn, "orders", 0, "first", 0)
	second := NewStreamConnection(secondConn, "orders", 0, "second", 0)
	first.SetMessageSource(generation.snapshot)
	second.SetMessageSource(generation.snapshot)
	firstReads := make(chan struct{}, 2)
	secondReads := make(chan struct{}, 2)
	go first.Run(func(uint64, int) ([]types.Message, error) {
		firstReads <- struct{}{}
		return nil, nil
	})
	go second.Run(func(uint64, int) ([]types.Message, error) {
		secondReads <- struct{}{}
		return nil, nil
	})

	waitForRead := func(name string, reads <-chan struct{}) {
		t.Helper()
		select {
		case <-reads:
		case <-time.After(time.Second):
			t.Fatalf("%s stream was not woken", name)
		}
	}
	waitForRead("first initial", firstReads)
	waitForRead("second initial", secondReads)
	generation.notify()
	waitForRead("first generation", firstReads)
	waitForRead("second generation", secondReads)
	first.Stop()
	second.Stop()
}

func TestStreamConnectionWaitsForTransientOffsetFence(t *testing.T) {
	generation := newTestMessageGeneration()
	server, client := net.Pipe()
	defer func() { _ = client.Close() }()

	sc := NewStreamConnection(server, "orders", 0, "workers", 7)
	sc.SetMessageSource(generation.snapshot)
	metric := &dto.Metric{}
	require.NoError(t, metrics.StreamTransientOffsetFences.Write(metric))
	before := metric.GetCounter().GetValue()
	var mu sync.Mutex
	reads := 0
	readFn := func(offset uint64, _ int) ([]types.Message, error) {
		mu.Lock()
		defer mu.Unlock()
		reads++
		if reads <= 2 {
			return nil, fmt.Errorf("ERROR: unstable_offset_commit group=workers topic=orders partition=0")
		}
		return []types.Message{{Offset: offset, Payload: "next"}}, nil
	}
	done := make(chan struct{})
	go func() {
		sc.Run(readFn)
		close(done)
	}()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return reads == 1
	}, time.Second, time.Millisecond)
	generation.notify()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return reads == 2
	}, time.Second, time.Millisecond)
	select {
	case <-done:
		t.Fatal("transient offset fence closed the stream")
	default:
	}
	generation.notify()
	require.NoError(t, client.SetReadDeadline(time.Now().Add(time.Second)))
	data, err := util.ReadWithLength(client)
	require.NoError(t, err)
	batch, err := util.DecodeBatchMessages(data)
	require.NoError(t, err)
	require.Len(t, batch.Messages, 1)
	require.Equal(t, uint64(7), batch.Messages[0].Offset)
	metric.Reset()
	require.NoError(t, metrics.StreamTransientOffsetFences.Write(metric))
	require.Equal(t, before+1, metric.GetCounter().GetValue(), "one blocked episode must increment the metric once")

	require.NoError(t, client.Close())
	sc.Stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream did not stop")
	}
}

func TestStreamConnectionStillClosesForOtherProtocolErrors(t *testing.T) {
	for name, response := range map[string]string{
		"coordinator moved":   "ERROR: NOT_COORDINATOR",
		"generation mismatch": "ERROR: GEN_MISMATCH",
	} {
		t.Run(name, func(t *testing.T) {
			server, client := net.Pipe()
			defer func() { _ = client.Close() }()
			sc := NewStreamConnection(server, "orders", 0, "workers", 7)
			done := make(chan struct{})
			go func() {
				sc.Run(func(uint64, int) ([]types.Message, error) {
					return nil, fmt.Errorf("%s", response)
				})
				close(done)
			}()

			require.NoError(t, client.SetReadDeadline(time.Now().Add(time.Second)))
			control, err := util.ReadWithLength(client)
			require.NoError(t, err)
			require.Contains(t, string(control), "reason=error")
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("non-fence protocol error did not stop the stream")
			}
		})
	}
}
