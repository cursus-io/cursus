package sdk

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A transactional handler commits the group offset on another connection. A
// STREAM disconnect must wait for that handler and resume at the broker offset.
func TestPartitionStreamReconnectWaitsForTransactionalOffset(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	commands := make(chan string, 4)
	serverErrors := make(chan error, 1)
	batch, err := EncodeBatchMessages("events", 0, "all", false, []Message{{Offset: 56, Payload: "settle"}})
	require.NoError(t, err)
	go func() {
		for i, response := range []string{"OK offset=56", string(batch), "OK offset=57", ""} {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				serverErrors <- acceptErr
				return
			}
			wireConn, request, command, readErr := acceptWireTestRequest(conn)
			if readErr != nil {
				_ = conn.Close()
				serverErrors <- readErr
				return
			}
			commands <- command
			if response != "" {
				if writeErr := writeWireTestResponse(wireConn, request, response); writeErr != nil {
					_ = conn.Close()
					serverErrors <- writeErr
					return
				}
			}
			if i < 3 {
				_ = conn.Close()
			} else {
				// Keep the replacement STREAM connected until the consumer stops.
				<-time.After(time.Second)
				_ = conn.Close()
			}
		}
		serverErrors <- nil
	}()

	cfg := NewDefaultConsumerConfig()
	cfg.BrokerAddrs = []string{listener.Addr().String()}
	cfg.Topic = "events"
	cfg.GroupID = "settlement"
	cfg.Mode = ModeStreaming
	cfg.EnableAutoCommit = false
	cfg.HandlerMaxRetries = 0
	cfg.ConnectRetryBackoffMS = 1
	c, err := NewConsumer(cfg)
	require.NoError(t, err)
	c.state.Store(uint32(ConsumerStateRunning))
	c.assignmentGeneration.Store(1)
	c.coordinatorAddr = listener.Addr().String()
	c.offsets[0] = 9 // stale local offset after an external transactional commit
	c.memberID = "member-1"
	c.generation = 1

	handling := make(chan struct{})
	release := make(chan struct{})
	c.MessageHandler = func(msg Message) error {
		if msg.Offset != 56 {
			return fmt.Errorf("unexpected replay at offset %d", msg.Offset)
		}
		close(handling)
		<-release
		return nil // transaction committed offset 57 before returning
	}
	pc := &PartitionConsumer{partitionID: 0, consumer: c, fetchOffset: 9, assignmentGeneration: 1, ctx: c.assignmentContext()}
	finished := make(chan struct{})
	go func() { pc.startStreamLoop(); close(finished) }()
	defer func() {
		c.cancelAssignment()
		pc.closeConnection()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("stream loop did not stop")
		}
		c.wg.Wait()
	}()

	require.Equal(t, "FETCH_OFFSET topic=events partition=0 group=settlement", receiveReconnectCommand(t, commands))
	require.True(t, strings.HasPrefix(receiveReconnectCommand(t, commands), "STREAM topic=events partition=0 group=settlement offset=56 "))
	select {
	case <-handling:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not receive the first stream batch")
	}
	select {
	case command := <-commands:
		t.Fatalf("reconnected before transactional handler finished: %s", command)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.Equal(t, "FETCH_OFFSET topic=events partition=0 group=settlement", receiveReconnectCommand(t, commands))
	require.True(t, strings.HasPrefix(receiveReconnectCommand(t, commands), "STREAM topic=events partition=0 group=settlement offset=57 "))
	require.NoError(t, <-serverErrors)
	require.NoError(t, c.Err())
}

func receiveReconnectCommand(t *testing.T, commands <-chan string) string {
	t.Helper()
	select {
	case command := <-commands:
		return command
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for broker command")
		return ""
	}
}

func TestManualStreamKeepsOffsetOutOfRangeResetOnReconnect(t *testing.T) {
	addr, commands := startSDKCommandServer(t, "")
	cfg := NewDefaultConsumerConfig()
	cfg.BrokerAddrs = []string{addr}
	cfg.Topic = "events"
	cfg.GroupID = "workers"
	cfg.EnableAutoCommit = false
	c, err := NewConsumer(cfg)
	require.NoError(t, err)
	c.state.Store(uint32(ConsumerStateRunning))
	c.assignmentGeneration.Store(1)
	c.MessageHandler = func(Message) error { return nil }
	pc := &PartitionConsumer{partitionID: 0, consumer: c, fetchOffset: 4, assignmentGeneration: 1, ctx: c.assignmentContext()}
	require.True(t, pc.handleOffsetOutOfRange(offsetOutOfRangeFrame{Requested: 4, Earliest: 8, Latest: 12}))
	finished := make(chan struct{})
	go func() { pc.startStreamLoop(); close(finished) }()
	command := requireSDKCommand(t, commands)
	require.True(t, strings.HasPrefix(command, "STREAM topic=events partition=0 group=workers offset=8 "), command)
	c.cancelAssignment()
	pc.closeConnection()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("stream loop did not stop")
	}
	c.wg.Wait()
}

func TestTransactionalStreamReconnectAgainstBroker(t *testing.T) {
	rawAddrs := strings.TrimSpace(os.Getenv("CURSUS_INTEGRATION_ADDRS"))
	if rawAddrs == "" {
		t.Skip("set CURSUS_INTEGRATION_ADDRS to run broker integration validation")
	}
	addrs := strings.Split(rawAddrs, ",")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	topic := "sdk-reconnect-inbox-" + suffix
	markerTopic := "sdk-reconnect-marker-" + suffix
	group := "sdk-reconnect-group-" + suffix
	cfg := NewDefaultConsumerConfig()
	cfg.BrokerAddrs = addrs
	cfg.Topic = topic
	cfg.GroupID = group
	cfg.Mode = ModeStreaming
	cfg.BatchSize = 1
	cfg.EnableAutoCommit = false
	cfg.HandlerMaxRetries = 0
	client, err := NewConsumerClient(cfg)
	require.NoError(t, err)
	for _, name := range []string{topic, markerTopic} {
		_, err = integrationOKCommand(client, "CREATE topic="+name+" partitions=1")
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, name := range []string{topic, markerTopic} {
			_, _ = integrationOKCommand(client, "DELETE topic="+name)
		}
	})
	require.NoError(t, publishSagaIntegrationInput(client, topic, "Created", "first", 1))

	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)
	var mu sync.Mutex
	counts := map[uint64]int{}
	firstDone := make(chan struct{})
	secondDone := make(chan struct{})
	startDone := make(chan error, 1)
	go func() {
		startDone <- consumer.Start(func(message Message) error {
			mu.Lock()
			counts[message.Offset]++
			count := counts[message.Offset]
			mu.Unlock()
			if count != 1 {
				return fmt.Errorf("redelivered committed offset %d", message.Offset)
			}
			consumer.mu.RLock()
			member, generation := consumer.memberID, consumer.generation
			consumer.mu.RUnlock()
			producer, err := client.NewTransactionalProducer("sdk-reconnect-txn-" + suffix + "-" + strconv.FormatUint(message.Offset, 10))
			if err != nil {
				return err
			}
			if err := producer.Begin(); err != nil {
				return err
			}
			if err := producer.Publish(markerTopic, 0, Message{SeqNum: 1, Payload: "processed"}); err != nil {
				_ = producer.Abort()
				return err
			}
			if err := producer.SendOffsets(topic, group, member, int(generation), map[int]uint64{0: message.Offset + 1}); err != nil {
				_ = producer.Abort()
				return err
			}
			if err := producer.Commit(); err != nil {
				return err
			}
			if message.Offset == 0 {
				consumer.mu.RLock()
				partition := consumer.partitionConsumers[0]
				consumer.mu.RUnlock()
				partition.closeConnection() // simulate a dropped STREAM after commit
				close(firstDone)
			} else {
				close(secondDone)
			}
			return nil
		})
	}()
	defer func() {
		require.NoError(t, consumer.Close())
		select {
		case err := <-startDone:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("consumer did not stop")
		}
	}()
	select {
	case <-firstDone:
	case err := <-startDone:
		t.Fatalf("consumer stopped before first commit: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("first transactional message was not processed")
	}
	require.NoError(t, publishSagaIntegrationInput(client, topic, "Created", "second", 2))
	select {
	case <-secondDone:
	case err := <-startDone:
		t.Fatalf("consumer failed after reconnect: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("consumer did not process second message after reconnect")
	}
	assertSagaIntegrationOffset(t, client, topic, group, 2)
	mu.Lock()
	require.Equal(t, map[uint64]int{0: 1, 1: 1}, counts)
	mu.Unlock()
}
