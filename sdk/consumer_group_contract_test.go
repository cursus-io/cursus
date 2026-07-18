package sdk

import (
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type scriptedSDKCommand struct {
	command string
	err     error
}

func startSDKCommandServer(t *testing.T, responses ...string) (string, <-chan scriptedSDKCommand) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	results := make(chan scriptedSDKCommand, len(responses))
	go func() {
		defer close(results)
		for _, response := range responses {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				results <- scriptedSDKCommand{err: acceptErr}
				return
			}

			request, readErr := ReadWithLength(conn)
			if readErr != nil {
				_ = conn.Close()
				results <- scriptedSDKCommand{err: readErr}
				return
			}
			if len(request) < 2 {
				_ = conn.Close()
				results <- scriptedSDKCommand{err: fmt.Errorf("short encoded command: %d bytes", len(request))}
				return
			}

			topicLength := int(binary.BigEndian.Uint16(request[:2]))
			commandStart := 2 + topicLength
			if commandStart > len(request) {
				_ = conn.Close()
				results <- scriptedSDKCommand{err: fmt.Errorf("invalid topic length %d for %d-byte command", topicLength, len(request))}
				return
			}

			results <- scriptedSDKCommand{command: string(request[commandStart:])}
			if response != "" {
				if writeErr := WriteWithLength(conn, []byte(response)); writeErr != nil {
					_ = conn.Close()
					results <- scriptedSDKCommand{err: writeErr}
					return
				}
			}
			_ = conn.Close()
		}
	}()

	return listener.Addr().String(), results
}

func requireSDKCommand(t *testing.T, results <-chan scriptedSDKCommand) string {
	t.Helper()

	select {
	case result, ok := <-results:
		require.True(t, ok, "scripted SDK command server closed early")
		require.NoError(t, result.err)
		return result.command
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for SDK command")
		return ""
	}
}

func newGroupContractConsumer(t *testing.T, addr string) *Consumer {
	t.Helper()

	cfg := NewDefaultConsumerConfig()
	cfg.BrokerAddrs = []string{addr}
	cfg.Topic = "events"
	cfg.GroupID = "workers"
	cfg.ConsumerID = "worker"
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)
	consumer.coordinatorAddr = addr
	t.Cleanup(consumer.mainCancel)
	return consumer
}

func TestConsumerJoinGroupResumesExistingMember(t *testing.T) {
	addr, commands := startSDKCommandServer(
		t,
		"OK generation=7 member=worker-1234 assignments=[0,1] resumed=true",
	)
	consumer := newGroupContractConsumer(t, addr)
	consumer.memberID = "worker-1234"
	consumer.generation = 7

	generation, member, assignments, err := consumer.joinGroup()
	require.NoError(t, err)
	assert.Equal(t, int64(7), generation)
	assert.Equal(t, "worker-1234", member)
	assert.Equal(t, []int{0, 1}, assignments)
	assert.Equal(t, "JOIN_GROUP topic=events group=workers member=worker-1234 generation=7", requireSDKCommand(t, commands))
}

func TestConsumerJoinGroupSynchronizesExistingMemberAfterGenerationMismatch(t *testing.T) {
	addr, commands := startSDKCommandServer(
		t,
		"ERROR: GEN_MISMATCH current=8 requested=7 group=workers member=worker-1234",
		"OK generation=8 member=worker-1234 assignments=[1,2]",
	)
	consumer := newGroupContractConsumer(t, addr)
	consumer.memberID = "worker-1234"
	consumer.generation = 7

	generation, member, assignments, err := consumer.joinGroup()
	require.NoError(t, err)
	assert.Equal(t, int64(8), generation)
	assert.Equal(t, "worker-1234", member)
	assert.Equal(t, []int{1, 2}, assignments)
	assert.Equal(t, "JOIN_GROUP topic=events group=workers member=worker-1234 generation=7", requireSDKCommand(t, commands))
	assert.Equal(t, "SYNC_GROUP topic=events group=workers member=worker-1234 generation=8", requireSDKCommand(t, commands))
}

func TestConsumerJoinGroupCreatesFreshMemberAfterExpiration(t *testing.T) {
	addr, commands := startSDKCommandServer(
		t,
		"ERROR: member_not_found member=worker-1234 group=workers",
		"OK generation=9 member=worker-9876 assignments=[0,1,2]",
	)
	consumer := newGroupContractConsumer(t, addr)
	consumer.memberID = "worker-1234"
	consumer.generation = 7

	generation, member, assignments, err := consumer.joinGroup()
	require.NoError(t, err)
	assert.Equal(t, int64(9), generation)
	assert.Equal(t, "worker-9876", member)
	assert.Equal(t, []int{0, 1, 2}, assignments)
	assert.Equal(t, "JOIN_GROUP topic=events group=workers member=worker-1234 generation=7", requireSDKCommand(t, commands))
	assert.Equal(t, "JOIN_GROUP topic=events group=workers member=worker", requireSDKCommand(t, commands))
}
func TestConsumerHeartbeatIncludesMemberAndGeneration(t *testing.T) {
	addr, commands := startSDKCommandServer(
		t,
		"OK member=worker-1234 generation=7",
	)
	consumer := newGroupContractConsumer(t, addr)
	consumer.memberID = "worker-1234"
	consumer.generation = 7
	consumer.config.HeartbeatIntervalMS = 5

	finished := make(chan struct{})
	go func() {
		consumer.heartbeatLoop()
		close(finished)
	}()

	assert.Equal(t, "HEARTBEAT topic=events group=workers member=worker-1234 generation=7", requireSDKCommand(t, commands))
	close(consumer.doneCh)

	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("heartbeat loop did not stop")
	}
}

func TestConsumerCloseLeavesCurrentGeneration(t *testing.T) {
	addr, commands := startSDKCommandServer(t, "")
	consumer := newGroupContractConsumer(t, addr)
	consumer.memberID = "worker-1234"
	consumer.generation = 7

	require.NoError(t, consumer.Close())
	assert.Equal(t, "LEAVE_GROUP topic=events group=workers member=worker-1234 generation=7", requireSDKCommand(t, commands))
}

func TestConsumerSchedulesRebalanceRetryUntilClosed(t *testing.T) {
	c := newTestConsumer(t)
	c.config.ConnectRetryBackoffMS = 1
	c.scheduleRebalanceRetry()

	select {
	case <-c.rebalanceSig:
	case <-time.After(time.Second):
		t.Fatal("rebalance retry was not scheduled")
	}

	require.NoError(t, c.Close())
	c.scheduleRebalanceRetry()
	select {
	case <-c.rebalanceSig:
		t.Fatal("closed consumer scheduled another rebalance")
	case <-time.After(200 * time.Millisecond):
	}
}
