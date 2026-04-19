package sdk

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConsumerClient_BasicConstruction(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	client, _ := NewConsumerClient(cfg)

	if client == nil {
		t.Fatal("expected non-nil ConsumerClient")
	}
	if client.ID == "" {
		t.Error("expected non-empty ID")
	}
	if client.config != cfg {
		t.Error("expected config to be stored")
	}

	// Leader should be initialized with empty addr
	info := client.leader.Load()
	if info == nil {
		t.Fatal("expected non-nil leader info")
	}
	if info.addr != "" {
		t.Errorf("expected empty leader addr, got %q", info.addr)
	}
}

func TestNewConsumerClient_UniqueIDs(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	c1, _ := NewConsumerClient(cfg)
	c2, _ := NewConsumerClient(cfg)

	if c1.ID == c2.ID {
		t.Error("expected different IDs for different clients")
	}
}

func TestConsumerClient_UpdateLeader(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	client, _ := NewConsumerClient(cfg)

	client.UpdateLeader("broker-1:9000")
	info := client.leader.Load()
	assert.Equal(t, "broker-1:9000", info.addr)
	assert.False(t, info.updated.IsZero())
}

func TestConsumerClient_UpdateLeader_SameAddrNoOp(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	client, _ := NewConsumerClient(cfg)

	client.UpdateLeader("broker-1:9000")
	firstUpdate := client.leader.Load().updated

	time.Sleep(1 * time.Millisecond)
	client.UpdateLeader("broker-1:9000")
	secondUpdate := client.leader.Load().updated

	assert.Equal(t, firstUpdate, secondUpdate)
}

func TestConsumerClient_UpdateLeader_DifferentAddrUpdates(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	client, _ := NewConsumerClient(cfg)

	client.UpdateLeader("broker-1:9000")
	first := client.leader.Load()

	time.Sleep(1 * time.Millisecond)
	client.UpdateLeader("broker-2:9000")
	second := client.leader.Load()

	assert.NotEqual(t, first.addr, second.addr)
	assert.Equal(t, "broker-2:9000", second.addr)
}

func TestConsumerClient_ConnectWithFailover_NoBrokers(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	cfg.BrokerAddrs = []string{}
	client, _ := NewConsumerClient(cfg)

	_, _, err := client.ConnectWithFailover()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no broker addresses configured")
}

func TestConsumerClient_LeaderStaleness(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	cfg.LeaderStaleness = 50 * time.Millisecond
	cfg.BrokerAddrs = []string{"unreachable:9999"}
	client, _ := NewConsumerClient(cfg)

	client.leader.Store(&consumerLeaderInfo{
		addr:    "stale-leader:9000",
		updated: time.Now().Add(-100 * time.Millisecond),
	})

	_, _, err := client.ConnectWithFailover()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "all brokers unreachable")
}

func TestConsumer_HandleLeaderRedirection_WithLeaderIs(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)

	consumer.handleLeaderRedirection("ERROR NOT_LEADER LEADER_IS broker-3:9000")

	info := consumer.client.leader.Load()
	assert.Equal(t, "broker-3:9000", info.addr)
}

func TestConsumer_HandleLeaderRedirection_NoLeaderIs(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)

	consumer.handleLeaderRedirection("ERROR some other error")

	info := consumer.client.leader.Load()
	assert.Equal(t, "", info.addr)
}

func TestConsumer_HandleLeaderRedirection_LeaderIsAtEnd(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)

	consumer.handleLeaderRedirection("LEADER_IS")

	info := consumer.client.leader.Load()
	assert.Equal(t, "", info.addr)
}

func TestConsumer_HandleLeaderRedirection_EmptyResponse(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)

	consumer.handleLeaderRedirection("")

	info := consumer.client.leader.Load()
	assert.Equal(t, "", info.addr)
}

func TestNewConsumer_Construction(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	assert.NotNil(t, consumer.config)
	assert.NotNil(t, consumer.client)
	assert.NotNil(t, consumer.partitionConsumers)
	assert.NotNil(t, consumer.offsets)
	assert.NotNil(t, consumer.currentOffsets)
	assert.NotNil(t, consumer.commitRetryMap)
	assert.NotNil(t, consumer.rebalanceSig)
	assert.NotNil(t, consumer.doneCh)
	assert.NotNil(t, consumer.mainCtx)
	assert.NotNil(t, consumer.mainCancel)
	assert.NotNil(t, consumer.commitCh)
}

func TestConsumer_Done(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)

	select {
	case <-consumer.Done():
		t.Fatal("done channel should not be closed yet")
	default:
	}
}

func TestConsumer_OwnsPartition_Empty(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)

	assert.False(t, consumer.ownsPartition(0))
	assert.False(t, consumer.ownsPartition(1))
}

func TestConsumer_OwnsPartition_WithAssignment(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)

	consumer.mu.Lock()
	consumer.partitionConsumers[0] = &PartitionConsumer{
		partitionID: 0,
		consumer:    consumer,
	}
	consumer.mu.Unlock()

	assert.True(t, consumer.ownsPartition(0))
	assert.False(t, consumer.ownsPartition(1))
}

func TestConsumer_OwnsPartition_ClosedPartition(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)

	consumer.mu.Lock()
	consumer.partitionConsumers[0] = &PartitionConsumer{
		partitionID: 0,
		consumer:    consumer,
		closed:      true,
	}
	consumer.mu.Unlock()

	assert.False(t, consumer.ownsPartition(0))
}
