package sdk

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func newCatalogContractConsumer(t *testing.T, addr string) *Consumer {
	t.Helper()
	cfg := NewDefaultConsumerConfig()
	cfg.BrokerAddrs = []string{addr}
	cfg.CoordinatorAddr = addr
	cfg.Topic = "orders"
	cfg.GroupID = "workers"
	cfg.ConsumerID = "worker"
	cfg.RequireExistingCatalog = true
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)
	return consumer
}

func TestConsumerExistingCatalogValidationSucceeds(t *testing.T) {
	addr, commands := startSDKCommandServer(
		t,
		"OK leaders=broker-1:9000 cleanup_policy=delete",
		`{"status":"OK","group_name":"workers","topic_name":"orders"}`,
	)
	consumer := newCatalogContractConsumer(t, addr)
	consumer.setCoordinatorCache(addr, coordinatorRouteCache, coordinatorReasonInitial)

	require.NoError(t, consumer.validateExistingCatalog())
	require.Equal(t, "METADATA topic=orders", requireSDKCommand(t, commands))
	require.Equal(t, "GROUP_STATUS group=workers", requireSDKCommand(t, commands))
}

func TestConsumerExistingCatalogRejectsMissingGroupBeforeJoin(t *testing.T) {
	addr, commands := startSDKCommandServer(
		t,
		"OK leaders=broker-1:9000 cleanup_policy=delete",
		`ERROR: group_status_failed class=not_found retryable=false reason="group_not_found"`,
	)
	consumer := newCatalogContractConsumer(t, addr)

	err := consumer.Start(func(Message) error { return nil })
	require.ErrorIs(t, err, ErrConsumerGroupNotFound)
	require.Equal(t, "METADATA topic=orders", requireSDKCommand(t, commands))
	require.Equal(t, "GROUP_STATUS group=workers", requireSDKCommand(t, commands))
	_, ok := <-commands
	require.False(t, ok, "catalog rejection must not send JOIN_GROUP")
}

func TestConsumerExistingCatalogRejectsTopicMismatch(t *testing.T) {
	addr, commands := startSDKCommandServer(
		t,
		"OK leaders=broker-1:9000 cleanup_policy=delete",
		`{"status":"OK","group_name":"workers","topic_name":"payments"}`,
	)
	consumer := newCatalogContractConsumer(t, addr)
	consumer.setCoordinatorCache(addr, coordinatorRouteCache, coordinatorReasonInitial)

	err := consumer.validateExistingCatalog()
	require.ErrorIs(t, err, ErrConsumerCatalogMismatch)
	require.Equal(t, "METADATA topic=orders", requireSDKCommand(t, commands))
	require.Equal(t, "GROUP_STATUS group=workers", requireSDKCommand(t, commands))
}

func TestCatalogStatusContainsTopicSelectors(t *testing.T) {
	require.True(t, catalogStatusContainsTopic(consumerCatalogGroupStatus{TopicName: "orders"}, "orders"))
	require.True(t, catalogStatusContainsTopic(consumerCatalogGroupStatus{Topics: []string{"payments", "orders"}}, "orders"))
	require.True(t, catalogStatusContainsTopic(consumerCatalogGroupStatus{TopicPattern: "orders-*"}, "orders-created"))
	require.False(t, catalogStatusContainsTopic(consumerCatalogGroupStatus{TopicPattern: "payments-*"}, "orders-created"))

	brokerErr := &BrokerError{Code: "group_status_failed", Message: "group_not_found"}
	require.True(t, errors.Is(brokerErr, ErrConsumerGroupNotFound))
}
