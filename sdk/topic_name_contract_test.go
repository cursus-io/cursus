package sdk

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSDKTopicEntryPointsRejectInvalidNameBeforeNetwork(t *testing.T) {
	publisherConfig := NewDefaultPublisherConfig()
	publisherConfig.Topic = "../escape"
	_, err := NewProducer(publisherConfig)
	require.ErrorContains(t, err, "invalid topic name")

	consumerConfig := NewDefaultConsumerConfig()
	consumerConfig.Topic = "../escape"
	consumer, err := NewConsumer(consumerConfig)
	require.NoError(t, err)
	defer consumer.mainCancel()
	require.ErrorContains(t, consumer.Start(func(Message) error { return nil }), "invalid topic name")

	store := NewEventStore("127.0.0.1:1", "../escape", "producer")
	require.ErrorContains(t, store.CreateTopic(1), "invalid topic name")

	client := &ConsumerClient{}
	_, err = client.ListOffsets("../escape")
	require.ErrorContains(t, err, "invalid topic name")

	err = client.TransactionalPublish("txn", "../escape", 0, Message{
		ProducerID: "producer",
		SeqNum:     1,
	})
	require.ErrorContains(t, err, "invalid topic name")

	_, err = buildSendOffsetsToTransactionCommand(
		"txn",
		"producer",
		"../escape",
		"group",
		"member",
		1,
		1,
		map[int]uint64{0: 1},
	)
	require.ErrorContains(t, err, "invalid topic name")
}
