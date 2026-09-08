package controller

import (
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestExternalPublishCannotWriteConsumerMetadataTopic(t *testing.T) {
	handler := NewCommandHandler(nil, config.DefaultConfig(), nil, nil, nil)
	t.Cleanup(func() { require.NoError(t, handler.Close()) })
	client := NewClientContext("", 0)

	response := handler.HandleCommand(
		"PUBLISH topic="+config.ConsumerOffsetsTopicName+" producerId=external message=forbidden",
		client,
	)
	require.Equal(
		t,
		"ERROR: internal_topic_write_forbidden topic="+config.ConsumerOffsetsTopicName,
		response,
	)
}

func TestExternalBatchCannotWriteConsumerMetadataTopic(t *testing.T) {
	handler := NewCommandHandler(nil, config.DefaultConfig(), nil, nil, nil)
	t.Cleanup(func() { require.NoError(t, handler.Close()) })
	client := NewClientContext("", 0)
	data, err := util.EncodeBatchMessages(
		config.ConsumerOffsetsTopicName,
		0,
		"1",
		false,
		[]types.Message{{ProducerID: "external", Payload: "forbidden"}},
	)
	require.NoError(t, err)

	response, err := handler.HandleBatchMessage(data, nil, client)
	require.NoError(t, err)
	require.Equal(
		t,
		"ERROR: internal_topic_write_forbidden topic="+config.ConsumerOffsetsTopicName,
		response,
	)
}

func TestCoordinatorCanWriteConsumerMetadataTopicInternally(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.MinInSyncReplicas = 1
	topicManager := topic.NewTopicManager(cfg, &testMockHandlerProvider{}, nil)
	handler := NewCommandHandler(topicManager, cfg, nil, nil, nil)
	t.Cleanup(func() { require.NoError(t, handler.Close()) })
	require.NoError(t, topicManager.CreateTopic(config.ConsumerOffsetsTopicName, 1, false, false))
	record := coordinator.ConsumerMetadataRecord{
		Version:        coordinator.ConsumerMetadataRecordVersion,
		Type:           coordinator.ConsumerMetadataRecordRegistration,
		Group:          "workers",
		Topic:          "orders",
		PartitionCount: 1,
		Epoch:          1,
		Timestamp:      time.Now(),
	}
	require.NoError(t, handler.writeConsumerOffsetRecord(record))
}
