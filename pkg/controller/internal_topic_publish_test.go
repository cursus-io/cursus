package controller

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
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
