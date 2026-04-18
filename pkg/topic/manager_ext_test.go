package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTopicManager_GetTopic(t *testing.T) {
	tm := NewTopicManager(config.DefaultConfig(), nil, nil)

	// Topic doesn't exist
	assert.Nil(t, tm.GetTopic("non-existent"))

	// Add a topic manually for testing
	topicName := "test-topic"
	tm.topics[topicName] = &Topic{Name: topicName}

	assert.NotNil(t, tm.GetTopic(topicName))
	assert.Equal(t, topicName, tm.GetTopic(topicName).Name)
}

func TestTopicManager_GetLastOffset(t *testing.T) {
	hp := new(MockHandlerProvider)
	tm := NewTopicManager(config.DefaultConfig(), hp, nil)
	topicName := "test-topic"

	// Case 1: Topic doesn't exist
	assert.Equal(t, uint64(0), tm.GetLastOffset(topicName, 0))

	// Case 2: Topic exists but invalid partition
	mockHandler := new(MockStorageHandler)
	mockHandler.On("GetLatestOffset").Return(uint64(0))
	hp.On("GetHandler", topicName, 0).Return(mockHandler, nil)

	err := tm.CreateTopic(topicName, 1, false, false)
	assert.NoError(t, err)

	assert.Equal(t, uint64(0), tm.GetLastOffset(topicName, -1))
	assert.Equal(t, uint64(0), tm.GetLastOffset(topicName, 1))

	// Case 3: Valid partition
	mockHandler.On("GetAbsoluteOffset").Return(uint64(123))
	assert.Equal(t, uint64(123), tm.GetLastOffset(topicName, 0))
}

func TestTopicManager_Publish(t *testing.T) {
	hp := new(MockHandlerProvider)
	tm := NewTopicManager(config.DefaultConfig(), hp, nil)
	topicName := "test-topic"

	// Case 1: Topic doesn't exist
	msg := &types.Message{Payload: "hello"}
	err := tm.Publish(topicName, msg)
	assert.Error(t, err)

	// Case 2: Success
	mockHandler := new(MockStorageHandler)
	mockHandler.On("GetLatestOffset").Return(uint64(0))
	hp.On("GetHandler", topicName, 0).Return(mockHandler, nil)

	err = tm.CreateTopic(topicName, 1, false, false)
	assert.NoError(t, err)

	// mock.Anything is used because we don't care about the specific message object for this test
	mockHandler.On("AppendMessage", topicName, 0, mock.Anything).Return(uint64(1), nil)
	err = tm.Publish(topicName, msg)
	assert.NoError(t, err)

	mockHandler.AssertExpectations(t)
}

func TestTopicManager_PublishWithAck(t *testing.T) {
	hp := new(MockHandlerProvider)
	tm := NewTopicManager(config.DefaultConfig(), hp, nil)
	topicName := "test-topic"

	// Case 1: Topic doesn't exist
	msg := &types.Message{Payload: "hello"}
	err := tm.PublishWithAck(topicName, msg)
	assert.Error(t, err)

	// Case 2: Success
	mockHandler := new(MockStorageHandler)
	mockHandler.On("GetLatestOffset").Return(uint64(0))
	hp.On("GetHandler", topicName, 0).Return(mockHandler, nil)

	err = tm.CreateTopic(topicName, 1, false, false)
	assert.NoError(t, err)

	mockHandler.On("AppendMessageSync", topicName, 0, mock.Anything).Return(uint64(1), nil)
	err = tm.PublishWithAck(topicName, msg)
	assert.NoError(t, err)

	mockHandler.AssertExpectations(t)
}

func TestTopicManager_PublishBatch(t *testing.T) {
	hp := new(MockHandlerProvider)
	tm := NewTopicManager(config.DefaultConfig(), hp, nil)
	topicName := "test-topic"

	msgs := []types.Message{
		{Payload: "m1"},
		{Payload: "m2"},
	}

	// Case 1: Topic doesn't exist
	err := tm.PublishBatchSync(topicName, msgs)
	assert.Error(t, err)
	err = tm.PublishBatchAsync(topicName, msgs)
	assert.Error(t, err)

	// Case 2: Empty batch
	err = tm.PublishBatchSync(topicName, []types.Message{})
	assert.NoError(t, err)

	// Case 3: Success
	mockHandler := new(MockStorageHandler)
	mockHandler.On("GetLatestOffset").Return(uint64(0))
	hp.On("GetHandler", topicName, 0).Return(mockHandler, nil)

	err = tm.CreateTopic(topicName, 1, false, false)
	assert.NoError(t, err)

	// Two messages in Sync batch
	mockHandler.On("AppendMessageSync", topicName, 0, mock.Anything).Return(uint64(1), nil).Twice()
	err = tm.PublishBatchSync(topicName, msgs)
	assert.NoError(t, err)

	// Two messages in Async batch
	mockHandler.On("AppendMessage", topicName, 0, mock.Anything).Return(uint64(1), nil).Twice()
	err = tm.PublishBatchAsync(topicName, msgs)
	assert.NoError(t, err)
}

func TestTopicManager_DeleteAndList(t *testing.T) {
	tm := NewTopicManager(config.DefaultConfig(), nil, nil)

	tm.topics["t1"] = &Topic{Name: "t1"}
	tm.topics["t2"] = &Topic{Name: "t2"}

	topics := tm.ListTopics()
	assert.Len(t, topics, 2)
	assert.Contains(t, topics, "t1")
	assert.Contains(t, topics, "t2")

	assert.True(t, tm.DeleteTopic("t1"))
	assert.False(t, tm.DeleteTopic("non-existent"))
	assert.Len(t, tm.ListTopics(), 1)
}


func TestTopicManager_Flush(t *testing.T) {
	hp := new(MockHandlerProvider)
	tm := NewTopicManager(config.DefaultConfig(), hp, nil)
	topicName := "test-topic"

	mockHandler := new(MockStorageHandler)
	mockHandler.On("GetLatestOffset").Return(uint64(0))
	hp.On("GetHandler", topicName, 0).Return(mockHandler, nil)

	err := tm.CreateTopic(topicName, 1, false, false)
	assert.NoError(t, err)

	mockHandler.On("Flush").Return().Once()
	tm.Flush()

	mockHandler.AssertExpectations(t)
}

func TestTopicManager_EnsureDefaultGroups(t *testing.T) {
	tm := NewTopicManager(config.DefaultConfig(), nil, nil)
	topicName := "test-topic"
	tm.topics[topicName] = &Topic{Name: topicName, consumerGroups: make(map[string]*types.ConsumerGroup)}

	tm.EnsureDefaultGroups()

	topicObj := tm.GetTopic(topicName)
	assert.Contains(t, topicObj.consumerGroups, "default-group")
}
