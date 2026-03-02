package topic

import (
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/cursus-io/cursus/pkg/stream"
)

type MockHandlerProvider struct {
	mock.Mock
}

func (m *MockHandlerProvider) GetHandler(topic string, partitionID int) (types.StorageHandler, error) {
	args := m.Called(topic, partitionID)
	handler := args.Get(0)
	if handler == nil {
		return nil, args.Error(1)
	}
	return handler.(types.StorageHandler), args.Error(1)
}

type MockStorageHandler struct {
	mock.Mock
}

func (m *MockStorageHandler) AppendMessage(topic string, partition int, msg *types.Message) (uint64, error) {
	args := m.Called(topic, partition, msg)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockStorageHandler) AppendMessageSync(topic string, partition int, msg *types.Message) (uint64, error) {
	args := m.Called(topic, partition, msg)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockStorageHandler) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	args := m.Called(offset, max)
	msgs := args.Get(0)
	if msgs == nil {
		return nil, args.Error(1)
	}
	return msgs.([]types.Message), args.Error(1)
}

func (m *MockStorageHandler) GetAbsoluteOffset() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

func (m *MockStorageHandler) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockStorageHandler) GetLatestOffset() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

func (m *MockStorageHandler) GetSegmentPath(baseOffset uint64) string {
	args := m.Called(baseOffset)
	return args.Get(0).(string)
}

func (m *MockStorageHandler) WriteBatch(batch []types.DiskMessage) error {
	args := m.Called(batch)
	return args.Error(0)
}

func (m *MockStorageHandler) Flush() {
	m.Called()
}

type MockStreamManager struct {
	mock.Mock
}

func (m *MockStreamManager) AddStream(key string, streamConn *stream.StreamConnection, readFn func(offset uint64, max int) ([]types.Message, error), commitInterval time.Duration) error {
	args := m.Called(key, streamConn, readFn, commitInterval)
	return args.Error(0)
}

func (m *MockStreamManager) RemoveStream(key string) {
	m.Called(key)
}

func (m *MockStreamManager) GetStreamsForPartition(topic string, partition int) []*stream.StreamConnection {
	args := m.Called(topic, partition)
	return args.Get(0).([]*stream.StreamConnection)
}

func (m *MockStreamManager) StopStream(key string) {
	m.Called(key)
}

func TestNewTopicManager(t *testing.T) {
	cfg := config.DefaultConfig()
	hp := new(MockHandlerProvider)
	sm := new(MockStreamManager)

	tm := NewTopicManager(cfg, hp, sm)

	assert.NotNil(t, tm)
	assert.NotNil(t, tm.topics)
	assert.Equal(t, time.Duration(cfg.CleanupInterval)*time.Second, tm.cleanupInt)
	assert.NotNil(t, tm.stopCh)
	assert.Equal(t, hp, tm.hp)
	assert.Equal(t, cfg, tm.cfg)
	assert.Equal(t, sm, tm.StreamManager)

	tm.Stop()
	select {
	case <-tm.stopCh:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cleanupLoop didn't stop in time")
	}
}

func TestTopicManagerCreateTopic(t *testing.T) {
	cfg := config.DefaultConfig()
	hp := new(MockHandlerProvider)
	sm := new(MockStreamManager)
	tm := NewTopicManager(cfg, hp, sm)

	topicName := "test-topic"
	partitionCount := 3

	for i := 0; i < partitionCount; i++ {
		mockStorageHandler := new(MockStorageHandler)
		mockStorageHandler.On("GetLatestOffset").Return(uint64(0)).Once()
		hp.On("GetHandler", topicName, i).Return(mockStorageHandler, nil).Once()
	}

	tm.CreateTopic(topicName, partitionCount)
	assert.NotNil(t, tm.GetTopic(topicName))
	assert.Equal(t, partitionCount, len(tm.GetTopic(topicName).Partitions))
	hp.AssertExpectations(t)

	tm.CreateTopic(topicName, partitionCount)
	assert.Equal(t, partitionCount, len(tm.GetTopic(topicName).Partitions))
	hp.AssertExpectations(t)

	newPartitionCount := 5
	for i := partitionCount; i < newPartitionCount; i++ {
		mockStorageHandler := new(MockStorageHandler)
		mockStorageHandler.On("GetLatestOffset").Return(uint64(0)).Once()
		hp.On("GetHandler", topicName, i).Return(mockStorageHandler, nil).Once()
	}

	tm.CreateTopic(topicName, newPartitionCount)
	assert.Equal(t, newPartitionCount, len(tm.GetTopic(topicName).Partitions))
	hp.AssertExpectations(t)

	tm.CreateTopic(topicName, 2)
	assert.Equal(t, newPartitionCount, len(tm.GetTopic(topicName).Partitions)) // should still be 5
	hp.AssertExpectations(t)

	errorTopicName := "error-topic"
	hp.On("GetHandler", errorTopicName, 0).Return(nil, assert.AnError).Once()

	tm.CreateTopic(errorTopicName, 1)
	assert.Nil(t, tm.GetTopic(errorTopicName))
	hp.AssertExpectations(t)

	tm.Stop()
}
