package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestTopicManagerRuntimeSnapshot(t *testing.T) {
	handler := new(MockStorageHandler)
	handler.On("GetLatestOffset").Return(uint64(4)).Once()
	handler.On("GetFirstOffset").Return(uint64(2)).Once()
	provider := new(MockHandlerProvider)
	provider.On("GetHandler", "orders", 0).Return(handler, nil).Once()

	manager := NewTopicManager(config.DefaultConfig(), provider, nil)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	snapshot := manager.RuntimeSnapshot()

	require.Equal(t, 1, snapshot.TopicCount)
	require.Len(t, snapshot.Partitions, 1)
	require.Equal(t, PartitionRuntimeSnapshot{
		Topic: "orders", Partition: 0, LogStart: 2, LogEnd: 5, HighWatermark: 5,
	}, snapshot.Partitions[0])
}
