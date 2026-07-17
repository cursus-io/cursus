package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestAddPartitionsDoesNotExposePartialInitialization(t *testing.T) {
	cfg := config.DefaultConfig()
	provider := new(MockHandlerProvider)
	initial := new(MockStorageHandler)
	initial.On("GetLatestOffset").Return(uint64(0)).Once()
	provider.On("GetHandler", "orders", 0).Return(initial, nil).Once()

	created, err := NewTopic("orders", 1, provider, cfg, nil, false, false)
	assert.NoError(t, err)
	defer created.Partitions[0].Close()

	staged := new(MockStorageHandler)
	staged.On("GetLatestOffset").Return(uint64(0)).Once()
	staged.On("Close").Return(nil).Once()
	provider.On("GetHandler", "orders", 1).Return(staged, nil).Once()
	provider.On("GetHandler", "orders", 2).Return(nil, assert.AnError).Once()

	err = created.AddPartitions(2, provider)
	assert.Error(t, err)
	assert.Len(t, created.Partitions, 1)
	provider.AssertExpectations(t)
	staged.AssertExpectations(t)
}
