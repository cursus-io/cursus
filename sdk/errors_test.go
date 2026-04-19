package sdk

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSentinelErrors_Defined(t *testing.T) {
	assert.NotNil(t, ErrProducerClosed)
	assert.NotNil(t, ErrConsumerClosed)
	assert.NotNil(t, ErrTopicNotFound)
	assert.NotNil(t, ErrInvalidPartition)
	assert.NotNil(t, ErrNotLeader)
}

func TestSentinelErrors_Messages(t *testing.T) {
	assert.Equal(t, "producer closed", ErrProducerClosed.Error())
	assert.Equal(t, "consumer closed", ErrConsumerClosed.Error())
	assert.Equal(t, "topic not found", ErrTopicNotFound.Error())
	assert.Equal(t, "invalid partition", ErrInvalidPartition.Error())
	assert.Equal(t, "not leader", ErrNotLeader.Error())
}

func TestSentinelErrors_Wrapping(t *testing.T) {
	wrapped := fmt.Errorf("operation failed: %w", ErrProducerClosed)
	assert.True(t, errors.Is(wrapped, ErrProducerClosed))
	assert.False(t, errors.Is(wrapped, ErrConsumerClosed))
}
