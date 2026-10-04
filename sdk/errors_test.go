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
	assert.NotNil(t, ErrConsumerRebalancing)
	assert.NotNil(t, ErrTopicNotFound)
	assert.NotNil(t, ErrInvalidPartition)
	assert.NotNil(t, ErrNotLeader)
}

func TestSentinelErrors_Messages(t *testing.T) {
	assert.Equal(t, "producer closed", ErrProducerClosed.Error())
	assert.Equal(t, "consumer closed", ErrConsumerClosed.Error())
	assert.Equal(t, "consumer assignment is rebalancing", ErrConsumerRebalancing.Error())
	assert.Equal(t, "topic not found", ErrTopicNotFound.Error())
	assert.Equal(t, "invalid partition", ErrInvalidPartition.Error())
	assert.Equal(t, "not leader", ErrNotLeader.Error())
}

func TestSentinelErrors_Wrapping(t *testing.T) {
	wrapped := fmt.Errorf("operation failed: %w", ErrProducerClosed)
	assert.True(t, errors.Is(wrapped, ErrProducerClosed))
	assert.False(t, errors.Is(wrapped, ErrConsumerClosed))
}

func TestParseBrokerErrorPreservesClassificationAndQuotedFields(t *testing.T) {
	err, ok := ParseBrokerError(`ERROR: NOT_LEADER leader="[::1]:9000" class=routing retryable=true reason="leadership moved"`)
	assert.True(t, ok)
	assert.Equal(t, ErrorClassRouting, err.Class)
	assert.True(t, err.Retryable)
	assert.Equal(t, "[::1]:9000", err.Fields["leader"])
	assert.Equal(t, "leadership moved", err.Message)
	err, ok = ParseBrokerError("ERROR NOT_LEADER retryable=false")
	assert.True(t, ok)
	assert.False(t, err.Retryable)
	err, ok = ParseBrokerError("ERROR: NOT_LEADER leader=localhost:9000")
	assert.True(t, ok)
	assert.True(t, err.Retryable, "legacy responses use the shared protocol classification")
}
