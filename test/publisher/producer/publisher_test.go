package producer

import (
	"math"
	"testing"

	"github.com/cursus-io/cursus/test/publisher/config"
	"github.com/stretchr/testify/require"
)

func TestNewPublisherRejectsUnsafeConfigurationBeforeConnecting(t *testing.T) {
	_, err := NewPublisher(nil)
	require.ErrorContains(t, err, "must not be nil")

	_, err = NewPublisher(&config.PublisherConfig{})
	require.ErrorContains(t, err, "partitions")

	_, err = NewPublisher(&config.PublisherConfig{Partitions: 1})
	require.ErrorContains(t, err, "batch size")

	_, err = NewPublisher(&config.PublisherConfig{Partitions: 1, BatchSize: 1})
	require.ErrorContains(t, err, "broker address")
}

func TestCounterAsIntSaturatesInsteadOfWrapping(t *testing.T) {
	require.Equal(t, 7, counterAsInt(7))
	require.Equal(t, math.MaxInt, counterAsInt(math.MaxUint64))
}
