package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/stretchr/testify/require"
)

func TestConsumerHistoryPublicationRetriesOnlyUnenteredFence(t *testing.T) {
	calls := 0
	err := retryConsumerHistoryFence(context.Background(), func() (bool, error) {
		calls++
		if calls < 3 {
			return false, fsm.ErrConsumerMetadataFencePending
		}
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, calls)
	for _, entered := range []bool{false, true} {
		calls = 0
		failure := errors.New("storage generation changed")
		err = retryConsumerHistoryFence(context.Background(), func() (bool, error) {
			calls++
			return entered, failure
		})
		require.ErrorIs(t, err, failure)
		require.Equal(t, 1, calls)
	}
	calls = 0
	err = retryConsumerHistoryFence(context.Background(), func() (bool, error) {
		calls++
		return true, fsm.ErrConsumerMetadataFencePending
	})
	require.ErrorIs(t, err, fsm.ErrConsumerMetadataFencePending)
	require.Equal(t, 1, calls, "an entered publication cannot be retried")
	ctx, cancel := context.WithCancel(context.Background())
	calls = 0
	err = retryConsumerHistoryFence(ctx, func() (bool, error) {
		calls++
		cancel()
		return false, fsm.ErrConsumerMetadataFencePending
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}
