package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPreparedStartupWaitsForISRWithoutChangingDecision(t *testing.T) {
	for _, response := range []string{
		"ERROR: insufficient_in_sync_replicas current=2 required=3",
		"ERROR: insufficient_in_sync_replicas class=availability retryable=true current=2 required=3",
	} {
		t.Run(response, func(t *testing.T) {
			calls := 0
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := awaitPreparedTransactionRecovery(ctx, true, func() error {
				calls++
				if calls == 1 {
					return errors.Join(fmt.Errorf("recover prepared transaction: %w", errors.New(response)), errors.New(response))
				}
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 2, calls)
		})
	}
}

func TestPreparedStartupPreservesFatalAndStandaloneErrors(t *testing.T) {
	isr := errors.New("ERROR: insufficient_in_sync_replicas current=2 required=3")
	for _, tc := range []struct {
		name        string
		distributed bool
		err         error
	}{
		{"standalone", false, isr},
		{"storage", true, errors.New("fsync failed")},
		{"mixed", true, errors.Join(isr, errors.New("fsync failed"))},
		{"fenced", true, errors.New("ERROR: producer_fenced")},
		{"explicit nonretryable", true, errors.New("ERROR: insufficient_in_sync_replicas class=availability retryable=false")},
		{"embedded text", true, errors.New("unknown failure containing ERROR: insufficient_in_sync_replicas")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := awaitPreparedTransactionRecovery(context.Background(), tc.distributed, func() error { calls++; return tc.err })
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, 1, calls)
		})
	}
}

func TestPreparedStartupStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := awaitPreparedTransactionRecovery(ctx, true, func() error {
		calls++
		cancel()
		return errors.New("ERROR: insufficient_in_sync_replicas current=2 required=3")
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
	err = awaitPreparedTransactionRecovery(ctx, true, func() error { calls++; return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}
