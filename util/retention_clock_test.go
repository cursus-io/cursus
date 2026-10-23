package util

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRetentionClockStepBoundary(t *testing.T) {
	start := time.Unix(1700000000, 0)
	for _, delta := range []time.Duration{-time.Hour, -time.Minute - time.Millisecond, -time.Minute, 0, time.Minute, time.Minute + time.Millisecond, time.Hour} {
		require.Equal(t, delta >= -time.Minute && delta <= time.Minute, RetentionClockSafe(start, start.Add(time.Hour+delta), time.Hour), delta)
	}
}
