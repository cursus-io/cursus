package disk_test

import (
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadMessagesBoundedStopsBeforeExceedingDecodedBudget(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushBatchSize = 1
	cfg.SegmentSize = 400
	handler, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, handler.Close()) })

	for _, payload := range []string{strings.Repeat("a", 256), strings.Repeat("b", 256), strings.Repeat("c", 256)} {
		_, err := handler.AppendMessageSync("orders", 0, &types.Message{Payload: payload})
		require.NoError(t, err)
	}

	first, firstBytes, err := handler.ReadMessagesBounded(0, 3, 1, true)
	require.NoError(t, err)
	require.Len(t, first, 1, "one oversized first record must remain consumable")
	assert.Greater(t, firstBytes, 1)

	rejected, rejectedBytes, err := handler.ReadMessagesBounded(0, 3, 1, false)
	require.NoError(t, err)
	assert.Empty(t, rejected, "later wildcard topics must not exceed the remaining request budget")
	assert.Zero(t, rejectedBytes)

	bounded, decodedBytes, err := handler.ReadMessagesBounded(0, 3, firstBytes+1, true)
	require.NoError(t, err)
	require.Len(t, bounded, 1)
	assert.Equal(t, firstBytes, decodedBytes)
	assert.Equal(t, strings.Repeat("a", 256), bounded[0].Payload)

	next, _, err := handler.ReadMessagesBounded(1, 3, firstBytes+1, true)
	require.NoError(t, err)
	require.Len(t, next, 1)
	assert.Equal(t, strings.Repeat("b", 256), next[0].Payload)
}
