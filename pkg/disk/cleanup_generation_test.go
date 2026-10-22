package disk

import (
	"os"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCleanupGenerationSurvivesRestartAndFencesRewrite(t *testing.T) {
	cfg := compactionTestConfig(t)
	d, err := NewDiskHandler(cfg, "metadata", 0)
	require.NoError(t, err)
	_, err = d.AppendMessageSync("metadata", 0, &types.Message{Payload: "a"})
	require.NoError(t, err)
	first, err := d.CleanupGeneration()
	require.NoError(t, err)
	_, err = d.AppendMessageSync("metadata", 0, &types.Message{Payload: "b"})
	require.NoError(t, err)
	second, err := d.CleanupGeneration()
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.NoError(t, d.Close())
	d, err = NewDiskHandler(cfg, "metadata", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()
	second, err = d.CleanupGeneration()
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.NoError(t, d.TruncateTo(1))
	third, err := d.CleanupGeneration()
	require.NoError(t, err)
	require.NotEqual(t, first, third)
}

func TestCleanupGenerationFailurePreventsTruncation(t *testing.T) {
	d, err := NewDiskHandler(compactionTestConfig(t), "metadata", 0)
	require.NoError(t, err)
	defer func() { _ = d.Close() }()
	for range 2 {
		_, err = d.AppendMessageSync("metadata", 0, &types.Message{Payload: "x"})
		require.NoError(t, err)
	}
	_, err = d.CleanupGeneration()
	require.NoError(t, err)
	path := d.BaseName + ".cleanup-generation"
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Mkdir(path, 0o700))
	require.Error(t, d.TruncateTo(1))
	info, err := os.Stat(d.GetSegmentPath(0))
	require.NoError(t, err)
	_, next, partial, err := scanSegmentTail(d.GetSegmentPath(0), 0, 0)
	require.NoError(t, err)
	require.False(t, partial)
	require.Equal(t, uint64(2), next)
	require.Positive(t, info.Size())
}
