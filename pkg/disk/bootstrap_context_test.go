package disk

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBootstrapCanceledExportRemovesStagingAndReleasesBudget(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "copy me"})
	require.NoError(t, err)
	for _, when := range []string{"before", "after-copy"} {
		t.Run(when, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if when == "before" {
				cancel()
			} else {
				d.ioMu.Lock()
				d.syncFileFn = func(f *os.File) error {
					if filepath.Base(f.Name()) == "records" {
						cancel()
					}
					return f.Sync()
				}
				d.ioMu.Unlock()
				defer func() { d.ioMu.Lock(); d.syncFileFn = nil; d.ioMu.Unlock() }()
			}
			bootstrapBudget.Lock()
			beforeActive, beforeBytes := bootstrapBudget.active, bootstrapBudget.bytes
			bootstrapBudget.Unlock()
			e, err := d.ExportBootstrapContext(ctx, 0, 1, bootstrapTestFence(1, 1), nil)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, e)
			entries, err := os.ReadDir(filepath.Dir(d.BaseName))
			require.NoError(t, err)
			for _, entry := range entries {
				require.False(t, strings.HasPrefix(entry.Name(), "partition_1.bootstrap-"), entry.Name())
			}
			bootstrapBudget.Lock()
			afterActive, afterBytes := bootstrapBudget.active, bootstrapBudget.bytes
			bootstrapBudget.Unlock()
			require.Equal(t, beforeActive, afterActive)
			require.Equal(t, beforeBytes, afterBytes)
		})
	}
}

type bootstrapCancelingReader struct {
	cancel context.CancelFunc
	reads  int
}

func (r *bootstrapCancelingReader) Read(data []byte) (int, error) {
	r.reads++
	r.cancel()
	data[0] = 1
	return 1, nil
}

func TestBootstrapChecksumReaderStopsAtCancellationBoundary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	underlying := &bootstrapCancelingReader{cancel: cancel}
	reader := &bootstrapContextReader{context: ctx, reader: underlying}
	_, err := io.CopyBuffer(io.Discard, reader, make([]byte, BootstrapChunkBytes))
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, underlying.reads)
}

func TestBootstrapOwnedLifetimeHasNoInnerTimer(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	legacy, err := d.ExportBootstrapContext(context.Background(), 0, 0, bootstrapTestFence(1, 0), nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, legacy.Close()) }()
	owned, err := d.ExportBootstrapOwnedContext(context.Background(), 0, 0, bootstrapTestFence(1, 0), nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, owned.Close()) }()
	require.NotNil(t, legacy.transfer.timer)
	require.Nil(t, owned.transfer.timer)
}

func TestBootstrapFullExportWorkCountsWholeIntersectingSegment(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	for i := 0; i < 3; i++ {
		_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "actual bytes"})
		require.NoError(t, err)
	}
	export, err := d.ExportBootstrapOwnedContext(context.Background(), 0, 1, bootstrapTestFence(1, 1), []byte("catalog"))
	require.NoError(t, err)
	defer func() { require.NoError(t, export.Close()) }()
	work := export.FullWork()
	require.EqualValues(t, 3, work.DecodedFrames, "full scanner validates future frames in the same physical segment")
	require.Greater(t, work.DecodedFrameBytes, work.CanonicalWriteBytes)
	require.EqualValues(t, export.Manifest().Files[0].Size, work.CanonicalWriteBytes)
	require.EqualValues(t, 7, work.CatalogWriteBytes)
	require.Equal(t, work.CanonicalWriteBytes+work.CatalogWriteBytes, work.HashReadBytes)
}
