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
