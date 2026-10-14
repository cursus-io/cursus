package disk

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestContentGenerationAppendOverlapAndABA(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	g, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	// Allocation is not evidence that a record has reached the physical log.
	atomic.StoreUint64(&d.AbsoluteOffset, 20)
	require.NoError(t, d.WriteDirect("orders", 1, types.Message{Offset: 0, Payload: "first"}))
	require.NoError(t, d.WriteBatch([]types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 1}}))
	require.NoError(t, d.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 2}}))
	require.NoError(t, d.CheckContentGeneration(g))
	// An overlap can regress FlushedOffset. The written extent must not regress.
	require.NoError(t, d.WriteBatch([]types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 0}}))
	require.ErrorIs(t, d.CheckContentGeneration(g), ErrStaleContentGeneration)
	g, err = d.CaptureContentGeneration()
	require.NoError(t, err)
	require.NoError(t, d.WriteDirect("orders", 1, types.Message{Offset: 2}))
	require.ErrorIs(t, d.CheckContentGeneration(g), ErrStaleContentGeneration)
}

func TestContentGenerationConditionalTruncate(t *testing.T) {
	d, cfg := bootstrapTestHandler(t, 1)
	for i := 0; i < 3; i++ {
		_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "record"})
		require.NoError(t, err)
	}
	g, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	require.ErrorIs(t, d.TruncateToIfGeneration(3, ContentGeneration{}), ErrStaleContentGeneration)
	require.Error(t, d.TruncateToIfGeneration(4, g))
	require.NoError(t, d.CheckContentGeneration(g))
	require.Equal(t, uint64(3), d.GetFlushedOffset())
	require.NoError(t, d.TruncateToIfGeneration(1, g))
	require.ErrorIs(t, d.CheckContentGeneration(g), ErrStaleContentGeneration)
	called := false
	require.ErrorIs(t, d.PublishIfContentGeneration(g, func() { called = true }), ErrStaleContentGeneration)
	require.False(t, called)
	for i := 0; i < 2; i++ {
		_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "replacement"})
		require.NoError(t, err)
	}
	require.ErrorIs(t, d.CheckContentGeneration(g), ErrStaleContentGeneration)
	g, err = d.CaptureContentGeneration()
	require.NoError(t, err)
	require.NoError(t, d.Close())
	reopened, err := NewDiskHandler(cfg, "orders", 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	require.ErrorIs(t, reopened.CheckContentGeneration(g), ErrStaleContentGeneration)
}

func TestContentGenerationAvailabilityAndCompactedExtension(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	g, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	require.NoError(t, d.WriteCompactedReplicaRange(0, 8, nil))
	require.NoError(t, d.CheckContentGeneration(g))
	require.NoError(t, d.RollSegmentAt(8))
	require.NoError(t, d.CheckContentGeneration(g))
	_, err = d.AppendMessage("orders", 1, &types.Message{Payload: "async"})
	require.NoError(t, err)
	d.Flush()
	require.NoError(t, d.CheckContentGeneration(g))
	called := false
	d.bootstrapPending.Store(true)
	require.ErrorIs(t, d.PublishIfContentGeneration(g, func() { called = true }), ErrBootstrapPending)
	d.bootstrapPending.Store(false)
	injected := errors.New("terminal failure")
	_ = d.markWriteUnavailable(injected)
	require.ErrorIs(t, d.PublishIfContentGeneration(g, func() { called = true }), injected)
	require.False(t, called)
	_, err = d.CaptureContentGeneration()
	require.ErrorIs(t, err, injected)
}

func TestContentGenerationPublicationExcludesMutationAndFailure(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "first"})
	require.NoError(t, err)
	g, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	// The callback itself never waits: TryLock proves both mutation and terminal
	// failure publication are excluded for its entire scalar publication window.
	called := false
	require.NoError(t, d.PublishIfContentGeneration(g, func() {
		called = true
		if d.mu.TryLock() {
			d.mu.Unlock()
			t.Error("publication did not exclude prefix mutation")
		}
		if d.writeFailureMu.TryLock() {
			d.writeFailureMu.Unlock()
			t.Error("publication did not exclude terminal failure")
		}
	}))
	require.True(t, called)
	foreign, _ := bootstrapTestHandler(t, 1)
	require.ErrorIs(t, foreign.CheckContentGeneration(g), ErrStaleContentGeneration)
	require.NoError(t, d.Close())
	_, err = d.CaptureContentGeneration()
	require.Error(t, err)
	require.Error(t, d.PublishIfContentGeneration(g, func() { t.Error("closed publication") }))
}

func TestContentGenerationTruncatePartialFailureIsUnavailable(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "record"})
	require.NoError(t, err)
	g, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	// A nonempty directory at the index path forces an error after the active
	// writer has closed, at the first destructive index removal.
	require.NoError(t, d.CloseIndexFiles())
	require.NoError(t, os.Remove(d.GetIndexPath(0)))
	require.NoError(t, os.Mkdir(d.GetIndexPath(0), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(d.GetIndexPath(0), "block"), []byte("x"), 0o600))
	require.Error(t, d.TruncateToIfGeneration(0, g))
	require.NotEqual(t, g, d.contentGenerationLocked())
	_, err = d.CaptureContentGeneration()
	require.ErrorContains(t, err, "unavailable")
	called := false
	require.Error(t, d.PublishIfContentGeneration(g, func() { called = true }))
	require.False(t, called)
}

func TestContentGenerationRetentionCompactionAndBootstrap(t *testing.T) {
	t.Run("retention", func(t *testing.T) {
		d, cfg := bootstrapTestHandler(t, 1)
		_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "record"})
		require.NoError(t, err)
		require.NoError(t, d.RollSegmentAt(1))
		g, err := d.CaptureContentGeneration()
		require.NoError(t, err)
		d.SetRetentionPolicy(0, 1)
		d.EnforceRetention(cfg)
		require.Equal(t, uint64(1), d.GetFirstOffset())
		require.ErrorIs(t, d.CheckContentGeneration(g), ErrStaleContentGeneration)
	})
	t.Run("compaction", func(t *testing.T) {
		d, _ := bootstrapTestHandler(t, 1)
		d.SetCleanupPolicy("compact")
		for i := 0; i < 3; i++ {
			_, err := d.AppendMessageSync("orders", 1, &types.Message{Key: "same", Payload: "record"})
			require.NoError(t, err)
		}
		require.NoError(t, d.RollSegmentAt(3))
		g, err := d.CaptureContentGeneration()
		require.NoError(t, err)
		result, err := d.EnforceCompaction()
		require.NoError(t, err)
		require.Positive(t, result.RecordsRemoved)
		require.ErrorIs(t, d.CheckContentGeneration(g), ErrStaleContentGeneration)
	})
	t.Run("bootstrap", func(t *testing.T) {
		source, _ := bootstrapTestHandler(t, 1)
		_, err := source.AppendMessageSync("orders", 1, &types.Message{Payload: "source"})
		require.NoError(t, err)
		target, _ := bootstrapTestHandler(t, 1)
		g, err := target.CaptureContentGeneration()
		require.NoError(t, err)
		export := bootstrapTestExport(t, source, 0, 1)
		receiver := bootstrapTestReceive(t, target, export)
		require.NoError(t, receiver.Install())
		require.NotEqual(t, g, target.contentGenerationLocked())
		require.ErrorIs(t, target.CheckContentGeneration(g), ErrBootstrapPending)
		require.NoError(t, target.CompleteBootstrap(export.Manifest().TransferID))
		require.ErrorIs(t, target.CheckContentGeneration(g), ErrStaleContentGeneration)
		g, err = target.CaptureContentGeneration()
		require.NoError(t, err)
		_, err = target.AppendMessageSync("orders", 1, &types.Message{Payload: "append"})
		require.NoError(t, err)
		require.NoError(t, target.CheckContentGeneration(g))
	})
}

func TestContentGenerationTruncateValidatesWrittenExtent(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "record"})
	require.NoError(t, err)
	atomic.StoreUint64(&d.AbsoluteOffset, 5)
	g, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	require.Error(t, d.TruncateToIfGeneration(5, g))
	require.NoError(t, d.CheckContentGeneration(g))
	require.Equal(t, uint64(5), d.GetAbsoluteOffset())
	require.NoError(t, d.TruncateToIfGeneration(1, g))
	require.Equal(t, uint64(1), d.GetAbsoluteOffset())
	require.Equal(t, uint64(1), d.GetFlushedOffset())
	g, err = d.CaptureContentGeneration()
	require.NoError(t, err)
	_, err = d.AppendMessageSync("orders", 1, &types.Message{Payload: "append"})
	require.NoError(t, err)
	require.NoError(t, d.CheckContentGeneration(g))
}

func TestContentGenerationDestructivePartialFailures(t *testing.T) {
	t.Run("compaction", func(t *testing.T) {
		d, _ := bootstrapTestHandler(t, 1)
		d.SetCleanupPolicy("compact")
		for i := 0; i < 3; i++ {
			_, err := d.AppendMessageSync("orders", 1, &types.Message{Key: "same", Payload: "record"})
			require.NoError(t, err)
		}
		require.NoError(t, d.RollSegmentAt(3))
		g, err := d.CaptureContentGeneration()
		require.NoError(t, err)
		require.NoError(t, os.Remove(d.GetIndexPath(0)))
		require.NoError(t, os.Mkdir(d.GetIndexPath(0), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(d.GetIndexPath(0), "block"), []byte("x"), 0o600))
		_, err = d.EnforceCompaction()
		require.Error(t, err)
		require.NotEqual(t, g, d.contentGenerationLocked())
		_, err = d.CaptureContentGeneration()
		require.ErrorContains(t, err, "unavailable")
	})
	t.Run("bootstrap", func(t *testing.T) {
		source, _ := bootstrapTestHandler(t, 1)
		require.NoError(t, source.WriteCompactedReplicaRange(0, 4, nil))
		target, _ := bootstrapTestHandler(t, 1)
		g, err := target.CaptureContentGeneration()
		require.NoError(t, err)
		receiver := bootstrapTestReceive(t, target, bootstrapTestExport(t, source, 0, 4))
		bootstrapAfterReplace = func(string) error { return errors.New("injected replacement crash") }
		defer func() { bootstrapAfterReplace = nil }()
		require.ErrorContains(t, receiver.Install(), "injected replacement crash")
		require.NotEqual(t, g, target.contentGenerationLocked())
		_, err = target.CaptureContentGeneration()
		require.ErrorIs(t, err, ErrBootstrapPending)
		require.ErrorIs(t, target.PublishIfContentGeneration(g, func() { t.Error("pending publication") }), ErrBootstrapPending)
	})
}

func TestContentGenerationTerminalFailureWinsWaitingPublication(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	g, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	d.mu.Lock()
	published := make(chan error, 1)
	go func() {
		published <- d.PublishIfContentGeneration(g, func() { t.Error("published after terminal failure") })
	}()
	injected := errors.New("concurrent terminal failure")
	failed := make(chan struct{})
	go func() {
		_ = d.markWriteUnavailable(injected)
		close(failed)
	}()
	<-failed
	d.mu.Unlock()
	require.ErrorIs(t, <-published, injected)
}

func TestContentGenerationRotationBehindWrittenTail(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, d.WriteBatchSync([]types.DiskMessage{
		{Topic: "orders", Partition: 1, Offset: 0},
		{Topic: "orders", Partition: 1, Offset: 1},
		{Topic: "orders", Partition: 1, Offset: 2},
	}))
	// Legacy overlapping writes regress FlushedOffset; the timer uses that
	// value for rotation. This is not an append-only segment roll.
	require.NoError(t, d.WriteDirect("orders", 1, types.Message{Offset: 0}))
	g, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	d.mu.Lock()
	d.ioMu.Lock()
	err = d.rotateSegment(atomic.LoadUint64(&d.FlushedOffset))
	d.ioMu.Unlock()
	d.mu.Unlock()
	require.NoError(t, err)
	require.ErrorIs(t, d.CheckContentGeneration(g), ErrStaleContentGeneration)
}

func TestContentGenerationTruncateDrainsQueuedAutomaticAppend(t *testing.T) {
	for _, synchronous := range []bool{false, true} {
		name := "async"
		if synchronous {
			name = "sync"
		}
		t.Run(name, func(t *testing.T) {
			initial, cfg := bootstrapTestHandler(t, 1)
			require.NoError(t, initial.Close())
			// Only explicit flush requests may write during this interleaving.
			cfg.LingerMS = int(time.Hour / time.Millisecond)
			cfg.DiskFlushIntervalMS = int(time.Hour / time.Millisecond)
			cfg.DiskFlushBatchSize = 1000
			d, err := NewDiskHandler(cfg, "orders", 1)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, d.Close()) })
			require.NoError(t, d.WriteBatchSync([]types.DiskMessage{
				{Topic: "orders", Partition: 1, Offset: 0},
				{Topic: "orders", Partition: 1, Offset: 1},
				{Topic: "orders", Partition: 1, Offset: 2},
			}))
			generation, err := d.CaptureContentGeneration()
			require.NoError(t, err)
			firstFlush := make(chan struct{})
			appendFlush := make(chan struct{})
			releaseAppendFlush := make(chan struct{})
			var flushCount atomic.Int32
			d.ioMu.Lock()
			d.syncFileFn = func(f *os.File) error {
				if err := f.Sync(); err != nil {
					return err
				}
				switch flushCount.Add(1) {
				case 1:
					close(firstFlush)
				case 2:
					if synchronous {
						close(appendFlush)
						<-releaseAppendFlush
					}
				}
				return nil
			}
			d.ioMu.Unlock()
			d.maintenanceMu.Lock()
			truncated := make(chan error, 1)
			go func() { truncated <- d.TruncateToIfGeneration(1, generation) }()
			<-firstFlush // initial drain is already complete; maintenance is blocked
			appended := make(chan error, 1)
			if synchronous {
				go func() {
					_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "queued"})
					appended <- err
				}()
				<-appendFlush // append owns appendMu until its flush returns
			} else {
				offset, err := d.AppendMessage("orders", 1, &types.Message{Payload: "queued"})
				require.NoError(t, err)
				require.Equal(t, uint64(3), offset)
			}
			d.maintenanceMu.Unlock()
			if synchronous {
				close(releaseAppendFlush)
				require.NoError(t, <-appended)
			}
			require.NoError(t, <-truncated)
			postTruncate, err := d.CaptureContentGeneration()
			require.NoError(t, err)
			d.Flush()
			require.Equal(t, uint64(1), d.GetFlushedOffset(), "old allocated writes must not arrive after truncate")
			require.Equal(t, uint64(1), d.GetAbsoluteOffset())
			require.NoError(t, d.CheckContentGeneration(postTruncate))
			page, err := d.ReadRetainedPage(context.Background(), 0, 1, 10, 1<<20)
			require.NoError(t, err)
			require.Len(t, page.Messages, 1)
			offset, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "after truncate"})
			require.NoError(t, err)
			require.Equal(t, uint64(1), offset)
			require.NoError(t, d.CheckContentGeneration(postTruncate))
		})
	}
}
