package disk

import (
	"errors"
	"github.com/cursus-io/cursus/pkg/types"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContentPublicationRejectsWholeSetAndReleasesBusyOwners(t *testing.T) {
	a, _ := bootstrapTestHandler(t, 1)
	b, _ := bootstrapTestHandler(t, 2)
	_, err := b.AppendMessageSync("orders", 2, &types.Message{Payload: "original"})
	require.NoError(t, err)
	ga, err := a.CaptureContentGeneration()
	require.NoError(t, err)
	gb, err := b.CaptureContentGeneration()
	require.NoError(t, err)
	calls := 0
	b.mu.Lock()
	err = TryPublishIfContentGenerations([]ContentGeneration{ga, gb}, func() { calls++ })
	b.mu.Unlock()
	require.ErrorIs(t, err, ErrContentPublicationBusy)
	require.True(t, a.mu.TryLock(), "busy second owner must release the first")
	a.mu.Unlock()
	require.Zero(t, calls)
	require.NoError(t, TryPublishIfContentGenerations([]ContentGeneration{ga, ga, gb}, func() { calls++ }))
	require.Equal(t, 1, calls, "duplicate owner must publish exactly once")
	require.NoError(t, b.TruncateTo(0))
	require.ErrorIs(t, TryPublishIfContentGenerations([]ContentGeneration{ga, gb}, func() { calls++ }), ErrStaleContentGeneration)
	require.Equal(t, 1, calls)
	fresh, err := b.CaptureContentGeneration()
	require.NoError(t, err)
	require.ErrorIs(t, TryPublishIfContentGenerations([]ContentGeneration{fresh, gb}, func() { calls++ }), ErrStaleContentGeneration)
	require.Equal(t, 1, calls)
	require.NoError(t, a.Close())
	require.Error(t, TryPublishIfContentGenerations([]ContentGeneration{ga, fresh}, func() { calls++ }))
	require.Equal(t, 1, calls)
}

func TestContentPublicationOpposingOrdersNeverBlockOrPartiallyPublish(t *testing.T) {
	a, _ := bootstrapTestHandler(t, 1)
	b, _ := bootstrapTestHandler(t, 2)
	ga, err := a.CaptureContentGeneration()
	require.NoError(t, err)
	gb, err := b.CaptureContentGeneration()
	require.NoError(t, err)
	var calls atomic.Int64
	var wg sync.WaitGroup
	failures := make(chan error, 2)
	for _, tokens := range [][]ContentGeneration{{ga, gb}, {gb, ga}} {
		wg.Add(1)
		go func(tokens []ContentGeneration) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				if err := TryPublishIfContentGenerations(tokens, func() { calls.Add(1) }); err != nil && !errors.Is(err, ErrContentPublicationBusy) {
					failures <- err
					return
				}
			}
		}(tokens)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	require.Positive(t, calls.Load())
	require.NoError(t, a.CheckContentGeneration(ga))
	require.NoError(t, b.CheckContentGeneration(gb))
}
