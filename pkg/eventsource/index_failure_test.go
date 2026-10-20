package eventsource

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestIndexProgressPublicationRejectsClosedOrReplacedCache(t *testing.T) {
	idx, err := NewStreamIndex(t.TempDir(), 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = idx.Close() })
	generation := disk.ContentGeneration{}
	end := uint64(1)
	closed := &Handler{closed: true}
	require.ErrorContains(t, closed.publishIndexProgress("orders:0", idx, &end, &generation), "closed")
	require.ErrorContains(t, closed.publishIndexProgress("orders:0", idx, nil, nil), "closed")
	replaced := &Handler{indexes: map[string]*StreamIndex{"orders:0": {}}, indexedHWM: map[string]uint64{"orders:0": 9}}
	require.ErrorContains(t, replaced.publishIndexProgress("orders:0", idx, &end, &generation), "stream_index_replaced")
	require.ErrorContains(t, replaced.publishIndexProgress("orders:0", idx, nil, nil), "stream_index_replaced")
	require.Equal(t, uint64(9), replaced.indexedHWM["orders:0"])
	require.Nil(t, replaced.replicaIndexGenerations)
	failed := &Handler{indexes: map[string]*StreamIndex{"orders:0": idx}, indexedHWM: map[string]uint64{"orders:0": 0}}
	idx.Invalidate(errors.New("append failed before retirement"))
	require.ErrorIs(t, failed.publishIndexProgress("orders:0", idx, &end, &generation), ErrStreamIndexInvalid)
	require.Zero(t, failed.indexedHWM["orders:0"])
	require.Nil(t, failed.replicaIndexGenerations)
}

func TestIndexCommittedNoOpChecksIndexWithoutRegressingTail(t *testing.T) {
	h := newTestHandler(t)
	t.Cleanup(func() { _ = h.Close(); h.tm.Stop() })
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=a version=1 message=one"), "OK")
	require.NoError(t, h.IndexCommittedToHWM("orders", 0, 0))
	require.Equal(t, uint64(1), h.indexedHWM["orders:0"], "an earlier requested cut must not regress the indexed tail")
	idx := h.indexes["orders:0"]
	idx.Invalidate(errors.New("append failed before outer retirement"))
	require.ErrorIs(t, h.IndexCommittedToHWM("orders", 0, 1), ErrStreamIndexInvalid, "an unchanged tail is not evidence that the cached index remains valid")
}

func assertIndexUnusable(t *testing.T, idx *StreamIndex) {
	t.Helper()
	_, err := idx.CheckedVersion("a")
	require.ErrorIs(t, err, ErrStreamIndexInvalid)
	_, err = idx.CheckedVersion("missing")
	require.ErrorIs(t, err, ErrStreamIndexInvalid)
	_, err = idx.Lookup("a", 1)
	require.ErrorIs(t, err, ErrStreamIndexInvalid)
	_, err = idx.LookupRange("a", 0, 0, 0)
	require.ErrorIs(t, err, ErrStreamIndexInvalid)
	require.ErrorIs(t, idx.Append("a", 2, 1, 0), ErrStreamIndexInvalid)
	require.ErrorIs(t, idx.SeedVersion("a", 1), ErrStreamIndexInvalid)
	_, _, err = idx.CheckAndAppend("a", 2, 1, 0)
	require.ErrorIs(t, err, ErrStreamIndexInvalid)
	called := false
	_, _, err = idx.CheckEnqueueAndAppend("a", 2, func() (uint64, error) { called = true; return 1, nil })
	require.ErrorIs(t, err, ErrStreamIndexInvalid)
	require.False(t, called, "an escaped failed index must reject before writing the authoritative log")
	require.ErrorIs(t, idx.Reset(), ErrStreamIndexInvalid)
	require.ErrorIs(t, idx.Sync(), ErrStreamIndexInvalid)
}

func TestStreamIndexResetFailureIsTerminal(t *testing.T) {
	for _, afterTruncate := range []bool{false, true} {
		t.Run(fmt.Sprintf("after_truncate_%t", afterTruncate), func(t *testing.T) {
			idx, err := NewStreamIndex(t.TempDir(), 0)
			require.NoError(t, err)
			t.Cleanup(func() { _ = idx.Close() })
			require.NoError(t, idx.Append("a", 1, 0, 0))
			before, err := idx.ResetRevision()
			require.NoError(t, err)
			if afterTruncate {
				require.NoError(t, idx.sidecarFile.Close())
			} else {
				require.NoError(t, idx.indexFile.Close())
			}
			require.Error(t, idx.Reset())
			revision, err := idx.ResetRevision()
			require.ErrorIs(t, err, ErrStreamIndexInvalid)
			require.Equal(t, before+1, revision, "reset revision advances before its first fallible operation")
			if afterTruncate {
				info, statErr := idx.indexFile.Stat()
				require.NoError(t, statErr)
				require.Zero(t, info.Size())
			}
			assertIndexUnusable(t, idx)
		})
	}
}

func TestStreamIndexWriteAndSyncFailuresAreTerminal(t *testing.T) {
	for _, op := range []string{"index", "sidecar", "sync", "closed", "revision_overflow", "enqueue"} {
		t.Run(op, func(t *testing.T) {
			idx, err := NewStreamIndex(t.TempDir(), 0)
			require.NoError(t, err)
			t.Cleanup(func() { _ = idx.Close() })
			require.NoError(t, idx.Append("a", 1, 0, 0))
			switch op {
			case "index":
				require.NoError(t, idx.indexFile.Close())
				err = idx.Append("a", 2, 1, 0)
			case "sidecar":
				require.NoError(t, idx.sidecarFile.Close())
				err = idx.Append("b", 1, 1, 0)
			case "sync":
				require.NoError(t, idx.sidecarFile.Close())
				err = idx.Sync()
			case "closed":
				require.NoError(t, idx.Close())
				err = idx.Append("a", 2, 1, 0)
			case "revision_overflow":
				idx.resetRevision = ^uint64(0)
				err = idx.Reset()
			case "enqueue":
				_, _, err = idx.CheckEnqueueAndAppend("a", 2, func() (uint64, error) { return 0, errors.New("commit outcome unknown") })
			}
			require.Error(t, err)
			assertIndexUnusable(t, idx)
		})
	}
}

func TestHandlerAppendFailureDoesNotRecoverOverReplacement(t *testing.T) {
	h := newTestHandler(t)
	t.Cleanup(func() { _ = h.Close(); h.tm.Stop() })
	old, err := h.getIndex("orders", 0)
	require.NoError(t, err)
	fresh, err := NewStreamIndex(t.TempDir(), 0)
	require.NoError(t, err)
	require.NoError(t, fresh.Append("a", 7, 6, 0))
	p, err := h.tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	release := p.BeginReplicationMutation()
	_, response := h.AppendStreamWithControllerFence("APPEND_STREAM topic=orders key=a version=1 message=one", AppendOptions{AfterAppend: func(string, int, types.Message) error {
		h.mu.Lock()
		h.indexes["orders:0"] = fresh
		h.indexedHWM["orders:0"] = 7
		h.mu.Unlock()
		return errors.New("replication failed after cache replacement")
	}})
	release()
	require.Contains(t, response, "stream_index_replaced retryable=true")
	require.Same(t, fresh, h.indexes["orders:0"])
	require.Equal(t, uint64(7), h.indexedHWM["orders:0"])
	version, err := fresh.CheckedVersion("a")
	require.NoError(t, err)
	require.Equal(t, uint64(7), version)
	assertIndexUnusable(t, old)
}

func TestHandlerFencedAppendRetiresIndexWithoutErasingHistory(t *testing.T) {
	h := newTestHandler(t)
	t.Cleanup(func() { _ = h.Close(); h.tm.Stop() })
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=history version=1 message=old"), "OK")
	idx, err := h.getIndex("orders", 0)
	require.NoError(t, err)
	path := idx.indexFile.Name()
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotEmpty(t, before)
	p, err := h.tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	marker := errors.New("commit response lost")
	release := p.BeginReplicationMutation()
	_, response := h.AppendStreamWithControllerFence("APPEND_STREAM topic=orders key=new version=1 message=new", AppendOptions{AfterAppend: func(string, int, types.Message) error {
		p.MarkReconciliationPending(marker)
		return marker
	}})
	release()
	require.Contains(t, response, "append_stream_failed")
	require.NotContains(t, h.indexes, "orders:0")
	assertIndexUnusable(t, idx)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after, "fenced retirement must not truncate history")
	require.NoError(t, p.CompleteReconciliation(p.ReconciliationError(), 2))
	version, _, err := h.CurrentVersion("orders", "history")
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
}

func TestRetireFailedIndexPreservesReplacement(t *testing.T) {
	old, err := NewStreamIndex(t.TempDir(), 0)
	require.NoError(t, err)
	fresh, err := NewStreamIndex(t.TempDir(), 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fresh.Close() })
	require.NoError(t, fresh.Append("a", 7, 6, 0))
	h := &Handler{indexes: map[string]*StreamIndex{"orders:0": fresh}, indexedHWM: map[string]uint64{"orders:0": 7}, replicaIndexGenerations: map[string]disk.ContentGeneration{"orders:0": {}}}
	require.False(t, h.retireIndex("orders:0", old, errors.New("old replay failed")))
	require.Same(t, fresh, h.indexes["orders:0"])
	require.Equal(t, uint64(7), h.indexedHWM["orders:0"])
	require.Contains(t, h.replicaIndexGenerations, "orders:0")
	version, err := fresh.CheckedVersion("a")
	require.NoError(t, err)
	require.Equal(t, uint64(7), version)
	assertIndexUnusable(t, old)
}

func TestHandlerFailedRecoveryRequiresColdLogProof(t *testing.T) {
	h, hp := newTestHandlerWithProvider(t)
	t.Cleanup(func() { _ = h.Close(); h.tm.Stop() })
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=a version=1 message=one"), "OK")
	idx, err := h.getIndex("orders", 0)
	require.NoError(t, err)
	storage := hp.handlers["orders:0"]
	storage.readErr = errors.New("durable record unreadable")
	require.Error(t, h.RecoverIndexFromLog("orders", 0, idx))
	assertIndexUnusable(t, idx)
	require.NotContains(t, h.indexes, "orders:0")
	require.NotContains(t, h.indexedHWM, "orders:0")
	before := storage.offset
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=a version=1 message=bad"), "durable record unreadable")
	require.Equal(t, before, storage.offset)
	storage.readErr = nil
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=a version=1 message=bad"), "version_conflict current=1")
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=a version=2 message=two"), "OK")
	require.NotSame(t, idx, h.indexes["orders:0"])
}

func TestHandlerAppendWriteFailureRecoversCommittedVersionWithFreshIndex(t *testing.T) {
	h := newTestHandler(t)
	t.Cleanup(func() { _ = h.Close(); h.tm.Stop() })
	idx, err := h.getIndex("orders", 0)
	require.NoError(t, err)
	p, err := h.tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	release := p.BeginReplicationMutation()
	result, response := h.AppendStreamWithControllerFence("APPEND_STREAM topic=orders key=a version=1 message=one", AppendOptions{AfterAppend: func(string, int, types.Message) error {
		return idx.indexFile.Close()
	}})
	release()
	require.Empty(t, response)
	require.Equal(t, uint64(1), result.Version)
	require.NotSame(t, idx, h.indexes["orders:0"])
	assertIndexUnusable(t, idx)
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=a version=2 message=two"), "OK")
}

func TestHandlerDetachClosesOutsideCacheLock(t *testing.T) {
	for _, operation := range []string{"bootstrap", "delete", "close"} {
		t.Run(operation, func(t *testing.T) {
			h := newTestHandler(t)
			t.Cleanup(func() { _ = h.Close(); h.tm.Stop() })
			idx, err := h.getIndex("orders", 0)
			require.NoError(t, err)
			idx.mu.Lock()
			locked := true
			defer func() {
				if locked {
					idx.mu.Unlock()
				}
			}()
			done := make(chan error, 1)
			go func() {
				switch operation {
				case "bootstrap":
					done <- h.DetachPartitionForBootstrap("orders", 0)
				case "delete":
					done <- h.DeleteTopic("orders")
				case "close":
					done <- h.Close()
				}
			}()
			deadline := time.After(5 * time.Second)
			for {
				if h.mu.TryRLock() {
					detached := h.indexes["orders:0"] == nil
					h.mu.RUnlock()
					if detached {
						break
					}
				}
				select {
				case <-deadline:
					t.Fatal("cleanup held cache lock while waiting for index close")
				default:
					time.Sleep(time.Millisecond)
				}
			}
			idx.mu.Unlock()
			locked = false
			require.NoError(t, <-done)
			assertIndexUnusable(t, idx)
		})
	}
}
