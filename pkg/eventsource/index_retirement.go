package eventsource

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/disk"
)

// publishIndexProgress admits only the cache object whose replay just finished.
// Lifecycle cleanup may detach it while a read-view replay is in progress.
// A nil end checks a no-op reconciliation without changing its recorded tail.
func (h *Handler) publishIndexProgress(key string, idx *StreamIndex, end *uint64, generation *disk.ContentGeneration) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if err := idx.checkLocked(); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return fmt.Errorf("handler is closed")
	}
	if h.indexes[key] != idx {
		return fmt.Errorf("stream_index_replaced retryable=true")
	}
	if end != nil {
		h.indexedHWM[key] = *end
	}
	if generation != nil {
		if h.replicaIndexGenerations == nil {
			h.replicaIndexGenerations = make(map[string]disk.ContentGeneration)
		}
		h.replicaIndexGenerations[key] = *generation
	}
	return nil
}

// retireIndex invalidates escaped references before detaching the matching cache
// entry. Never hold the handler lock while waiting for an index lock: an append
// can hold the index lock through its replication callback.
func (h *Handler) retireIndex(key string, failed *StreamIndex, cause error) bool {
	failed.Invalidate(cause)
	h.mu.Lock()
	matched := h.indexes[key] == failed
	if matched {
		delete(h.indexes, key)
		delete(h.indexedHWM, key)
		delete(h.replicaIndexGenerations, key)
	}
	h.mu.Unlock()
	_ = failed.Close()
	return matched
}
