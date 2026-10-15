package eventsource

import (
	"strconv"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
)

// IndexReplicaCatchupWithView repairs cached index provenance before receiver
// admission. Unknown or rewritten generations require one reset/replay; appends
// within the same generation extend only the not-yet-indexed stable suffix.
// The caller validates the view's original disk generation after this returns.
func (h *Handler) IndexReplicaCatchupWithView(name string, partition int, end uint64, view *topic.PartitionReadView) error {
	generation, err := view.ReplicaCatchupIndexGeneration(name, partition, end)
	if err != nil {
		return err
	}
	h.indexSyncMu.Lock()
	defer h.indexSyncMu.Unlock()
	key := name + ":" + strconv.Itoa(partition)
	h.mu.RLock()
	existing := h.indexes[key] != nil
	previous, certified := h.replicaIndexGenerations[key]
	h.mu.RUnlock()
	idx, err := h.getIndexWithView(name, partition, view)
	if err != nil {
		return err
	}
	h.mu.Lock()
	start := h.indexedHWM[key]
	// Partial replay must never leave a generation-certified cached index.
	delete(h.replicaIndexGenerations, key)
	h.mu.Unlock()
	stable := view.LastStableOffset()
	if existing && (!certified || previous != generation || start > stable) {
		if err := h.recoverIndexWithView(name, partition, idx, view, stable); err != nil {
			return err
		}
	} else if stable > start {
		if err := h.replayIndex(idx, view, start, stable); err != nil {
			return err
		}
	}
	h.mu.Lock()
	if h.replicaIndexGenerations == nil {
		h.replicaIndexGenerations = make(map[string]disk.ContentGeneration)
	}
	h.indexedHWM[key] = stable
	h.replicaIndexGenerations[key] = generation
	h.mu.Unlock()
	return nil
}
