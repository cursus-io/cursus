package eventsource

import (
	"context"
	"fmt"

	"github.com/cursus-io/cursus/pkg/topic"
)

// PreparedReplicaSnapshot binds a single immutable candidate to already opened
// cache objects and the original detached partition proof. It grants no reads.
type PreparedReplicaSnapshot struct {
	owner         *Handler
	candidate     SnapshotResult
	index         *certifiedSnapshotIndex
	store         *SnapshotStore
	storeRevision uint64
	record        []byte
	publication   *topic.ReplicaSnapshotPublication
}

// RetirePreparedReplicaSnapshot handles failure of the owning cut's final
// validation after preparation. Call it after leaving the partition cut; only
// the exact prepared cache is retired, even if another index has replaced it.
func (h *Handler) RetirePreparedReplicaSnapshot(prepared *PreparedReplicaSnapshot, cause error) {
	if prepared == nil || prepared.owner != h {
		return
	}
	h.retireIndex(snapshotCacheKey(prepared.candidate.Topic, prepared.candidate.Partition), prepared.index.idx, cause)
}

// PrepareReplicaSnapshot runs only in the actual-witness cut, before the FSM
// publication fence. All replay, file opening and candidate validation happen here.
func (h *Handler) PrepareReplicaSnapshot(ctx context.Context, result SnapshotResult, cut *topic.SnapshotReadCut) (*PreparedReplicaSnapshot, error) {
	h.indexSyncMu.Lock()
	defer h.indexSyncMu.Unlock()
	proof, err := h.certifySnapshotIndex(ctx, result.Topic, result.Partition, cut)
	if err != nil {
		return nil, err
	}
	version, err := proof.idx.VersionBefore(result.Key, cut.LastStableOffset())
	if err != nil {
		return nil, err
	}
	if result.Version > version {
		return nil, fmt.Errorf("snapshot_version_exceeds_stream version=%d current=%d", result.Version, version)
	}
	ss, err := h.getSnapshotWithView(result.Topic, result.Partition, cut.View())
	if err != nil {
		return nil, err
	}
	ss.mu.RLock()
	revision := ss.revision
	err = ss.validateSaveLocked(result.Key, result.Version, result.Payload)
	ss.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	record, err := encodeSnapshotRecord(result.Key, result.Version, result.Payload)
	if err != nil {
		return nil, err
	}
	key := snapshotCacheKey(result.Topic, result.Partition)
	if err = h.validateSnapshotIndex(ctx, key, cut, proof, ss); err != nil {
		h.retireIndex(key, proof.idx, err)
		return nil, err
	}
	publication, err := cut.PrepareReplicaSnapshotPublication(ctx, proof.transactional)
	if err != nil {
		h.retireIndex(key, proof.idx, err)
		return nil, err
	}
	return &PreparedReplicaSnapshot{owner: h, candidate: result, index: proof, store: ss, storeRevision: revision, record: record, publication: publication}, nil
}

// PublishPreparedReplicaSnapshot requires the controller lifecycle/write and
// dedicated FSM snapshot fence. Its only disk work is one bounded store record.
func (h *Handler) PublishPreparedReplicaSnapshot(ctx context.Context, p *topic.Partition, prepared *PreparedReplicaSnapshot, actual *topic.ReplicaCatchupWitness, leader string, epoch int, lifecycle, hwm uint64) error {
	if prepared == nil || prepared.owner != h {
		return fmt.Errorf("prepared replica snapshot owner mismatch")
	}
	candidate := prepared.candidate
	key := snapshotCacheKey(candidate.Topic, candidate.Partition)
	err := p.WithReplicaSnapshotPublication(ctx, prepared.publication, actual, leader, epoch, lifecycle, hwm, func(validate func() error) error {
		h.indexSyncMu.Lock()
		defer h.indexSyncMu.Unlock()
		idx := prepared.index.idx
		idx.mu.RLock()
		defer idx.mu.RUnlock()
		if err := idx.checkLocked(); err != nil {
			return err
		}
		if idx.resetRevision != prepared.index.revision {
			return fmt.Errorf("snapshot index revision changed")
		}
		h.mu.RLock()
		defer h.mu.RUnlock()
		if h.closed || h.indexes[key] != idx || h.snapshots[key] != prepared.store {
			return fmt.Errorf("snapshot cache identity changed")
		}
		return prepared.store.savePreparedAtRevision(candidate.Key, candidate.Version, candidate.Payload, prepared.record, prepared.storeRevision, validate)
	})
	if err != nil {
		h.retireIndex(key, prepared.index.idx, err)
	}
	return err
}
