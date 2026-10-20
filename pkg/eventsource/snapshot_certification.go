package eventsource

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
)

type snapshotIndexCertificate struct {
	generation            disk.ContentGeneration
	anchor, end, revision uint64
}
type certifiedSnapshotIndex struct {
	idx           *StreamIndex
	revision      uint64
	transactional bool
}

func snapshotCacheKey(name string, partition int) string { return name + ":" + strconv.Itoa(partition) }

// openSnapshotIndex deliberately does not use dynamic legacy replay as proof.
// The caller holds a partition cut, followed by indexSyncMu.
func (h *Handler) openSnapshotIndex(name string, partition int, cut *topic.SnapshotReadCut) (*StreamIndex, error) {
	if err := cut.View().ValidateIdentity(name, partition); err != nil {
		return nil, err
	}
	key := snapshotCacheKey(name, partition)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, fmt.Errorf("handler is closed")
	}
	if idx := h.indexes[key]; idx != nil {
		return idx, nil
	}
	dir := h.tm.GetLogDir(name, partition)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	idx, err := NewStreamIndex(dir, partition)
	if err != nil {
		return nil, err
	}
	h.indexes[key] = idx
	return idx, nil
}

func transactionEvidence(m types.Message) bool {
	return m.TransactionalID != "" || m.TransactionState != "" || m.TransactionMarker != "" || m.ControlBatchType != "" || m.ControlBatchVersion != 0 || m.ControlBatchCoordinatorEpoch != 0 || len(m.ControlBatchKey) != 0 || len(m.ControlBatchValue) != 0
}

func (h *Handler) certifySnapshotIndex(ctx context.Context, name string, partition int, cut *topic.SnapshotReadCut) (proof *certifiedSnapshotIndex, err error) {
	idx, err := h.openSnapshotIndex(name, partition, cut)
	if err != nil {
		return nil, err
	}
	key := snapshotCacheKey(name, partition)
	defer func() {
		if err != nil {
			h.retireIndex(key, idx, err)
		}
	}()
	revision, err := idx.ResetRevision()
	if err != nil {
		return nil, err
	}
	idx.mu.RLock()
	certificate := idx.certificate
	idx.mu.RUnlock()
	warm := certificate != nil && certificate.generation == cut.ContentGeneration() && certificate.anchor == cut.FirstOffset() && certificate.revision == revision && certificate.end <= cut.LastStableOffset()
	start := cut.FirstOffset()
	if warm {
		start = certificate.end
	}
	transactional := cut.LastStableOffset() != cut.CommittedEnd()
	var retainedFirst map[string]uint64
	if cut.FirstOffset() > 0 {
		retainedFirst = make(map[string]uint64)
	}
	// Classify every physical record, even when visibility hides it completely.
	for offset := start; offset < cut.CommittedEnd(); {
		page, readErr := cut.ReadRawPage(ctx, offset, 256, 1<<20, true)
		if readErr != nil {
			return nil, readErr
		}
		for _, m := range page.Messages {
			transactional = transactional || transactionEvidence(m)
			if retainedFirst != nil && m.Key != "" && m.AggregateVersion > 0 {
				if _, ok := retainedFirst[m.Key]; !ok {
					retainedFirst[m.Key] = m.AggregateVersion
				}
			}
		}
		offset = page.EndOffset
	}
	if transactional {
		warm = false
	}
	if !warm {
		if err = h.rebuildSnapshotIndex(ctx, name, partition, idx, cut, transactional, retainedFirst); err != nil {
			return nil, err
		}
	} else {
		for offset := start; offset < cut.LastStableOffset(); {
			page, readErr := cut.ReadRawPage(ctx, offset, 256, 1<<20, true)
			if readErr != nil {
				return nil, readErr
			}
			if err = h.extendCertifiedRaw(idx, page.Messages, offset, page.EndOffset); err != nil {
				return nil, err
			}
			offset = page.EndOffset
		}
	}
	revision, err = idx.ResetRevision()
	if err != nil {
		return nil, err
	}
	proof = &certifiedSnapshotIndex{idx: idx, revision: revision, transactional: transactional}
	if err = h.validateSnapshotIndex(ctx, key, cut, proof, nil); err != nil {
		return nil, err
	}
	idx.mu.Lock()
	if !transactional {
		idx.certificate = &snapshotIndexCertificate{generation: cut.ContentGeneration(), anchor: cut.FirstOffset(), end: cut.CommittedEnd(), revision: revision}
	} else {
		idx.certificate = nil
	}
	idx.mu.Unlock()
	end := cut.LastStableOffset()
	if err = h.publishIndexProgress(key, idx, &end, nil); err != nil {
		return nil, err
	}
	return proof, nil
}

func (h *Handler) rebuildSnapshotIndex(ctx context.Context, name string, partition int, idx *StreamIndex, cut *topic.SnapshotReadCut, transactional bool, retainedFirst map[string]uint64) error {
	ahead, err := idx.resetForSnapshot(cut.LastStableOffset())
	if err != nil {
		return err
	}
	if cut.FirstOffset() > 0 {
		ss, err := h.getSnapshotWithView(name, partition, cut.View())
		if err != nil {
			return err
		}
		// Read only the already validated durable inventory; payloads are not
		// needed for a retained seed and must not be materialized together.
		seedErr := func() error {
			ss.mu.RLock()
			defer ss.mu.RUnlock()
			if len(ss.index) == 0 {
				return fmt.Errorf("retained event stream requires a durable snapshot checkpoint")
			}
			for key, record := range ss.index {
				if err := idx.seedVersionAt(key, record.version, cut.FirstOffset()); err != nil {
					return err
				}
			}
			return nil
		}()
		if seedErr != nil {
			return seedErr
		}
	}
	observed := make(map[string]uint64)
	for offset := cut.FirstOffset(); offset < cut.LastStableOffset(); {
		var messages []types.Message
		var next uint64
		if transactional {
			messages, _, next, err = cut.ReadVisiblePage(ctx, offset, 256, 1<<20, true)
		} else {
			var page disk.RetainedPage
			page, err = cut.ReadRawPage(ctx, offset, 256, 1<<20, true)
			messages, next = page.Messages, page.EndOffset
		}
		if err != nil {
			return err
		}
		for _, m := range messages {
			if m.Key == "" || m.AggregateVersion == 0 {
				continue
			}
			previous, seen := observed[m.Key]
			if !seen {
				idx.mu.RLock()
				previous = idx.seeds[m.Key]
				idx.mu.RUnlock()
			}
			if (seen && m.AggregateVersion != previous+1) || (!seen && m.AggregateVersion > previous+1) {
				return fmt.Errorf("stream index gap key=%s current=%d next=%d", m.Key, previous, m.AggregateVersion)
			}
			if err = idx.Append(m.Key, m.AggregateVersion, m.Offset, 0); err != nil {
				return err
			}
			observed[m.Key] = m.AggregateVersion
		}
		offset = next
	}
	idx.mu.RLock()
	for key, last := range observed {
		if idx.seeds[key] > last {
			err = fmt.Errorf("retained stream disagrees with snapshot checkpoint key=%s", key)
			break
		}
	}
	for key, first := range retainedFirst {
		if seed := idx.seeds[key]; seed >= first && observed[key] < seed {
			err = fmt.Errorf("retained transaction visibility disagrees with snapshot checkpoint key=%s", key)
			break
		}
	}
	idx.mu.RUnlock()
	if err != nil {
		return err
	}
	for _, r := range ahead {
		if err = idx.Append(r.key, r.entry.AggregateVersion, r.entry.Offset, r.entry.Position); err != nil {
			return err
		}
	}
	return nil
}

// Existing suffix entries must equal the exact raw key, version and offset;
// a cached currentVersion never permits skipping an unverified raw record.
func (h *Handler) extendCertifiedRaw(idx *StreamIndex, messages []types.Message, start, end uint64) error {
	existing, err := idx.recordsBetween(start, end)
	if err != nil {
		return err
	}
	at := 0
	for _, m := range messages {
		if m.Key == "" || m.AggregateVersion == 0 {
			continue
		}
		current, err := idx.VersionBefore(m.Key, m.Offset)
		if err != nil {
			return err
		}
		if current == ^uint64(0) || m.AggregateVersion != current+1 {
			return fmt.Errorf("certified stream suffix version mismatch key=%s", m.Key)
		}
		if at < len(existing) && existing[at].entry.Offset <= m.Offset {
			r := existing[at]
			if r.key != m.Key || r.entry.Offset != m.Offset || r.entry.AggregateVersion != m.AggregateVersion {
				return fmt.Errorf("certified stream suffix entry mismatch")
			}
			at++
		} else if err = idx.Append(m.Key, m.AggregateVersion, m.Offset, 0); err != nil {
			return err
		}
	}
	if at != len(existing) {
		return fmt.Errorf("certified stream suffix has unmatched entries")
	}
	return nil
}

func (h *Handler) validateSnapshotIndex(ctx context.Context, key string, cut *topic.SnapshotReadCut, proof *certifiedSnapshotIndex, ss *SnapshotStore) error {
	var err error
	if proof.transactional {
		err = cut.ValidateTransactional(ctx)
	} else {
		err = cut.ValidateRaw(ctx)
	}
	if err != nil {
		return err
	}
	proof.idx.mu.RLock()
	defer proof.idx.mu.RUnlock()
	if err = proof.idx.checkLocked(); err != nil {
		return err
	}
	if proof.idx.resetRevision != proof.revision {
		return fmt.Errorf("snapshot index revision changed")
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed || h.indexes[key] != proof.idx || (ss != nil && h.snapshots[key] != ss) {
		return fmt.Errorf("snapshot cache identity changed")
	}
	return nil
}

func (h *Handler) validateSnapshotCandidate(ctx context.Context, result SnapshotResult, cut *topic.SnapshotReadCut, save bool) (*StreamIndex, error) {
	h.indexSyncMu.Lock()
	defer h.indexSyncMu.Unlock()
	proof, err := h.certifySnapshotIndex(ctx, result.Topic, result.Partition, cut)
	if err != nil {
		return nil, err
	}
	current, err := proof.idx.VersionBefore(result.Key, cut.LastStableOffset())
	if err != nil {
		return nil, err
	}
	if result.Version > current {
		return nil, fmt.Errorf("snapshot_version_exceeds_stream version=%d current=%d", result.Version, current)
	}
	ss, err := h.getSnapshotWithView(result.Topic, result.Partition, cut.View())
	if err != nil {
		return nil, err
	}
	if err = ss.ValidateSave(result.Key, result.Version, result.Payload); err != nil {
		return nil, err
	}
	key := snapshotCacheKey(result.Topic, result.Partition)
	if err = h.validateSnapshotIndex(ctx, key, cut, proof, ss); err != nil {
		h.retireIndex(key, proof.idx, err)
		return nil, err
	}
	if !save {
		return proof.idx, nil
	}
	record, err := encodeSnapshotRecord(result.Key, result.Version, result.Payload)
	if err != nil {
		return nil, err
	}
	ss.mu.RLock()
	revision := ss.revision
	ss.mu.RUnlock()
	err = func() error {
		proof.idx.mu.RLock()
		defer proof.idx.mu.RUnlock()
		h.mu.RLock()
		defer h.mu.RUnlock()
		if err := proof.idx.checkLocked(); err != nil {
			return err
		}
		if h.closed || h.indexes[key] != proof.idx || h.snapshots[key] != ss || proof.idx.resetRevision != proof.revision {
			return fmt.Errorf("snapshot cache identity or revision changed")
		}
		return ss.savePreparedAtRevision(result.Key, result.Version, result.Payload, record, revision, func() error {
			if proof.transactional {
				return cut.ValidateTransactional(ctx)
			}
			return cut.ValidateRaw(ctx)
		})
	}()
	if err != nil {
		h.retireIndex(key, proof.idx, err)
		return nil, err
	}
	return proof.idx, nil
}

// ExportBootstrapWithCatalog keeps the original decision guard through the
// potentially long raw export. Failed exports remain unpublished and are closed.
func (h *Handler) ExportBootstrapWithCatalog(ctx context.Context, name string, partition int, source *topic.BootstrapSource, lifecycle uint64, fence disk.BootstrapFence) (*disk.BootstrapExport, error) {
	captured, err := h.captureBootstrapCatalog(ctx, name, partition, source, lifecycle)
	if err != nil {
		return nil, err
	}
	return captured.export(ctx, source, fence)
}
func (captured *capturedBootstrapCatalog) export(ctx context.Context, source *topic.BootstrapSource, fence disk.BootstrapFence) (*disk.BootstrapExport, error) {
	exported, err := source.Export(ctx, fence, captured.data)
	if err != nil {
		return nil, errors.Join(err, captured.validate(ctx))
	}
	if err = captured.validate(ctx); err != nil {
		return nil, errors.Join(err, exported.Close())
	}
	return exported, nil
}

// withSnapshotCandidate also retires the exact cache used by a successful
// callback if the owning partition's final raw validation then fails.
func (h *Handler) withSnapshotCandidate(ctx context.Context, p *topic.Partition, result SnapshotResult, save bool, before func(*topic.SnapshotReadCut) error) (*StreamIndex, error) {
	var used *StreamIndex
	key := snapshotCacheKey(result.Topic, result.Partition)
	err := p.WithSnapshotCertification(ctx, func(cut *topic.SnapshotReadCut) error {
		if before != nil {
			if err := before(cut); err != nil {
				return err
			}
		}
		var err error
		used, err = h.validateSnapshotCandidate(ctx, result, cut, save)
		return err
	})
	if err != nil && used != nil {
		h.retireIndex(key, used, err)
	}
	return used, err
}
