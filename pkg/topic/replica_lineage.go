package topic

import (
	"context"
	"errors"
	"github.com/cursus-io/cursus/pkg/disk"
	"time"
)

type replicaLineageStorage interface {
	LineageCandidates(uint64) ([]disk.LineagePrefix, error)
	MatchLineageCandidates([]disk.LineagePrefix, uint64) (*disk.LineagePrefix, error)
	VerifyLineagePrefix(disk.LineagePrefix, uint64) (bool, error)
}

// ReplicaLineageCandidates returns durable cumulative proofs of the retained log.
func (p *Partition) ReplicaLineageCandidates(tail uint64) ([]disk.LineagePrefix, error) {
	storage, ok := p.dh.(replicaLineageStorage)
	if !ok {
		return nil, disk.ErrLineageUnsupported
	}
	return storage.LineageCandidates(tail)
}

// ReplicaLineageCandidatesContext cancels queued and incremental proof work.
func (p *Partition) ReplicaLineageCandidatesContext(ctx context.Context, tail uint64) ([]disk.LineagePrefix, error) {
	storage, ok := p.dh.(interface {
		LineageCandidatesContext(context.Context, uint64) ([]disk.LineagePrefix, error)
	})
	if !ok {
		return nil, ErrReplicaSourceUnsupported
	}
	return storage.LineageCandidatesContext(ctx, tail)
}

func (p *Partition) MatchReplicaLineage(candidates []disk.LineagePrefix, hwm uint64) (*disk.LineagePrefix, error) {
	storage, ok := p.dh.(replicaLineageStorage)
	if !ok {
		return nil, disk.ErrLineageUnsupported
	}
	return storage.MatchLineageCandidates(candidates, hwm)
}

func (p *Partition) VerifyReplicaLineage(prefix disk.LineagePrefix, hwm uint64) (bool, error) {
	storage, ok := p.dh.(replicaLineageStorage)
	if !ok {
		return false, disk.ErrLineageUnsupported
	}
	return storage.VerifyLineagePrefix(prefix, hwm)
}

// ErrReplicaSourceUnsupported means storage cannot certify source generation or
// raw page boundaries. A legacy StorageHandler is never proof authority.
var ErrReplicaSourceUnsupported = errors.New("storage does not support certified replica source reads")

type replicaGenerationStorage interface {
	CaptureContentGenerationContext(context.Context) (disk.ContentGeneration, error)
	CheckContentGenerationContext(context.Context, disk.ContentGeneration) error
}
type replicaPageStorage interface {
	ReadRetainedPage(context.Context, uint64, uint64, int, int) (disk.RetainedPage, error)
}
type replicaLineageContextStorage interface {
	MatchLineageCandidatesContext(context.Context, []disk.LineagePrefix, uint64) (*disk.LineagePrefix, error)
}

// ReplicaRetainedPrefixContext certifies raw retained records, including marked
// holes. It must run without p.mu; the caller fences partition replacement.
func (p *Partition) ReplicaRetainedPrefixContext(ctx context.Context, start, end uint64) (disk.RetainedPrefixResult, error) {
	storage, ok := p.dh.(interface {
		RetainedPrefixContext(context.Context, uint64, uint64) (disk.RetainedPrefixResult, error)
	})
	if !ok {
		return disk.RetainedPrefixResult{}, ErrReplicaSourceUnsupported
	}
	return storage.RetainedPrefixContext(ctx, start, end)
}

// These optional accessors never hold p.mu across disk maintenance. Compaction
// may call GetHWM while holding maintenanceMu, so reversing that order deadlocks.
// The FSM caller fences partition replacement; disk tokens fence prefix rewrites.
func (p *Partition) CaptureReplicaContentGeneration(ctx context.Context) (disk.ContentGeneration, error) {
	storage, ok := p.dh.(replicaGenerationStorage)
	if !ok {
		return disk.ContentGeneration{}, ErrReplicaSourceUnsupported
	}
	return storage.CaptureContentGenerationContext(ctx)
}
func (p *Partition) CheckReplicaContentGeneration(ctx context.Context, generation disk.ContentGeneration) error {
	storage, ok := p.dh.(replicaGenerationStorage)
	if !ok {
		return ErrReplicaSourceUnsupported
	}
	return storage.CheckContentGenerationContext(ctx, generation)
}
func (p *Partition) ReadReplicaRetainedPage(ctx context.Context, start, end uint64, maxRecords, maxBytes int) (disk.RetainedPage, error) {
	storage, ok := p.dh.(replicaPageStorage)
	if !ok {
		return disk.RetainedPage{}, ErrReplicaSourceUnsupported
	}
	return storage.ReadRetainedPage(ctx, start, end, maxRecords, maxBytes)
}
func (p *Partition) MatchReplicaLineageContext(ctx context.Context, candidates []disk.LineagePrefix, hwm uint64) (*disk.LineagePrefix, error) {
	storage, ok := p.dh.(replicaLineageContextStorage)
	if !ok {
		return nil, ErrReplicaSourceUnsupported
	}
	return storage.MatchLineageCandidatesContext(ctx, candidates, hwm)
}

// ReplicaFirstOffsetContext avoids the legacy read-view lock path; the source
// already fences partition replacement and checks disk generation across scans.
func (p *Partition) ReplicaFirstOffsetContext(ctx context.Context) (uint64, error) {
	storage, ok := p.dh.(interface {
		GetFirstOffsetContext(context.Context) (uint64, error)
	})
	if !ok {
		return 0, ErrReplicaSourceUnsupported
	}
	return storage.GetFirstOffsetContext(ctx)
}

// ReplicaSourceHWMContext bounds the wait behind leader writes that hold p.mu
// through durable storage I/O. Release p.mu before any source disk maintenance.
func (p *Partition) ReplicaSourceHWMContext(ctx context.Context) (uint64, error) {
	return p.replicaSourceHWMContext(ctx, false)
}

func (p *Partition) replicaSourceHWMContext(ctx context.Context, requireReady bool) (uint64, error) {
	if ctx == nil {
		return 0, errors.New("replica source HWM requires context")
	}
	var ticker *time.Ticker
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if p.mu.TryRLock() {
			hwm := p.HWM
			var readiness error
			if requireReady {
				readiness = p.hwmReadinessErrorLocked()
				if p.closed || p.snapshotRecovery {
					readiness = errors.New("metadata bootstrap source partition is not ready")
				}
			}
			p.mu.RUnlock()
			if readiness != nil {
				return 0, readiness
			}
			return hwm, nil
		}
		if ticker == nil {
			ticker = time.NewTicker(time.Millisecond)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ticker.C:
		}
	}
}
