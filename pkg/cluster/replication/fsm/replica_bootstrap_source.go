package fsm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
)

// ReplicaBootstrapSourceTicket permits a private full capture, never remote
// publication. Its original storage generation precedes definition and bounds
// inspection; the complete definition is detached from both live owners.
type ReplicaBootstrapSourceTicket struct {
	owner      *BrokerFSM
	partition  *topic.Partition
	request    BootstrapBeginRequest
	local      BootstrapProcessIdentity
	definition topic.Definition
	generation disk.ContentGeneration
	tokens     []disk.ContentGeneration
	anchor     uint64
}

// ReplicaBootstrapCapture binds the actual cut and export to their source ticket.
// Callers cannot supply a different end or manufacture publication authority.
type ReplicaBootstrapCapture struct {
	ticket *ReplicaBootstrapSourceTicket
	export *disk.BootstrapExport
	end    uint64
}

func (c *ReplicaBootstrapCapture) Manifest() disk.BootstrapMetadata { return c.export.Manifest() }
func (c *ReplicaBootstrapCapture) ReadChunk(id string, offset int64, size int) ([]byte, error) {
	return c.export.ReadChunk(id, offset, size)
}
func (c *ReplicaBootstrapCapture) Close() error { return c.export.Close() }

// FullWork reports diagnostics for this private export, not publication authority.
func (c *ReplicaBootstrapCapture) FullWork() disk.FullBootstrapWork { return c.export.FullWork() }

func (f *BrokerFSM) PrepareReplicaBootstrapSource(ctx context.Context, local BootstrapProcessIdentity, r BootstrapBeginRequest) (*ReplicaBootstrapSourceTicket, error) {
	if ctx == nil {
		return nil, fmt.Errorf("replica bootstrap requires context")
	}
	if err := r.Validate(); err != nil {
		return nil, err
	}
	if r.Purpose != BootstrapPurposeReplica {
		return nil, ErrBootstrapTransferUnsupported
	}
	unlock, err := lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
	if err != nil {
		return nil, err
	}
	defer unlock()
	p, err := f.replicaBootstrapPartition(local, r, r.MinimumEnd)
	if err != nil {
		return nil, err
	}
	generation, err := p.CaptureReplicaContentGeneration(ctx)
	if err != nil {
		return nil, metadataBootstrapStorageError(err)
	}
	anchor, err := p.ReplicaFirstOffsetContext(ctx)
	if err != nil {
		return nil, metadataBootstrapStorageError(err)
	}
	definition, err := f.replicaBootstrapDefinition(ctx, p, r, r.MinimumEnd)
	if err != nil {
		return nil, err
	}
	if err = p.CheckReplicaContentGeneration(ctx, generation); err != nil {
		return nil, metadataBootstrapStorageError(err)
	}
	return &ReplicaBootstrapSourceTicket{owner: f, partition: p, request: r, local: local, definition: definition, generation: generation, tokens: []disk.ContentGeneration{generation}, anchor: anchor}, nil
}

// Caller owns transition/materialization. This lookup authorizes a known topic
// before any storage call; it never constructs paths from the wire name.
func (f *BrokerFSM) replicaBootstrapPartition(local BootstrapProcessIdentity, r BootstrapBeginRequest, end uint64) (*topic.Partition, error) {
	if local != r.Source {
		return nil, ErrBootstrapTransferStaleFence
	}
	f.mu.RLock()
	tm := f.tm
	d := f.topicState[r.Topic]
	m := f.partitionMetadata[fmt.Sprintf("%s-%d", r.Topic, r.Partition)]
	valid := d != nil && d.Name == r.Topic && d.Partitions > r.Partition && d.LifecycleEpoch == r.LifecycleEpoch &&
		m != nil && m.PartitionCount == d.Partitions && m.LifecycleEpoch == r.LifecycleEpoch && m.Leader == r.Leader && m.LeaderEpoch == r.LeaderEpoch && m.CommittedHWMKnown && m.CommittedHWM >= end &&
		containsString(m.Replicas, r.Source.BrokerID) && containsString(m.ISR, r.Source.BrokerID) && containsString(m.Replicas, r.Requester.BrokerID) && !containsString(m.ISR, r.Requester.BrokerID) && containsString(m.Replicas, r.Leader) &&
		currentBootstrapProcess(f.brokers[r.Source.BrokerID], r.Source) && currentBootstrapProcess(f.brokers[r.Requester.BrokerID], r.Requester)
	f.mu.RUnlock()
	if !valid {
		return nil, ErrBootstrapTransferStaleFence
	}
	if tm == nil {
		return nil, ErrBootstrapTransferUnavailable
	}
	t := tm.GetTopic(r.Topic)
	if t == nil {
		return nil, ErrBootstrapTransferUnavailable
	}
	p, err := t.GetPartition(r.Partition)
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, ErrBootstrapTransferUnavailable
	}
	return p, nil
}

func (f *BrokerFSM) replicaBootstrapDefinition(ctx context.Context, p *topic.Partition, r BootstrapBeginRequest, end uint64) (topic.Definition, error) {
	f.mu.RLock()
	d := *f.topicState[r.Topic]
	d.Policy = d.Policy.Clone()
	tm := f.tm
	f.mu.RUnlock()
	normalized, err := d.Normalize()
	if err != nil || !reflect.DeepEqual(d, normalized) {
		return topic.Definition{}, ErrBootstrapTransferStaleFence
	}
	t := tm.GetTopic(r.Topic)
	if t == nil || !reflect.DeepEqual(d, t.Definition()) {
		return topic.Definition{}, ErrBootstrapTransferStaleFence
	}
	actual, err := t.GetPartition(r.Partition)
	if err != nil || actual != p {
		return topic.Definition{}, ErrBootstrapTransferStaleFence
	}
	if err = p.RecoveryError(); err != nil {
		return topic.Definition{}, errors.Join(ErrBootstrapTransferUnavailable, err)
	}
	hwm, err := p.MetadataBootstrapSourceHWMContext(ctx)
	if err != nil {
		return topic.Definition{}, errors.Join(ErrBootstrapTransferUnavailable, err)
	}
	if hwm < end {
		return topic.Definition{}, ErrBootstrapTransferUnavailable
	}
	return d, nil
}

// CaptureContext runs outside FSM/registry gates. The callback must export the
// complete catalog and raw data under this one active cut, retaining its original
// catalog decision guard through export. The full copy pauses this partition;
// cancellation cannot preempt a mutex wait or an individual filesystem call.
func (t *ReplicaBootstrapSourceTicket) CaptureContext(ctx context.Context, catalogExport func(*topic.BootstrapSource, disk.BootstrapFence) (*disk.BootstrapExport, error)) (*ReplicaBootstrapCapture, error) {
	if t == nil || t.owner == nil || ctx == nil || catalogExport == nil {
		return nil, fmt.Errorf("invalid replica bootstrap capture")
	}
	var exported *disk.BootstrapExport
	var end uint64
	err := t.partition.WithBootstrapSourceContext(ctx, func(source *topic.BootstrapSource) error {
		cut, err := source.SnapshotReadCut()
		if err != nil {
			return err
		}
		if cut.ContentGeneration() != t.generation || cut.FirstOffset() != t.anchor {
			return ErrBootstrapTransferStaleFence
		}
		end = cut.CommittedEnd()
		if end < t.request.MinimumEnd {
			return ErrBootstrapTransferUnavailable
		}
		fence := t.request.diskFence()
		fence.CommittedHWM = end
		exported, err = catalogExport(source, fence)
		if err != nil {
			return err
		}
		if exported == nil {
			return ErrBootstrapTransferUnavailable
		}
		m := exported.Manifest()
		if m.StartOffset != t.anchor || m.EndOffset != end || m.Fence != fence {
			return ErrBootstrapTransferStaleFence
		}
		response := BootstrapBeginResponse{Version: BootstrapTransferVersion, Request: t.request, CaptureMode: BootstrapCaptureFull, StartOffset: m.StartOffset, Manifest: m}
		return response.ValidateFor(t.request)
	})
	if err != nil {
		if exported != nil {
			err = errors.Join(err, exported.Close())
		}
		return nil, metadataBootstrapStorageError(err)
	}
	return &ReplicaBootstrapCapture{ticket: t, export: exported, end: end}, nil
}

type ReplicaBootstrapPublication struct {
	mu      sync.Mutex
	active  bool
	ctx     context.Context
	capture *ReplicaBootstrapCapture
}

// WithReplicaBootstrapSourceFence completes all partition/definition checks
// before entering the caller's registry lock. Only the original generation's
// tiny conditional publication may run inside that lock; no export is repeated.
func (f *BrokerFSM) WithReplicaBootstrapSourceFence(ctx context.Context, capture *ReplicaBootstrapCapture, fn func(*ReplicaBootstrapPublication) error) error {
	if ctx == nil || fn == nil {
		return fmt.Errorf("replica bootstrap fence requires context and callback")
	}
	if capture == nil || capture.ticket == nil || capture.ticket.owner != f || capture.export == nil {
		return ErrBootstrapTransferStaleFence
	}
	t := capture.ticket
	unlock, err := lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
	if err != nil {
		return err
	}
	defer unlock()
	p, err := f.replicaBootstrapPartition(t.local, t.request, capture.end)
	if err != nil {
		return err
	}
	if p != t.partition {
		return ErrBootstrapTransferStaleFence
	}
	definition, err := f.replicaBootstrapDefinition(ctx, p, t.request, capture.end)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(t.definition, definition) {
		return ErrBootstrapTransferStaleFence
	}
	anchor, err := p.ReplicaFirstOffsetContext(ctx)
	if err != nil {
		return metadataBootstrapStorageError(err)
	}
	if anchor != t.anchor {
		return ErrBootstrapTransferStaleFence
	}
	if err = p.CheckReplicaContentGeneration(ctx, t.generation); err != nil {
		return metadataBootstrapStorageError(err)
	}
	scope := &ReplicaBootstrapPublication{active: true, ctx: ctx, capture: capture}
	defer func() { scope.mu.Lock(); scope.active = false; scope.mu.Unlock() }()
	return fn(scope)
}

// TryPublishOriginal permits pointer/scalar assignments only. The scope expires
// on success or when its enclosing FSM callback returns.
func (s *ReplicaBootstrapPublication) TryPublishOriginal(publish func()) error {
	if s == nil {
		return ErrBootstrapTransferStaleFence
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return ErrBootstrapTransferStaleFence
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if err := disk.TryPublishIfContentGenerations(s.capture.ticket.tokens, publish); err != nil {
		return metadataBootstrapStorageError(err)
	}
	s.active = false
	return nil
}
