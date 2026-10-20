package fsm

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
)

// MetadataBootstrapSourceTicket is local authority to prepare a private export,
// not authority to publish one. All fields are immutable and never serialized.
type MetadataBootstrapSourceTicket struct {
	owner          *BrokerFSM
	partition      *topic.Partition
	request        BootstrapBeginRequest
	local          BootstrapProcessIdentity
	generation     disk.ContentGeneration
	tokens         []disk.ContentGeneration
	anchor, start  uint64
	partitionCount int
}

// MetadataBootstrapContinuity can only be obtained after successful publication.
// Registry certificate IDs refer to these local objects, never wire-built proofs.
type MetadataBootstrapContinuity struct {
	owner          *BrokerFSM
	partition      *topic.Partition
	generation     disk.ContentGeneration
	source         BootstrapProcessIdentity
	topic          string
	partitionID    int
	leader         string
	leaderEpoch    int
	lifecycle      uint64
	anchor, end    uint64
	partitionCount int
}

// PrepareMetadataBootstrapSource captures the ORIGINAL generation before anchor
// inspection. The caller supplies the actual local process identity and a finite
// creating owner/context. FSM gates are released before ExportContext returns any
// bytes; consumer readiness and reload/drain gates are not source prerequisites.
// A nil prior selects a full capture, including a registry certificate lookup
// miss. A supplied stale prior returns ErrBootstrapContinuityMiss; the registry
// may retry with nil without changing the original request fingerprint.
func (f *BrokerFSM) PrepareMetadataBootstrapSource(ctx context.Context, local BootstrapProcessIdentity, request BootstrapBeginRequest, prior *MetadataBootstrapContinuity) (*MetadataBootstrapSourceTicket, error) {
	if ctx == nil {
		return nil, fmt.Errorf("metadata bootstrap requires context")
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	unlock, err := lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
	if err != nil {
		return nil, err
	}
	defer unlock()
	p, count, err := f.metadataBootstrapPartition(ctx, local, request)
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
	if err = p.CheckReplicaContentGeneration(ctx, generation); err != nil {
		return nil, metadataBootstrapStorageError(err)
	}
	start := anchor
	if prior != nil {
		if request.PriorCertificateID == "" || prior.owner != f || prior.partition != p || prior.generation != generation || prior.source != local || prior.topic != request.Topic || prior.partitionID != request.Partition || prior.leader != request.Leader || prior.leaderEpoch != request.LeaderEpoch || prior.lifecycle != request.LifecycleEpoch || prior.anchor != anchor || prior.end != request.SuffixStart || prior.partitionCount != count || request.SuffixStart < anchor {
			return nil, ErrBootstrapContinuityMiss
		}
		start = request.SuffixStart
	}
	if start > request.FixedEnd {
		return nil, ErrBootstrapTransferStaleFence
	}
	return &MetadataBootstrapSourceTicket{owner: f, partition: p, request: request, local: local, generation: generation, tokens: []disk.ContentGeneration{generation}, anchor: anchor, start: start, partitionCount: count}, nil
}

func metadataBootstrapStorageError(err error) error {
	if errors.Is(err, topic.ErrReplicaSourceUnsupported) {
		return errors.Join(ErrBootstrapTransferUnsupported, err)
	}
	return classifyBootstrapStorageError(err)
}

// metadataBootstrapPartition is called only under transition/materialization.
// No topic, partition or storage operation is performed while holding f.mu.
func (f *BrokerFSM) metadataBootstrapPartition(ctx context.Context, local BootstrapProcessIdentity, r BootstrapBeginRequest) (*topic.Partition, int, error) {
	if local != r.Source {
		return nil, 0, ErrBootstrapTransferStaleFence
	}
	f.mu.RLock()
	tm := f.tm
	definition := f.topicState[r.Topic]
	meta := f.partitionMetadata[fmt.Sprintf("%s-%d", r.Topic, r.Partition)]
	source, requester := f.brokers[r.Source.BrokerID], f.brokers[r.Requester.BrokerID]
	count := 0
	valid := definition != nil && meta != nil && definition.Name == r.Topic && definition.Partitions > 0 && r.Partition < definition.Partitions && definition.LifecycleEpoch == r.LifecycleEpoch && meta.PartitionCount == definition.Partitions && meta.LifecycleEpoch == r.LifecycleEpoch && meta.Leader == r.Leader && meta.LeaderEpoch == r.LeaderEpoch && meta.CommittedHWMKnown && meta.CommittedHWM >= r.FixedEnd && containsString(meta.Replicas, r.Source.BrokerID) && containsString(meta.ISR, r.Source.BrokerID) && containsString(meta.Replicas, r.Leader) && currentBootstrapProcess(source, r.Source) && currentBootstrapProcess(requester, r.Requester)
	if valid {
		count = definition.Partitions
	}
	f.mu.RUnlock()
	if !valid {
		return nil, 0, ErrBootstrapTransferStaleFence
	}
	if tm == nil {
		return nil, 0, ErrBootstrapTransferUnavailable
	}
	localTopic := tm.GetTopic(r.Topic)
	if localTopic == nil {
		return nil, 0, ErrBootstrapTransferUnavailable
	}
	def := localTopic.Definition()
	if def.Name != r.Topic || def.Partitions != count || def.LifecycleEpoch != r.LifecycleEpoch {
		return nil, 0, ErrBootstrapTransferStaleFence
	}
	p, err := localTopic.GetPartition(r.Partition)
	if err != nil {
		return nil, 0, err
	}
	if p == nil {
		return nil, 0, ErrBootstrapTransferUnavailable
	}
	if err = p.RecoveryError(); err != nil {
		return nil, 0, errors.Join(ErrBootstrapTransferUnavailable, err)
	}
	hwm, err := p.MetadataBootstrapSourceHWMContext(ctx)
	if err != nil {
		return nil, 0, err
	}
	if hwm < r.FixedEnd {
		return nil, 0, ErrBootstrapTransferUnavailable
	}
	return p, count, nil
}
func currentBootstrapProcess(b *BrokerInfo, p BootstrapProcessIdentity) bool {
	return b != nil && b.ID == p.BrokerID && b.Status == "active" && b.IncarnationID == p.IncarnationID && b.IncarnationEpoch == p.IncarnationEpoch && b.RecoveryProtocol >= BootstrapRecoveryProtocolVersion
}

// ExportContext must run outside all FSM, registry and partition gates. Success
// transfers Close responsibility to the caller, including rejected publications.
func (t *MetadataBootstrapSourceTicket) ExportContext(ctx context.Context) (*disk.BootstrapExport, error) {
	if t == nil || t.owner == nil || ctx == nil {
		return nil, fmt.Errorf("invalid metadata bootstrap ticket or context")
	}
	export, err := t.partition.ExportMetadataBootstrapContext(ctx, t.generation, t.anchor, t.start, t.request.FixedEnd, t.request.diskFence())
	return export, metadataBootstrapStorageError(err)
}

// MetadataBootstrapPublication is a single-use scope. Its mutex ensures that
// closing the scope waits for any in-flight publication before FSM gates release.
type MetadataBootstrapPublication struct {
	mu         sync.Mutex
	active     bool
	ctx        context.Context
	ticket     *MetadataBootstrapSourceTicket
	continuity *MetadataBootstrapContinuity
}

// WithMetadataBootstrapSourceFence revalidates source authority. Enter registry
// locks INSIDE fn, then TryPublishCurrent. Never enter this method while holding
// a registry lock. fn is synchronous, bounded and performs no export/file I/O.
// Partition locks and f.mu have been released before fn; close rejected exports
// only after this method returns. Do not store or pass the scope to another task.
func (f *BrokerFSM) WithMetadataBootstrapSourceFence(ctx context.Context, t *MetadataBootstrapSourceTicket, fn func(*MetadataBootstrapPublication) error) error {
	if ctx == nil || fn == nil {
		return fmt.Errorf("metadata bootstrap fence requires context and callback")
	}
	if t == nil || t.owner != f {
		return ErrBootstrapTransferStaleFence
	}
	unlock, err := lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
	if err != nil {
		return err
	}
	defer unlock()
	p, count, err := f.metadataBootstrapPartition(ctx, t.local, t.request)
	if err != nil {
		return err
	}
	if p != t.partition || count != t.partitionCount {
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
	cert := &MetadataBootstrapContinuity{owner: f, partition: p, generation: t.generation, source: t.local, topic: t.request.Topic, partitionID: t.request.Partition, leader: t.request.Leader, leaderEpoch: t.request.LeaderEpoch, lifecycle: t.request.LifecycleEpoch, anchor: t.anchor, end: t.request.FixedEnd, partitionCount: count}
	scope := &MetadataBootstrapPublication{active: true, ctx: ctx, ticket: t, continuity: cert}
	defer func() { scope.mu.Lock(); scope.active = false; scope.mu.Unlock() }()
	return fn(scope)
}

// TryPublishCurrent takes the original disk token under the caller's registry
// lock. publish may only assign prepared pointers/scalars: no I/O, allocations,
// growing maps, foreign locks, or reentry. A certificate is returned only after
// this callback succeeds while BOTH the FSM and disk fences are held.
func (s *MetadataBootstrapPublication) TryPublishCurrent(publish func()) (*MetadataBootstrapContinuity, error) {
	if s == nil {
		return nil, ErrBootstrapTransferStaleFence
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return nil, ErrBootstrapTransferStaleFence
	}
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	if err := disk.TryPublishIfContentGenerations(s.ticket.tokens, publish); err != nil {
		return nil, metadataBootstrapStorageError(err)
	}
	s.active = false
	return s.continuity, nil
}
