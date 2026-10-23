package fsm

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
)

type retainedSourceState struct {
	partition        *topic.Partition
	definition       topic.Definition
	source, receiver BootstrapProcessIdentity
	cut              uint64
}

type retainedSourceTicket struct {
	state      retainedSourceState
	request    ReplicaCatchupRequest
	generation disk.ContentGeneration
	anchor     uint64
}

func (f *BrokerFSM) retainedSourceAvailable(r ReplicaCatchupRequest) bool {
	if r.RecoveryProtocol != IndexedRecoveryProtocolVersion {
		return false
	}
	sourceID := r.SourceBroker
	if sourceID == "" {
		sourceID = r.Leader
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	source, receiver := f.brokers[sourceID], f.brokers[r.BrokerID]
	return source != nil && receiver != nil && source.RecoveryProtocol >= BootstrapRecoveryProtocolVersion && receiver.RecoveryProtocol >= BootstrapRecoveryProtocolVersion
}

// Caller holds transition/materialization gates. This function performs only
// metadata, identity, partition and local HWM checks; never prefix scans.
func (f *BrokerFSM) retainedSourceState(ctx context.Context, r ReplicaCatchupRequest, fixedCut *uint64) (retainedSourceState, error) {
	var state retainedSourceState
	if r.Topic == "" || r.Partition < 0 || r.BrokerID == "" || r.Leader == "" || r.MaxRecords <= 0 || r.MaxRecords > MaxReplicaCatchupRecords {
		return state, fmt.Errorf("invalid retained source request")
	}
	sourceID := r.SourceBroker
	if sourceID == "" {
		sourceID = r.Leader
	}
	f.mu.RLock()
	meta := f.partitionMetadata[fmt.Sprintf("%s-%d", r.Topic, r.Partition)]
	source, receiver := f.brokers[sourceID], f.brokers[r.BrokerID]
	definition := copyTopicDefinition(f.topicState[r.Topic])
	tm := f.tm
	valid := meta != nil && definition != nil && source != nil && receiver != nil &&
		meta.CommittedHWMKnown && meta.CommittedHWM >= r.CommittedHWM && meta.Leader == r.Leader && meta.LeaderEpoch == r.LeaderEpoch && meta.LifecycleEpoch == r.LifecycleEpoch &&
		definition.Name == r.Topic && definition.LifecycleEpoch == r.LifecycleEpoch && meta.PartitionCount == definition.Partitions &&
		containsString(meta.Replicas, sourceID) && containsString(meta.ISR, sourceID) && containsString(meta.Replicas, r.BrokerID) && !containsString(meta.ISR, r.BrokerID) &&
		source.ID == sourceID && receiver.ID == r.BrokerID && source.Status == "active" && receiver.Status == "active" && source.RecoveryProtocol >= BootstrapRecoveryProtocolVersion && receiver.RecoveryProtocol >= BootstrapRecoveryProtocolVersion
	var modeErr error
	if valid {
		modeErr = validateReplicaRecoveryRequest(r, *source, *receiver, *meta)
		state.cut = meta.CommittedHWM
		state.source = BootstrapProcessIdentity{BrokerID: source.ID, IncarnationID: source.IncarnationID, IncarnationEpoch: source.IncarnationEpoch}
		state.receiver = BootstrapProcessIdentity{BrokerID: receiver.ID, IncarnationID: receiver.IncarnationID, IncarnationEpoch: receiver.IncarnationEpoch}
	}
	f.mu.RUnlock()
	if !valid || !state.source.valid() || !state.receiver.valid() {
		return retainedSourceState{}, fmt.Errorf("retained source fence changed")
	}
	if modeErr != nil {
		return retainedSourceState{}, modeErr
	}
	if r.RecoveryProtocol == IndexedRecoveryProtocolVersion && r.NextOffset > state.cut {
		return retainedSourceState{}, fmt.Errorf("indexed offset exceeds committed HWM")
	}
	if tm == nil {
		return retainedSourceState{}, fmt.Errorf("source topic manager unavailable")
	}
	local := tm.GetTopic(r.Topic)
	if local == nil || !reflect.DeepEqual(*definition, local.Definition()) {
		return retainedSourceState{}, fmt.Errorf("source definition changed")
	}
	p, err := local.GetPartition(r.Partition)
	if err != nil {
		return retainedSourceState{}, err
	}
	hwm, err := p.ReplicaSourceHWMContext(ctx)
	if err != nil {
		return retainedSourceState{}, err
	}
	minimum := state.cut
	if fixedCut != nil {
		minimum = *fixedCut
	}
	if hwm < minimum {
		return retainedSourceState{}, fmt.Errorf("source HWM is behind committed cut")
	}
	state.partition, state.definition = p, *definition
	return state, nil
}

func (f *BrokerFSM) prepareRetainedSource(ctx context.Context, r ReplicaCatchupRequest) (*retainedSourceTicket, error) {
	unlock, err := lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := f.retainedSourceState(ctx, r, nil)
	if err != nil {
		return nil, err
	}
	generation, err := state.partition.CaptureReplicaContentGeneration(ctx)
	if err != nil {
		return nil, err
	}
	anchor, err := state.partition.ReplicaFirstOffsetContext(ctx)
	if err != nil {
		return nil, err
	}
	if err = state.partition.CheckReplicaContentGeneration(ctx, generation); err != nil {
		return nil, err
	}
	return &retainedSourceTicket{state: state, request: r, generation: generation, anchor: anchor}, nil
}

func (f *BrokerFSM) publishRetainedSource(ctx context.Context, ticket *retainedSourceTicket, batch ReplicaCatchupBatch) (ReplicaCatchupBatch, error) {
	// Serialize and hash before reacquiring the metadata gates. The conditional
	// publication callback below only assigns this already detached value.
	sealed, err := SealReplicaCatchupBatch(batch)
	if err != nil {
		return ReplicaCatchupBatch{}, err
	}
	unlock, err := lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
	if err != nil {
		return ReplicaCatchupBatch{}, err
	}
	defer unlock()
	current, err := f.retainedSourceState(ctx, ticket.request, &ticket.state.cut)
	if err != nil {
		return ReplicaCatchupBatch{}, err
	}
	old := ticket.state
	if current.partition != old.partition || current.source != old.source || current.receiver != old.receiver || current.cut < old.cut || !reflect.DeepEqual(current.definition, old.definition) {
		return ReplicaCatchupBatch{}, fmt.Errorf("retained source changed during scan")
	}
	if err := ctx.Err(); err != nil {
		return ReplicaCatchupBatch{}, err
	}
	var published ReplicaCatchupBatch
	err = disk.TryPublishIfContentGenerations([]disk.ContentGeneration{ticket.generation}, func() { published = sealed })
	return published, err
}

func (f *BrokerFSM) fetchRetainedRecovery(ctx context.Context, request ReplicaCatchupRequest) (ReplicaCatchupBatch, error) {
	// Detach the challenge from its caller for the whole scan/publication cycle.
	request.PrefixCandidates = append([]disk.LineagePrefix(nil), request.PrefixCandidates...)
	if request.RetainedPrefix != nil {
		prefix := *request.RetainedPrefix
		request.RetainedPrefix = &prefix
	}
	ticket, err := f.prepareRetainedSource(ctx, request)
	if err != nil {
		return ReplicaCatchupBatch{}, err
	}
	state, p := ticket.state, ticket.state.partition
	batch := ReplicaCatchupBatch{
		Topic: request.Topic, Partition: request.Partition, BrokerID: request.BrokerID,
		StartOffset: request.NextOffset, EndOffset: request.NextOffset, CommittedHWM: state.cut,
		Leader: request.Leader, SourceBroker: state.source.BrokerID, LeaderEpoch: request.LeaderEpoch, LifecycleEpoch: request.LifecycleEpoch,
		RecoveryProtocol: request.RecoveryProtocol, RetainedPrefix: request.RetainedPrefix,
	}
	prefix := func(end uint64) (disk.RetainedPrefixResult, error) {
		result, err := p.ReplicaRetainedPrefixContext(ctx, ticket.anchor, end)
		if err == nil && result.ContentGeneration != ticket.generation {
			err = disk.ErrStaleContentGeneration
		}
		return result, err
	}
	readPage := func(start, end uint64, max int) (disk.RetainedPage, error) {
		page, err := readRecoveryPage(ctx, p, start, end, max, request.MaxBytes)
		if err != nil {
			return disk.RetainedPage{}, err
		}
		if page.ContentGeneration != ticket.generation {
			return disk.RetainedPage{}, disk.ErrStaleContentGeneration
		}
		// Preserve the established detached wire representation. Raw proof hashes
		// still include the storage identity validated by the disk reader.
		for i := range page.Messages {
			page.Messages[i].Topic = ""
			page.Messages[i].Partition = 0
		}
		return page, nil
	}
	if request.RecoveryProtocol == BootstrapRecoveryProtocolVersion {
		challenge := request.RetainedPrefix
		batch.BootstrapRequired = challenge.Start != ticket.anchor || challenge.End > state.cut
		if !batch.BootstrapRequired {
			actual, err := prefix(challenge.End)
			if err != nil {
				return ReplicaCatchupBatch{}, err
			}
			batch.BootstrapRequired = actual.Proof != *challenge
		}
		if batch.BootstrapRequired {
			return f.publishRetainedSource(ctx, ticket, batch)
		}
		matched := *challenge
		batch.MatchedRetainedPrefix = &matched
	} else {
		matched, err := p.MatchReplicaLineageContext(ctx, request.PrefixCandidates, state.cut)
		if errors.Is(err, disk.ErrLineageUnsupported) {
			// A marker alone cannot turn corrupt or unmarked gaps into a continuity
			// miss. Validate the entire fixed cut under the original generation.
			actual, scanErr := prefix(state.cut)
			if scanErr != nil {
				return ReplicaCatchupBatch{}, scanErr
			}
			if !actual.UsedMarkedAuthority {
				return ReplicaCatchupBatch{}, err
			}
			batch.BootstrapRequired = true
			return f.publishRetainedSource(ctx, ticket, batch)
		}
		if err != nil {
			return ReplicaCatchupBatch{}, err
		}
		batch.MatchedPrefix, batch.BootstrapRequired = matched, matched == nil
		if matched == nil {
			return f.publishRetainedSource(ctx, ticket, batch)
		}
		if matched.EndOffset < request.NextOffset {
			boundary := matched.EndOffset
			batch.TruncateTo = &boundary
			return f.publishRetainedSource(ctx, ticket, batch)
		}
		if request.PreviousRecordDigest != "" {
			if request.NextOffset == 0 {
				return ReplicaCatchupBatch{}, fmt.Errorf("previous record before zero")
			}
			previous, err := readPage(request.NextOffset-1, request.NextOffset, 1)
			if err != nil {
				return ReplicaCatchupBatch{}, err
			}
			if len(previous.Messages) != 1 {
				return ReplicaCatchupBatch{}, fmt.Errorf("previous record missing")
			}
			digest, err := replicaRecordDigest(previous.Messages[0])
			if err != nil {
				return ReplicaCatchupBatch{}, err
			}
			if digest != request.PreviousRecordDigest {
				return ReplicaCatchupBatch{}, fmt.Errorf("previous record conflicts with matched prefix")
			}
		}
	}
	page, err := readPage(request.NextOffset, state.cut, request.MaxRecords)
	if err != nil {
		return ReplicaCatchupBatch{}, err
	}
	if request.RecoveryProtocol == IndexedRecoveryProtocolVersion && page.Compacted {
		return ReplicaCatchupBatch{}, fmt.Errorf("indexed source changed to marked history")
	}
	batch.EndOffset, batch.Messages, batch.Compacted = page.EndOffset, page.Messages, page.Compacted
	batch.Verified = page.Exhausted && page.EndOffset == state.cut
	return f.publishRetainedSource(ctx, ticket, batch)
}
