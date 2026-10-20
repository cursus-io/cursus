package fsm

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
)

// ReplicaCatchupCandidate is metadata only; discovery never reads or changes logs.
type ReplicaCatchupCandidate struct {
	Topic     string
	Partition int
}

func (f *BrokerFSM) ReplicaCatchupCandidates(brokerID string) []ReplicaCatchupCandidate {
	if brokerID == "" {
		return nil
	}
	f.mu.RLock()
	var candidates []ReplicaCatchupCandidate
	for key, meta := range f.partitionMetadata {
		if meta == nil || !meta.CommittedHWMKnown || !containsString(meta.Replicas, brokerID) {
			continue
		}
		separator := strings.LastIndex(key, "-")
		if separator <= 0 {
			continue
		}
		id, err := strconv.Atoi(key[separator+1:])
		if err == nil && id >= 0 {
			candidates = append(candidates, ReplicaCatchupCandidate{key[:separator], id})
		}
	}
	f.mu.RUnlock()
	sort.Slice(candidates, func(i, j int) bool {
		// Repair offsets before ordinary data, so consumer admission can reopen.
		if candidates[i].Topic != candidates[j].Topic {
			if candidates[i].Topic == config.ConsumerOffsetsTopicName {
				return true
			}
			if candidates[j].Topic == config.ConsumerOffsetsTopicName {
				return false
			}
			return candidates[i].Topic < candidates[j].Topic
		}
		return candidates[i].Partition < candidates[j].Partition
	})
	return candidates
}

type replicaRequestState struct {
	partition        *topic.Partition
	definition       *topic.Definition
	meta             PartitionMetadata
	source, receiver BrokerInfo
	snapshot         bool
}

// Caller holds transition/materialization gates. Copy only the requested tuple.
func (f *BrokerFSM) replicaRequestState(brokerID, name string, id int) (replicaRequestState, bool) {
	var state replicaRequestState
	f.mu.RLock()
	meta := f.partitionMetadata[fmt.Sprintf("%s-%d", name, id)]
	if meta == nil || !meta.CommittedHWMKnown || !containsString(meta.Replicas, brokerID) {
		f.mu.RUnlock()
		return state, false
	}
	state.meta = *meta
	state.meta.Replicas = append([]string(nil), meta.Replicas...)
	state.meta.ISR = append([]string(nil), meta.ISR...)
	state.meta.RecoveryReplicas = append([]string(nil), meta.RecoveryReplicas...)
	state.definition = copyTopicDefinition(f.topicState[name])
	brokers := make(map[string]BrokerInfo, len(meta.Replicas))
	for _, member := range meta.Replicas {
		if broker := f.brokers[member]; broker != nil {
			brokers[member] = *broker
		}
	}
	tm := f.tm
	f.mu.RUnlock()
	var ok bool
	state.source, ok = selectReplicaCatchupSource(state.meta, brokerID, brokers)
	state.receiver = brokers[brokerID]
	if !ok || tm == nil || state.definition == nil {
		return state, false
	}
	local := tm.GetTopic(name)
	if local == nil || state.definition.LifecycleEpoch != state.meta.LifecycleEpoch || !reflect.DeepEqual(local.Definition(), *state.definition) {
		return state, false
	}
	var err error
	state.partition, err = local.GetPartition(id)
	if err != nil || state.partition.BootstrapPending() {
		return state, false
	}
	state.snapshot = local.IsEventSourcing
	return state, true
}

func sameRequestBroker(a, b BrokerInfo) bool {
	return a.ID == b.ID && a.Addr == b.Addr && a.Status == b.Status && a.RecoveryProtocol == b.RecoveryProtocol && a.IncarnationID == b.IncarnationID && a.IncarnationEpoch == b.IncarnationEpoch
}

func (s replicaRequestState) same(current replicaRequestState, exactHWM bool) bool {
	hwmOK := s.meta.CommittedHWM == current.meta.CommittedHWM || (!exactHWM && s.meta.CommittedHWM < current.meta.CommittedHWM)
	return s.partition == current.partition && reflect.DeepEqual(s.definition, current.definition) &&
		s.meta.Leader == current.meta.Leader && s.meta.LeaderEpoch == current.meta.LeaderEpoch && s.meta.LifecycleEpoch == current.meta.LifecycleEpoch && hwmOK &&
		reflect.DeepEqual(s.meta.Replicas, current.meta.Replicas) && reflect.DeepEqual(s.meta.ISR, current.meta.ISR) &&
		sameRequestBroker(s.source, current.source) && sameRequestBroker(s.receiver, current.receiver)
}

// BuildReplicaCatchupRequestContext performs proof work only for one partition.
// Marked history selects retained recovery before any last-record read or trim.
func (f *BrokerFSM) BuildReplicaCatchupRequestContext(ctx context.Context, brokerID, name string, id int) (ReplicaCatchupRequest, bool, error) {
	var empty ReplicaCatchupRequest
	if ctx == nil || brokerID == "" || name == "" || id < 0 {
		return empty, false, fmt.Errorf("invalid recovery request builder arguments")
	}
	unlock, err := lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
	if err != nil {
		return empty, false, err
	}
	state, ok := f.replicaRequestState(brokerID, name, id)
	unlock()
	if !ok {
		return empty, false, nil
	}
	p := state.partition
	leo := p.NextOffset()
	inISR := containsString(state.meta.ISR, brokerID)
	r := ReplicaCatchupRequest{localAuthority: &state, Topic: name, Partition: id, BrokerID: brokerID, NextOffset: leo, CommittedHWM: state.meta.CommittedHWM,
		Leader: state.meta.Leader, SourceBroker: state.source.ID, LeaderEpoch: state.meta.LeaderEpoch, LifecycleEpoch: state.meta.LifecycleEpoch,
		MaxRecords: MaxReplicaCatchupRecords, SourceAddress: state.source.Addr, SnapshotCatchup: state.snapshot}
	proof := func() error {
		return addReplicaRecoveryProofContext(ctx, p, &r, state.receiver, state.source, inISR, state.definition)
	}
	check := func(exactHWM bool) error {
		current, exists := f.replicaRequestState(brokerID, name, id)
		if !exists || !state.same(current, exactHWM) {
			return fmt.Errorf("recovery request metadata changed")
		}
		return ctx.Err()
	}
	if inISR && leo >= r.CommittedHWM {
		unlock, err = lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
		if err != nil {
			return empty, false, err
		}
		defer unlock()
		if err = check(true); err == nil {
			err = p.ApplyReplicaHWM(r.CommittedHWM)
		}
		if err == nil {
			p.FlushDisk()
		}
		return empty, false, err
	}
	capable := !inISR && state.receiver.RecoveryProtocol >= BootstrapRecoveryProtocolVersion && state.source.RecoveryProtocol >= BootstrapRecoveryProtocolVersion
	if capable {
		if err = proof(); err != nil {
			return empty, false, err
		}
	}
	if leo > r.CommittedHWM && r.RecoveryProtocol != BootstrapRecoveryProtocolVersion {
		unlock, err = lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
		if err != nil {
			return empty, false, err
		}
		if err = check(true); err == nil && capable {
			err = p.ValidateReplicaRequestWitness(ctx, r.LocalWitness, leo)
		}
		if err == nil {
			err = p.TruncateReplicaTailContext(ctx, r.CommittedHWM)
		}
		if err == nil {
			p.FlushDisk()
		}
		unlock()
		if err != nil {
			return empty, false, err
		}
		r.NextOffset, r.LocalWitness, r.PrefixCandidates = p.NextOffset(), nil, nil
		r.PreviousRecordDigest, r.PreviousLeaderEpoch = "", 0
	}
	if r.LocalWitness == nil {
		if err = proof(); err != nil {
			return empty, false, err
		}
	}
	unlock, err = lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
	if err != nil {
		return empty, false, err
	}
	defer unlock()
	if err = check(false); err != nil {
		return empty, false, err
	}
	if err = p.ValidateReplicaRequestWitness(ctx, r.LocalWitness, r.NextOffset); err != nil {
		return empty, false, err
	}
	return r, true, nil
}
