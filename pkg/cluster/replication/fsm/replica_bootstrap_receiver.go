package fsm

import (
	"context"
	"fmt"
	"reflect"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
)

// ReplicaBootstrapReceiverTicket retains the actual local partition and full
// definition captured before transfer. HWM growth does not change this attempt.
type ReplicaBootstrapReceiverTicket struct {
	owner      *BrokerFSM
	partition  *topic.Partition
	request    BootstrapBeginRequest
	local      BootstrapProcessIdentity
	definition topic.Definition
}

func (t *ReplicaBootstrapReceiverTicket) Partition() *topic.Partition { return t.partition }

// ReplicaBootstrapEventSourcingPrelocked selects derived-state recovery from
// local authoritative metadata while the caller owns the install fence.
func (f *BrokerFSM) ReplicaBootstrapEventSourcingPrelocked(p *topic.Partition, manifest disk.BootstrapMetadata) (bool, error) {
	d, err := f.replicaBootstrapReceiverDefinitionPrelocked(manifest.Fence.Topic, manifest.Fence.Partition, p)
	if err != nil {
		return false, err
	}
	if d.LifecycleEpoch != manifest.Fence.LifecycleEpoch {
		return false, ErrBootstrapTransferStaleFence
	}
	return d.EventSourcing, nil
}

// ReplicaBootstrapReceiverCurrent is an observational cancellation check only.
// A true result grants no installation authority; final validation still runs
// under transition/materialization exclusion. It never waits for those gates.
func (f *BrokerFSM) ReplicaBootstrapReceiverCurrent(t *ReplicaBootstrapReceiverTicket) bool {
	if t == nil || t.owner != f {
		return false
	}
	r := t.request
	f.mu.RLock()
	defer f.mu.RUnlock()
	d := f.topicState[r.Topic]
	m := f.partitionMetadata[fmt.Sprintf("%s-%d", r.Topic, r.Partition)]
	return d != nil && reflect.DeepEqual(*d, t.definition) && m != nil && m.PartitionCount == d.Partitions && m.LifecycleEpoch == r.LifecycleEpoch && m.Leader == r.Leader && m.LeaderEpoch == r.LeaderEpoch && m.CommittedHWMKnown && m.CommittedHWM >= r.MinimumEnd && containsString(m.Replicas, r.Source.BrokerID) && containsString(m.ISR, r.Source.BrokerID) && containsString(m.Replicas, r.Requester.BrokerID) && !containsString(m.ISR, r.Requester.BrokerID) && currentBootstrapProcess(f.brokers[r.Source.BrokerID], r.Source) && currentBootstrapProcess(f.brokers[r.Requester.BrokerID], r.Requester)
}

func (f *BrokerFSM) PrepareReplicaBootstrapReceiver(ctx context.Context, local BootstrapProcessIdentity, r BootstrapBeginRequest) (*ReplicaBootstrapReceiverTicket, error) {
	if ctx == nil || local != r.Requester {
		return nil, ErrBootstrapTransferStaleFence
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
	p, err := f.replicaBootstrapPartition(r.Source, r, r.MinimumEnd)
	if err != nil {
		return nil, err
	}
	definition, err := f.replicaBootstrapReceiverDefinitionPrelocked(r.Topic, r.Partition, p)
	if err != nil {
		return nil, err
	}
	if p.BootstrapPending() {
		return nil, topic.ErrBootstrapPending
	}
	return &ReplicaBootstrapReceiverTicket{owner: f, partition: p, request: r, local: local, definition: definition}, nil
}

// Caller owns transition/materialization. Copy authority under f.mu, release it
// before touching partitions or invoking local callbacks.
func (f *BrokerFSM) replicaBootstrapReceiverDefinitionPrelocked(name string, id int, p *topic.Partition) (topic.Definition, error) {
	f.mu.RLock()
	tm := f.tm
	current := f.topicState[name]
	var definition topic.Definition
	if current != nil {
		definition = *current
		definition.Policy = current.Policy.Clone()
	}
	f.mu.RUnlock()
	if tm == nil || current == nil {
		return topic.Definition{}, ErrBootstrapTransferUnavailable
	}
	normalized, err := definition.Normalize()
	if err != nil || !reflect.DeepEqual(definition, normalized) {
		return topic.Definition{}, ErrBootstrapTransferStaleFence
	}
	t := tm.GetTopic(name)
	if t == nil || !reflect.DeepEqual(definition, t.Definition()) {
		return topic.Definition{}, ErrBootstrapTransferStaleFence
	}
	actual, err := t.GetPartition(id)
	if err != nil || actual == nil || actual != p {
		return topic.Definition{}, ErrBootstrapTransferStaleFence
	}
	return definition, nil
}

// ValidateReplicaBootstrapReceiverPrelocked is for an already-owned FSM fence,
// including the consumer metadata fence. It never acquires that fence again.
func (f *BrokerFSM) ValidateReplicaBootstrapReceiverPrelocked(t *ReplicaBootstrapReceiverTicket, end uint64) error {
	if t == nil || t.owner != f || t.local != t.request.Requester || end < t.request.MinimumEnd {
		return ErrBootstrapTransferStaleFence
	}
	p, err := f.replicaBootstrapPartition(t.request.Source, t.request, end)
	if err != nil {
		return err
	}
	if p != t.partition || p.BootstrapPending() {
		return ErrBootstrapTransferStaleFence
	}
	definition, err := f.replicaBootstrapReceiverDefinitionPrelocked(t.request.Topic, t.request.Partition, p)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(t.definition, definition) {
		return ErrBootstrapTransferStaleFence
	}
	return nil
}

func (f *BrokerFSM) WithReplicaBootstrapInstallFence(ctx context.Context, fn func() error) error {
	if ctx == nil || fn == nil {
		return fmt.Errorf("bootstrap install requires context and callback")
	}
	unlock, err := lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
	if err != nil {
		return err
	}
	defer unlock()
	return fn()
}

// Durable pending is local recovery authority, independent of the historical
// source, leader and process. Current metadata must still configure this replica
// in the same lifecycle and know at least the persisted end. No ISR is changed.
func (f *BrokerFSM) ValidateReplicaBootstrapResumePrelocked(local string, p *topic.Partition, manifest disk.BootstrapMetadata) error {
	if local == "" || manifest.Fence.ReceiverID != local || p == nil || !p.BootstrapPending() {
		return ErrBootstrapTransferStaleFence
	}
	f.mu.RLock()
	m := f.partitionMetadata[fmt.Sprintf("%s-%d", manifest.Fence.Topic, manifest.Fence.Partition)]
	d := f.topicState[manifest.Fence.Topic]
	valid := m != nil && d != nil && m.PartitionCount == d.Partitions && m.LifecycleEpoch == manifest.Fence.LifecycleEpoch && d.LifecycleEpoch == manifest.Fence.LifecycleEpoch && m.CommittedHWMKnown && m.CommittedHWM >= manifest.EndOffset && containsString(m.Replicas, local) && !containsString(m.ISR, local)
	f.mu.RUnlock()
	if !valid {
		return ErrBootstrapTransferStaleFence
	}
	_, err := f.replicaBootstrapReceiverDefinitionPrelocked(manifest.Fence.Topic, manifest.Fence.Partition, p)
	return err
}
