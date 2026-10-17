package fsm

import (
	"context"
	"fmt"
	"reflect"
)

// WithReplicaSnapshotRecoveryFence excludes metadata transitions through one
// prepared snapshot publication. It authorizes current recovery participants
// independently of wire recovery mode or prefix-batch validation.
func (f *BrokerFSM) WithReplicaSnapshotRecoveryFence(request ReplicaCatchupRequest, publish func() error) error {
	return f.WithReplicaSnapshotRecoveryFenceContext(context.Background(), request, publish)
}

func (f *BrokerFSM) WithReplicaSnapshotRecoveryFenceContext(ctx context.Context, request ReplicaCatchupRequest, publish func() error) error {
	unlock, err := lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
	if err != nil {
		return err
	}
	defer unlock()
	f.mu.RLock()
	meta := f.partitionMetadata[fmt.Sprintf("%s-%d", request.Topic, request.Partition)]
	source, receiver := f.brokers[request.SourceBroker], f.brokers[request.BrokerID]
	valid := meta != nil && meta.CommittedHWMKnown && meta.Leader == request.Leader && meta.LeaderEpoch == request.LeaderEpoch && meta.LifecycleEpoch == request.LifecycleEpoch && meta.CommittedHWM >= request.CommittedHWM && containsString(meta.Replicas, request.SourceBroker) && containsString(meta.ISR, request.SourceBroker) && containsString(meta.Replicas, request.BrokerID) && !containsString(meta.ISR, request.BrokerID) && source != nil && source.Status == "active" && receiver != nil && receiver.Status == "active"
	f.mu.RUnlock()
	if !valid || publish == nil {
		return fmt.Errorf("replica snapshot recovery fence changed")
	}
	if err := f.validateReplicaRequestAuthority(request); err != nil {
		return err
	}
	return publish()
}

// Caller owns both metadata transition gates. Heartbeats may change activity
// timestamps without changing process identity or recovery authority.
func (f *BrokerFSM) validateReplicaRequestAuthority(request ReplicaCatchupRequest) error {
	if original := request.localAuthority; original != nil {
		current, ok := f.replicaRequestState(request.BrokerID, request.Topic, request.Partition)
		if !ok || original.partition != current.partition || !reflect.DeepEqual(original.definition, current.definition) ||
			!sameRequestBroker(original.source, current.source) || !sameRequestBroker(original.receiver, current.receiver) ||
			!reflect.DeepEqual(original.meta.Replicas, current.meta.Replicas) || !reflect.DeepEqual(original.meta.ISR, current.meta.ISR) {
			return fmt.Errorf("replica snapshot recovery authority changed")
		}
	}
	return nil
}
