package fsm

import "fmt"

// WithReplicaSnapshotRecoveryFence excludes metadata transitions through one
// prepared snapshot publication. It authorizes current recovery participants
// independently of wire recovery mode or prefix-batch validation.
func (f *BrokerFSM) WithReplicaSnapshotRecoveryFence(request ReplicaCatchupRequest, publish func() error) error {
	f.transitionMu.Lock()
	defer f.transitionMu.Unlock()
	f.materializationMu.Lock()
	defer f.materializationMu.Unlock()
	f.mu.RLock()
	meta := f.partitionMetadata[fmt.Sprintf("%s-%d", request.Topic, request.Partition)]
	source, receiver := f.brokers[request.SourceBroker], f.brokers[request.BrokerID]
	valid := meta != nil && meta.CommittedHWMKnown && meta.Leader == request.Leader && meta.LeaderEpoch == request.LeaderEpoch && meta.LifecycleEpoch == request.LifecycleEpoch && meta.CommittedHWM >= request.CommittedHWM && containsString(meta.Replicas, request.SourceBroker) && containsString(meta.ISR, request.SourceBroker) && containsString(meta.Replicas, request.BrokerID) && !containsString(meta.ISR, request.BrokerID) && source != nil && source.Status == "active" && receiver != nil && receiver.Status == "active"
	f.mu.RUnlock()
	if !valid || publish == nil {
		return fmt.Errorf("replica snapshot recovery fence changed")
	}
	return publish()
}
