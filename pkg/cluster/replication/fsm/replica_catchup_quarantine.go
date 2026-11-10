package fsm

import (
	"context"
	"fmt"
)

// BuildReplicaCatchupQuarantine prepares an exactly fenced membership decision
// for a follower that cannot apply marked or divergent history while in ISR.
// The caller validates the complete response first and submits only after this
// method releases its gates. The response never authorizes local mutation.
func (f *BrokerFSM) BuildReplicaCatchupQuarantine(ctx context.Context, request ReplicaCatchupRequest, batch ReplicaCatchupBatch) (*ISRQuarantineCommand, error) {
	if batch.RecoveryProtocol != 0 || (!batch.Compacted && batch.TruncateTo == nil) {
		return nil, nil
	}
	unlock, err := lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
	if err != nil {
		return nil, err
	}
	defer unlock()
	meta := f.GetPartitionMetadata(fmt.Sprintf("%s-%d", request.Topic, request.Partition))
	if meta == nil || !containsString(meta.ISR, request.BrokerID) {
		return nil, nil
	}
	current, ok := f.replicaRequestState(request.BrokerID, request.Topic, request.Partition)
	if !ok {
		return nil, fmt.Errorf("replica quarantine metadata unavailable")
	}
	if !containsString(current.meta.ISR, request.BrokerID) {
		return nil, nil
	}
	if request.localAuthority == nil || !request.localAuthority.same(current, false) ||
		request.BrokerID == current.meta.Leader || current.source.Status != "active" || current.receiver.Status != "active" ||
		current.source.RecoveryProtocol < BootstrapRecoveryProtocolVersion || current.receiver.RecoveryProtocol < BootstrapRecoveryProtocolVersion ||
		current.source.LifecycleProtocol < ReplicaGapRecoveryProtocolVersion || current.receiver.LifecycleProtocol < ReplicaGapRecoveryProtocolVersion {
		return nil, fmt.Errorf("replica quarantine authority changed or unsupported")
	}
	if batch.Topic != request.Topic || batch.Partition != request.Partition || batch.BrokerID != request.BrokerID ||
		batch.SourceBroker != current.source.ID || batch.Leader != current.meta.Leader || batch.LeaderEpoch != current.meta.LeaderEpoch ||
		batch.LifecycleEpoch != current.meta.LifecycleEpoch || batch.StartOffset != request.NextOffset ||
		batch.CommittedHWM < request.CommittedHWM || batch.CommittedHWM > current.meta.CommittedHWM {
		return nil, fmt.Errorf("replica quarantine response fence changed")
	}
	release, err := current.partition.BeginReplicationMutationContext(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := current.partition.ValidateReplicaRequestWitness(ctx, request.LocalWitness, request.NextOffset); err != nil {
		return nil, err
	}
	return &ISRQuarantineCommand{
		Topic: request.Topic, Partition: request.Partition, BrokerID: request.BrokerID,
		Leader: current.meta.Leader, LeaderEpoch: current.meta.LeaderEpoch, LifecycleEpoch: current.meta.LifecycleEpoch,
		CommittedHWM: current.meta.CommittedHWM, ExpectedISR: append([]string(nil), current.meta.ISR...),
		ExpectedReplicas: append([]string(nil), current.meta.Replicas...),
	}, nil
}
