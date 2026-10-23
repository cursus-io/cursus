package fsm

import (
	"context"
	"fmt"
)

// ReplicaSchedulingIdentity is metadata-only. It is an admission hint, never
// recovery authority: request construction and apply must still validate fences.
func (f *BrokerFSM) ReplicaSchedulingIdentity(brokerID string, candidate ReplicaCatchupCandidate) (source, generation string, maintenance, ok bool) {
	// Materialization has its own scheduler. Do not admit nonexistent local
	// owners to this queue, whose cap covers materialization admission.
	if f.tm == nil {
		return "", "", false, false
	}
	local := f.tm.GetTopic(candidate.Topic)
	if local == nil {
		return "", "", false, false
	}
	partition, err := local.GetPartition(candidate.Partition)
	if err != nil {
		return "", "", false, false
	}
	leo, hwm, positionKnown := partition.RecoverySchedulingPosition()
	f.mu.RLock()
	defer f.mu.RUnlock()
	meta := f.partitionMetadata[fmt.Sprintf("%s-%d", candidate.Topic, candidate.Partition)]
	if meta == nil || !meta.CommittedHWMKnown || !containsString(meta.Replicas, brokerID) {
		return "", "", false, false
	}
	if positionKnown && containsString(meta.ISR, brokerID) && leo >= meta.CommittedHWM && hwm >= meta.CommittedHWM {
		return "", "", false, false
	}
	brokers := make(map[string]BrokerInfo, len(meta.Replicas))
	for _, id := range meta.Replicas {
		if b := f.brokers[id]; b != nil {
			brokers[id] = *b
		}
	}
	b, ok := selectReplicaCatchupSource(*meta, brokerID, brokers)
	if !ok {
		return "", "", false, false
	}
	source = b.ID
	generation = fmt.Sprintf("%s/%d/%d/%s/%s/%d/%s/%v/%v", meta.Leader, meta.LeaderEpoch, meta.LifecycleEpoch, source, b.IncarnationID, b.IncarnationEpoch, b.Addr, meta.Replicas, meta.ISR)
	maintenance = containsString(meta.ISR, brokerID) && leo >= meta.CommittedHWM
	return source, generation, maintenance, true
}

// AdvanceReplicaISRWatermarkContext is healthy replication maintenance, not
// recovery. Recheck eligibility under the same gates as the request builder;
// a stale scheduling hint must never turn this control slot into a proof scan,
// fetch, rollback or bootstrap operation.
func (f *BrokerFSM) AdvanceReplicaISRWatermarkContext(ctx context.Context, brokerID string, candidate ReplicaCatchupCandidate) error {
	unlock, err := lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
	if err != nil {
		return err
	}
	defer unlock()
	state, ok := f.replicaRequestState(brokerID, candidate.Topic, candidate.Partition)
	if !ok || !containsString(state.meta.ISR, brokerID) || state.partition.NextOffset() < state.meta.CommittedHWM {
		return nil
	}
	if err := state.partition.ApplyReplicaHWM(state.meta.CommittedHWM); err != nil {
		return err
	}
	state.partition.FlushDisk()
	return ctx.Err()
}
