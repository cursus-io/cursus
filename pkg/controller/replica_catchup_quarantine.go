package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
)

// ErrReplicaCatchupQuarantined requires a fresh request after the replicated
// membership decision; the response fetched under the former ISR is discarded.
var ErrReplicaCatchupQuarantined = errors.New("replica catch-up quarantine requested")

func (ch *CommandHandler) quarantineReplicaCatchup(ctx context.Context, request fsm.ReplicaCatchupRequest, batch fsm.ReplicaCatchupBatch) error {
	if batch.RecoveryProtocol != 0 || (!batch.Compacted && batch.TruncateTo == nil) {
		return nil
	}
	if ch == nil || ch.Cluster == nil || ch.Cluster.RaftManager == nil || ch.Cluster.RaftManager.GetFSM() == nil || ch.Cluster.Router == nil || ch.Cluster.Router.BrokerID() != request.BrokerID {
		return fmt.Errorf("replica quarantine local identity unavailable")
	}
	command, err := ch.Cluster.RaftManager.GetFSM().BuildReplicaCatchupQuarantine(ctx, request, batch)
	if err != nil || command == nil {
		return err
	}
	_, err = ch.applyViaLeaderContext(ctx, "ISR_QUARANTINE", map[string]interface{}{
		"topic": command.Topic, "partition": command.Partition, "broker_id": command.BrokerID,
		"leader": command.Leader, "leader_epoch": command.LeaderEpoch, "lifecycle_epoch": command.LifecycleEpoch,
		"committed_hwm": command.CommittedHWM, "expected_isr": command.ExpectedISR, "expected_replicas": command.ExpectedReplicas,
	})
	if err != nil {
		return fmt.Errorf("request replica catch-up quarantine: %w", err)
	}
	return ErrReplicaCatchupQuarantined
}
