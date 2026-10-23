package controller

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/eventsource"
	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/recovery"
	"github.com/cursus-io/cursus/pkg/topic"
)

// CatchupReplicaSnapshots is the synchronous compatibility entry point. The
// production scheduler uses CatchupReplicaSnapshotPage to retain progress.
func (ch *CommandHandler) CatchupReplicaSnapshots(ctx context.Context, request fsm.ReplicaCatchupRequest, actual *topic.ReplicaCatchupWitness) error {
	progress := &clusterController.ReplicaSnapshotProgress{}
	for {
		done, err := func() (bool, error) {
			if ch == nil || ch.recoveryBudget == nil {
				return ch.CatchupReplicaSnapshotPage(ctx, request, actual, progress)
			}
			if ctx == nil {
				return false, fmt.Errorf("snapshot catch-up requires context")
			}
			lease, err := ch.recoveryBudget.AcquireEstimated(ctx, request.SourceBroker, ch.Config.RecoverySettings().QuantumBytes)
			if err != nil {
				return false, err
			}
			pageCtx, meter := recovery.Measure(ctx)
			defer func() { _ = lease.Finish(meter.Bytes()) }()
			return ch.CatchupReplicaSnapshotPage(pageCtx, request, actual, progress)
		}()
		if err != nil || done {
			return err
		}
	}
}

// CatchupReplicaSnapshotPage fetches one bounded page outside every local gate.
// Only the original terminal apply witness authorizes pending-replica saves.
func (ch *CommandHandler) CatchupReplicaSnapshotPage(ctx context.Context, request fsm.ReplicaCatchupRequest, actual *topic.ReplicaCatchupWitness, progress *clusterController.ReplicaSnapshotProgress) (bool, error) {
	if ctx == nil {
		return false, fmt.Errorf("snapshot catch-up requires context")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if actual == nil || progress == nil {
		return false, fmt.Errorf("actual receiver verification witness and progress are required")
	}
	if ch == nil || ch.Cluster == nil || ch.Cluster.RaftManager == nil || ch.ESHandler == nil || ch.TopicManager == nil || ch.Cluster.RaftManager.GetFSM() == nil {
		return false, fmt.Errorf("snapshot catch-up dependencies are unavailable")
	}
	if request.SourceAddress == "" {
		return false, fmt.Errorf("snapshot catch-up source address is unavailable")
	}
	if ch.Cluster.Router == nil || ch.Cluster.Router.BrokerID() != request.BrokerID {
		return false, fmt.Errorf("snapshot catch-up receiver identity mismatch")
	}
	if err := ch.Cluster.RaftManager.GetFSM().WithReplicaSnapshotRecoveryFenceContext(ctx, request, func() error { return nil }); err != nil {
		return false, err
	}
	cursor, revision := progress.AfterKey, progress.Revision
	command := fmt.Sprintf("LIST_SNAPSHOTS %stopic=%s partition=%d limit=1 recovery_requester=%s", ch.internalAuthPrefix(), request.Topic, request.Partition, request.BrokerID)
	if cursor != "" {
		command += " after_key=" + base64.RawURLEncoding.EncodeToString([]byte(cursor))
	}
	if revision != 0 {
		command += fmt.Sprintf(" revision=%d", revision)
	}
	response, err := ch.Cluster.ForwardCommandToBrokerContext(ctx, request.SourceAddress, command)
	if err != nil {
		return false, err
	}
	if protocol.IsErrorResponse(response) {
		return false, fmt.Errorf("%s", response)
	}
	payload := strings.TrimPrefix(response, "OK snapshot_page=")
	if payload == response {
		return false, fmt.Errorf("invalid snapshot catch-up response")
	}
	var page eventsource.SnapshotPage
	if err = json.Unmarshal([]byte(payload), &page); err != nil {
		return false, err
	}
	if page.Revision == 0 || (revision != 0 && page.Revision != revision) {
		return false, fmt.Errorf("snapshot catalog revision changed")
	}
	if len(page.Snapshots) > 256 {
		return false, fmt.Errorf("snapshot catalog page exceeds record limit")
	}
	progress.Revision = page.Revision
	for _, candidate := range page.Snapshots {
		if candidate.Topic != request.Topic || candidate.Partition != request.Partition || candidate.LifecycleEpoch != request.LifecycleEpoch || candidate.Key <= cursor {
			return false, fmt.Errorf("snapshot catch-up candidate identity or order mismatch")
		}
		if err = ch.publishCatchupSnapshot(ctx, request, actual, candidate); err != nil {
			return false, err
		}
		cursor = candidate.Key
		progress.AfterKey = cursor
	}
	if page.Done {
		return true, ch.Cluster.RaftManager.GetFSM().WithReplicaSnapshotRecoveryFenceContext(ctx, request, func() error { return nil })
	}
	if len(page.Snapshots) == 0 {
		return false, fmt.Errorf("snapshot catalog page did not advance")
	}
	return false, nil
}

func (ch *CommandHandler) publishCatchupSnapshot(ctx context.Context, request fsm.ReplicaCatchupRequest, actual *topic.ReplicaCatchupWitness, candidate eventsource.SnapshotResult) error {
	if ch.Cluster.Router == nil || ch.Cluster.Router.BrokerID() != request.BrokerID {
		return fmt.Errorf("snapshot catch-up receiver identity mismatch")
	}
	release, err := ch.topicLifecycleGates.acquire(ctx, request.Topic, false)
	if err != nil {
		return err
	}
	defer release()
	writeLock := ch.partitionWriteLock(request.Topic, request.Partition)
	if err = writeLock.LockContext(ctx); err != nil {
		return err
	}
	defer writeLock.Unlock()
	local := ch.TopicManager.GetTopic(request.Topic)
	if local == nil || local.Definition().LifecycleEpoch != request.LifecycleEpoch {
		return fmt.Errorf("snapshot catch-up lifecycle changed")
	}
	p, err := local.GetPartition(request.Partition)
	if err != nil {
		return err
	}
	var prepared *eventsource.PreparedReplicaSnapshot
	err = p.WithReplicaSnapshotCertification(ctx, actual, request.Leader, request.LeaderEpoch, request.LifecycleEpoch, request.CommittedHWM, func(cut *topic.SnapshotReadCut) error {
		var err error
		prepared, err = ch.ESHandler.PrepareReplicaSnapshot(ctx, candidate, cut)
		return err
	})
	if err != nil {
		ch.ESHandler.RetirePreparedReplicaSnapshot(prepared, err)
		return err
	}
	// Replay has released all partition/index gates before acquiring the FSM.
	return ch.Cluster.RaftManager.GetFSM().WithReplicaSnapshotRecoveryFenceContext(ctx, request, func() error {
		current := ch.TopicManager.GetTopic(request.Topic)
		if current != local || current.Definition().LifecycleEpoch != request.LifecycleEpoch {
			return fmt.Errorf("snapshot catch-up owner changed")
		}
		owner, err := current.GetPartition(request.Partition)
		if err != nil || owner != p {
			return fmt.Errorf("snapshot catch-up partition owner changed")
		}
		return ch.ESHandler.PublishPreparedReplicaSnapshot(ctx, p, prepared, actual, request.Leader, request.LeaderEpoch, request.LifecycleEpoch, request.CommittedHWM)
	})
}
