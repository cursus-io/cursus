package controller

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/eventsource"
	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/topic"
)

// CatchupReplicaSnapshots fetches each bounded page outside every local gate.
// Only the actual terminal apply witness can authorize pending-replica saves.
func (ch *CommandHandler) CatchupReplicaSnapshots(ctx context.Context, request fsm.ReplicaCatchupRequest, actual *topic.ReplicaCatchupWitness) error {
	if ctx == nil {
		return fmt.Errorf("snapshot catch-up requires context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if actual == nil {
		return fmt.Errorf("actual receiver verification witness is required")
	}
	if ch == nil || ch.Cluster == nil || ch.Cluster.RaftManager == nil || ch.ESHandler == nil || ch.TopicManager == nil || ch.Cluster.RaftManager.GetFSM() == nil {
		return fmt.Errorf("snapshot catch-up dependencies are unavailable")
	}
	if request.SourceAddress == "" {
		return fmt.Errorf("snapshot catch-up source address is unavailable")
	}
	if ch.Cluster.Router == nil || ch.Cluster.Router.BrokerID() != request.BrokerID {
		return fmt.Errorf("snapshot catch-up receiver identity mismatch")
	}
	cursor := ""
	var revision uint64
	for pageNumber := 0; pageNumber < 1_000_000; pageNumber++ {
		command := fmt.Sprintf("LIST_SNAPSHOTS %stopic=%s partition=%d limit=256", ch.internalAuthPrefix(), request.Topic, request.Partition)
		if cursor != "" {
			command += " after_key=" + base64.RawURLEncoding.EncodeToString([]byte(cursor))
		}
		if revision != 0 {
			command += fmt.Sprintf(" revision=%d", revision)
		}
		response, err := ch.Cluster.ForwardCommandToBrokerContext(ctx, request.SourceAddress, command)
		if err != nil {
			return err
		}
		if protocol.IsErrorResponse(response) {
			return fmt.Errorf("%s", response)
		}
		payload := strings.TrimPrefix(response, "OK snapshot_page=")
		if payload == response {
			return fmt.Errorf("invalid snapshot catch-up response")
		}
		var page eventsource.SnapshotPage
		if err = json.Unmarshal([]byte(payload), &page); err != nil {
			return err
		}
		if page.Revision == 0 || (revision != 0 && page.Revision != revision) {
			return fmt.Errorf("snapshot catalog revision changed")
		}
		if len(page.Snapshots) > 256 {
			return fmt.Errorf("snapshot catalog page exceeds record limit")
		}
		revision = page.Revision
		for _, candidate := range page.Snapshots {
			if candidate.Topic != request.Topic || candidate.Partition != request.Partition || candidate.LifecycleEpoch != request.LifecycleEpoch || candidate.Key <= cursor {
				return fmt.Errorf("snapshot catch-up candidate identity or order mismatch")
			}
			if err = ch.publishCatchupSnapshot(ctx, request, actual, candidate); err != nil {
				return err
			}
			cursor = candidate.Key
		}
		if page.Done {
			return nil
		}
		if len(page.Snapshots) == 0 {
			return fmt.Errorf("snapshot catalog page did not advance")
		}
	}
	return fmt.Errorf("snapshot catalog page limit exceeded")
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
	return ch.Cluster.RaftManager.GetFSM().WithReplicaSnapshotRecoveryFence(request, func() error {
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
