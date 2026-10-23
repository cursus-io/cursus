package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/recovery"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type recoveryFetchFunc func(context.Context, fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error)

func (fn recoveryFetchFunc) FetchReplicaCatchup(ctx context.Context, _ string, _ int, r fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
	return fn(ctx, r)
}

func setScheduledSource(t *testing.T, cc *ClusterController, partition int, source string) {
	t.Helper()
	state := cc.RaftManager.GetFSM()
	data, err := json.Marshal(fsm.BrokerInfo{ID: source, Addr: source + ":9000", Status: "active"})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)}))
	key := fmt.Sprintf("orders-%d", partition)
	meta := state.GetPartitionMetadata(key)
	meta.Leader, meta.Replicas, meta.ISR = source, []string{source, "receiver"}, []string{source}
	meta.LeaderEpoch++
	data, err = json.Marshal(meta)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:"+key+":"), data...)}))
}

func scheduledBatch(r fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
	end := min(r.NextOffset+1, r.CommittedHWM)
	b := fsm.ReplicaCatchupBatch{Topic: r.Topic, Partition: r.Partition, BrokerID: r.BrokerID, StartOffset: r.NextOffset, EndOffset: end, CommittedHWM: r.CommittedHWM, Leader: r.Leader, SourceBroker: r.SourceBroker, LeaderEpoch: r.LeaderEpoch, LifecycleEpoch: r.LifecycleEpoch, Verified: end == r.CommittedHWM}
	if end > r.NextOffset {
		b.Messages = []types.Message{{Offset: r.NextOffset, Payload: "record", LeaderEpoch: int64(r.LeaderEpoch)}}
	}
	return fsm.SealReplicaCatchupBatch(b)
}

func waitRecoverySignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("controlled recovery did not reach checkpoint")
	}
}

func TestDispatchUnresponsiveSourcesDoNotDelayExactSmallISRJoin(t *testing.T) {
	cc, partitions := recoverySchedulerFixture(t, 1, 10000, 10000, 2)
	for i := range partitions {
		setScheduledSource(t, cc, i, fmt.Sprintf("source-%d", i))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan struct{})
	var once sync.Once
	cc.SetReplicaCatchupProofSubmitter(func(ctx context.Context, proof fsm.ISRCatchupProof) error {
		data, err := json.Marshal(proof)
		if err != nil {
			return err
		}
		if result := cc.RaftManager.GetFSM().Apply(&raft.Log{Data: append([]byte("ISR_CATCHUP:"), data...)}); result != nil {
			return result.(error)
		}
		if proof.Partition == 3 {
			once.Do(func() { close(finished) })
		}
		return nil
	})
	fetch := recoveryFetchFunc(func(ctx context.Context, r fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
		if r.Partition < 2 {
			<-ctx.Done()
			return fsm.ReplicaCatchupBatch{}, ctx.Err()
		}
		return scheduledBatch(r)
	})
	cc.StartReplicaCatchupVerified(ctx, fetch, func(ctx context.Context, r fsm.ReplicaCatchupRequest, b fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
		return applySchedulerTestBatch(ctx, partitions[r.Partition], r, b)
	})
	waitRecoverySignal(t, finished)
	require.Contains(t, cc.RaftManager.GetFSM().GetPartitionMetadata("orders-3").ISR, "receiver")
	require.Less(t, partitions[2].NextOffset(), uint64(10000), "large lag must yield before completing")
	cancel()
	cc.StopReplicaCatchup()
}

func TestDispatchSourceReplacementJoinsOldOwnerBeforeNewApply(t *testing.T) {
	cc, _ := recoverySchedulerFixture(t, 1)
	limits, err := cc.Config.RecoverySettings().Limits()
	require.NoError(t, err)
	budget, err := recovery.NewBudget(limits, nil)
	require.NoError(t, err)
	cc.SetRecoveryBudget(budget)
	started, canceled, release, replaced := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce, startOnce, replaceOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	fetch := recoveryFetchFunc(func(ctx context.Context, r fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
		if r.SourceBroker == "source" {
			startOnce.Do(func() { close(started) })
			<-ctx.Done()
			close(canceled)
			<-release
		} else {
			replaceOnce.Do(func() { close(replaced) })
			<-ctx.Done()
		}
		return fsm.ReplicaCatchupBatch{}, ctx.Err()
	})
	cc.StartReplicaCatchupVerified(context.Background(), fetch, func(context.Context, fsm.ReplicaCatchupRequest, fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
		t.Error("failed fetch reached apply")
		return nil, nil
	})
	waitRecoverySignal(t, started)
	setScheduledSource(t, cc, 0, "replacement")
	waitRecoverySignal(t, canceled)
	require.Equal(t, 1, budget.Usage().Active, "cancel must not release a running owner")
	select {
	case <-replaced:
		t.Fatal("replacement raced old owner")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	waitRecoverySignal(t, replaced)
	cc.StopReplicaCatchup()
	require.Zero(t, budget.Usage().Active)
	require.Zero(t, budget.Usage().Waiters)
}

func TestResumableTurnRetainsWitnessAndRejectsMovedHWM(t *testing.T) {
	cc, partitions := recoverySchedulerFixtureWithSnapshots(t, true, 1)
	turn := &replicaTurn{}
	candidate := fsm.ReplicaCatchupCandidate{Topic: "orders", Partition: 0}
	apply := func(ctx context.Context, r fsm.ReplicaCatchupRequest, b fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
		return applySchedulerTestBatch(ctx, partitions[0], r, b)
	}
	cc.SetReplicaSnapshotCatchupStep(func(_ context.Context, _ fsm.ReplicaCatchupRequest, witness *topic.ReplicaCatchupWitness, progress *ReplicaSnapshotProgress) (bool, error) {
		require.Same(t, turn.witness, witness)
		progress.Revision, progress.AfterKey = 1, progress.AfterKey+"x"
		return len(progress.AfterKey) == 2, nil
	})
	fetch := recoveryFetchFunc(func(_ context.Context, r fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
		return scheduledBatch(r)
	})
	for range 4 {
		result := cc.advanceReplicaTurn(context.Background(), candidate, turn, fetch, apply)
		require.NoError(t, result.err)
		require.False(t, result.done)
	}
	require.NotNil(t, turn.witness)
	require.True(t, turn.snapshotDone)
	state := cc.RaftManager.GetFSM()
	meta := state.GetPartitionMetadata("orders-0")
	meta.CommittedHWM++
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:orders-0:"), data...)}))
	result := cc.advanceReplicaTurn(context.Background(), candidate, turn, fetch, apply)
	require.Error(t, result.err)
	require.Nil(t, turn.witness)
	require.NotContains(t, state.GetPartitionMetadata("orders-0").ISR, "receiver")
}

func TestDispatchHealthyWatermarkProgressesWithExhaustedRecoveryBudget(t *testing.T) {
	cc, partitions := recoverySchedulerFixture(t, 1)
	p := partitions[0]
	meta := cc.RaftManager.GetFSM().GetPartitionMetadata("orders-0")
	meta.ISR = append(meta.ISR, "receiver")
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	require.Nil(t, cc.RaftManager.GetFSM().Apply(&raft.Log{Data: append([]byte("PARTITION:orders-0:"), data...)}))
	require.NoError(t, p.ReplicaAppendWithMode([]types.Message{{Offset: 0, Payload: "healthy", ProducerID: "producer", SeqNum: 1}}, true))
	require.Zero(t, p.GetHWM(), "test must require actual watermark maintenance")
	limits, err := cc.Config.RecoverySettings().Limits()
	require.NoError(t, err)
	limits.BytesPerSecond, limits.SourceBytesPerSecond = 1, 1
	budget, err := recovery.NewBudget(limits, nil)
	require.NoError(t, err)
	lease, _, err := budget.TryAcquireEstimated(context.Background(), "source", limits.SourceBurstBytes)
	require.NoError(t, err)
	require.NoError(t, lease.Finish(limits.MaxUnitBytes))
	cc.SetRecoveryBudget(budget)
	cc.StartReplicaCatchupVerified(context.Background(), recoveryFetchFunc(func(context.Context, fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
		t.Error("healthy watermark maintenance fetched recovery data")
		return fsm.ReplicaCatchupBatch{}, context.Canceled
	}), func(context.Context, fsm.ReplicaCatchupRequest, fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
		t.Error("healthy watermark maintenance applied recovery data")
		return nil, context.Canceled
	})
	require.Eventually(t, func() bool { return p.GetHWM() == 1 }, 5*time.Second, time.Millisecond)
	cc.StopReplicaCatchup()
	require.Negative(t, budget.Usage().Tokens, "maintenance must not wait for debt repayment")
	require.Zero(t, budget.Usage().Active)
}
