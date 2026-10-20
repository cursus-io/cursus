package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestSnapshotContinuationPreservesWitnessAndYieldsToOtherPartitions(t *testing.T) {
	cc, partitions := recoverySchedulerFixtureWithSnapshots(t, true, 1, 1)
	fetcher := &recordingCatchupFetcher{}
	apply := func(ctx context.Context, r fsm.ReplicaCatchupRequest, b fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
		return applySchedulerTestBatch(ctx, partitions[r.Partition], r, b)
	}
	var original *topic.ReplicaCatchupWitness
	pages := 0
	cc.SetReplicaSnapshotCatchupStep(func(ctx context.Context, r fsm.ReplicaCatchupRequest, actual *topic.ReplicaCatchupWitness, progress *ReplicaSnapshotProgress) (bool, error) {
		progress.Revision = 7
		if r.Partition == 1 {
			return true, nil
		}
		if original == nil {
			original = actual
		}
		require.Same(t, original, actual)
		require.NoError(t, partitions[0].WithReplicaSnapshotCertification(ctx, actual, r.Leader, r.LeaderEpoch, r.LifecycleEpoch, r.CommittedHWM, func(*topic.SnapshotReadCut) error { return nil }))
		if pages == 0 {
			require.Empty(t, progress.AfterKey)
		} else {
			require.Equal(t, fmt.Sprintf("%03d", pages), progress.AfterKey)
		}
		pages++
		progress.AfterKey = fmt.Sprintf("%03d", pages)
		return pages == 70, nil
	})
	require.NoError(t, cc.RunReplicaCatchupOnceVerified(context.Background(), fetcher, apply))
	require.Equal(t, replicaCatchupTurnSteps-1, pages)
	require.Len(t, fetcher.requests, 2)
	proofs := cc.RaftManager.GetFSM().BuildISRCatchupProofs("receiver")
	require.Len(t, proofs, 1)
	require.Equal(t, 1, proofs[0].Partition)
	require.NoError(t, cc.RunReplicaCatchupOnceVerified(context.Background(), fetcher, apply))
	require.Equal(t, 70, pages)
	for _, r := range fetcher.requests[2:] {
		require.NotEqual(t, 0, r.Partition, "continuation must precede request rebuilding")
	}
	require.Len(t, cc.RaftManager.GetFSM().BuildISRCatchupProofs("receiver"), 2)
	require.Empty(t, cc.snapshotPending)
}

func TestSnapshotContinuationDiscardsChangedAuthorityBeforeCallback(t *testing.T) {
	cc, partitions := recoverySchedulerFixtureWithSnapshots(t, true, 1)
	fetcher := &recordingCatchupFetcher{}
	apply := func(ctx context.Context, r fsm.ReplicaCatchupRequest, b fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
		return applySchedulerTestBatch(ctx, partitions[0], r, b)
	}
	pages := 0
	cc.SetReplicaSnapshotCatchupStep(func(_ context.Context, _ fsm.ReplicaCatchupRequest, _ *topic.ReplicaCatchupWitness, progress *ReplicaSnapshotProgress) (bool, error) {
		pages++
		progress.Revision, progress.AfterKey = 7, fmt.Sprintf("%03d", pages)
		return false, nil
	})
	require.NoError(t, cc.RunReplicaCatchupOnceVerified(context.Background(), fetcher, apply))
	state := cc.RaftManager.GetFSM()
	meta := state.GetPartitionMetadata("orders-0")
	meta.LeaderEpoch++
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:orders-0:"), data...)}))
	require.ErrorContains(t, cc.RunReplicaCatchupOnceVerified(context.Background(), fetcher, apply), "fence changed")
	require.Equal(t, replicaCatchupTurnSteps-1, pages)
	require.Len(t, fetcher.requests, 1)
	require.Empty(t, cc.snapshotPending)
	require.Empty(t, state.BuildISRCatchupProofs("receiver"))
}

func TestSnapshotContinuationKeepsProgressAfterCanceledTurn(t *testing.T) {
	cc, partitions := recoverySchedulerFixtureWithSnapshots(t, true, 1)
	fetcher := &recordingCatchupFetcher{}
	apply := func(ctx context.Context, r fsm.ReplicaCatchupRequest, b fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
		return applySchedulerTestBatch(ctx, partitions[0], r, b)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	var original *topic.ReplicaCatchupWitness
	cc.SetReplicaSnapshotCatchupStep(func(ctx context.Context, _ fsm.ReplicaCatchupRequest, actual *topic.ReplicaCatchupWitness, progress *ReplicaSnapshotProgress) (bool, error) {
		calls++
		switch calls {
		case 1:
			original = actual
			progress.Revision, progress.AfterKey = 9, "a"
			return false, nil
		case 2:
			cancel()
			return false, ctx.Err()
		default:
			require.Same(t, original, actual)
			require.Equal(t, ReplicaSnapshotProgress{Revision: 9, AfterKey: "a"}, *progress)
			return true, nil
		}
	})
	require.ErrorIs(t, cc.RunReplicaCatchupOnceVerified(ctx, fetcher, apply), context.Canceled)
	require.Len(t, cc.snapshotPending, 1)
	require.NoError(t, cc.RunReplicaCatchupOnceVerified(context.Background(), fetcher, apply))
	require.Len(t, fetcher.requests, 1)
	require.Len(t, cc.RaftManager.GetFSM().BuildISRCatchupProofs("receiver"), 1)
	require.Empty(t, cc.snapshotPending)
}
