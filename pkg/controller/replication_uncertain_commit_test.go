package controller

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/ackpolicy"
	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type uncertainCommitExecutor struct {
	*barrierReplicationExecutor
	attempts chan partitionReplicationTask
	resolve  chan struct{}
}

type raftCommitExecutor struct {
	*barrierReplicationExecutor
	commit clusterPartitionReplicationExecutor
}

func (e raftCommitExecutor) Commit(task partitionReplicationTask) (partitionCommitResult, error) {
	return e.commit.Commit(task)
}

// TestUncertainPartitionCommitRetriesRaftWithoutRewritingLog retries the metadata commit while verifying unchanged partition log bytes.
func TestUncertainPartitionCommitRetriesRaftWithoutRewritingLog(t *testing.T) {
	h, manager, base := newDistributedAckTestHandler(t, 2)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	installPartitionMetadata(t, h, "orders", []string{"broker-1", "broker-2"})
	rm := &uncertainStreamCommitRaft{MockRaftManagerForForward: h.Cluster.RaftManager.(*MockRaftManagerForForward)}
	h.Cluster.RaftManager, h.Cluster.Router = rm, nil
	p, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Payload: "original"}}))
	files, err := filepath.Glob(filepath.Join(h.Config.LogDir, "orders", "*.log"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	before := map[string][]byte{}
	for _, path := range files {
		before[path], err = os.ReadFile(path)
		require.NoError(t, err)
	}
	close(base.barrier)
	queue := newPartitionReplicationCoordinator(1, raftCommitExecutor{base, clusterPartitionReplicationExecutor{handler: h}})
	t.Cleanup(queue.close)
	reservation, err := queue.reserve(context.Background(), "orders", 0)
	require.NoError(t, err)
	task := replicationTaskForMode(base, ackpolicy.All)
	task.partitionRef = p
	task.releaseMutation = p.BeginReplicationMutation()
	reservation.submit(task)
	select {
	case err = <-task.result:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("real Raft retry did not commit")
	}
	require.NotEmpty(t, rm.pending, "lost reply must have been injected")
	require.Nil(t, rm.state.Apply(&raft.Log{Data: rm.pending}), "delayed original commit must remain idempotent")
	require.Equal(t, uint64(1), p.GetHWM())
	for path, original := range before {
		after, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		require.Equal(t, original, after, "commit retry must rewrite zero retained log bytes")
	}
}

// Commit records each retry and simulates lost replies until explicitly released.
func (e *uncertainCommitExecutor) Commit(task partitionReplicationTask) (partitionCommitResult, error) {
	select {
	case e.attempts <- task:
	default:
	}
	select {
	case <-e.resolve:
		return e.barrierReplicationExecutor.Commit(task)
	default:
		return partitionCommitResult{}, errors.New("forward raft apply: reply lost")
	}
}

// TestUncertainPartitionCommitPreservesOwnershipAndHistory checks unresolved commits retain history and keep ownership fenced.
func TestUncertainPartitionCommitPreservesOwnershipAndHistory(t *testing.T) {
	for _, outcome := range []string{"delayed-commit", "delayed-recovery", "delayed-recovery-replaced", "delayed-recovery-replaced-fenced", "fenced", "metadata-deleted", "shutdown"} {
		t.Run(outcome, func(t *testing.T) {
			_, manager, base := newDistributedAckTestHandler(t, 2)
			require.NoError(t, manager.CreateTopic("orders", 1, false, false))
			p, err := manager.GetTopic("orders").GetPartition(0)
			require.NoError(t, err)
			message := types.Message{Offset: 0, Payload: "original", ProducerID: "p1", SeqNum: 1}
			require.NoError(t, p.ReplicaAppend([]types.Message{message}))
			executor := &uncertainCommitExecutor{barrierReplicationExecutor: base, attempts: make(chan partitionReplicationTask, 64), resolve: make(chan struct{})}
			close(base.barrier)
			queue := newPartitionReplicationCoordinator(1, executor)
			queue.uncertainCommitTimeout = 100 * time.Millisecond
			t.Cleanup(queue.close)
			reservation, err := queue.reserve(context.Background(), "orders", 0)
			require.NoError(t, err)
			task := replicationTaskForMode(base, ackpolicy.All)
			task.partitionRef = p
			task.command.Messages = []types.Message{message}
			task.releaseMutation = p.BeginReplicationMutation()
			released := make(chan struct{})
			task.releaseWrite = func() { close(released) }
			reservation.submit(task)
			select {
			case <-executor.attempts:
			case <-time.After(time.Second):
				t.Fatal("commit was not attempted")
			}
			// A lost commit reply must not release ownership or reuse offset zero.
			select {
			case <-released:
				t.Fatal("uncertain commit released write ownership")
			case <-time.After(60 * time.Millisecond):
			}
			require.Equal(t, uint64(1), p.NextOffset())
			require.Zero(t, p.GetHWM())
			var replacement error
			switch outcome {
			case "delayed-commit":
				close(executor.resolve)
			case "delayed-recovery":
				require.Eventually(t, func() bool {
					_, pending := p.PendingPartitionCommitState()
					return pending
				}, time.Second, 10*time.Millisecond)
				select {
				case <-released:
					t.Fatal("timed-out uncertain commit released write ownership")
				default:
				}
				close(executor.resolve)
			case "delayed-recovery-replaced":
				require.Eventually(t, func() bool {
					_, pending := p.PendingPartitionCommitState()
					return pending
				}, time.Second, 10*time.Millisecond)
				replacement = errors.New("new storage repair failure")
				p.MarkReconciliationPending(replacement)
				close(executor.resolve)
			case "delayed-recovery-replaced-fenced":
				require.Eventually(t, func() bool {
					_, pending := p.PendingPartitionCommitState()
					return pending
				}, time.Second, 10*time.Millisecond)
				replacement = errors.New("new storage repair failure")
				p.MarkReconciliationPending(replacement)
				base.mu.Lock()
				base.snapshot.LeaderEpoch++
				base.mu.Unlock()
			case "fenced":
				base.mu.Lock()
				base.snapshot.LeaderEpoch++
				base.mu.Unlock()
			case "metadata-deleted":
				base.mu.Lock()
				base.snapshotErr = clusterController.ErrPartitionMetadataNotFound
				base.mu.Unlock()
			case "shutdown":
				queue.close()
			}
			select {
			case err = <-task.result:
			case <-time.After(DefaultFSMApplyTimeout + 2*time.Second):
				t.Fatal("commit outcome did not resolve")
			}
			require.Equal(t, uint64(1), p.NextOffset())
			retained, readErr := p.ReadMessages(0, 1)
			require.NoError(t, readErr)
			require.Len(t, retained, 1)
			require.Equal(t, "original", retained[0].Payload)
			require.Equal(t, "p1", retained[0].ProducerID)
			require.Equal(t, uint64(1), retained[0].SeqNum)
			if outcome == "delayed-commit" || outcome == "delayed-recovery" {
				require.NoError(t, err)
				require.Equal(t, uint64(1), p.GetHWM())
				require.NoError(t, p.ReconciliationError())
				records, readErr := p.ReadCommitted(0, 1)
				require.NoError(t, readErr)
				require.Len(t, records, 1)
				require.Equal(t, "original", records[0].Payload)
			} else {
				require.Error(t, err)
				require.Error(t, p.ReconciliationError())
				if replacement != nil {
					require.ErrorIs(t, p.ReconciliationError(), replacement)
				}
				require.Error(t, p.EnqueueBatchLeaderWithMode([]types.Message{{Payload: "must not replace original"}}, true))
			}
			base.mu.Lock()
			require.Equal(t, 1, base.replicateCalls, "commit retry must not resend data")
			base.mu.Unlock()
			for len(executor.attempts) > 0 {
				retry := <-executor.attempts
				require.Equal(t, task.commitHWM, retry.commitHWM)
				require.Equal(t, task.command, retry.command)
				require.Equal(t, task.snapshot, retry.snapshot)
			}
		})
	}
}
