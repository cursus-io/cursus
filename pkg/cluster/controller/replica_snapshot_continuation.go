package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/topic"
)

// ReplicaSnapshotProgress belongs to one terminal receiver witness. It must
// never be reused with a newly built request, even at the same offset.
type ReplicaSnapshotProgress struct {
	AfterKey string
	Revision uint64
}

// ReplicaSnapshotStep transfers at most one bounded catalog page. A successful
// partial page advances progress without granting ISR admission.
type ReplicaSnapshotStep func(context.Context, fsm.ReplicaCatchupRequest, *topic.ReplicaCatchupWitness, *ReplicaSnapshotProgress) (bool, error)

type replicaSnapshotContinuation struct {
	request  fsm.ReplicaCatchupRequest
	witness  *topic.ReplicaCatchupWitness
	progress ReplicaSnapshotProgress
	complete bool
}

func (cc *ClusterController) SetReplicaSnapshotCatchupStep(step ReplicaSnapshotStep) {
	cc.snapshotStep = step
}

// The scheduler's run mutex owns this map and all progress. No background
// snapshot task can outlive cancellation or race a new request builder.
func (cc *ClusterController) continueReplicaSnapshots(ctx context.Context, key fsm.ReplicaCatchupCandidate, steps int) (err error) {
	pending := cc.snapshotPending[key]
	if pending == nil {
		return nil
	}
	defer func() {
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			delete(cc.snapshotPending, key)
		}
	}()
	request := pending.request
	for step := 0; step < steps; step++ {
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = cc.RaftManager.GetFSM().WithReplicaSnapshotRecoveryFenceContext(ctx, request, func() error { return nil }); err != nil {
			return err
		}
		if !pending.complete {
			before := pending.progress
			pending.complete, err = cc.snapshotStep(ctx, request, pending.witness, &pending.progress)
			if err != nil {
				return fmt.Errorf("event snapshot catch-up: %w", err)
			}
			if before.Revision != 0 && pending.progress.Revision != before.Revision {
				return fmt.Errorf("event snapshot catalog revision changed")
			}
			if !pending.complete && (pending.progress.Revision == 0 || pending.progress.AfterKey <= before.AfterKey) {
				return fmt.Errorf("event snapshot catch-up made no progress")
			}
			continue
		}
		if err = cc.RaftManager.GetFSM().MarkReplicaCatchupVerified(request, pending.witness); err != nil {
			return err
		}
		delete(cc.snapshotPending, key)
		if cc.proofSubmitter == nil {
			return nil
		}
		for _, proof := range cc.RaftManager.GetFSM().BuildISRCatchupProofs(request.BrokerID) {
			if proof.Topic == request.Topic && proof.Partition == request.Partition {
				proofCtx, cancel := context.WithTimeout(ctx, replicaCatchupProofSubmitTimeout)
				defer cancel()
				return cc.proofSubmitter(proofCtx, proof)
			}
		}
		return fmt.Errorf("replica proof eligibility changed")
	}
	return nil
}
