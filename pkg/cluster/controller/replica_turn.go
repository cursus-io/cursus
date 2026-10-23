package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/topic"
)

// replicaTurn is owned by exactly one scheduler entry. Network waits never own
// an FSM/storage gate. The original request/witness survive a scheduling yield.
type replicaTurn struct {
	request      *fsm.ReplicaCatchupRequest
	witness      *topic.ReplicaCatchupWitness
	snapshot     ReplicaSnapshotProgress
	snapshotDone bool
	refinements  int
}

type replicaTurnResult struct {
	bytes         int64
	done          bool
	sourceFailure bool
	err           error
}

func (cc *ClusterController) advanceReplicaTurn(ctx context.Context, candidate fsm.ReplicaCatchupCandidate, turn *replicaTurn, fetcher ReplicaCatchupFetcher, apply VerifiedReplicaCatchupApplier) (result replicaTurnResult) {
	state := cc.RaftManager.GetFSM()
	if cc.replicaBootstrap != nil && cc.replicaBootstrap.ReplicaBootstrapExcluded(candidate.Topic, candidate.Partition) {
		result.done = true
		return
	}
	if turn.request == nil {
		r, ok, err := state.BuildReplicaCatchupRequestContext(ctx, cc.brokerID, candidate.Topic, candidate.Partition)
		result.err, result.done = err, err == nil && !ok
		if err == nil && ok {
			r.MaxBytes = int(cc.Config.RecoverySettings().QuantumBytes)
			turn.request = &r
		}
		return
	}
	r := *turn.request
	if turn.witness != nil {
		// Recheck authority even when a previous proof RPC failed or its reply
		// was lost. A changed HWM requires a new exact proof, not readmission.
		if err := state.WithReplicaSnapshotRecoveryFenceContext(ctx, r, func() error { return nil }); err != nil {
			*turn = replicaTurn{}
			result.err = err
			return
		}
		if r.SnapshotCatchup && !turn.snapshotDone {
			if cc.snapshotStep == nil {
				result.err = fmt.Errorf("resumable snapshot recovery unavailable")
				return
			}
			before := turn.snapshot
			turn.snapshotDone, result.err = cc.snapshotStep(ctx, r, turn.witness, &turn.snapshot)
			if result.err == nil && ((before.Revision != 0 && before.Revision != turn.snapshot.Revision) || (!turn.snapshotDone && (turn.snapshot.Revision == 0 || turn.snapshot.AfterKey <= before.AfterKey))) {
				result.err = fmt.Errorf("snapshot continuation changed or made no progress")
				*turn = replicaTurn{}
			}
			return
		}
		if result.err = state.MarkReplicaCatchupVerifiedContext(ctx, r, turn.witness); result.err != nil {
			*turn = replicaTurn{}
			return
		}
		if cc.proofSubmitter == nil {
			result.done = true
			return
		}
		proof, found, err := state.BuildISRCatchupProofContext(ctx, r.BrokerID, r.Topic, r.Partition)
		if err != nil {
			result.err = err
			return
		}
		if found {
			result.err = cc.proofSubmitter(ctx, proof)
			result.done = result.err == nil
			return
		}
		meta := state.GetPartitionMetadata(fmt.Sprintf("%s-%d", r.Topic, r.Partition))
		if meta != nil && containsBroker(meta.ISR, r.BrokerID) {
			result.done = true
			return
		}
		result.err = fmt.Errorf("replica proof eligibility changed")
		*turn = replicaTurn{}
		return
	}
	port := 0
	if cc.Config != nil {
		port = cc.Config.DiscoveryPort
	}
	batch, err := fetcher.FetchReplicaCatchup(ctx, r.SourceAddress, port, r)
	if err != nil {
		var network net.Error
		result.err = err
		result.sourceFailure = errors.As(err, &network) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded)
		return
	}
	encoded, err := json.Marshal(batch)
	if err != nil {
		result.err = err
		return
	}
	result.bytes = int64(len(encoded))
	if err = validateReplicaCatchupBatch(r, batch); err != nil {
		// Bootstrap retains its process-owned lifetime and storage admission.
		if errors.Is(err, fsm.ErrReplicaBootstrapRequired) && cc.replicaBootstrap != nil {
			result.err = cc.replicaBootstrap.RecoverReplicaBootstrap(ctx, r, batch)
			result.done = result.err == nil
			return
		}
		result.err = err
		*turn = replicaTurn{}
		return
	}
	if _, needed := fsm.ReplicaPrefixRefinement(r, batch); needed {
		turn.refinements++
		if turn.refinements > 64 {
			result.err = fmt.Errorf("indexed prefix refinement did not converge")
			*turn = replicaTurn{}
			return
		}
		refined, err := state.RefineReplicaPrefixContext(ctx, r, batch)
		result.err = err
		if err == nil {
			turn.request = &refined
		} else {
			*turn = replicaTurn{}
		}
		return
	}
	witness, err := apply(ctx, r, batch)
	if err != nil {
		result.err = err
		*turn = replicaTurn{}
		return
	}
	if batch.TruncateTo != nil {
		*turn = replicaTurn{}
		return
	}
	r.CommittedHWM = batch.CommittedHWM
	if result.err = fsm.AdvanceReplicaCatchupRequest(&r, batch); result.err != nil {
		*turn = replicaTurn{}
		return
	}
	if r.NextOffset != r.CommittedHWM {
		*turn = replicaTurn{}
		return
	}
	if !batch.Verified || witness == nil {
		result.err = fmt.Errorf("terminal replica verification witness missing")
		*turn = replicaTurn{}
		return
	}
	turn.request, turn.witness = &r, witness
	return
}
