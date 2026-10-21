package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/util"
)

type ReplicaCatchupFetcher interface {
	FetchReplicaCatchup(context.Context, string, int, fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error)
}

// ReplicaBootstrapRecovery owns asynchronous full transfers and local pending
// recovery. Configure before starting the catch-up loop; capability is separate.
type ReplicaBootstrapRecovery interface {
	RecoverReplicaBootstrap(context.Context, fsm.ReplicaCatchupRequest, fsm.ReplicaCatchupBatch) error
	ResumePendingReplicaBootstraps(context.Context) (map[string]bool, error)
	ReplicaBootstrapExcluded(string, int) bool
}

func (cc *ClusterController) SetReplicaBootstrapRecovery(recovery ReplicaBootstrapRecovery) {
	cc.replicaBootstrap = recovery
}

type ReplicaCatchupApplier func(context.Context, fsm.ReplicaCatchupBatch) error
type VerifiedReplicaCatchupApplier func(context.Context, fsm.ReplicaCatchupRequest, fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error)

const replicaCatchupFetchTimeout = 30 * time.Second
const replicaCatchupProofSubmitTimeout = 5 * time.Second
const replicaCatchupProofRetryBackoff = 25 * time.Millisecond
const replicaCatchupTurnTimeout = 30 * time.Second
const replicaCatchupTurnSteps = 64

// StartReplicaCatchup periodically fills local replicas that are outside ISR.
// ISR admission remains separate and happens only through the existing proof
// emitted by a later heartbeat.
func (cc *ClusterController) StartReplicaCatchupVerified(ctx context.Context, fetcher ReplicaCatchupFetcher, apply VerifiedReplicaCatchupApplier) {
	if cc == nil || cc.RaftManager == nil || fetcher == nil || apply == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cc.catchupMu.Lock()
	if cc.catchupStopped || cc.catchupDone != nil {
		cc.catchupMu.Unlock()
		return
	}
	ctx, cc.catchupCancel = context.WithCancel(ctx)
	done := make(chan struct{})
	cc.catchupDone = done
	cc.catchupMu.Unlock()
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			if ctx.Err() != nil {
				return
			}
			if err := cc.RunReplicaCatchupOnceVerified(ctx, fetcher, apply); err != nil {
				util.Debug("replica catch-up pending on %s: %v", cc.brokerID, err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// StopReplicaCatchup cancels and joins the single owned loop before its storage
// and transport dependencies close. It also prevents a subsequent start.
func (cc *ClusterController) StopReplicaCatchup() {
	if cc == nil {
		return
	}
	cc.catchupMu.Lock()
	cc.catchupStopped = true
	if cc.catchupCancel != nil {
		cc.catchupCancel()
	}
	done := cc.catchupDone
	cc.catchupMu.Unlock()
	if done != nil {
		<-done
	}
}

func (cc *ClusterController) RunReplicaCatchupOnceVerified(ctx context.Context, fetcher ReplicaCatchupFetcher, apply VerifiedReplicaCatchupApplier) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if cc == nil || cc.RaftManager == nil || cc.RaftManager.GetFSM() == nil {
		return fmt.Errorf("FSM is unavailable")
	}
	if fetcher == nil || apply == nil {
		return fmt.Errorf("replica catch-up dependencies are unavailable")
	}
	if !cc.catchupRunMu.TryLock() {
		return fmt.Errorf("replica catch-up pass already running")
	}
	defer cc.catchupRunMu.Unlock()
	fsmState := cc.RaftManager.GetFSM()
	var catchupErr error
	var excluded map[string]bool
	if cc.replicaBootstrap != nil {
		excluded, catchupErr = cc.replicaBootstrap.ResumePendingReplicaBootstraps(ctx)
	}
	_ = fsmState.ReconcileReplicaMaterializations(cc.brokerID)
	candidates := fsmState.ReplicaCatchupCandidates(cc.brokerID)
	current := make(map[fsm.ReplicaCatchupCandidate]bool, len(candidates))
	for _, candidate := range candidates {
		current[candidate] = true
	}
	for key := range cc.snapshotPending {
		if !current[key] {
			delete(cc.snapshotPending, key)
		}
	}
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return errors.Join(catchupErr, err)
		}
		if excluded[fmt.Sprintf("%s-%d", candidate.Topic, candidate.Partition)] || (cc.replicaBootstrap != nil && cc.replicaBootstrap.ReplicaBootstrapExcluded(candidate.Topic, candidate.Partition)) {
			delete(cc.snapshotPending, candidate)
			continue
		}
		turnCtx, cancel := context.WithTimeout(ctx, replicaCatchupTurnTimeout)
		var err error
		if cc.snapshotPending[candidate] != nil {
			err = cc.continueReplicaSnapshots(turnCtx, candidate, replicaCatchupTurnSteps)
		} else {
			var request fsm.ReplicaCatchupRequest
			var ok bool
			request, ok, err = fsmState.BuildReplicaCatchupRequestContext(turnCtx, cc.brokerID, candidate.Topic, candidate.Partition)
			if err == nil && ok {
				err = cc.catchupReplicaTurn(turnCtx, fetcher, apply, request)
			}
		}
		cancel()
		if err != nil {
			catchupErr = errors.Join(catchupErr, fmt.Errorf("%s-%d: %w", candidate.Topic, candidate.Partition, err))
		}
	}
	_ = fsmState.ReconcileReplicaMaterializations(cc.brokerID)
	return catchupErr
}

func (cc *ClusterController) catchupReplica(ctx context.Context, fetcher ReplicaCatchupFetcher, apply VerifiedReplicaCatchupApplier, request fsm.ReplicaCatchupRequest) error {
	turnCtx, cancel := context.WithTimeout(ctx, replicaCatchupTurnTimeout)
	defer cancel()
	return cc.catchupReplicaTurn(turnCtx, fetcher, apply, request)
}

func (cc *ClusterController) catchupReplicaTurn(ctx context.Context, fetcher ReplicaCatchupFetcher, apply VerifiedReplicaCatchupApplier, request fsm.ReplicaCatchupRequest) (result error) {
	started, start := time.Now(), request.NextOffset
	phase, exchanges, refinements := "incremental", 0, 0
	defer func() {
		var lag uint64
		if request.CommittedHWM > request.NextOffset {
			lag = request.CommittedHWM - request.NextOffset
		}
		util.Debug("replica_recovery phase=%s protocol=%d topic=%q partition=%d source=%q leader_epoch=%d lifecycle=%d start=%d current=%d target=%d remaining=%d exchanges=%d elapsed_ms=%d error=%q", phase, request.RecoveryProtocol, request.Topic, request.Partition, request.SourceBroker, request.LeaderEpoch, request.LifecycleEpoch, start, request.NextOffset, request.CommittedHWM, lag, exchanges, time.Since(started).Milliseconds(), fmt.Sprint(result))
	}()
	discoveryPort := 0
	if cc.Config != nil {
		discoveryPort = cc.Config.DiscoveryPort
	}
	for step := 0; step < replicaCatchupTurnSteps; step++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		fetchCtx, cancel := context.WithTimeout(ctx, replicaCatchupFetchTimeout)
		batch, err := fetcher.FetchReplicaCatchup(fetchCtx, request.SourceAddress, discoveryPort, request)
		exchanges++
		cancel()
		if err != nil {
			return err
		}
		if err := validateReplicaCatchupBatch(request, batch); err != nil {
			if errors.Is(err, fsm.ErrReplicaBootstrapRequired) && cc.replicaBootstrap != nil {
				phase = "bootstrap_required"
				return cc.replicaBootstrap.RecoverReplicaBootstrap(ctx, request, batch)
			}
			return err
		}
		if _, needed := fsm.ReplicaPrefixRefinement(request, batch); needed {
			phase = "prefix_refinement"
			refinements++
			if refinements > 64 {
				return fmt.Errorf("indexed prefix refinement did not converge")
			}
			refined, err := cc.RaftManager.GetFSM().RefineReplicaPrefixContext(ctx, request, batch)
			if err != nil {
				return err
			}
			request = refined
			// Up to 64 read-only probes cover any uint64 interval. Keep them
			// separate from the bounded mutation steps so the final adjacent
			// proof can still be applied; the same turn deadline bounds both.
			step--
			continue
		}
		witness, err := apply(ctx, request, batch)
		if err != nil {
			return err
		}

		if batch.TruncateTo != nil {
			phase = "divergence_repaired"
			request.NextOffset = *batch.TruncateTo
			return nil
		}
		// The committed HWM may advance while a live partition is being
		// recovered. Treat the source's authenticated HWM as the next target
		// instead of restarting from the old boundary on every write.
		request.CommittedHWM = batch.CommittedHWM
		if err := fsm.AdvanceReplicaCatchupRequest(&request, batch); err != nil {
			return err
		}
		nextOffset := request.NextOffset
		if nextOffset != request.CommittedHWM {
			{
				if step+1 == replicaCatchupTurnSteps {
					return nil
				}
				refreshed, ok := cc.refreshReplicaCatchupRequestContext(ctx, request)
				if !ok || refreshed.RecoveryProtocol != request.RecoveryProtocol {
					return fmt.Errorf("indexed recovery proofs could not be refreshed")
				}
				request = refreshed
			}
			continue
		}
		if !batch.Verified {
			return fmt.Errorf("replica catch-up completed without prefix verification")
		}
		if witness == nil {
			return fmt.Errorf("actual receiver verification witness is required")
		}
		if request.SnapshotCatchup {
			phase = "snapshot_replay"
			if cc.snapshotStep != nil {
				if cc.snapshotPending == nil {
					cc.snapshotPending = make(map[fsm.ReplicaCatchupCandidate]*replicaSnapshotContinuation)
				}
				key := fsm.ReplicaCatchupCandidate{Topic: request.Topic, Partition: request.Partition}
				cc.snapshotPending[key] = &replicaSnapshotContinuation{request: request, witness: witness}
				return cc.continueReplicaSnapshots(ctx, key, replicaCatchupTurnSteps-step-1)
			}
			if cc.snapshotCatchup == nil {
				return fmt.Errorf("event snapshot catch-up is unavailable")
			}
			if err := cc.snapshotCatchup(ctx, request, witness); err != nil {
				return fmt.Errorf("event snapshot catch-up: %w", err)
			}
		}
		if err := cc.RaftManager.GetFSM().MarkReplicaCatchupVerified(request, witness); err != nil {
			if step+1 == replicaCatchupTurnSteps {
				return err
			}
			refreshed, ok := cc.refreshReplicaCatchupRequestContext(ctx, request)
			if !ok {
				return err
			}
			request = refreshed
			continue
		}
		if cc.proofSubmitter == nil {
			phase = "prefix_verified"
			return nil
		}
		var proof fsm.ISRCatchupProof
		found := false
		for _, candidate := range cc.RaftManager.GetFSM().BuildISRCatchupProofs(request.BrokerID) {
			if candidate.Topic == request.Topic && candidate.Partition == request.Partition {
				proof = candidate
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("replica proof eligibility changed")
		}
		proofCtx, cancel := context.WithTimeout(ctx, replicaCatchupProofSubmitTimeout)
		err = cc.proofSubmitter(proofCtx, proof)
		cancel()
		if err != nil {
			if step+1 == replicaCatchupTurnSteps {
				return fmt.Errorf("submit ISR catch-up proof: %w", err)
			}
			refreshed, ok := cc.refreshReplicaCatchupRequestContext(ctx, request)
			if !ok {
				metadata := cc.RaftManager.GetFSM().GetPartitionMetadata(fmt.Sprintf("%s-%d", request.Topic, request.Partition))
				if metadata != nil && containsBroker(metadata.ISR, request.BrokerID) {
					return nil
				}
				return fmt.Errorf("submit ISR catch-up proof: %w", err)
			}
			if refreshed.Leader != request.Leader || refreshed.LeaderEpoch != request.LeaderEpoch ||
				refreshed.LifecycleEpoch != request.LifecycleEpoch {
				return fmt.Errorf("submit ISR catch-up proof after fence change: %w", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(replicaCatchupProofRetryBackoff):
			}
			request = refreshed
			continue
		}
		phase = "proof_submitted"
		return nil
	}
	return nil
}

func (cc *ClusterController) refreshReplicaCatchupRequest(previous fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupRequest, bool) {
	return cc.refreshReplicaCatchupRequestContext(context.Background(), previous)
}

func (cc *ClusterController) refreshReplicaCatchupRequestContext(ctx context.Context, previous fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupRequest, bool) {
	// Refresh only the requested partition. Building another partition's
	// request can mutate its tail/HWM, including a bootstrap completed this pass.
	if cc.replicaBootstrap != nil && cc.replicaBootstrap.ReplicaBootstrapExcluded(previous.Topic, previous.Partition) {
		return fsm.ReplicaCatchupRequest{}, false
	}
	candidate, ok, err := cc.RaftManager.GetFSM().BuildReplicaCatchupRequestContext(ctx, cc.brokerID, previous.Topic, previous.Partition)
	return candidate, err == nil && ok && candidate.BrokerID == previous.BrokerID
}

func validateReplicaCatchupBatch(request fsm.ReplicaCatchupRequest, batch fsm.ReplicaCatchupBatch) error {
	if batch.Topic != request.Topic || batch.Partition != request.Partition || batch.BrokerID != request.BrokerID {
		return fmt.Errorf("replica catch-up response identity mismatch")
	}
	if batch.Leader != request.Leader || batch.SourceBroker != request.SourceBroker ||
		batch.LeaderEpoch != request.LeaderEpoch || batch.LifecycleEpoch != request.LifecycleEpoch {
		return fmt.Errorf("replica catch-up response fence mismatch")
	}
	if err := fsm.ValidateReplicaCatchupBatchDigest(batch); err != nil {
		return err
	}
	if batch.RecoveryProtocol != request.RecoveryProtocol {
		return fmt.Errorf("replica catch-up response protocol mismatch")
	}
	if batch.CommittedHWM < request.CommittedHWM || batch.StartOffset != request.NextOffset {
		return fmt.Errorf("replica catch-up response boundary mismatch")
	}
	if batch.RecoveryProtocol == fsm.BootstrapRecoveryProtocolVersion {
		if request.RetainedPrefix == nil || batch.RetainedPrefix == nil || *request.RetainedPrefix != *batch.RetainedPrefix {
			return fmt.Errorf("retained recovery response challenge mismatch")
		}
	}
	if err := fsm.ValidateReplicaRecoveryMode(batch); err != nil {
		return err
	}
	if batch.RecoveryProtocol == fsm.IndexedRecoveryProtocolVersion {
		found := false
		for _, candidate := range request.PrefixCandidates {
			if candidate == *batch.MatchedPrefix {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("matched recovery prefix was not requested")
		}
	}
	if batch.TruncateTo != nil {
		if len(batch.Messages) != 0 || batch.Verified || *batch.TruncateTo >= batch.StartOffset || batch.EndOffset != batch.StartOffset {
			return fmt.Errorf("invalid replica catch-up truncation boundary")
		}
		return nil
	}
	if batch.Verified && batch.StartOffset == batch.CommittedHWM {
		if len(batch.Messages) != 0 || batch.EndOffset != batch.StartOffset {
			return fmt.Errorf("invalid replica catch-up verification batch")
		}
		return nil
	}
	if len(batch.Messages) > request.MaxRecords || (!batch.Compacted && len(batch.Messages) == 0) {
		return fmt.Errorf("invalid replica catch-up batch size %d", len(batch.Messages))
	}
	endOffset := batch.EndOffset
	if endOffset == 0 && len(batch.Messages) > 0 {
		endOffset = batch.Messages[len(batch.Messages)-1].Offset + 1
	}
	if endOffset <= request.NextOffset || endOffset > batch.CommittedHWM {
		return fmt.Errorf("invalid replica catch-up end offset %d", endOffset)
	}
	next := request.NextOffset
	for _, message := range batch.Messages {
		if message.Offset >= endOffset || (!batch.Compacted && message.Offset != next) || (batch.Compacted && message.Offset < next) {
			return fmt.Errorf("invalid replica catch-up offset: expected=%d got=%d hwm=%d", next, message.Offset, request.CommittedHWM)
		}
		next = message.Offset + 1
	}
	return nil
}

// ValidateReplicaCatchupBatch validates the original request before local apply.
func ValidateReplicaCatchupBatch(request fsm.ReplicaCatchupRequest, batch fsm.ReplicaCatchupBatch) error {
	return validateReplicaCatchupBatch(request, batch)
}

// The legacy callback API can still apply data, but never creates admission
// authority. Callers must migrate to the typed verified API for ISR recovery.
func legacyReplicaCatchupApplier(apply ReplicaCatchupApplier) VerifiedReplicaCatchupApplier {
	if apply == nil {
		return nil
	}
	return func(ctx context.Context, _ fsm.ReplicaCatchupRequest, batch fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
		return nil, apply(ctx, batch)
	}
}
func (cc *ClusterController) StartReplicaCatchup(ctx context.Context, fetcher ReplicaCatchupFetcher, apply ReplicaCatchupApplier) {
	cc.StartReplicaCatchupVerified(ctx, fetcher, legacyReplicaCatchupApplier(apply))
}
func (cc *ClusterController) RunReplicaCatchupOnce(ctx context.Context, fetcher ReplicaCatchupFetcher, apply ReplicaCatchupApplier) error {
	return cc.RunReplicaCatchupOnceVerified(ctx, fetcher, legacyReplicaCatchupApplier(apply))
}
