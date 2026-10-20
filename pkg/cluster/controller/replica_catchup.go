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

type ReplicaCatchupApplier func(context.Context, fsm.ReplicaCatchupBatch) error
type VerifiedReplicaCatchupApplier func(context.Context, fsm.ReplicaCatchupRequest, fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error)

const replicaCatchupFetchTimeout = 30 * time.Second
const replicaCatchupProofSubmitTimeout = 5 * time.Second
const replicaCatchupProofRetryBackoff = 25 * time.Millisecond

// StartReplicaCatchup periodically fills local replicas that are outside ISR.
// ISR admission remains separate and happens only through the existing proof
// emitted by a later heartbeat.
func (cc *ClusterController) StartReplicaCatchupVerified(ctx context.Context, fetcher ReplicaCatchupFetcher, apply VerifiedReplicaCatchupApplier) {
	if cc == nil || cc.RaftManager == nil || fetcher == nil || apply == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
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
	fsmState := cc.RaftManager.GetFSM()
	_ = fsmState.ReconcileReplicaMaterializations(cc.brokerID)
	requests := fsmState.BuildReplicaCatchupRequests(cc.brokerID)
	var catchupErr error
	for _, request := range requests {
		if err := ctx.Err(); err != nil {
			return errors.Join(catchupErr, err)
		}
		if err := cc.catchupReplica(ctx, fetcher, apply, request); err != nil {
			catchupErr = errors.Join(catchupErr, fmt.Errorf("%s-%d: %w", request.Topic, request.Partition, err))
		}
	}
	_ = fsmState.ReconcileReplicaMaterializations(cc.brokerID)
	return catchupErr
}

func (cc *ClusterController) catchupReplica(ctx context.Context, fetcher ReplicaCatchupFetcher, apply VerifiedReplicaCatchupApplier, request fsm.ReplicaCatchupRequest) error {
	discoveryPort := 0
	if cc.Config != nil {
		discoveryPort = cc.Config.DiscoveryPort
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		fetchCtx, cancel := context.WithTimeout(ctx, replicaCatchupFetchTimeout)
		batch, err := fetcher.FetchReplicaCatchup(fetchCtx, request.SourceAddress, discoveryPort, request)
		cancel()
		if err != nil {
			return err
		}
		if err := validateReplicaCatchupBatch(request, batch); err != nil {
			return err
		}
		witness, err := apply(ctx, request, batch)
		if err != nil {
			return err
		}

		if batch.TruncateTo != nil {
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
				refreshed, ok := cc.refreshReplicaCatchupRequest(request)
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
		if request.SnapshotCatchup {
			if cc.snapshotCatchup == nil {
				return fmt.Errorf("event snapshot catch-up is unavailable")
			}
			if err := cc.snapshotCatchup(ctx, request); err != nil {
				return fmt.Errorf("event snapshot catch-up: %w", err)
			}
		}
		if witness == nil {
			return fmt.Errorf("actual receiver verification witness is required")
		}
		if err := cc.RaftManager.GetFSM().MarkReplicaCatchupVerified(request, witness); err != nil {
			refreshed, ok := cc.refreshReplicaCatchupRequest(request)
			if !ok {
				return err
			}
			request = refreshed
			continue
		}
		if cc.proofSubmitter == nil {
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
			refreshed, ok := cc.refreshReplicaCatchupRequest(request)
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
		return nil
	}
}

func (cc *ClusterController) refreshReplicaCatchupRequest(previous fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupRequest, bool) {
	for _, candidate := range cc.RaftManager.GetFSM().BuildReplicaCatchupRequests(cc.brokerID) {
		if candidate.Topic == previous.Topic && candidate.Partition == previous.Partition && candidate.BrokerID == previous.BrokerID {
			return candidate, true
		}
	}
	return fsm.ReplicaCatchupRequest{}, false
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
	if err := fsm.ValidateReplicaRecoveryMode(batch); err != nil {
		return err
	}
	if batch.RecoveryProtocol != 0 {
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
