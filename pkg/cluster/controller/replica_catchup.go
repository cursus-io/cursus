package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/util"
)

type ReplicaCatchupFetcher interface {
	FetchReplicaCatchup(context.Context, string, int, fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error)
}

type ReplicaCatchupApplier func(fsm.ReplicaCatchupBatch) error

// StartReplicaCatchup periodically fills local replicas that are outside ISR.
// ISR admission remains separate and happens only through the existing proof
// emitted by a later heartbeat.
func (cc *ClusterController) StartReplicaCatchup(ctx context.Context, fetcher ReplicaCatchupFetcher, apply ReplicaCatchupApplier) {
	if cc == nil || cc.RaftManager == nil || fetcher == nil || apply == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			if err := cc.RunReplicaCatchupOnce(ctx, fetcher, apply); err != nil {
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

func (cc *ClusterController) RunReplicaCatchupOnce(ctx context.Context, fetcher ReplicaCatchupFetcher, apply ReplicaCatchupApplier) error {
	if cc == nil || cc.RaftManager == nil || cc.RaftManager.GetFSM() == nil {
		return fmt.Errorf("FSM is unavailable")
	}
	if fetcher == nil || apply == nil {
		return fmt.Errorf("replica catch-up dependencies are unavailable")
	}
	requests := cc.RaftManager.GetFSM().BuildReplicaCatchupRequests(cc.brokerID)
	var catchupErr error
	for _, request := range requests {
		if err := cc.catchupReplica(ctx, fetcher, apply, request); err != nil {
			catchupErr = errors.Join(catchupErr, fmt.Errorf("%s-%d: %w", request.Topic, request.Partition, err))
		}
	}
	return catchupErr
}

func (cc *ClusterController) catchupReplica(ctx context.Context, fetcher ReplicaCatchupFetcher, apply ReplicaCatchupApplier, request fsm.ReplicaCatchupRequest) error {
	discoveryPort := 0
	if cc.Config != nil {
		discoveryPort = cc.Config.DiscoveryPort
	}
	for request.NextOffset < request.CommittedHWM {
		fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		batch, err := fetcher.FetchReplicaCatchup(fetchCtx, request.LeaderAddress, discoveryPort, request)
		cancel()
		if err != nil {
			return err
		}
		if err := validateReplicaCatchupBatch(request, batch); err != nil {
			return err
		}
		if err := apply(batch); err != nil {
			return err
		}
		nextOffset := batch.EndOffset
		if nextOffset == 0 && len(batch.Messages) > 0 {
			nextOffset = batch.Messages[len(batch.Messages)-1].Offset + 1
		}
		request.NextOffset = nextOffset
	}
	return nil
}

func validateReplicaCatchupBatch(request fsm.ReplicaCatchupRequest, batch fsm.ReplicaCatchupBatch) error {
	if batch.Topic != request.Topic || batch.Partition != request.Partition || batch.BrokerID != request.BrokerID {
		return fmt.Errorf("replica catch-up response identity mismatch")
	}
	if batch.Leader != request.Leader || batch.LeaderEpoch != request.LeaderEpoch || batch.LifecycleEpoch != request.LifecycleEpoch {
		return fmt.Errorf("replica catch-up response fence mismatch")
	}
	if batch.CommittedHWM != request.CommittedHWM || batch.StartOffset != request.NextOffset {
		return fmt.Errorf("replica catch-up response boundary mismatch")
	}
	if len(batch.Messages) > request.MaxRecords || (!batch.Compacted && len(batch.Messages) == 0) {
		return fmt.Errorf("invalid replica catch-up batch size %d", len(batch.Messages))
	}
	endOffset := batch.EndOffset
	if endOffset == 0 && len(batch.Messages) > 0 {
		endOffset = batch.Messages[len(batch.Messages)-1].Offset + 1
	}
	if endOffset <= request.NextOffset || endOffset > request.CommittedHWM {
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
