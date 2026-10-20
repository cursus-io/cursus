package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
)

var ErrConsumerRecoveryPending = errors.New("consumer metadata local recovery pending")

type consumerRecoveryReader struct {
	ctx    context.Context
	views  []*topic.PartitionReadView
	ends   []uint64
	staged StagedConsumerMetadataRecovery
}

func (r *consumerRecoveryReader) ExistingPartitionCount(string) (int, error) {
	return len(r.views), r.ctx.Err()
}
func (r *consumerRecoveryReader) EarliestTopicOffset(_ string, p int) (uint64, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.views[p] == nil {
		return r.staged.EarliestTopicOffset(config.ConsumerOffsetsTopicName, p)
	}
	return r.views[p].FirstOffset(), r.ctx.Err()
}
func (r *consumerRecoveryReader) EndTopicOffset(_ string, p int) (uint64, error) {
	return r.ends[p], r.ctx.Err()
}
func (r *consumerRecoveryReader) ReadCommittedConsumerMetadata(_ string, p int, offset uint64, max int) ([]types.Message, error) {
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	if r.views[p] == nil {
		return r.staged.ReadCommittedConsumerMetadata(config.ConsumerOffsetsTopicName, p, offset, max)
	}
	return r.views[p].ReadCommittedMetadataThrough(offset, r.ends[p], max)
}

// RecoverDistributedConsumerMetadata makes one bounded local attempt. Waiting
// for topology, Restore, catchup or pending installation owns no exclusive lease.
func (ch *CommandHandler) RecoverDistributedConsumerMetadata(ctx context.Context) error {
	if ch == nil || ch.Coordinator == nil || ch.Cluster == nil || ch.Cluster.RaftManager == nil || ch.Cluster.RaftManager.GetFSM() == nil {
		return fmt.Errorf("consumer metadata recovery dependencies unavailable")
	}
	if err := ch.ReconstructConsumerBootstrapTickets(); err != nil {
		return fmt.Errorf("%w: %v", ErrConsumerRecoveryPending, err)
	}
	requirement := ch.Coordinator.ConsumerRecoveryRequirement()
	if !requirement.Pending() {
		return ch.consumerReadinessError()
	}
	if requirement.Installing {
		return ErrConsumerRecoveryPending
	}
	if requirement.Failure != nil {
		return requirement.Failure
	}
	state := ch.Cluster.RaftManager.GetFSM()
	metadata, err := state.ConsumerMetadataRecoverySnapshot()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrConsumerRecoveryPending, err)
	}
	// Compatibility replay is selected by unknown authority, never by a failed
	// coverage check. The coordinator also requires a pending startup/restore
	// generation, so known authority cannot fall back to a partial local overlay.
	legacy := errors.Is(ch.Coordinator.ValidateConsumerMetadataBootstrapEligibility(0), coordinator.ErrConsumerMetadataBootstrapUnknown)
	plan, err := ch.prepareConsumerRecoveryPlan(ctx, metadata, -1, legacy)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrConsumerRecoveryPending, err)
	}
	defer plan.close()
	attempt, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	release, err := ch.Coordinator.BeginConsumerMetadataRecovery(attempt)
	if err != nil {
		return err
	}
	defer release()
	if !ch.groupRecoveryReloadMu.TryLock() {
		return ErrConsumerRecoveryPending
	}
	defer ch.groupRecoveryReloadMu.Unlock()
	err = state.WithConsumerMetadataRecoveryFence(func(metadata []fsm.PartitionMetadata) error {
		current := ch.Coordinator.ConsumerRecoveryRequirement()
		if current.RequiredGeneration != requirement.RequiredGeneration || current.Installing || current.Failure != nil {
			return ErrConsumerRecoveryPending
		}
		if err := ch.validateConsumerRecoveryPlan(plan, metadata); err != nil {
			return err
		}
		return ch.withConsumerRecoveryPlan(ctx, plan, -1, nil, func(reader *consumerRecoveryReader) error {
			var prepared *coordinator.PreparedConsumerMetadataReplay
			var err error
			if plan.legacy {
				prepared, err = ch.Coordinator.PrepareLegacyConsumerMetadataReplay(reader)
			} else {
				prepared, err = ch.Coordinator.PrepareFullConsumerMetadataReplay(reader)
			}
			if err != nil {
				return err
			}
			if err := ch.Coordinator.ApplyFullConsumerMetadataReplay(prepared); err != nil {
				return err
			}
			epochs, err := ch.localGroupRecoveryEpochs()
			if err != nil {
				return err
			}
			ch.groupRecoveryMu.Lock()
			ch.groupRecoveryEpoch = epochs
			ch.groupRecoveryMu.Unlock()
			return ch.Coordinator.CompleteConsumerRecovery(current.RequiredGeneration)
		})
	})
	if errors.Is(err, fsm.ErrConsumerMetadataFencePending) {
		return fmt.Errorf("%w: %v", ErrConsumerRecoveryPending, err)
	}
	return err
}

func (ch *CommandHandler) consumerReadinessError() error {
	if ch == nil || !ch.isDistributed() || ch.Coordinator == nil {
		return nil
	}
	return ch.Coordinator.ConsumerOperationsReadinessError()
}

func (ch *CommandHandler) consumerCommandReadinessError(input commandInput) error {
	switch input.Name {
	case "GROUP", "REGISTER_GROUP", "JOIN_GROUP", "SYNC_GROUP", "LEAVE_GROUP", "LIST_GROUPS", "GROUP_STATUS", "COMMIT", "COMMIT_OFFSET", "BATCH_COMMIT", "FETCH_OFFSET", "HEARTBEAT", "FETCH", "CONSUME", "STREAM", "INIT_PRODUCER_ID", "SEND_OFFSETS_TO_TXN", "END_TXN":
		return ch.consumerReadinessError()
	case "PUBLISH", "BATCH_PUBLISH":
		if input.Args["topic"] == config.ConsumerOffsetsTopicName {
			return ch.consumerReadinessError()
		}
	}
	return nil
}
