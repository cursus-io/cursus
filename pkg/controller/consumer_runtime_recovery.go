package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
)

var ErrConsumerRecoveryPending = errors.New("consumer metadata local recovery pending")

func (ch *CommandHandler) legacyConsumerRecoveryRequired() bool {
	return ch.Coordinator != nil && errors.Is(ch.Coordinator.ValidateConsumerMetadataBootstrapEligibility(0), coordinator.ErrConsumerMetadataBootstrapUnknown)
}

// ConsumerRoutingRecoveryReady advertises readiness for NEW strict routing
// activation, not authority for destructive recovery. Legacy recovery can serve
// requests without satisfying this stronger, deliberately conservative gate.
func (ch *CommandHandler) ConsumerRoutingRecoveryReady() bool {
	if ch == nil || ch.Coordinator == nil || ch.Coordinator.ConsumerRecoveryRequirement().Pending() ||
		ch.Coordinator.ConsumerOperationsReadinessError() != nil || ch.Coordinator.ValidateConsumerMetadataBootstrapEligibility(0) != nil {
		return false
	}
	epochs, err := ch.localGroupRecoveryEpochs()
	if err != nil {
		return false
	}
	for partition, epoch := range epochs {
		if !ch.consumerOwnershipRecovered(partition, epoch) {
			return false
		}
	}
	return true
}

type consumerRecoveryReader struct {
	legacyCoverageValidated bool
	measureReplay           bool
	indexTopicInstance      string
	repairGenerations       map[int]disk.ContentGeneration
	ctx                     context.Context
	views                   []*topic.PartitionReadView
	ends                    []uint64
	staged                  StagedConsumerMetadataRecovery
	metadata                []fsm.PartitionMetadata
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
func (r *consumerRecoveryReader) ReadCommittedConsumerMetadata(_ string, p int, offset uint64, max int) (messages []types.Message, err error) {
	defer func() { r.observeReplayRead(messages) }()
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
	return ch.recoverDistributedConsumerMetadata(ctx, false)
}

func (ch *CommandHandler) recoverDistributedConsumerMetadata(ctx context.Context, ownershipChange bool) error {
	if ch == nil || ch.Coordinator == nil || ch.Cluster == nil || ch.Cluster.RaftManager == nil || ch.Cluster.RaftManager.GetFSM() == nil {
		return fmt.Errorf("consumer metadata recovery dependencies unavailable")
	}
	if err := ch.ReconstructConsumerBootstrapTickets(); err != nil {
		return fmt.Errorf("%w: %v", ErrConsumerRecoveryPending, err)
	}
	requirement := ch.Coordinator.ConsumerRecoveryRequirement()
	if !requirement.Pending() && !ownershipChange {
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
	// coverage check. It requires full physical coverage under the final fence;
	// known authority must always use its independent historical baseline.
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
		return ch.withConsumerRecoveryPlan(ctx, plan, -1, nil, func(reader *consumerRecoveryReader) (replayErr error) {
			reader.measureReplay = true
			defer func() { metrics.RecordRecoveryAttempt(metrics.RecoveryConsumerReplay, replayErr) }()
			var prepared *coordinator.PreparedConsumerMetadataReplay
			var err error
			if plan.legacy {
				prepared, err = ch.Coordinator.PrepareFencedLegacyConsumerMetadataReplay(reader)
			} else {
				prepared, err = ch.Coordinator.PrepareFullConsumerMetadataReplay(reader)
			}
			if err != nil {
				return err
			}
			if !requirement.Pending() {
				unchanged := make(map[int]bool)
				ch.groupRecoveryMu.Lock()
				for partition, after := range plan.owned {
					before, ok := ch.groupRecoveryOwnership[partition]
					if ok && before.Epoch == after.Epoch && before.Lifecycle == after.Lifecycle && before.PartitionCount == after.PartitionCount {
						// Router partitions are reduced modulo an int partition count.
						unchanged[int(partition)] = true // #nosec G115
					}
				}
				ch.groupRecoveryMu.Unlock()
				route := func(group string) (int, error) {
					_, _, partition, _, err := ch.Cluster.Router.FindCoordinatorWithEpoch(group)
					return int(partition), err // #nosec G115 -- router bounds partition by its int partition count.
				}
				if err := ch.Coordinator.PreserveConsumerReplayClocks(prepared, route, unchanged); err != nil {
					return err
				}
			}
			epochs := make(map[uint64]int, len(plan.owned))
			for partition, proof := range plan.owned {
				epochs[partition] = proof.Epoch
			}
			if err := ch.Coordinator.ApplyFullConsumerMetadataReplayAndComplete(prepared); err != nil {
				return err
			}
			ch.groupRecoveryMu.Lock()
			ch.groupRecoveryEpoch = epochs
			if state.ConsumerMetadataRoutingEnabled() {
				ch.groupRecoveryOwnership = plan.owned
			} else {
				ch.groupRecoveryOwnership = nil
			}
			ch.groupRecoveryMu.Unlock()
			return nil
		})
	})
	if errors.Is(err, fsm.ErrConsumerMetadataFencePending) {
		return fmt.Errorf("%w: %v", ErrConsumerRecoveryPending, err)
	}
	return err
}

func (r *consumerRecoveryReader) observeReplayRead(messages []types.Message) {
	if !r.measureReplay {
		return
	}
	metrics.RecordRecoveryWork(metrics.RecoveryConsumerReplay, metrics.RecoveryPages, 1)
	metrics.RecordRecoveryBytes(metrics.RecoveryConsumerReplay, metrics.RecoveryDecodedFrames, int64(len(messages)))
	var decodedBytes int64
	for _, m := range messages {
		decodedBytes += int64(len(m.Key) + len(m.Payload))
	}
	metrics.RecordRecoveryBytes(metrics.RecoveryConsumerReplay, metrics.RecoveryDecodedBytes, decodedBytes)
	consumerReplayMeasurementFault(len(messages))
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
