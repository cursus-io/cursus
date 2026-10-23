package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
)

type consumerRawView struct {
	view      *topic.PartitionReadView
	partition int
	end       uint64
}

// ReconstructConsumerBootstrapTickets runs before ordinary startup/monitor
// recovery. Manifest errors retain an unknown barrier instead of reopening.
func (ch *CommandHandler) ReconstructConsumerBootstrapTickets() error {
	if ch == nil || !ch.isDistributed() || ch.Coordinator == nil || ch.TopicManager == nil {
		return nil
	}
	t := ch.TopicManager.GetTopic(config.ConsumerOffsetsTopicName)
	if t == nil {
		return nil
	}
	// Offsets topic delete/truncate and definition lifecycle replacement or
	// partition decrease are prohibited. Normal materialization preserves these
	// partition objects, so this scan may retain the topic reference without a
	// controller lock; observation revisions fence concurrent ticket completion.
	definition := t.Definition()
	for i := 0; i < definition.Partitions; i++ {
		observation := ch.Coordinator.CaptureConsumerBootstrapObservation()
		p, err := t.GetPartition(i)
		if err != nil {
			observeErr := ch.Coordinator.ObservePendingConsumerBootstrap(coordinator.ConsumerBootstrapIdentity{Partition: i, Lifecycle: definition.LifecycleEpoch}, observation)
			return errors.Join(err, observeErr)
		}
		if !p.BootstrapPending() {
			continue
		}
		manifest, statusErr := p.BootstrapStatus()
		identity := coordinator.ConsumerBootstrapIdentity{Partition: i, Lifecycle: definition.LifecycleEpoch}
		if manifest != nil {
			identity.Attempt = manifest.TransferID
			identity.Lifecycle = manifest.Fence.LifecycleEpoch
		}
		if err := ch.Coordinator.ObservePendingConsumerBootstrap(identity, observation); err != nil {
			if errors.Is(err, coordinator.ErrConsumerBootstrapObservationStale) {
				return fmt.Errorf("%w: %v", ErrConsumerRecoveryPending, err)
			}
			return err
		}
		if manifest == nil {
			return errors.Join(fmt.Errorf("pending consumer bootstrap manifest unavailable"), statusErr)
		}
	}
	return nil
}

func (r *consumerRawView) EarliestTopicOffset(_ string, p int) (uint64, error) {
	if p != r.partition {
		return 0, fmt.Errorf("raw metadata partition mismatch")
	}
	return r.view.FirstOffset(), nil
}
func (r *consumerRawView) EndTopicOffset(_ string, p int) (uint64, error) {
	if p != r.partition {
		return 0, fmt.Errorf("raw metadata partition mismatch")
	}
	return r.end, nil
}
func (r *consumerRawView) ReadTopicPartition(_ string, p int, offset uint64, max int) ([]types.Message, error) {
	if p != r.partition {
		return nil, fmt.Errorf("raw metadata partition mismatch")
	}
	return r.view.ReadMessages(offset, max)
}

// RunConsumerMetadataBootstrap installs/resumes already staged local data. It
// stages any required nonlocal metadata before taking locks and does not itself
// advertise or authorize a transfer protocol. The optional validate callback
// runs inside the final FSM fence before ticket creation or disk intent, with
// resuming=true for an existing durable pending partition. Protocol callers use
// it to validate source/receiver/capability fences with distinct resume rules.
// All install callbacks must be local and cannot acquire lifecycle
// gates, reconcile topics, or call Raft. The exclusive drain precedes every gate.
func (ch *CommandHandler) RunConsumerMetadataBootstrap(ctx context.Context, identity coordinator.ConsumerBootstrapIdentity, receiver *disk.BootstrapReceiver, detach func() error, rebuild func(*topic.PartitionReadView, []byte) error, validate ...func(resuming bool) error) error {
	if len(validate) > 1 {
		return fmt.Errorf("only one consumer bootstrap validator is allowed")
	}
	if ch == nil || ch.Coordinator == nil || ch.TopicManager == nil || ch.Cluster == nil || ch.Cluster.RaftManager == nil || ch.Cluster.RaftManager.GetFSM() == nil || ch.Cluster.Router == nil {
		return fmt.Errorf("consumer bootstrap dependencies unavailable")
	}
	if err := ch.ReconstructConsumerBootstrapTickets(); err != nil {
		return err
	}
	requirement := ch.Coordinator.ConsumerRecoveryRequirement()
	if requirement.Installing || requirement.Failure != nil {
		return ErrConsumerRecoveryPending
	}
	if receiver != nil {
		m := receiver.Manifest()
		if m.TransferID != identity.Attempt || m.Fence.Partition != identity.Partition || m.Fence.Topic != config.ConsumerOffsetsTopicName || m.Fence.LifecycleEpoch != identity.Lifecycle {
			return fmt.Errorf("consumer bootstrap transfer identity mismatch")
		}
	}
	metadata, err := ch.Cluster.RaftManager.GetFSM().ConsumerMetadataBootstrapSnapshot()
	if err != nil {
		return err
	}
	plan, err := ch.prepareConsumerRecoveryPlan(ctx, metadata, identity.Partition, false)
	if err != nil {
		return err
	}
	defer plan.close()
	// Dependency staging can perform network transfers and atomic storage work.
	// Reserve installation only after that work returns, before any drain/FSM/
	// storage gate. Otherwise a one-slot budget deadlocks its own provider.
	source := "consumer-bootstrap-install"
	if receiver != nil {
		source = receiver.Manifest().Fence.SourceID
	} else if local := ch.TopicManager.GetTopic(config.ConsumerOffsetsTopicName); local != nil {
		if p, err := local.GetPartition(identity.Partition); err == nil {
			if manifest, _ := p.BootstrapStatus(); manifest != nil {
				source = manifest.Fence.SourceID
			}
		}
	}
	releaseWork, err := ch.recoveryBudget.AcquireWork(ctx, source)
	if err != nil {
		return err
	}
	defer releaseWork()
	release, err := ch.Coordinator.BeginConsumerMetadataRecovery(ctx)
	if err != nil {
		return err
	}
	defer release()
	if !ch.groupRecoveryReloadMu.TryLock() {
		return ErrConsumerRecoveryPending
	}
	defer ch.groupRecoveryReloadMu.Unlock()
	releaseLifecycle, err := ch.topicLifecycleGates.acquire(ctx, config.ConsumerOffsetsTopicName, false)
	if err != nil {
		return err
	}
	defer releaseLifecycle()
	writeLock := ch.partitionWriteLock(config.ConsumerOffsetsTopicName, identity.Partition)
	if err := writeLock.LockContext(ctx); err != nil {
		return err
	}
	defer writeLock.Unlock()
	return ch.Cluster.RaftManager.GetFSM().WithConsumerMetadataBootstrapFence(identity.Partition, identity.Lifecycle, func(metadata []fsm.PartitionMetadata) error {
		if err := ch.validateConsumerRecoveryPlan(plan, metadata); err != nil {
			return err
		}
		if len(validate) == 1 && validate[0] != nil {
			t := ch.TopicManager.GetTopic(config.ConsumerOffsetsTopicName)
			if t == nil {
				return fmt.Errorf("consumer bootstrap local topic missing")
			}
			p, err := t.GetPartition(identity.Partition)
			if err != nil {
				return err
			}
			if err := validate[0](p.BootstrapPending()); err != nil {
				return err
			}
		}
		// Recheck every other local view before intent; the target is the only
		// deliberately absent view in this preflight and no records are read.
		if err := ch.withConsumerRecoveryPlan(ctx, plan, -1, nil, func(*consumerRecoveryReader) error { return nil }); err != nil {
			return err
		}
		return ch.runConsumerBootstrapSession(ctx, identity, receiver, plan, detach, rebuild)
	})
}

func (ch *CommandHandler) runConsumerBootstrapSession(ctx context.Context, identity coordinator.ConsumerBootstrapIdentity, receiver *disk.BootstrapReceiver, plan *consumerRecoveryPlan, detach func() error, rebuild func(*topic.PartitionReadView, []byte) error) (result error) {
	c := ch.Coordinator
	// Full scratch reconstruction needs every other committed partition. Refuse
	// a second physical pending install before intent; otherwise neither pending
	// partition could acquire a complete reader on resume. Independent tickets
	// remain supported and keep admission closed until each resolves.
	ticket, err := c.RetainConsumerBootstrapTicket(identity, nil)
	if err != nil {
		return err
	}
	t := ch.TopicManager.GetTopic(config.ConsumerOffsetsTopicName)
	if t == nil || t.Definition().LifecycleEpoch != identity.Lifecycle {
		return fmt.Errorf("consumer bootstrap local partition missing")
	}
	p, err := t.GetPartition(identity.Partition)
	if err != nil {
		return err
	}
	route := func(group string) (int, error) {
		_, _, partition, _, err := ch.Cluster.Router.FindCoordinatorWithEpoch(group)
		// #nosec G115 -- the router reduces partition modulo the positive int PartitionCount; error paths return zero.
		return int(partition), err
	}
	if !p.BootstrapPending() {
		err = p.WithReadView(func(v *topic.PartitionReadView) error {
			scope, captureErr := c.CaptureConsumerMetadataScope(identity.Partition, &consumerRawView{v, identity.Partition, p.NextOffset()}, route)
			_, retainErr := c.RetainConsumerBootstrapTicket(identity, scope)
			return errors.Join(captureErr, retainErr)
		})
		if err != nil {
			// No session was opened and no install/splice was attempted.
			cleanupErr := c.FinishConsumerBootstrapTicket(ticket, coordinator.ConsumerBootstrapPreIntentAbort, p.BootstrapPending(), 0)
			return errors.Join(err, cleanupErr)
		}
	} else {
		manifest, err := p.BootstrapStatus()
		if manifest == nil {
			return errors.Join(fmt.Errorf("pending consumer bootstrap manifest unavailable"), err)
		}
		if manifest.TransferID != identity.Attempt || manifest.Fence.LifecycleEpoch != identity.Lifecycle {
			return fmt.Errorf("pending consumer bootstrap identity mismatch")
		}
		// A retained attempt error is retryable; Complete revalidates storage.
	}
	session, err := p.BeginBootstrapSession()
	if err != nil {
		return err
	}
	var preparedView uint64
	defer func() {
		session.Close()
		outcome := coordinator.ConsumerBootstrapUncertain
		switch session.ClosedOutcome() {
		case topic.BootstrapOutcomePreIntentAbort:
			outcome = coordinator.ConsumerBootstrapPreIntentAbort
		case topic.BootstrapOutcomeCompleted:
			outcome = coordinator.ConsumerBootstrapCompleted
		}
		finishErr := c.FinishConsumerBootstrapTicket(ticket, outcome, p.BootstrapPending(), preparedView)
		if finishErr != nil && result == nil {
			result = finishErr
		}
	}()
	if receiver != nil {
		if err := session.Install(receiver, detach); err != nil {
			return err
		}
	}
	prepare := func(v *topic.PartitionReadView) error {
		preparedView = v.Generation()
		return ch.withConsumerRecoveryPlan(ctx, plan, identity.Partition, v, func(reader *consumerRecoveryReader) error {
			scope, err := c.ConsumerBootstrapScope(ticket)
			if err != nil {
				return err
			}
			prepared, err := c.PrepareScopedConsumerMetadataReplay(reader, &consumerRawView{v, identity.Partition, v.CommittedEnd()}, scope, route)
			if err != nil {
				return err
			}
			return c.ApplyConsumerBootstrapReplay(ticket, prepared, preparedView)
		})
	}
	if session.Prepared() {
		if err := session.WithPreparedReadView(func(v *topic.PartitionReadView) error {
			preparedView = v.Generation()
			if c.ConsumerBootstrapPreparedCurrent(ticket, preparedView) {
				return nil
			}
			return prepare(v)
		}); err != nil {
			return err
		}
	}
	return session.Complete(func(v *topic.PartitionReadView, catalog []byte) error {
		if rebuild != nil {
			if err := rebuild(v, catalog); err != nil {
				return err
			}
		}
		return prepare(v)
	})
}
