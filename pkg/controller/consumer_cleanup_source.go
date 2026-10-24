package controller

import (
	"context"
	"fmt"
	"reflect"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/types"
)

type stagedCleanupReader interface {
	stagedConsumerHistory
	ReadConsumerCleanupPage(int, uint64, int, int) (types.ReplayPage, error)
}

type preparedConsumerCleanup struct {
	ch                        *CommandHandler
	ctx                       context.Context
	plan                      *consumerRecoveryPlan
	instance                  string
	processes                 []fsm.BootstrapProcessIdentity
	remote                    map[int]coordinator.ConsumerCleanupPartition
	localProof, combinedProof []coordinator.ConsumerCleanupPartition
}

// Staging precedes coordinator lifecycle/global locks. Neither view below nor
// its final proof checker can transfer bytes or install recovered group state.
func (ch *CommandHandler) prepareConsumerCleanup(ctx context.Context) (coordinator.ConsumerCleanupReader, func(), error) {
	state := ch.Cluster.RaftManager.GetFSM()
	metadata, err := state.ConsumerMetadataRecoverySnapshot()
	if err != nil {
		return nil, nil, err
	}
	plan, err := ch.prepareConsumerRecoveryPlan(ctx, metadata, -1, false)
	if err != nil {
		return nil, nil, err
	}
	if len(plan.missing) == 0 {
		plan.close()
		return ch.TopicManager, nil, nil
	}
	raw, ok := plan.staged.(stagedCleanupReader)
	if !ok {
		plan.close()
		return nil, nil, fmt.Errorf("staged cleanup raw reader unavailable")
	}
	t := ch.TopicManager.GetTopic(config.ConsumerOffsetsTopicName)
	if t == nil {
		plan.close()
		return nil, nil, ErrConsumerRecoveryPending
	}
	r := &preparedConsumerCleanup{ch: ch, ctx: ctx, plan: plan, instance: t.Definition().InstanceID, remote: make(map[int]coordinator.ConsumerCleanupPartition), processes: []fsm.BootstrapProcessIdentity{plan.requester}}
	for _, p := range plan.missing {
		identity, err := raw.ConsumerHistoryIdentity(p)
		if err != nil {
			plan.close()
			return nil, nil, err
		}
		first, err := plan.staged.EarliestTopicOffset(config.ConsumerOffsetsTopicName, p)
		if err != nil {
			plan.close()
			return nil, nil, err
		}
		source := plan.sources[p]
		if !containsReplica(metadata[p].ISR, source.BrokerID) || !containsReplica(metadata[p].Replicas, source.BrokerID) {
			plan.close()
			return nil, nil, ErrConsumerRecoveryPending
		}
		r.processes = append(r.processes, fsm.BootstrapProcessIdentity{BrokerID: source.BrokerID, IncarnationID: source.IncarnationID, IncarnationEpoch: source.IncarnationEpoch})
		// Remote evidence is ephemeral. Never serialize a transfer generation
		// as a durable local cleanup checkpoint or current-state replay index.
		r.remote[p] = coordinator.ConsumerCleanupPartition{TopicInstance: r.instance, TopicEpoch: metadata[p].LifecycleEpoch, Identity: identity, Start: first, End: plan.ends[p]}
	}
	return r, plan.close, nil
}

func (r *preparedConsumerCleanup) WithConsumerCleanupView(fn func(coordinator.ConsumerCleanupView) error) error {
	return r.WithConsumerCleanupViewContext(r.ctx, fn)
}

func (r *preparedConsumerCleanup) WithConsumerCleanupViewContext(ctx context.Context, fn func(coordinator.ConsumerCleanupView) error) error {
	state := r.ch.Cluster.RaftManager.GetFSM()
	return state.WithConsumerMetadataRecoveryFence(func([]fsm.PartitionMetadata) error {
		if err := state.CheckConsumerCleanupAuthority(r.instance, r.plan.metadata, r.processes); err != nil {
			return err
		}
		return r.ch.TopicManager.WithConsumerCleanupViewContext(ctx, func(local coordinator.ConsumerCleanupView) error {
			bounds := append([]coordinator.ConsumerCleanupPartition(nil), local.Partitions()...)
			if len(bounds) != len(r.plan.metadata) {
				return ErrConsumerRecoveryPending
			}
			r.localProof = append([]coordinator.ConsumerCleanupPartition(nil), bounds...)
			for p, remote := range r.remote {
				bounds[p] = remote
			}
			r.combinedProof = append([]coordinator.ConsumerCleanupPartition(nil), bounds...)
			return fn(&consumerCleanupCompositeView{ConsumerCleanupView: local, prepared: r, bounds: bounds})
		})
	})
}

func (r *preparedConsumerCleanup) CheckConsumerCleanupProof(bounds []coordinator.ConsumerCleanupPartition) error {
	if !reflect.DeepEqual(bounds, r.combinedProof) {
		return ErrConsumerRecoveryPending
	}
	if err := r.ch.Cluster.RaftManager.GetFSM().CheckConsumerCleanupAuthority(r.instance, r.plan.metadata, r.processes); err != nil {
		return err
	}
	return r.ch.TopicManager.CheckConsumerCleanupProof(r.localProof)
}

type consumerCleanupCompositeView struct {
	coordinator.ConsumerCleanupView
	prepared *preparedConsumerCleanup
	bounds   []coordinator.ConsumerCleanupPartition
}

func (v *consumerCleanupCompositeView) Partitions() []coordinator.ConsumerCleanupPartition {
	return v.bounds
}
func (v *consumerCleanupCompositeView) Read(p int, next uint64, cursor types.ReplayCursor, maxRecords, maxBytes int) (types.ReplayPage, error) {
	if _, remote := v.prepared.remote[p]; remote {
		return v.prepared.plan.staged.(stagedCleanupReader).ReadConsumerCleanupPage(p, next, maxRecords, maxBytes)
	}
	return v.ConsumerCleanupView.Read(p, next, cursor, maxRecords, maxBytes)
}
