package controller

import (
	"context"
	"fmt"
	"reflect"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
)

type ConsumerMetadataRecoveryStageRequest struct {
	RequiredGeneration uint64
	Metadata           []fsm.PartitionMetadata
	Partitions         []int
}
type ConsumerMetadataRecoverySource struct {
	BrokerID, IncarnationID string
	IncarnationEpoch        uint64
}

// StagedConsumerMetadataRecovery owns detached, certified immutable RAW records
// and markers through the requested ends. Its CommittedReader MUST filter those
// bytes with the receiver's current transaction-decision view during final FSM
// exclusion (or verify equivalent decision identity). Source-filtered omissions
// are unsafe: transaction decisions can change without a partition HWM change.
// Sources must identify the certified source incarnation per staged partition.
// All methods except Close are local, bounded, and cannot perform network I/O.
type StagedConsumerMetadataRecovery interface {
	coordinator.ConsumerMetadataCommittedReader
	Sources() map[int]ConsumerMetadataRecoverySource
	Close() error
}
type ConsumerMetadataRecoveryProvider func(context.Context, ConsumerMetadataRecoveryStageRequest) (StagedConsumerMetadataRecovery, error)

// SetConsumerMetadataRecoveryProvider wires read-only recovery staging. Protocol
// implementation and registered-broker authorization belong to the transfer
// layer; installing this callback does not activate any capability version.
func (ch *CommandHandler) SetConsumerMetadataRecoveryProvider(provider ConsumerMetadataRecoveryProvider) {
	ch.consumerRecoveryProviderMu.Lock()
	ch.consumerRecoveryProvider = provider
	ch.consumerRecoveryProviderMu.Unlock()
}

type consumerRecoveryPlan struct {
	generation uint64
	metadata   []fsm.PartitionMetadata
	ends       []uint64
	legacy     bool
	local      []*topic.Partition
	missing    []int
	staged     StagedConsumerMetadataRecovery
	sources    map[int]ConsumerMetadataRecoverySource
	requester  fsm.BootstrapProcessIdentity
}

func cloneConsumerRecoveryMetadata(metadata []fsm.PartitionMetadata) []fsm.PartitionMetadata {
	copy := append([]fsm.PartitionMetadata(nil), metadata...)
	for i := range copy {
		copy[i].Replicas = append([]string(nil), metadata[i].Replicas...)
		copy[i].ISR = append([]string(nil), metadata[i].ISR...)
		copy[i].RecoveryReplicas = append([]string(nil), metadata[i].RecoveryReplicas...)
	}
	return copy
}

func (ch *CommandHandler) prepareConsumerRecoveryPlan(ctx context.Context, metadata []fsm.PartitionMetadata, skip int, legacy bool) (*consumerRecoveryPlan, error) {
	plan := &consumerRecoveryPlan{generation: ch.Coordinator.ConsumerRecoveryRequirement().RequiredGeneration, metadata: cloneConsumerRecoveryMetadata(metadata), local: make([]*topic.Partition, len(metadata)), ends: make([]uint64, len(metadata)), legacy: legacy}
	if ch.TopicManager == nil || ch.Cluster.Router == nil {
		return nil, fmt.Errorf("%w: local recovery dependencies unavailable", ErrConsumerRecoveryPending)
	}
	t := ch.TopicManager.GetTopic(config.ConsumerOffsetsTopicName)
	if t == nil || t.Definition().Partitions != len(metadata) {
		return nil, ErrConsumerRecoveryPending
	}
	localID := ch.Cluster.Router.BrokerID()
	for i, m := range metadata {
		plan.ends[i] = m.CommittedHWM
		if i == skip {
			continue
		}
		if t.Definition().LifecycleEpoch != m.LifecycleEpoch {
			return nil, ErrConsumerRecoveryPending
		}
		p, err := t.GetPartition(i)
		if err != nil {
			return nil, err
		}
		if p.BootstrapPending() || p.SnapshotRecoveryPending() {
			return nil, ErrConsumerRecoveryPending
		}
		if err := p.RecoveryError(); err != nil {
			return nil, err
		}
		covered := false
		if p.HWMKnown() {
			err = p.WithReadView(func(v *topic.PartitionReadView) error {
				if legacy {
					// Unknown-authority snapshots keep historical local
					// compatibility replay, without acquiring completeness proof.
					plan.ends[i] = v.CommittedEnd()
					covered = true
				} else {
					covered = v.CommittedEnd() >= m.CommittedHWM
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
		if covered {
			plan.local[i] = p
			continue
		}
		if legacy || containsReplica(m.Replicas, localID) {
			return nil, ErrConsumerRecoveryPending
		}
		plan.missing = append(plan.missing, i)
	}
	if len(plan.missing) == 0 {
		return plan, nil
	}
	ch.consumerRecoveryProviderMu.RLock()
	provider := ch.consumerRecoveryProvider
	ch.consumerRecoveryProviderMu.RUnlock()
	if provider == nil {
		return nil, fmt.Errorf("%w: nonreplica metadata staging provider unavailable", ErrConsumerRecoveryPending)
	}
	requester := ch.Cluster.RaftManager.GetFSM().GetBroker(localID)
	if !consumerRecoveryProcess(requester) {
		return nil, fmt.Errorf("%w: metadata requester unavailable", ErrConsumerRecoveryPending)
	}
	plan.requester = fsm.BootstrapProcessIdentity{BrokerID: requester.ID, IncarnationID: requester.IncarnationID, IncarnationEpoch: requester.IncarnationEpoch}
	staged, err := provider(ctx, ConsumerMetadataRecoveryStageRequest{RequiredGeneration: plan.generation, Metadata: cloneConsumerRecoveryMetadata(metadata), Partitions: append([]int(nil), plan.missing...)})
	if err != nil {
		if staged != nil {
			_ = staged.Close()
		}
		return nil, err
	}
	if staged == nil {
		return nil, fmt.Errorf("%w: metadata provider returned no staged data", ErrConsumerRecoveryPending)
	}
	plan.staged = staged
	plan.sources = make(map[int]ConsumerMetadataRecoverySource)
	for p, source := range staged.Sources() {
		plan.sources[p] = source
	}
	if err := ctx.Err(); err != nil {
		_ = staged.Close()
		return nil, err
	}
	return plan, nil
}

func (p *consumerRecoveryPlan) close() {
	if p != nil && p.staged != nil {
		_ = p.staged.Close()
	}
}

func (ch *CommandHandler) validateConsumerRecoveryPlan(plan *consumerRecoveryPlan, metadata []fsm.PartitionMetadata) error {
	if err := plan.restageLocalEnds(metadata); err != nil {
		return err
	}
	r := ch.Coordinator.ConsumerRecoveryRequirement()
	if r.RequiredGeneration != plan.generation || r.Installing || r.Failure != nil || !reflect.DeepEqual(plan.metadata, metadata) {
		return ErrConsumerRecoveryPending
	}
	t := ch.TopicManager.GetTopic(config.ConsumerOffsetsTopicName)
	if t == nil || t.Definition().Partitions != len(metadata) {
		return ErrConsumerRecoveryPending
	}
	for i, p := range plan.local {
		if p == nil {
			continue
		}
		current, err := t.GetPartition(i)
		if err != nil || current != p || t.Definition().LifecycleEpoch != metadata[i].LifecycleEpoch {
			return ErrConsumerRecoveryPending
		}
	}
	state := ch.Cluster.RaftManager.GetFSM()
	if len(plan.missing) > 0 {
		requester := state.GetBroker(ch.Cluster.Router.BrokerID())
		if !consumerRecoveryProcess(requester) || requester.ID != plan.requester.BrokerID || requester.IncarnationID != plan.requester.IncarnationID || requester.IncarnationEpoch != plan.requester.IncarnationEpoch {
			return fmt.Errorf("%w: staged metadata requester changed", ErrConsumerRecoveryPending)
		}
	}
	for _, i := range plan.missing {
		source, ok := plan.sources[i]
		broker := state.GetBroker(source.BrokerID)
		if !ok || !consumerRecoveryProcess(broker) || broker.IncarnationID != source.IncarnationID || broker.IncarnationEpoch != source.IncarnationEpoch || !containsReplica(metadata[i].Replicas, source.BrokerID) || !containsReplica(metadata[i].ISR, source.BrokerID) {
			return fmt.Errorf("%w: staged metadata source changed", ErrConsumerRecoveryPending)
		}
		end, err := plan.staged.EndTopicOffset(config.ConsumerOffsetsTopicName, i)
		if err != nil {
			return err
		}
		if end != metadata[i].CommittedHWM {
			return fmt.Errorf("%w: staged metadata end changed", ErrConsumerRecoveryPending)
		}
	}
	return nil
}

// withConsumerRecoveryPlan opens only local fixed views. Remote bytes were
// staged before the lease, reload mutex, lifecycle gates, and FSM fence.
func (ch *CommandHandler) withConsumerRecoveryPlan(ctx context.Context, plan *consumerRecoveryPlan, target int, targetView *topic.PartitionReadView, fn func(*consumerRecoveryReader) error) error {
	reader := &consumerRecoveryReader{ctx: ctx, views: make([]*topic.PartitionReadView, len(plan.metadata)), ends: make([]uint64, len(plan.metadata)), staged: plan.staged, metadata: plan.metadata}
	var visit func(int) error
	visit = func(i int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if i == len(reader.views) {
			return fn(reader)
		}
		reader.ends[i] = plan.ends[i]
		accept := func(v *topic.PartitionReadView) error {
			if plan.legacy {
				// Compatibility replay uses the durable local cut, including
				// commits that arrived while admission was being drained.
				reader.ends[i] = v.CommittedEnd()
			}
			if v.CommittedEnd() < reader.ends[i] {
				return ErrConsumerRecoveryPending
			}
			reader.views[i] = v
			return visit(i + 1)
		}
		if i == target {
			// Installed replacement authority is its manifest end. The FSM HWM
			// may already be ahead; ordinary catch-up fills that later tail.
			reader.ends[i] = targetView.CommittedEnd()
			return accept(targetView)
		}
		if plan.local[i] == nil {
			return visit(i + 1)
		}
		p := plan.local[i]
		if p.BootstrapPending() || p.SnapshotRecoveryPending() || !p.HWMKnown() {
			return ErrConsumerRecoveryPending
		}
		if err := p.RecoveryError(); err != nil {
			return err
		}
		return p.WithReadView(accept)
	}
	return visit(0)
}
