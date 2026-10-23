package coordinator

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

const ConsumerRecoveryEnvelopeVersion = 1

// ErrConsumerMetadataBootstrapUnknown is stable across missing legacy envelopes
// and unprovable history. Scanning surviving records cannot recover this proof.
var ErrConsumerMetadataBootstrapUnknown = errors.New("consumer metadata bootstrap authority is unknown")

// ErrConsumerSnapshotBusy lets the Raft FSM retry a snapshot without blocking
// the Apply loop needed by a consumer metadata writer holding these locks.
var ErrConsumerSnapshotBusy = errors.New("consumer snapshot busy")

// ConsumerRecoveryEnvelope keeps historical Raft state separate from the live
// disk overlay. SourcePartitions is a conservative, monotonic physical history,
// including names that no longer have live records after deletion/compaction.
type ConsumerRecoveryEnvelope struct {
	Version                int                            `json:"version"`
	KnownComplete          bool                           `json:"known_complete"`
	RaftCompatibilityKnown bool                           `json:"raft_compatibility_known"`
	SourcePartitionsKnown  bool                           `json:"source_partitions_known"`
	RaftCompatibilityState map[string]*GroupStateSnapshot `json:"raft_compatibility_state"`
	SourcePartitions       map[string][]int               `json:"source_partitions"`
}

// This object has no constructor side effects, callbacks, publisher, resolver,
// topic handler or workers. Only private replay helpers may operate on it.
func newCompatibilityCoordinator() *Coordinator {
	return &Coordinator{groups: make(map[string]*GroupMetadata), groupEpochs: make(map[string]uint64), lifecyclePending: make(map[string]bool), ownershipSince: make(map[string]time.Time)}
}

// HistoricalConsumerGroupsAbsent is a nonblocking Raft-apply proof. Local disk
// overlays cannot establish absence of older Raft-only registrations. Contention
// or unknown compatibility provenance conservatively returns false.
func (c *Coordinator) HistoricalConsumerGroupsAbsent() bool {
	if c == nil || !c.authorityMu.TryLock() {
		return false
	}
	defer c.authorityMu.Unlock()
	return c.raftCompatibilityKnown && c.raftCompatibility != nil && len(c.raftCompatibility.groups) == 0
}

type lifecycleWriteMode bool

const (
	replayLifecycle  lifecycleWriteMode = false
	publishLifecycle lifecycleWriteMode = true
)

func (mode lifecycleWriteMode) write(write func() error) error {
	if mode == replayLifecycle {
		return nil
	}
	return write()
}

// applyHistorical serializes the successful live transition and its independent
// proof with paired snapshots. A modern overlay can allow an operation that the
// baseline cannot prove: preserve ordinary apply, but permanently lose proof.
func (c *Coordinator) applyHistorical(group string, apply func(*Coordinator) error) error {
	c.authorityMu.Lock()
	defer c.authorityMu.Unlock()
	if err := apply(c); err != nil {
		return err
	}
	c.authorityRevision++
	if c.raftCompatibilityKnown {
		candidate := newCompatibilityCoordinator()
		if c.raftCompatibility == nil {
			c.raftCompatibilityKnown = false
		} else {
			_ = candidate.importStateValidated(c.raftCompatibility.exportState(&group))
			if err := apply(candidate); err != nil {
				c.raftCompatibilityKnown = false
			} else if err := validateCompatibilityState(candidate.ExportState()); err != nil {
				c.raftCompatibilityKnown = false
			} else {
				if updated := candidate.groups[group]; updated != nil {
					c.raftCompatibility.groups[group] = updated
				} else {
					delete(c.raftCompatibility.groups, group)
				}
				if epoch, ok := candidate.groupEpochs[group]; ok {
					c.raftCompatibility.groupEpochs[group] = epoch
				}
			}
		}
	}
	return nil
}

func (c *Coordinator) RegisterGroupFromFSM(topic, group string, count int) error {
	return c.applyHistorical(group, func(target *Coordinator) error { return target.registerGroup(topic, group, count, replayLifecycle) })
}
func (c *Coordinator) RegisterGroupSubscriptionFromFSM(group string, topics []string, pattern string, counts map[string]int) error {
	return c.applyHistorical(group, func(target *Coordinator) error {
		return target.registerGroupSubscription(group, topics, pattern, counts, replayLifecycle)
	})
}

// JoinGroupFromFSM performs the historical implicit registration and join as one
// authority transition, checking existence independently on each state.
func (c *Coordinator) JoinGroupFromFSM(group, member, topic string, count int) error {
	return c.applyHistorical(group, func(target *Coordinator) error {
		target.mu.RLock()
		existing := target.groups[group]
		subscription := existing != nil && len(existing.TopicPartitions) > 0
		target.mu.RUnlock()
		if topic == "" && !subscription {
			return fmt.Errorf("GROUP_SYNC JOIN without topic requires a registered subscription")
		}
		if existing == nil {
			if err := target.registerGroup(topic, group, count, replayLifecycle); err != nil {
				return err
			}
		}
		_, err := target.addConsumer(group, member, replayLifecycle)
		return err
	})
}
func (c *Coordinator) RemoveConsumerForGenerationFromFSM(group, member string, generation int) error {
	return c.applyHistorical(group, func(target *Coordinator) error {
		return target.removeConsumerForGeneration(group, member, generation, replayLifecycle)
	})
}
func (c *Coordinator) ExpireConsumersFromFSM(group string, generation int, members []string) error {
	return c.applyHistorical(group, func(target *Coordinator) error {
		return target.expireConsumers(group, generation, members, replayLifecycle)
	})
}
func (c *Coordinator) ApplyOffsetUpdateFromFSM(group, topic string, offsets []OffsetItem) error {
	return c.applyHistorical(group, func(target *Coordinator) error { return target.applyOffsetUpdateFromFSM(group, topic, offsets) })
}
func (c *Coordinator) ApplyFencedOffsetUpdateFromFSM(group, topic, member string, generation int, offsets []OffsetItem) error {
	return c.applyHistorical(group, func(target *Coordinator) error {
		return target.applyFencedOffsetUpdateFromFSM(group, topic, member, generation, offsets)
	})
}
func (c *Coordinator) ApplyFencedTopicOffsetUpdateFromFSM(group, member string, generation int, epoch uint64, offsets map[string][]OffsetItem) error {
	return c.applyHistorical(group, func(target *Coordinator) error {
		return target.applyFencedTopicOffsetUpdateFromFSM(group, member, generation, epoch, offsets)
	})
}

// RecordConsumerMetadataSource takes ONLY provenanceMu: callers may already
// hold coordinator/group locks. Call before publishing or exposing live values.
func (c *Coordinator) RecordConsumerMetadataSource(group string, partition int) {
	if group == "" || partition < 0 {
		return
	}
	c.provenanceMu.Lock()
	defer c.provenanceMu.Unlock()
	if c.metadataSources == nil {
		c.metadataSources = make(map[string]map[int]struct{})
	}
	if c.metadataSources[group] == nil {
		c.metadataSources[group] = make(map[int]struct{})
	}
	c.metadataSources[group][partition] = struct{}{}
}

// SetRoutedOffsetRecordWriter installs a writer that MUST record the actual
// physical partition via RecordConsumerMetadataSource before durable/live
// publish. Opaque callbacks must use SetOffsetRecordWriter instead.
func (c *Coordinator) SetRoutedOffsetRecordWriter(writer func(ConsumerMetadataRecord) error) {
	c.offsetRecordWriterMu.Lock()
	c.offsetRecordWriter = writer
	c.offsetRecordWriterRouted = true
	c.offsetRecordWriterMu.Unlock()
}
func (c *Coordinator) markConsumerMetadataSourcesUnknown() {
	c.provenanceMu.Lock()
	c.metadataSourcesKnown = false
	c.provenanceMu.Unlock()
}

func (c *Coordinator) ValidateConsumerMetadataBootstrapEligibility(partition int) error {
	if partition < 0 {
		return fmt.Errorf("invalid consumer metadata partition %d", partition)
	}
	c.authorityMu.Lock()
	defer c.authorityMu.Unlock()
	c.provenanceMu.Lock()
	defer c.provenanceMu.Unlock()
	if !c.raftCompatibilityKnown || !c.metadataSourcesKnown {
		return ErrConsumerMetadataBootstrapUnknown
	}
	return nil
}

func (c *Coordinator) ExportStateWithRecovery() (map[string]*GroupStateSnapshot, *ConsumerRecoveryEnvelope, error) {
	return c.exportStateWithRecovery(false)
}

// TryExportStateWithRecovery returns either the complete detached state/proof
// pair or an error. It never waits for consumer locks from the Raft Apply loop.
func (c *Coordinator) TryExportStateWithRecovery() (map[string]*GroupStateSnapshot, *ConsumerRecoveryEnvelope, error) {
	return c.exportStateWithRecovery(true)
}

func (c *Coordinator) exportStateWithRecovery(nonblocking bool) (map[string]*GroupStateSnapshot, *ConsumerRecoveryEnvelope, error) {
	if nonblocking {
		if !c.authorityMu.TryLock() {
			return nil, nil, ErrConsumerSnapshotBusy
		}
	} else {
		c.authorityMu.Lock()
	}
	defer c.authorityMu.Unlock()
	// Do not wait for any group lock while holding provenanceMu. Recording
	// precedes exposing values, so this order includes every exported source.
	groups, err := c.exportStateWithLockMode(nil, nonblocking)
	if err != nil {
		return nil, nil, err
	}
	baseline := make(map[string]*GroupStateSnapshot)
	if c.raftCompatibility != nil {
		baseline, err = c.raftCompatibility.exportStateWithLockMode(nil, nonblocking)
		if err != nil {
			return nil, nil, err
		}
	}
	if nonblocking {
		if !c.provenanceMu.TryLock() {
			return nil, nil, ErrConsumerSnapshotBusy
		}
	} else {
		c.provenanceMu.Lock()
	}
	defer c.provenanceMu.Unlock()
	envelope := &ConsumerRecoveryEnvelope{Version: ConsumerRecoveryEnvelopeVersion, RaftCompatibilityKnown: c.raftCompatibilityKnown, SourcePartitionsKnown: c.metadataSourcesKnown, RaftCompatibilityState: baseline, SourcePartitions: make(map[string][]int)}
	envelope.KnownComplete = envelope.RaftCompatibilityKnown && envelope.SourcePartitionsKnown
	for group, sources := range c.metadataSources {
		for partition := range sources {
			envelope.SourcePartitions[group] = append(envelope.SourcePartitions[group], partition)
		}
		sort.Ints(envelope.SourcePartitions[group])
	}
	if err := ValidateConsumerRecoveryEnvelope(envelope); err != nil {
		return nil, nil, err
	}
	return groups, envelope, nil
}

func ValidateConsumerRecoveryEnvelope(envelope *ConsumerRecoveryEnvelope) error {
	if envelope == nil {
		return nil
	}
	if envelope.Version != ConsumerRecoveryEnvelopeVersion {
		return fmt.Errorf("unsupported consumer recovery envelope version %d", envelope.Version)
	}
	if envelope.KnownComplete != (envelope.RaftCompatibilityKnown && envelope.SourcePartitionsKnown) {
		return fmt.Errorf("inconsistent consumer recovery completeness flags")
	}
	if envelope.RaftCompatibilityKnown && envelope.RaftCompatibilityState == nil {
		return fmt.Errorf("known consumer compatibility state is missing")
	}
	if envelope.SourcePartitionsKnown && envelope.SourcePartitions == nil {
		return fmt.Errorf("known consumer source partitions are missing")
	}
	if err := validateCompatibilityState(envelope.RaftCompatibilityState); err != nil {
		return err
	}
	for group, partitions := range envelope.SourcePartitions {
		if group == "" || len(partitions) == 0 {
			return fmt.Errorf("invalid consumer metadata source group %q", group)
		}
		seen := make(map[int]bool)
		for _, partition := range partitions {
			if partition < 0 || seen[partition] {
				return fmt.Errorf("invalid consumer metadata source partition %d for %q", partition, group)
			}
			seen[partition] = true
		}
	}
	return nil
}

// Historical OFFSET replay can create an unregistered shell. It is legal only
// in the compatibility baseline; live GroupState validation remains unchanged.
func validateCompatibilityState(state map[string]*GroupStateSnapshot) error {
	for name, snap := range state {
		if snap == nil || snap.RegistrationEpoch != 0 {
			if err := ValidateImportState(map[string]*GroupStateSnapshot{name: snap}); err != nil {
				return err
			}
			continue
		}
		if name == "" || snap.TopicName == "" || snap.Deleted || snap.RegistrationInferred || snap.Generation != 0 || len(snap.Members) != 0 || len(snap.TopicAssignments) != 0 || len(snap.Partitions) != 0 || len(snap.Topics) != 0 || snap.TopicPattern != "" || len(snap.TopicPartitions) != 0 || len(snap.OffsetRevisions) != 0 || len(snap.OffsetReservations) != 0 || snap.ReservationRevision != 0 || len(snap.ReservationDecisions) != 0 || !snap.LastActivity.IsZero() || !snap.LastRebalance.IsZero() {
			return fmt.Errorf("invalid historical consumer offset shell %q", name)
		}
		for topic, offsets := range snap.Offsets {
			if !groupTopicMatches(snap.TopicName, topic) {
				return fmt.Errorf("historical consumer offset shell %q has mismatched topic", name)
			}
			for partition := range offsets {
				if partition < 0 {
					return fmt.Errorf("historical consumer offset shell %q has negative partition", name)
				}
			}
		}
	}
	return nil
}

func (c *Coordinator) ImportStateWithRecovery(groups map[string]*GroupStateSnapshot, envelope *ConsumerRecoveryEnvelope) error {
	if err := ValidateImportState(groups); err != nil {
		return err
	}
	if err := ValidateConsumerRecoveryEnvelope(envelope); err != nil {
		return err
	}
	baseline := newCompatibilityCoordinator()
	sources := make(map[string]map[int]struct{})
	if envelope != nil {
		if err := baseline.importStateValidated(envelope.RaftCompatibilityState); err != nil {
			return err
		}
		for group, partitions := range envelope.SourcePartitions {
			sources[group] = make(map[int]struct{})
			for _, partition := range partitions {
				sources[group][partition] = struct{}{}
			}
		}
	}
	c.authorityMu.Lock()
	defer c.authorityMu.Unlock()
	// Restore may hold FSM transition locks while a lifecycle publisher is
	// waiting for Raft with lifecycleMu held. Do not wait for that publisher;
	// its existing group identity checks fence completion against this import.
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.importStateLocked(groups); err != nil {
		return err
	}
	c.raftCompatibility = baseline
	c.authorityRevision++
	c.raftCompatibilityKnown = envelope != nil && envelope.RaftCompatibilityKnown
	c.provenanceMu.Lock()
	c.metadataSources = sources
	c.metadataSourcesKnown = envelope != nil && envelope.SourcePartitionsKnown
	c.provenanceMu.Unlock()
	return nil
}
