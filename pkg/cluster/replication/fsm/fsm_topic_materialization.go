package fsm

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
)

const (
	TopicMaterializationCreate   = "create"
	TopicMaterializationRestore  = "restore"
	TopicMaterializationDelete   = "delete"
	TopicMaterializationTruncate = "truncate"
)

// TopicMaterializationIssue describes node-local work that has not converged
// with the authoritative topic state committed by Raft.
type TopicMaterializationIssue struct {
	Topic           string                           `json:"topic"`
	Operation       string                           `json:"operation"`
	Error           string                           `json:"error"`
	PendingSince    time.Time                        `json:"pending_since"`
	ConsumerCleanup *coordinator.TopicCleanupPending `json:"consumer_cleanup,omitempty"`
	Revision        uint64                           `json:"revision,omitempty"`
	LifecycleEpoch  uint64                           `json:"lifecycle_epoch,omitempty"`
}

// TopicMaterializationAttempts counts completed node-local attempts by result.
type TopicMaterializationAttempts struct {
	Success uint64
	Failure uint64
}

// TopicMaterializationRuntimeSnapshot is a detached operational view of local
// topic convergence.
type TopicMaterializationRuntimeSnapshot struct {
	PendingByOperation  map[string]int
	AttemptsByOperation map[string]TopicMaterializationAttempts
	OldestPending       time.Duration
}

func (f *BrokerFSM) TopicMaterializationIssues() []TopicMaterializationIssue {
	f.mu.RLock()
	defer f.mu.RUnlock()

	issues := make([]TopicMaterializationIssue, 0, len(f.topicMaterialization))
	for _, issue := range f.topicMaterialization {
		issues = append(issues, issue)
	}
	sort.Slice(issues, func(i, j int) bool { return issues[i].Topic < issues[j].Topic })
	return issues
}

// TopicMaterializationRuntimeSnapshot returns aggregate metrics without
// exposing mutable FSM maps.
func (f *BrokerFSM) TopicMaterializationRuntimeSnapshot() TopicMaterializationRuntimeSnapshot {
	snapshot := TopicMaterializationRuntimeSnapshot{
		PendingByOperation:  make(map[string]int),
		AttemptsByOperation: make(map[string]TopicMaterializationAttempts),
	}
	if f == nil {
		return snapshot
	}

	now := time.Now()
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, issue := range f.topicMaterialization {
		snapshot.PendingByOperation[issue.Operation]++
		if issue.PendingSince.IsZero() {
			continue
		}
		age := now.Sub(issue.PendingSince)
		if age < 0 {
			age = 0
		}
		if age > snapshot.OldestPending {
			snapshot.OldestPending = age
		}
	}
	for operation, attempts := range f.topicMaterializationRuns {
		snapshot.AttemptsByOperation[operation] = attempts
	}
	return snapshot
}

func (f *BrokerFSM) TopicMaterializationReadinessError() error {
	issues := f.TopicMaterializationIssues()
	if len(issues) == 0 {
		return nil
	}
	first := issues[0]
	if first.Error == "" {
		return fmt.Errorf("%d topic materialization operation(s) pending: %s %s", len(issues), first.Operation, first.Topic)
	}
	return fmt.Errorf("%d topic materialization operation(s) pending: %s %s: %s", len(issues), first.Operation, first.Topic, first.Error)
}

// ReconcileTopicMaterializations retries node-local operations against the
// authoritative desired state. Calls are serialized with Apply side effects.
func (f *BrokerFSM) ReconcileTopicMaterializations() error {
	return f.reconcileTopicMaterializations(true)
}

func (f *BrokerFSM) reconcileTopicMaterializations(allowGroupWrites bool) error {
	f.mu.RLock()
	desired := make(map[string]*topic.Definition)
	deletes := make([]string, 0)
	for name, issue := range f.topicMaterialization {
		switch issue.Operation {
		case TopicMaterializationCreate, TopicMaterializationRestore, TopicMaterializationTruncate:
			desired[name] = copyTopicDefinition(f.topicState[name])
		case TopicMaterializationDelete:
			deletes = append(deletes, name)
		}
	}
	f.mu.RUnlock()

	names := make([]string, 0, len(desired))
	for name := range desired {
		names = append(names, name)
	}
	sort.Strings(names)
	sort.Strings(deletes)

	var reconcileErr error
	for _, name := range names {
		f.mu.RLock()
		operation := f.topicMaterialization[name].Operation
		f.mu.RUnlock()
		var err error
		switch operation {
		case TopicMaterializationRestore:
			err = f.materializeTopicRestore(desired[name])
		case TopicMaterializationTruncate:
			if allowGroupWrites {
				err = f.reconcileTopicGroups(name, operation, desired[name])
			}
			if err == nil {
				err = f.materializeTopicTruncate(desired[name])
			}
		default:
			err = f.materializeTopicCreate(desired[name])
		}
		if err != nil {
			reconcileErr = errors.Join(reconcileErr, err)
		}
	}
	for _, name := range deletes {
		var err error
		if allowGroupWrites {
			err = f.reconcileTopicGroups(name, TopicMaterializationDelete, nil)
		}
		if err == nil {
			err = f.materializeTopicDelete(name)
		}
		if err != nil {
			reconcileErr = errors.Join(reconcileErr, err)
		}
	}
	return reconcileErr
}

// Never hold materializationMu while originating a consumer metadata write:
// its partition commit must be able to pass through Raft Apply/Restore.
func (f *BrokerFSM) reconcileTopicGroups(name, operation string, expected *topic.Definition) error {
	f.mu.RLock()
	c := f.cd
	f.mu.RUnlock()
	if c == nil {
		return nil
	}
	_, err := c.ReconcileTopicGroupsIf(name, func() bool {
		f.mu.RLock()
		defer f.mu.RUnlock()
		return reflect.DeepEqual(f.topicState[name], expected) && !f.recoveryReplayPending
	})
	if err != nil {
		f.recordTopicMaterializationIfCurrent(name, operation, expected, err)
	}
	return err
}

func (f *BrokerFSM) materializeTopicCreate(definition *topic.Definition) error {
	if definition == nil || f.tm == nil {
		return nil
	}

	f.materializationMu.Lock()
	defer f.materializationMu.Unlock()

	f.mu.RLock()
	current := copyTopicDefinition(f.topicState[definition.Name])
	replaying := f.recoveryReplayPending
	f.mu.RUnlock()
	if !reflect.DeepEqual(current, definition) {
		return nil
	}

	var err error
	if f.tm.IsTruncationPending(definition.Name) {
		// A later Raft definition update may supersede a truncate whose local
		// reset is still pending. Rebuild the empty generation at the latest
		// authoritative definition before publishing its epoch marker.
		err = f.tm.RestoreDefinitions([]topic.Definition{*definition})
		if err == nil {
			err = f.tm.CompleteTruncation(definition.Name)
		}
	} else {
		err = f.tm.ApplyDefinition(*definition)
	}
	if err == nil {
		err = f.reconcileMaterializedTopicHWM(definition.Name, replaying)
	}
	f.recordTopicMaterializationIfCurrent(definition.Name, TopicMaterializationCreate, definition, err)
	if err != nil {
		return fmt.Errorf("materialize topic %q: %w", definition.Name, err)
	}
	return nil
}

func (f *BrokerFSM) materializeTopicRestore(definition *topic.Definition) error {
	if definition == nil {
		return nil
	}
	if f.tm == nil {
		f.recordTopicMaterialization(definition.Name, TopicMaterializationRestore, nil)
		return nil
	}

	f.materializationMu.Lock()
	defer f.materializationMu.Unlock()

	f.mu.RLock()
	current := copyTopicDefinition(f.topicState[definition.Name])
	f.mu.RUnlock()
	if !reflect.DeepEqual(current, definition) {
		return nil
	}

	err := f.tm.RestoreDefinitions([]topic.Definition{*definition})
	if err == nil && f.tm.IsTruncationPending(definition.Name) {
		err = f.tm.CompleteTruncation(definition.Name)
	}
	if err == nil {
		err = f.reconcileMaterializedTopicHWM(definition.Name, true)
	}
	f.recordTopicMaterializationIfCurrent(definition.Name, TopicMaterializationRestore, definition, err)
	if err != nil {
		return fmt.Errorf("restore local topic %q: %w", definition.Name, err)
	}
	return nil
}

func (f *BrokerFSM) reconcileMaterializedTopicHWM(topicName string, restoringSnapshot bool) error {
	if f.tm == nil {
		return nil
	}
	f.mu.RLock()
	definition := copyTopicDefinition(f.topicState[topicName])
	metadata := make(map[int]PartitionMetadata)
	if definition != nil {
		for partition := 0; partition < definition.Partitions; partition++ {
			if current := f.partitionMetadata[fmt.Sprintf("%s-%d", topicName, partition)]; current != nil {
				metadata[partition] = *current
			}
		}
	}
	f.mu.RUnlock()
	if definition == nil {
		return nil
	}
	currentTopic := f.tm.GetTopic(topicName)
	if currentTopic == nil {
		return fmt.Errorf("topic is not materialized")
	}
	for partition := 0; partition < definition.Partitions; partition++ {
		partitionMetadata, ok := metadata[partition]
		if !ok {
			// Authoritative restore/apply validation requires this metadata.
			// A missing entry here can only be a concurrently superseded local
			// materialization attempt, so leave it to the next desired state.
			continue
		}
		if !partitionMetadata.CommittedHWMKnown {
			continue
		}
		currentPartition, err := currentTopic.GetPartition(partition)
		if err != nil {
			return err
		}
		var reconcileErr error
		if restoringSnapshot {
			if !currentPartition.SnapshotRecoveryPending() {
				reconcileErr = currentPartition.ReconcileSnapshotHWM(partitionMetadata.CommittedHWM)
			}
		} else {
			// Live definition application must not wait for replication ownership:
			// its HWM command may be queued behind this same FSM application.
			// Publish only the committed visibility boundary and preserve any
			// in-flight tail. Leader promotion owns destructive reconciliation.
			reconcileErr = currentPartition.ApplyReplicaHWM(partitionMetadata.CommittedHWM)
		}
		if reconcileErr != nil {
			return fmt.Errorf("reconcile partition %d committed HWM: %w", partition, reconcileErr)
		}
		currentPartition.FlushDisk()
	}
	return nil
}

func (f *BrokerFSM) materializeTopicTruncate(definition *topic.Definition) error {
	if definition == nil {
		return nil
	}

	f.materializationMu.Lock()
	defer f.materializationMu.Unlock()
	f.mu.RLock()
	current := copyTopicDefinition(f.topicState[definition.Name])
	f.mu.RUnlock()
	if !reflect.DeepEqual(current, definition) {
		return nil
	}

	var err error
	if f.tm != nil {
		_, err = f.tm.ApplyTruncateDefinition(*definition)
	}
	if err == nil {
		err = f.cleanupTopicDependencies(definition.Name)
	}
	if err == nil && f.tm != nil {
		err = f.tm.CompleteTruncation(definition.Name)
	}
	f.recordTopicMaterializationIfCurrent(definition.Name, TopicMaterializationTruncate, definition, err)
	if err != nil {
		return fmt.Errorf("truncate local topic %q: %w", definition.Name, err)
	}
	return nil
}

func (f *BrokerFSM) materializeTopicDelete(name string) error {
	if name == config.ConsumerOffsetsTopicName {
		f.recordTopicMaterialization(name, TopicMaterializationDelete, nil)
		return nil
	}

	f.materializationMu.Lock()
	defer f.materializationMu.Unlock()

	f.mu.RLock()
	desired := f.topicState[name]
	replaying := f.recoveryReplayPending
	f.mu.RUnlock()
	if desired != nil {
		return nil
	}
	if replaying {
		// A topic absent from the snapshot may be recreated by committed
		// Raft commands still waiting to replay. Its local log must survive.
		// Keep cleanup pending even when the delete itself is replayed.
		f.mu.Lock()
		pendingSince := time.Now()
		if old := f.topicMaterialization[name]; old.Operation == TopicMaterializationDelete && !old.PendingSince.IsZero() {
			pendingSince = old.PendingSince
		}
		f.topicMaterialization[name] = TopicMaterializationIssue{
			Topic: name, Operation: TopicMaterializationDelete, PendingSince: pendingSince,
		}
		f.mu.Unlock()
		return nil
	}

	var err error
	if f.tm != nil {
		var deleted bool
		deleted, err = f.tm.DeleteTopicDurable(name)
		if err == nil && !deleted {
			err = f.tm.CleanupTopicStorage(name)
		}
	}
	if err == nil {
		err = f.cleanupTopicDependencies(name)
	}
	f.recordTopicMaterializationIfCurrent(name, TopicMaterializationDelete, nil, err)
	if err != nil {
		return fmt.Errorf("delete local topic %q: %w", name, err)
	}
	return nil
}

func (f *BrokerFSM) cleanupTopicDependencies(name string) error {
	f.mu.RLock()
	coordinatorRef := f.cd
	transactionManager := f.txn
	f.mu.RUnlock()

	if coordinatorRef != nil {
		if _, err := coordinatorRef.CheckTopicGroupCleanup(name); err != nil {
			return fmt.Errorf("delete consumer groups for topic %q: %w", name, err)
		}
	}
	if transactionManager != nil {
		if _, err := transactionManager.PruneTopicReferences(name); err != nil {
			return fmt.Errorf("prune transaction references for topic %q: %w", name, err)
		}
	}
	return nil
}

func (f *BrokerFSM) recordTopicMaterialization(name, operation string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordTopicMaterializationLocked(name, operation, err)
}

func (f *BrokerFSM) recordTopicMaterializationIfCurrent(name, operation string, expected *topic.Definition, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !reflect.DeepEqual(f.topicState[name], expected) {
		return
	}
	f.recordTopicMaterializationLocked(name, operation, err)
}

func (f *BrokerFSM) recordTopicMaterializationLocked(name, operation string, err error) {
	if f.topicMaterialization == nil {
		f.topicMaterialization = make(map[string]TopicMaterializationIssue)
	}
	if f.topicMaterializationRuns == nil {
		f.topicMaterializationRuns = make(map[string]TopicMaterializationAttempts)
	}
	attempts := f.topicMaterializationRuns[operation]
	if err == nil {
		attempts.Success++
		f.topicMaterializationRuns[operation] = attempts
		delete(f.topicMaterialization, name)
		return
	}
	attempts.Failure++
	f.topicMaterializationRuns[operation] = attempts
	pendingSince := time.Now()
	if existing, ok := f.topicMaterialization[name]; ok && !existing.PendingSince.IsZero() {
		pendingSince = existing.PendingSince
	}
	var cleanup *coordinator.TopicCleanupPending
	errors.As(err, &cleanup)
	var revision, lifecycleEpoch uint64
	if definition := f.topicState[name]; definition != nil {
		revision, lifecycleEpoch = definition.Revision, definition.LifecycleEpoch
	}
	f.topicMaterialization[name] = TopicMaterializationIssue{
		Topic:           name,
		Operation:       operation,
		Error:           err.Error(),
		PendingSince:    pendingSince,
		ConsumerCleanup: cleanup,
		Revision:        revision,
		LifecycleEpoch:  lifecycleEpoch,
	}
}
