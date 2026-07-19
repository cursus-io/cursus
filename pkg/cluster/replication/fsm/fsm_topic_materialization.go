package fsm

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/cursus-io/cursus/pkg/topic"
)

const (
	TopicMaterializationCreate  = "create"
	TopicMaterializationRestore = "restore"
	TopicMaterializationDelete  = "delete"
)

// TopicMaterializationIssue describes node-local work that has not converged
// with the authoritative topic state committed by Raft.
type TopicMaterializationIssue struct {
	Topic        string    `json:"topic"`
	Operation    string    `json:"operation"`
	Error        string    `json:"error"`
	PendingSince time.Time `json:"pending_since"`
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
	f.mu.RLock()
	desired := make(map[string]*topic.Definition)
	deletes := make([]string, 0)
	for name, issue := range f.topicMaterialization {
		switch issue.Operation {
		case TopicMaterializationCreate, TopicMaterializationRestore:
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
		if operation == TopicMaterializationRestore {
			err = f.materializeTopicRestore(desired[name])
		} else {
			err = f.materializeTopicCreate(desired[name])
		}
		if err != nil {
			reconcileErr = errors.Join(reconcileErr, err)
		}
	}
	for _, name := range deletes {
		if err := f.materializeTopicDelete(name); err != nil {
			reconcileErr = errors.Join(reconcileErr, err)
		}
	}
	return reconcileErr
}

func (f *BrokerFSM) materializeTopicCreate(definition *topic.Definition) error {
	if definition == nil || f.tm == nil {
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

	err := f.tm.CreateTopicWithPolicy(definition.Name, definition.Partitions, definition.Idempotent, definition.EventSourcing, definition.Policy)
	f.recordTopicMaterialization(definition.Name, TopicMaterializationCreate, err)
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
	f.recordTopicMaterialization(definition.Name, TopicMaterializationRestore, err)
	if err != nil {
		return fmt.Errorf("restore local topic %q: %w", definition.Name, err)
	}
	return nil
}

func (f *BrokerFSM) materializeTopicDelete(name string) error {
	if f.tm == nil {
		f.recordTopicMaterialization(name, TopicMaterializationDelete, nil)
		return nil
	}

	f.materializationMu.Lock()
	defer f.materializationMu.Unlock()

	f.mu.RLock()
	desired := f.topicState[name]
	f.mu.RUnlock()
	if desired != nil {
		return nil
	}

	deleted, err := f.tm.DeleteTopicDurable(name)
	if err == nil && !deleted {
		err = f.tm.CleanupTopicStorage(name)
	}
	f.recordTopicMaterialization(name, TopicMaterializationDelete, err)
	if err != nil {
		return fmt.Errorf("delete local topic %q: %w", name, err)
	}
	return nil
}

func (f *BrokerFSM) recordTopicMaterialization(name, operation string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	f.topicMaterialization[name] = TopicMaterializationIssue{
		Topic:        name,
		Operation:    operation,
		Error:        err.Error(),
		PendingSince: pendingSince,
	}
}
