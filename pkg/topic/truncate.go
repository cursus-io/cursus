package topic

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/config"
)

const lifecycleEpochMarkerName = ".cursus_lifecycle_epoch"

var ErrTopicRevisionConflict = errors.New("topic revision conflict")

// TruncateResult reports the authoritative lifecycle transition. Once
// Truncated is true, the old lifecycle epoch must never be served again even
// when node-local cleanup still needs a retry.
type TruncateResult struct {
	Topic          string
	Truncated      bool
	CleanupPending bool
	Definition     Definition
}

// TruncateTopicDurable commits a new standalone lifecycle epoch before
// replacing local storage with an empty generation. The caller must clean
// group/transaction dependencies and call CompleteTruncation before the topic
// is made available again.
func (tm *TopicManager) TruncateTopicDurable(name string, expectedRevision uint64) (TruncateResult, error) {
	if tm == nil {
		return TruncateResult{}, fmt.Errorf("topic manager is not available")
	}
	if name == config.ConsumerOffsetsTopicName {
		return TruncateResult{}, fmt.Errorf("cannot truncate broker-owned internal consumer metadata topic")
	}
	if err := ValidateName(name); err != nil {
		return TruncateResult{}, fmt.Errorf("invalid topic name: %w", err)
	}

	tm.mu.Lock()
	target, pending := tm.pendingTruncations[name]
	current := tm.topics[name]
	if !pending && current == nil {
		tm.mu.Unlock()
		return TruncateResult{}, fmt.Errorf("%w: %s", ErrTopicNotFound, name)
	}
	if pending {
		if expectedRevision == math.MaxUint64 || target.Revision != expectedRevision+1 {
			tm.mu.Unlock()
			return TruncateResult{}, revisionConflict(name, target.Revision, expectedRevision)
		}
	} else {
		definition := current.Definition()
		if definition.Revision != expectedRevision {
			tm.mu.Unlock()
			return TruncateResult{}, revisionConflict(name, definition.Revision, expectedRevision)
		}
		if definition.Revision == math.MaxUint64 || definition.LifecycleEpoch == math.MaxUint64 {
			tm.mu.Unlock()
			return TruncateResult{}, fmt.Errorf("topic lifecycle counter overflow for %q", name)
		}
		target = definition
		target.Revision++
		target.LifecycleEpoch++
	}
	if tm.deleting[name] {
		tm.mu.Unlock()
		return TruncateResult{}, fmt.Errorf("topic %q lifecycle operation is already in progress", name)
	}
	tm.deleting[name] = true
	hook := tm.deleteHook
	tm.mu.Unlock()

	committed := pending
	if hook != nil {
		if err := hook(name); err != nil {
			tm.mu.Lock()
			delete(tm.deleting, name)
			tm.mu.Unlock()
			return TruncateResult{Topic: name, Truncated: committed, CleanupPending: committed, Definition: target}, fmt.Errorf("close derived topic state %q: %w", name, err)
		}
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()
	defer delete(tm.deleting, name)
	if !pending {
		if err := tm.persistDefinitionLocked(target); err != nil {
			return TruncateResult{Topic: name, Definition: current.Definition()}, err
		}
		tm.pendingTruncations[name] = target
		committed = true
	}
	if err := tm.resetTopicStorageLocked(target); err != nil {
		return TruncateResult{Topic: name, Truncated: committed, CleanupPending: true, Definition: target}, err
	}
	return TruncateResult{Topic: name, Truncated: true, CleanupPending: true, Definition: target}, nil
}

// ApplyTruncateDefinition materializes an authoritative Raft lifecycle epoch.
// It deliberately does not clear the pending fence; the FSM does that only
// after all replicated dependency state has been updated.
func (tm *TopicManager) ApplyTruncateDefinition(raw Definition) (TruncateResult, error) {
	if tm == nil {
		return TruncateResult{}, fmt.Errorf("topic manager is not available")
	}
	target, err := raw.Normalize()
	if err != nil {
		return TruncateResult{}, err
	}
	if target.Name == config.ConsumerOffsetsTopicName {
		return TruncateResult{}, fmt.Errorf("cannot truncate broker-owned internal consumer metadata topic")
	}

	tm.mu.Lock()
	pendingTarget, pending := tm.pendingTruncations[target.Name]
	current := tm.topics[target.Name]
	if pending {
		if !reflect.DeepEqual(pendingTarget, target) {
			tm.mu.Unlock()
			return TruncateResult{}, fmt.Errorf("pending truncate definition mismatch for %q", target.Name)
		}
	} else {
		if current == nil {
			tm.mu.Unlock()
			return TruncateResult{}, fmt.Errorf("%w: %s", ErrTopicNotFound, target.Name)
		}
		definition := current.Definition()
		if target.Revision != definition.Revision+1 || target.LifecycleEpoch != definition.LifecycleEpoch+1 {
			tm.mu.Unlock()
			return TruncateResult{}, fmt.Errorf(
				"invalid truncate transition for %q: revision=%d/%d lifecycle_epoch=%d/%d",
				target.Name, definition.Revision, target.Revision, definition.LifecycleEpoch, target.LifecycleEpoch,
			)
		}
	}
	if tm.deleting[target.Name] {
		tm.mu.Unlock()
		return TruncateResult{}, fmt.Errorf("topic %q lifecycle operation is already in progress", target.Name)
	}
	// Raft has already committed this lifecycle transition before calling this
	// method. Fence the old local generation before any fallible derived-state
	// or storage work so it can never be served after the authoritative commit.
	if !pending {
		tm.pendingTruncations[target.Name] = target
	}
	tm.deleting[target.Name] = true
	hook := tm.deleteHook
	tm.mu.Unlock()

	if hook != nil {
		if err := hook(target.Name); err != nil {
			tm.mu.Lock()
			delete(tm.deleting, target.Name)
			tm.mu.Unlock()
			return TruncateResult{Topic: target.Name, Truncated: true, CleanupPending: true, Definition: target}, fmt.Errorf("close derived topic state %q: %w", target.Name, err)
		}
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()
	defer delete(tm.deleting, target.Name)
	// Retrying a pending distributed materialization must also retry this
	// node-local manifest write; the first attempt may have failed after the
	// in-memory fence was installed.
	if err := tm.persistDefinitionLocked(target); err != nil {
		return TruncateResult{Topic: target.Name, Truncated: true, CleanupPending: true, Definition: target}, err
	}
	if err := tm.resetTopicStorageLocked(target); err != nil {
		return TruncateResult{Topic: target.Name, Truncated: true, CleanupPending: true, Definition: target}, err
	}
	return TruncateResult{Topic: target.Name, Truncated: true, CleanupPending: true, Definition: target}, nil
}

func (tm *TopicManager) resetTopicStorageLocked(target Definition) error {
	current := tm.topics[target.Name]
	delete(tm.topics, target.Name)
	if current != nil {
		for _, partition := range current.Partitions {
			partition.Close()
		}
	}
	if closer, ok := tm.hp.(topicHandlerCloser); ok {
		closer.CloseTopicHandlers(target.Name)
	} else if current != nil {
		for _, partition := range current.Partitions {
			if err := partition.dh.Close(); err != nil {
				return fmt.Errorf("close topic storage %q[%d]: %w", target.Name, partition.ID(), err)
			}
		}
	}
	if err := tm.deleteTopicLogDirLocked(target.Name); err != nil {
		return fmt.Errorf("remove old lifecycle storage %q: %w", target.Name, err)
	}
	recreated, err := newTopicWithDefinition(target, tm.hp, tm.cfg, tm.StreamManager)
	if err != nil {
		return fmt.Errorf("create empty lifecycle storage %q: %w", target.Name, err)
	}
	recreated.SetTransactionDecisionResolver(tm.txnResolver)
	tm.topics[target.Name] = recreated
	return nil
}

// CompleteTruncation publishes the local epoch marker only after every
// dependency store is clean. Until then callers can use IsTruncationPending to
// fail closed.
func (tm *TopicManager) CompleteTruncation(name string) error {
	if tm == nil {
		return fmt.Errorf("topic manager is not available")
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	target, pending := tm.pendingTruncations[name]
	if !pending {
		return nil
	}
	current := tm.topics[name]
	if current == nil || current.LifecycleEpoch != target.LifecycleEpoch {
		return fmt.Errorf("topic %q empty lifecycle storage is not materialized", name)
	}
	if err := tm.writeLifecycleEpochMarkerLocked(target); err != nil {
		return err
	}
	delete(tm.pendingTruncations, name)
	return nil
}

func (tm *TopicManager) IsTruncationPending(name string) bool {
	if tm == nil {
		return false
	}
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	_, pending := tm.pendingTruncations[name]
	return pending
}

func (tm *TopicManager) PendingTruncations() []Definition {
	if tm == nil {
		return nil
	}
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	definitions := make([]Definition, 0, len(tm.pendingTruncations))
	for _, definition := range tm.pendingTruncations {
		definitions = append(definitions, definition)
	}
	sort.Slice(definitions, func(i, j int) bool { return definitions[i].Name < definitions[j].Name })
	return definitions
}

func revisionConflict(name string, current, expected uint64) error {
	return fmt.Errorf("%w for topic %q: current=%d expected=%d", ErrTopicRevisionConflict, name, current, expected)
}

func (tm *TopicManager) prepareTruncationRecoveryLocked(definition Definition) error {
	logDir := ""
	if tm.cfg != nil {
		logDir = tm.cfg.LogDir
	}
	pending, err := lifecycleEpochResetPendingConfig(logDir, definition)
	if err != nil {
		return err
	}
	if !pending {
		return nil
	}
	if current := tm.topics[definition.Name]; current != nil {
		delete(tm.topics, definition.Name)
		for _, partition := range current.Partitions {
			partition.Close()
		}
	}
	if closer, ok := tm.hp.(topicHandlerCloser); ok {
		closer.CloseTopicHandlers(definition.Name)
	}
	if err := tm.deleteTopicLogDirLocked(definition.Name); err != nil {
		return fmt.Errorf("recover pending truncate storage %q: %w", definition.Name, err)
	}
	tm.pendingTruncations[definition.Name] = definition
	return nil
}

func lifecycleEpochResetPendingConfig(logDir string, definition Definition) (bool, error) {
	if strings.TrimSpace(logDir) == "" || definition.LifecycleEpoch <= InitialLifecycleEpoch {
		return false, nil
	}
	marker, present, err := readLifecycleEpochMarker(filepath.Join(logDir, definition.Name, lifecycleEpochMarkerName))
	if err != nil {
		return false, err
	}
	if present && marker > definition.LifecycleEpoch {
		return false, fmt.Errorf("topic %q storage lifecycle epoch %d is ahead of metadata epoch %d", definition.Name, marker, definition.LifecycleEpoch)
	}
	return !present || marker != definition.LifecycleEpoch, nil
}

func readLifecycleEpochMarker(path string) (uint64, bool, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path is derived from the broker-owned log root.
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read lifecycle epoch marker: %w", err)
	}
	epoch, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || epoch == 0 {
		return 0, false, fmt.Errorf("invalid lifecycle epoch marker %q", strings.TrimSpace(string(data)))
	}
	return epoch, true, nil
}

func (tm *TopicManager) writeLifecycleEpochMarkerLocked(definition Definition) error {
	if tm.cfg == nil || strings.TrimSpace(tm.cfg.LogDir) == "" {
		return nil
	}
	dir := filepath.Join(tm.cfg.LogDir, definition.Name)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create lifecycle marker directory: %w", err)
	}
	path := filepath.Join(dir, lifecycleEpochMarkerName)
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) // #nosec G304 -- path is broker-owned.
	if err != nil {
		return fmt.Errorf("open lifecycle epoch marker: %w", err)
	}
	if _, err := file.WriteString(strconv.FormatUint(definition.LifecycleEpoch, 10) + "\n"); err != nil {
		_ = file.Close()
		return fmt.Errorf("write lifecycle epoch marker: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync lifecycle epoch marker: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close lifecycle epoch marker: %w", err)
	}
	if err := replaceCheckpointFile(tmp, path); err != nil {
		return fmt.Errorf("replace lifecycle epoch marker: %w", err)
	}
	if err := syncTopicMetadataDirectoryFn(dir); err != nil {
		return fmt.Errorf("sync lifecycle epoch marker directory: %w", err)
	}
	return nil
}
