package topic

import (
	"fmt"
	"math"
)

// ValidInstanceID accepts only bounded, wire-safe opaque broker identifiers.
func ValidInstanceID(id string) bool {
	if len(id) < 16 || len(id) > 64 {
		return false
	}
	for _, c := range id {
		valid := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_'
		if !valid {
			return false
		}
	}
	return true
}

func (d Definition) ValidateTruncateReceipt() error {
	if d.InstanceID != "" && !ValidInstanceID(d.InstanceID) {
		return fmt.Errorf("invalid topic instance identifier")
	}
	if d.TruncateRevision == 0 && d.TruncateEpoch == 0 {
		return nil
	}
	if d.InstanceID == "" || d.TruncateRevision < 2 || d.TruncateRevision > d.Revision ||
		d.TruncateEpoch < 2 || d.TruncateEpoch != d.LifecycleEpoch {
		return fmt.Errorf("invalid truncate receipt for topic %q", d.Name)
	}
	return nil
}

// BindInstance upgrades a legacy definition only on an explicit admin CREATE.
// A configuration update and initial binding consume one revision together.
func BindInstance(target Definition, candidate string, previousRevision uint64) (Definition, error) {
	if target.InstanceID != "" || candidate == "" {
		return target, nil
	}
	if !ValidInstanceID(candidate) {
		return target, fmt.Errorf("invalid topic instance identifier")
	}
	if previousRevision != 0 && target.Revision == previousRevision {
		if target.Revision == math.MaxUint64 {
			return target, fmt.Errorf("topic definition revision overflow")
		}
		target.Revision++
	}
	target.InstanceID = candidate
	return target, nil
}

// CheckTruncate identifies only the immediately retained reset. Ordinary writes
// do not invalidate the receipt; another definition mutation does. A receipt is
// a historical result, never permission to clear the newly published generation.
func (d Definition) CheckTruncate(expected uint64, instance string) (bool, error) {
	if instance != "" && (instance != d.InstanceID || !ValidInstanceID(instance)) {
		return false, fmt.Errorf("%w: topic instance changed or is unbound", ErrTopicRevisionConflict)
	}
	if instance != "" && expected != math.MaxUint64 && d.TruncateRevision == expected+1 &&
		d.Revision == d.TruncateRevision && d.LifecycleEpoch == d.TruncateEpoch {
		return true, nil
	}
	if expected != d.Revision {
		return false, revisionConflict(d.Name, d.Revision, expected)
	}
	return false, nil
}

// TruncateRetryResult is read-only and includes the local publication fence.
func (tm *TopicManager) TruncateRetryResult(name string, expected uint64, instance string) (TruncateResult, bool, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	d, pending := tm.pendingTruncations[name]
	if !pending {
		t := tm.topics[name]
		if t == nil {
			return TruncateResult{}, false, fmt.Errorf("%w: %s", ErrTopicNotFound, name)
		}
		d = t.Definition()
	}
	replay, err := d.CheckTruncate(expected, instance)
	return TruncateResult{Topic: name, Truncated: true, Definition: d, CleanupPending: pending, Replayed: replay}, replay, err
}
