package topic

import (
	"fmt"
	"math"
	"reflect"

	"github.com/cursus-io/cursus/pkg/config"
)

const (
	DefaultPartitionCount     = 4
	DefaultReplicationFactor  = 3
	InitialDefinitionRevision = 1
	InitialLifecycleEpoch     = 1
)

// DefinitionPatch represents only fields explicitly supplied by a CREATE
// command. A nil pointer means omission; pointers to zero, false, or an empty
// ACL are authoritative values.
type DefinitionPatch struct {
	Partitions        *int      `json:"partitions,omitempty"`
	ReplicationFactor *int      `json:"replication_factor,omitempty"`
	Idempotent        *bool     `json:"idempotent,omitempty"`
	EventSourcing     *bool     `json:"event_sourcing,omitempty"`
	MinInSyncReplicas *int      `json:"min_in_sync_replicas,omitempty"`
	CleanupPolicy     *string   `json:"cleanup_policy,omitempty"`
	RetentionHours    *int      `json:"retention_hours,omitempty"`
	RetentionBytes    *int64    `json:"retention_bytes,omitempty"`
	Partitioner       *string   `json:"partitioner,omitempty"`
	AuthPolicy        *string   `json:"auth_policy,omitempty"`
	ReadACL           *[]string `json:"read_acl,omitempty"`
	WriteACL          *[]string `json:"write_acl,omitempty"`
}

// DefaultDefinition returns the complete definition used only when a topic is
// absent. Existing topics must be updated with MergeDefinitionPatch instead.
func DefaultDefinition(name string, cfg *config.Config) Definition {
	policy := DefaultPolicy()
	replicationFactor := DefaultReplicationFactor
	if cfg != nil {
		if cfg.CleanupPolicy != "" {
			policy.CleanupPolicy = cfg.CleanupPolicy
		}
		if cfg.DefaultReplicationFactor > 0 {
			replicationFactor = cfg.DefaultReplicationFactor
		}
	}
	return Definition{
		Name:              name,
		Revision:          InitialDefinitionRevision,
		LifecycleEpoch:    InitialLifecycleEpoch,
		Partitions:        DefaultPartitionCount,
		ReplicationFactor: replicationFactor,
		Policy:            policy,
	}
}

// MergeDefinitionPatch overlays explicit fields on a complete definition.
// Existing immutable modes may be restated but not changed.
func MergeDefinitionPatch(current Definition, patch DefinitionPatch, existing bool) (Definition, error) {
	base, err := current.Normalize()
	if err != nil {
		return current, err
	}
	next := base

	if patch.Partitions != nil {
		if *patch.Partitions <= 0 {
			return current, fmt.Errorf("partitions must be > 0")
		}
		if existing && *patch.Partitions < base.Partitions {
			return current, fmt.Errorf(
				"cannot decrease partition count for topic %q: %d -> %d",
				base.Name,
				base.Partitions,
				*patch.Partitions,
			)
		}
		next.Partitions = *patch.Partitions
	}
	if patch.ReplicationFactor != nil {
		if *patch.ReplicationFactor <= 0 {
			return current, fmt.Errorf("replication_factor must be > 0")
		}
		if existing && *patch.ReplicationFactor != base.ReplicationFactor {
			return current, fmt.Errorf(
				"replication_factor is immutable for existing topic %q: current=%d requested=%d",
				base.Name,
				base.ReplicationFactor,
				*patch.ReplicationFactor,
			)
		}
		next.ReplicationFactor = *patch.ReplicationFactor
	}
	if patch.Idempotent != nil {
		if existing && *patch.Idempotent != base.Idempotent {
			return current, fmt.Errorf(
				"idempotent mode is immutable for existing topic %q: current=%t requested=%t",
				base.Name,
				base.Idempotent,
				*patch.Idempotent,
			)
		}
		next.Idempotent = *patch.Idempotent
	}
	if patch.EventSourcing != nil {
		if existing && *patch.EventSourcing != base.EventSourcing {
			return current, fmt.Errorf(
				"event_sourcing mode is immutable for existing topic %q: current=%t requested=%t",
				base.Name,
				base.EventSourcing,
				*patch.EventSourcing,
			)
		}
		next.EventSourcing = *patch.EventSourcing
	}
	if patch.MinInSyncReplicas != nil {
		value := *patch.MinInSyncReplicas
		next.Policy.MinInSyncReplicas = &value
	}
	if patch.CleanupPolicy != nil {
		next.Policy.CleanupPolicy = *patch.CleanupPolicy
	}
	if patch.RetentionHours != nil {
		next.Policy.RetentionHours = *patch.RetentionHours
	}
	if patch.RetentionBytes != nil {
		next.Policy.RetentionBytes = *patch.RetentionBytes
	}
	if patch.Partitioner != nil {
		next.Policy.Partitioner = *patch.Partitioner
	}
	if patch.AuthPolicy != nil {
		next.Policy.AuthPolicy = *patch.AuthPolicy
	}
	if patch.ReadACL != nil {
		next.Policy.ReadACL = append([]string(nil), (*patch.ReadACL)...)
	}
	if patch.WriteACL != nil {
		next.Policy.WriteACL = append([]string(nil), (*patch.WriteACL)...)
	}

	next, err = next.Normalize()
	if err != nil {
		return current, err
	}
	if !existing {
		next.Revision = InitialDefinitionRevision
		return next, nil
	}
	if definitionsEqualWithoutRevision(base, next) {
		next.Revision = base.Revision
		return next, nil
	}
	if base.Revision == math.MaxUint64 {
		return current, fmt.Errorf("topic definition revision overflow for %q", base.Name)
	}
	next.Revision = base.Revision + 1
	return next, nil
}

// UpdateDefinitionMinInSyncReplicas updates or clears the optional durable
// override while preserving the definition revision contract.
func UpdateDefinitionMinInSyncReplicas(current Definition, value *int) (Definition, error) {
	base, err := current.Normalize()
	if err != nil {
		return current, err
	}
	next := base
	next.Policy.MinInSyncReplicas = nil
	if value != nil {
		copyValue := *value
		next.Policy.MinInSyncReplicas = &copyValue
	}
	next, err = next.Normalize()
	if err != nil {
		return current, err
	}
	if definitionsEqualWithoutRevision(base, next) {
		next.Revision = base.Revision
		return next, nil
	}
	if base.Revision == math.MaxUint64 {
		return current, fmt.Errorf("topic definition revision overflow for %q", base.Name)
	}
	next.Revision = base.Revision + 1
	return next, nil
}

func definitionsEqualWithoutRevision(left, right Definition) bool {
	left.Revision = 0
	right.Revision = 0
	left.Policy.ReadACL = canonicalACL(left.Policy.ReadACL)
	left.Policy.WriteACL = canonicalACL(left.Policy.WriteACL)
	right.Policy.ReadACL = canonicalACL(right.Policy.ReadACL)
	right.Policy.WriteACL = canonicalACL(right.Policy.WriteACL)
	return reflect.DeepEqual(left, right)
}

func canonicalACL(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return values
}
