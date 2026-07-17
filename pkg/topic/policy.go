package topic

import (
	"fmt"
	"strings"

	"github.com/cursus-io/cursus/pkg/config"
)

const (
	PartitionerHashKey    = "hash_key"
	PartitionerRoundRobin = "round_robin"
	AuthPolicyOpen        = "open"
	AuthPolicyDenyWrite   = "deny_write"
	AuthPolicyDenyRead    = "deny_read"
	AuthPolicyACL         = "acl"
)

type Policy struct {
	RetentionHours int      `json:"retention_hours,omitempty"`
	RetentionBytes int64    `json:"retention_bytes,omitempty"`
	CleanupPolicy  string   `json:"cleanup_policy"`
	Partitioner    string   `json:"partitioner"`
	AuthPolicy     string   `json:"auth_policy"`
	ReadACL        []string `json:"read_acl,omitempty"`
	WriteACL       []string `json:"write_acl,omitempty"`
}

func DefaultPolicy() Policy {
	return Policy{
		CleanupPolicy: config.CleanupPolicyDelete,
		Partitioner:   PartitionerHashKey,
		AuthPolicy:    AuthPolicyOpen,
	}
}

func (p Policy) Normalize() (Policy, error) {
	if p.CleanupPolicy == "" {
		p.CleanupPolicy = config.CleanupPolicyDelete
	}
	normalizedCleanup, ok := config.NormalizeCleanupPolicy(p.CleanupPolicy)
	if !ok {
		return p, fmt.Errorf("invalid cleanup policy %q", p.CleanupPolicy)
	}
	p.CleanupPolicy = normalizedCleanup

	if p.Partitioner == "" {
		p.Partitioner = PartitionerHashKey
	}
	p.Partitioner = strings.ToLower(strings.TrimSpace(p.Partitioner))
	switch p.Partitioner {
	case PartitionerHashKey, PartitionerRoundRobin:
	default:
		return p, fmt.Errorf("invalid partitioner %q", p.Partitioner)
	}

	if p.AuthPolicy == "" {
		p.AuthPolicy = AuthPolicyOpen
	}
	p.AuthPolicy = strings.ToLower(strings.TrimSpace(p.AuthPolicy))
	switch p.AuthPolicy {
	case AuthPolicyOpen, AuthPolicyDenyWrite, AuthPolicyDenyRead, AuthPolicyACL:
	default:
		return p, fmt.Errorf("invalid auth policy %q", p.AuthPolicy)
	}

	if p.RetentionHours < 0 {
		return p, fmt.Errorf("retention_hours must be >= 0")
	}
	if p.RetentionBytes < 0 {
		return p, fmt.Errorf("retention_bytes must be >= 0")
	}
	return p, nil
}

func validateCleanupPolicyForTopic(policy Policy, cfg *config.Config, eventSourcing bool) error {
	if !config.HasCleanupPolicy(policy.CleanupPolicy, config.CleanupPolicyCompact) {
		return nil
	}
	if eventSourcing {
		return fmt.Errorf("cleanup policy compact is not supported for event-sourcing topics")
	}
	if cfg != nil && cfg.EnabledDistribution {
		return fmt.Errorf("cleanup policy compact is not supported in distributed mode")
	}
	return nil
}

func (p Policy) CanRead() bool {
	return p.AuthPolicy != AuthPolicyDenyRead
}

func (p Policy) CanWrite() bool {
	return p.AuthPolicy != AuthPolicyDenyWrite
}

func (p Policy) CanReadPrincipal(principal string) bool {
	if p.AuthPolicy == AuthPolicyDenyRead {
		return false
	}
	if p.AuthPolicy != AuthPolicyACL {
		return true
	}
	return aclContains(p.ReadACL, principal)
}

func (p Policy) CanWritePrincipal(principal string) bool {
	if p.AuthPolicy == AuthPolicyDenyWrite {
		return false
	}
	if p.AuthPolicy != AuthPolicyACL {
		return true
	}
	return aclContains(p.WriteACL, principal)
}

func aclContains(acl []string, principal string) bool {
	principal = strings.TrimSpace(principal)
	if principal == "" {
		return false
	}
	for _, item := range acl {
		item = strings.TrimSpace(item)
		if item == "*" || item == principal {
			return true
		}
	}
	return false
}
