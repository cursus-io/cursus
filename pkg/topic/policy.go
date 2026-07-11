package topic

import (
	"fmt"
	"strings"
)

const (
	PartitionerHashKey    = "hash_key"
	PartitionerRoundRobin = "round_robin"
	AuthPolicyOpen        = "open"
	AuthPolicyDenyWrite   = "deny_write"
	AuthPolicyDenyRead    = "deny_read"
)

type Policy struct {
	RetentionHours int    `json:"retention_hours,omitempty"`
	RetentionBytes int64  `json:"retention_bytes,omitempty"`
	Partitioner    string `json:"partitioner"`
	AuthPolicy     string `json:"auth_policy"`
}

func DefaultPolicy() Policy {
	return Policy{
		Partitioner: PartitionerHashKey,
		AuthPolicy:  AuthPolicyOpen,
	}
}

func (p Policy) Normalize() (Policy, error) {
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
	case AuthPolicyOpen, AuthPolicyDenyWrite, AuthPolicyDenyRead:
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

func (p Policy) CanRead() bool {
	return p.AuthPolicy != AuthPolicyDenyRead
}

func (p Policy) CanWrite() bool {
	return p.AuthPolicy != AuthPolicyDenyWrite
}
