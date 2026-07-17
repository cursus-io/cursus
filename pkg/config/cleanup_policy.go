package config

import (
	"sort"
	"strings"
)

const (
	CleanupPolicyDelete        = "delete"
	CleanupPolicyCompact       = "compact"
	CleanupPolicyDeleteCompact = "delete,compact"
)

// NormalizeCleanupPolicy returns the canonical cleanup policy and whether the
// input contains only supported policy names.
func NormalizeCleanupPolicy(value string) (string, bool) {
	parts := strings.Split(strings.ToLower(strings.TrimSpace(value)), ",")
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		switch part {
		case CleanupPolicyDelete, CleanupPolicyCompact:
			seen[part] = struct{}{}
		default:
			return "", false
		}
	}
	values := make([]string, 0, len(seen))
	for value := range seen {
		values = append(values, value)
	}
	sort.Strings(values)
	if len(values) == 2 {
		return CleanupPolicyDeleteCompact, true
	}
	return values[0], true
}

// HasCleanupPolicy reports whether a canonical cleanup policy includes the expected policy.
func HasCleanupPolicy(policy, expected string) bool {
	normalized, ok := NormalizeCleanupPolicy(policy)
	if !ok {
		return false
	}
	for _, value := range strings.Split(normalized, ",") {
		if value == expected {
			return true
		}
	}
	return false
}
