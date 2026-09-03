package controller

import (
	"fmt"
	"strconv"
	"strings"

	replicationFSM "github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
)

func distributedTopicCommandPayload(defaults topic.Definition, patch topic.DefinitionPatch, current *topic.Definition) (map[string]interface{}, error) {
	base := defaults
	existing := false
	if current != nil {
		base = *current
		existing = true
	}
	definition, err := topic.MergeDefinitionPatch(base, patch, existing)
	if err != nil {
		return nil, err
	}
	if definition.Policy.MinInSyncReplicas != nil && *definition.Policy.MinInSyncReplicas > definition.ReplicationFactor {
		return nil, fmt.Errorf("min_in_sync_replicas %d exceeds replication factor %d", *definition.Policy.MinInSyncReplicas, definition.ReplicationFactor)
	}
	return map[string]interface{}{
		"definition":            defaults,
		"patch":                 patch,
		"committed_hwm_version": replicationFSM.CommittedHWMVersionCurrent,
	}, nil
}

func parseTopicDefinitionPatch(args map[string]string) (topic.DefinitionPatch, string) {
	var patch topic.DefinitionPatch
	if value, ok := args["partitions"]; ok {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			return patch, "ERROR: invalid_partitions reason=\"must be a positive integer\""
		}
		patch.Partitions = &parsed
	}
	if value, ok := args["replication_factor"]; ok {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			return patch, "ERROR: invalid_replication_factor reason=\"must be a positive integer\""
		}
		patch.ReplicationFactor = &parsed
	}
	if value, ok := args["idempotent"]; ok {
		parsed, valid := parseCreateBool(value)
		if !valid {
			return patch, fmt.Sprintf("ERROR: invalid_idempotent value=%q", value)
		}
		patch.Idempotent = &parsed
	}
	if value, ok := args["event_sourcing"]; ok {
		parsed, valid := parseCreateBool(value)
		if !valid {
			return patch, fmt.Sprintf("ERROR: invalid_event_sourcing value=%q", value)
		}
		patch.EventSourcing = &parsed
	}
	if value, ok := args["min_in_sync_replicas"]; ok {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 {
			return patch, fmt.Sprintf("ERROR: invalid_min_in_sync_replicas value=%s", value)
		}
		patch.MinInSyncReplicas = &parsed
	}
	if value, ok := args["cleanup_policy"]; ok {
		if _, valid := config.NormalizeCleanupPolicy(value); !valid {
			return patch, fmt.Sprintf("ERROR: invalid_topic_policy field=cleanup_policy reason=%q", "invalid cleanup policy "+value)
		}
		patch.CleanupPolicy = &value
	}
	if value, ok := args["retention_hours"]; ok {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return patch, fmt.Sprintf("ERROR: invalid_retention_hours value=%s", value)
		}
		patch.RetentionHours = &parsed
	}
	if value, ok := args["retention_bytes"]; ok {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return patch, fmt.Sprintf("ERROR: invalid_retention_bytes value=%s", value)
		}
		patch.RetentionBytes = &parsed
	}
	if value, ok := args["partitioner"]; ok {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case topic.PartitionerHashKey, topic.PartitionerRoundRobin:
			patch.Partitioner = &value
		default:
			return patch, fmt.Sprintf("ERROR: invalid_topic_policy field=partitioner reason=%q", "invalid partitioner "+value)
		}
	}
	if value, ok := args["auth_policy"]; ok {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case topic.AuthPolicyOpen, topic.AuthPolicyDenyWrite, topic.AuthPolicyDenyRead, topic.AuthPolicyACL:
			patch.AuthPolicy = &value
		default:
			return patch, fmt.Sprintf("ERROR: invalid_topic_policy field=auth_policy reason=%q", "invalid auth policy "+value)
		}
	}
	if value, ok := args["read_acl"]; ok {
		parsed := parseACLArg(value)
		patch.ReadACL = &parsed
	}
	if value, ok := args["write_acl"]; ok {
		parsed := parseACLArg(value)
		patch.WriteACL = &parsed
	}
	return patch, ""
}

func parseCreateBool(value string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		return false, false
	}
}

func formatCreateTopicError(topicName string, err error) string {
	reason := err.Error()
	switch {
	case strings.Contains(reason, "cleanup policy compact is not supported in distributed mode"):
		return `ERROR: unsupported_topic_policy field=cleanup_policy reason="compaction is not supported in distributed mode"`
	case strings.Contains(reason, "distributed compaction requires broker protocol"):
		return fmt.Sprintf(`ERROR: unsupported_topic_policy field=cleanup_policy reason=%q`, reason)
	case strings.Contains(reason, "cleanup policy compact is not supported for event-sourcing topics"):
		return `ERROR: invalid_topic_policy field=cleanup_policy reason="compaction is not supported for event-sourcing topics"`
	case strings.Contains(reason, "min_in_sync_replicas"):
		return fmt.Sprintf("ERROR: invalid_min_in_sync_replicas reason=%q", reason)
	case strings.Contains(reason, "invalid cleanup policy"),
		strings.Contains(reason, "invalid partitioner"),
		strings.Contains(reason, "invalid auth policy"),
		strings.Contains(reason, "retention_hours"),
		strings.Contains(reason, "retention_bytes"):
		return fmt.Sprintf("ERROR: invalid_topic_policy reason=%q", reason)
	default:
		return fmt.Sprintf("ERROR: create_topic_failed topic=%s reason=%q", topicName, reason)
	}
}

func formatTopicDefinitionResponse(definition topic.Definition) string {
	return fmt.Sprintf(
		"OK topic=%s partitions=%d cleanup_policy=%s partitioner=%s auth_policy=%s read_acl=%s write_acl=%s retention_hours=%d retention_bytes=%d revision=%d replication_factor=%d idempotent=%t event_sourcing=%t lifecycle_epoch=%d",
		definition.Name,
		definition.Partitions,
		definition.Policy.CleanupPolicy,
		definition.Policy.Partitioner,
		definition.Policy.AuthPolicy,
		strings.Join(definition.Policy.ReadACL, ","),
		strings.Join(definition.Policy.WriteACL, ","),
		definition.Policy.RetentionHours,
		definition.Policy.RetentionBytes,
		definition.Revision,
		definition.ReplicationFactor,
		definition.Idempotent,
		definition.EventSourcing,
		definition.LifecycleEpoch,
	)
}

func parseACLArg(value string) []string {
	if strings.TrimSpace(value) == "" {
		// Keep an allocated empty slice so the distributed JSON command encodes
		// [] rather than null. Decoding null into *[]string loses field presence.
		return []string{}
	}
	parts := strings.Split(value, ",")
	acl := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			acl = append(acl, part)
		}
	}
	return acl
}
