package topic

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/config"
)

// ValidateDefinitionCapacity verifies the bounded broker topic registry that
// would result from materializing the supplied authoritative definitions.
func ValidateDefinitionCapacity(cfg *config.Config, definitions []Definition) error {
	defaults := config.DefaultConfig()
	maxTopics := defaults.MaxTopics
	maxPartitionsPerTopic := defaults.MaxPartitionsPerTopic
	maxPartitions := defaults.MaxPartitions
	if cfg != nil {
		if cfg.MaxTopics > 0 {
			maxTopics = cfg.MaxTopics
		}
		if cfg.MaxPartitionsPerTopic > 0 {
			maxPartitionsPerTopic = cfg.MaxPartitionsPerTopic
		}
		if cfg.MaxPartitions > 0 {
			maxPartitions = cfg.MaxPartitions
		}
	}
	seen := make(map[string]struct{}, len(definitions))
	totalPartitions := 0
	for _, definition := range definitions {
		if _, exists := seen[definition.Name]; exists {
			return fmt.Errorf("duplicate topic definition %q", definition.Name)
		}
		seen[definition.Name] = struct{}{}
		if definition.Partitions > maxPartitionsPerTopic {
			return fmt.Errorf(
				"topic capacity exceeded: topic=%q partitions=%d max_partitions_per_topic=%d",
				definition.Name, definition.Partitions, maxPartitionsPerTopic,
			)
		}
		if definition.Partitions < 0 || totalPartitions > maxPartitions-definition.Partitions {
			return fmt.Errorf(
				"topic capacity exceeded: total_partitions>%d max_partitions=%d",
				maxPartitions, maxPartitions,
			)
		}
		totalPartitions += definition.Partitions
	}
	if len(seen) > maxTopics {
		return fmt.Errorf("topic capacity exceeded: topics=%d max_topics=%d", len(seen), maxTopics)
	}
	return nil
}

// ValidateTargetCapacity applies target as an add-or-replace operation to a
// detached registry snapshot and verifies the resulting capacity.
func ValidateTargetCapacity(cfg *config.Config, definitions []Definition, target Definition) error {
	candidate := make([]Definition, 0, len(definitions)+1)
	replaced := false
	for _, definition := range definitions {
		if definition.Name == target.Name {
			if !replaced {
				candidate = append(candidate, target)
				replaced = true
			}
			continue
		}
		candidate = append(candidate, definition)
	}
	if !replaced {
		candidate = append(candidate, target)
	}
	return ValidateDefinitionCapacity(cfg, candidate)
}
