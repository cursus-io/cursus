package fsm

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/topic"
)

func copyTopicDefinition(definition *topic.Definition) *topic.Definition {
	if definition == nil {
		return nil
	}
	cloned := *definition
	cloned.Policy = definition.Policy.Clone()
	return &cloned
}

func copyTopicState(state map[string]*topic.Definition) map[string]*topic.Definition {
	cloned := make(map[string]*topic.Definition, len(state))
	for name, definition := range state {
		cloned[name] = copyTopicDefinition(definition)
	}
	return cloned
}

func topicStateFromDefinitions(definitions []topic.Definition) map[string]*topic.Definition {
	state := make(map[string]*topic.Definition, len(definitions))
	for i := range definitions {
		definition := definitions[i]
		state[definition.Name] = copyTopicDefinition(&definition)
	}
	return state
}

func validateSnapshotTopicDefinitionFields(
	state map[string]*topic.Definition,
	metadata map[string]*PartitionMetadata,
) error {
	for name, definition := range state {
		if definition == nil {
			continue
		}
		if definition.Revision == 0 {
			return fmt.Errorf("topic state %q is missing revision in snapshot version %d", name, SnapshotVersionCurrent)
		}
		if definition.ReplicationFactor == 0 {
			return fmt.Errorf("topic state %q is missing replication_factor in snapshot version %d", name, SnapshotVersionCurrent)
		}
		if definition.LifecycleEpoch == 0 {
			return fmt.Errorf("topic state %q is missing lifecycle_epoch in snapshot version %d", name, SnapshotVersionCurrent)
		}
		for partition := 0; partition < definition.Partitions; partition++ {
			partitionMetadata := metadata[name+"-"+strconv.Itoa(partition)]
			if partitionMetadata == nil {
				continue
			}
			if partitionMetadata.LifecycleEpoch == 0 {
				return fmt.Errorf("partition metadata %q is missing lifecycle_epoch in snapshot version %d", name+"-"+strconv.Itoa(partition), SnapshotVersionCurrent)
			}
			if partitionMetadata.LifecycleEpoch != definition.LifecycleEpoch {
				return fmt.Errorf(
					"partition metadata %q lifecycle epoch %d conflicts with topic %q epoch %d",
					name+"-"+strconv.Itoa(partition), partitionMetadata.LifecycleEpoch, name, definition.LifecycleEpoch,
				)
			}
		}
	}
	return nil
}

func copyPartitionMetadataState(metadata map[string]*PartitionMetadata) map[string]*PartitionMetadata {
	cloned := make(map[string]*PartitionMetadata, len(metadata))
	for key, current := range metadata {
		if current == nil {
			cloned[key] = nil
			continue
		}
		copy := *current
		copy.Replicas = append([]string(nil), current.Replicas...)
		copy.ISR = append([]string(nil), current.ISR...)
		cloned[key] = &copy
	}
	return cloned
}

func topicDefinitionsFromState(state map[string]*topic.Definition) ([]topic.Definition, error) {
	definitions := make([]topic.Definition, 0, len(state))
	for name, raw := range state {
		if raw == nil {
			return nil, fmt.Errorf("topic state %q is nil", name)
		}
		definition, err := raw.Normalize()
		if err != nil {
			return nil, fmt.Errorf("invalid topic state %q: %w", name, err)
		}
		if definition.Name != name {
			return nil, fmt.Errorf("topic state key %q does not match name %q", name, definition.Name)
		}
		definitions = append(definitions, definition)
	}
	sort.Slice(definitions, func(i, j int) bool { return definitions[i].Name < definitions[j].Name })
	return definitions, nil
}

func validateTopicState(
	state map[string]*topic.Definition,
	metadata map[string]*PartitionMetadata,
) ([]topic.Definition, error) {
	definitions, err := topicDefinitionsFromState(state)
	if err != nil {
		return nil, err
	}

	byName := make(map[string]topic.Definition, len(definitions))
	for _, definition := range definitions {
		byName[definition.Name] = definition
		for partition := 0; partition < definition.Partitions; partition++ {
			key := definition.Name + "-" + strconv.Itoa(partition)
			if metadata[key] == nil {
				return nil, fmt.Errorf("topic state %q is missing partition metadata %d", definition.Name, partition)
			}
		}
	}

	for key, partitionMetadata := range metadata {
		if partitionMetadata == nil {
			return nil, fmt.Errorf("partition metadata %q is nil", key)
		}
		separator := strings.LastIndex(key, "-")
		if separator <= 0 {
			return nil, fmt.Errorf("partition metadata key %q is invalid", key)
		}
		partition, parseErr := strconv.Atoi(key[separator+1:])
		if parseErr != nil || partition < 0 {
			return nil, fmt.Errorf("partition metadata key %q is invalid", key)
		}
		name := key[:separator]
		definition, exists := byName[name]
		if !exists {
			return nil, fmt.Errorf("partition metadata %q has no topic definition", key)
		}
		if partitionMetadata.PartitionCount != definition.Partitions {
			return nil, fmt.Errorf(
				"partition metadata %q declares partition count %d; topic %q declares %d",
				key,
				partitionMetadata.PartitionCount,
				name,
				definition.Partitions,
			)
		}
		if partitionMetadata.Idempotent != definition.Idempotent {
			return nil, fmt.Errorf(
				"partition metadata %q idempotent mode conflicts with topic %q",
				key,
				name,
			)
		}
		if partitionMetadata.LifecycleEpoch == 0 {
			return nil, fmt.Errorf("partition metadata %q is missing lifecycle epoch", key)
		}
		if partitionMetadata.LifecycleEpoch != definition.LifecycleEpoch {
			return nil, fmt.Errorf(
				"partition metadata %q lifecycle epoch %d conflicts with topic %q epoch %d",
				key, partitionMetadata.LifecycleEpoch, name, definition.LifecycleEpoch,
			)
		}
		if partition >= definition.Partitions {
			return nil, fmt.Errorf(
				"partition metadata %q exceeds topic %q partition count %d",
				key,
				name,
				definition.Partitions,
			)
		}
	}

	return definitions, nil
}
