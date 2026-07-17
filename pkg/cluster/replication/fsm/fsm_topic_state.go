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
	cloned.Policy.ReadACL = append([]string(nil), definition.Policy.ReadACL...)
	cloned.Policy.WriteACL = append([]string(nil), definition.Policy.WriteACL...)
	return &cloned
}

func copyTopicState(state map[string]*topic.Definition) map[string]*topic.Definition {
	cloned := make(map[string]*topic.Definition, len(state))
	for name, definition := range state {
		cloned[name] = copyTopicDefinition(definition)
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

func legacyTopicState(metadata map[string]*PartitionMetadata) map[string]*topic.Definition {
	state := make(map[string]*topic.Definition)
	for key, partitionMetadata := range metadata {
		if partitionMetadata == nil {
			continue
		}
		separator := strings.LastIndex(key, "-")
		if separator <= 0 {
			continue
		}
		partition, err := strconv.Atoi(key[separator+1:])
		if err != nil || partition < 0 {
			continue
		}
		name := key[:separator]
		partitions := partitionMetadata.PartitionCount
		if partitions <= partition {
			partitions = partition + 1
		}
		current := state[name]
		if current == nil {
			current = &topic.Definition{
				Name:       name,
				Partitions: partitions,
				Idempotent: partitionMetadata.Idempotent,
				Policy:     topic.DefaultPolicy(),
			}
			state[name] = current
			continue
		}
		if partitions > current.Partitions {
			current.Partitions = partitions
		}
		current.Idempotent = current.Idempotent || partitionMetadata.Idempotent
	}
	return state
}
