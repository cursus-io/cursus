package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestValidateTargetCapacityBoundsRegistryGrowth(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MaxTopics = 2
	cfg.MaxPartitionsPerTopic = 3
	cfg.MaxPartitions = 4
	definitions := []Definition{
		DefaultDefinition("first", cfg),
		DefaultDefinition("second", cfg),
	}

	t.Run("topic count", func(t *testing.T) {
		err := ValidateTargetCapacity(cfg, definitions, DefaultDefinition("third", cfg))
		require.ErrorContains(t, err, "max_topics=2")
	})

	t.Run("partitions per topic", func(t *testing.T) {
		target := definitions[0]
		target.Partitions = 4
		err := ValidateTargetCapacity(cfg, definitions, target)
		require.ErrorContains(t, err, "max_partitions_per_topic=3")
	})

	t.Run("total partitions", func(t *testing.T) {
		target := definitions[0]
		target.Partitions = 3
		err := ValidateTargetCapacity(cfg, definitions, target)
		require.ErrorContains(t, err, "max_partitions=4")
	})

	t.Run("replacement within bounds", func(t *testing.T) {
		target := definitions[0]
		target.Partitions = 2
		require.NoError(t, ValidateTargetCapacity(cfg, definitions, target))
	})
}
