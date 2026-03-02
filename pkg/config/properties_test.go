package config_test

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
)

func TestNormalizeDefaults(t *testing.T) {
	cfg := &config.Config{}
	cfg.Normalize()

	if cfg.BrokerPort != 9000 {
		t.Errorf("BrokerPort default incorrect: %d", cfg.BrokerPort)
	}
}

func TestStaticConsumerGroupsNormalize(t *testing.T) {
	cfg := &config.Config{
		StaticConsumerGroups: []config.ConsumerGroupConfig{
			{
				Name:          "",
				ConsumerCount: 0,
				Topics:        []string{"a"},
				TopicPartitions: map[string]int{
					"a": 0,
				},
			},
		},
	}

	cfg.Normalize()
	g := cfg.StaticConsumerGroups[0]

	if g.Name != "default-group" {
		t.Errorf("Name normalization failed")
	}
	if g.ConsumerCount != 1 {
		t.Errorf("ConsumerCount normalization failed")
	}
	if g.TopicPartitions["a"] != 1 {
		t.Errorf("TopicPartitions normalization failed")
	}
}
