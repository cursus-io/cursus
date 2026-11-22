package config_test

import (
	"encoding/json"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"gopkg.in/yaml.v3"
)

func TestNormalizeDefaults(t *testing.T) {
	cfg := &config.Config{}
	cfg.Normalize()

	if cfg.BrokerPort != 9000 {
		t.Errorf("BrokerPort default incorrect: %d", cfg.BrokerPort)
	}
	if !cfg.FlushSyncOnAck {
		t.Error("standalone normalization must require sync before acknowledgement")
	}
}

func TestNormalizeAllowsDeferredSyncOnlyForDistributedBrokers(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.FlushSyncOnAck = false

	cfg.Normalize()

	if cfg.FlushSyncOnAck {
		t.Error("distributed broker should retain explicitly disabled sync-on-ack")
	}
}

func TestFlushSyncOnAckConfigNames(t *testing.T) {
	t.Run("yaml", func(t *testing.T) {
		cfg := config.DefaultConfig()
		if err := yaml.Unmarshal([]byte("flush_sync_on_ack: false\n"), cfg); err != nil {
			t.Fatal(err)
		}
		if cfg.FlushSyncOnAck {
			t.Error("flush_sync_on_ack did not disable sync-on-ack")
		}
	})

	t.Run("json", func(t *testing.T) {
		cfg := config.DefaultConfig()
		if err := json.Unmarshal([]byte(`{"flush.sync_on_ack":false}`), cfg); err != nil {
			t.Fatal(err)
		}
		if cfg.FlushSyncOnAck {
			t.Error("flush.sync_on_ack did not disable sync-on-ack")
		}
	})
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
