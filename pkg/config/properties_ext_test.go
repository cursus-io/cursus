package config_test

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/util"
)

func TestDefaultConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	if cfg.BrokerPort != 9000 {
		t.Errorf("Expected default BrokerPort 9000, got %d", cfg.BrokerPort)
	}
	if cfg.LogLevel != util.LogLevelInfo {
		t.Errorf("Expected default LogLevel Info, got %v", cfg.LogLevel)
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	t.Setenv("BROKER_PORT", "9999")
	t.Setenv("LOG_RETENTION_HOURS", "24")

	cfg, err := config.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.BrokerPort != 9999 {
		t.Errorf("Expected BrokerPort 9999 from env, got %d", cfg.BrokerPort)
	}
	if cfg.RetentionHours != 24 {
		t.Errorf("Expected RetentionHours 24 from env, got %d", cfg.RetentionHours)
	}
}

func TestConfigNormalizeCleanupPolicies(t *testing.T) {
	for input, expected := range map[string]string{
		"compact":        config.CleanupPolicyCompact,
		"delete":         config.CleanupPolicyDelete,
		"compact,delete": config.CleanupPolicyDeleteCompact,
		"delete,compact": config.CleanupPolicyDeleteCompact,
	} {
		cfg := config.DefaultConfig()
		cfg.CleanupPolicy = input
		cfg.Normalize()
		if cfg.CleanupPolicy != expected {
			t.Fatalf("cleanup policy %q normalized to %q, want %q", input, cfg.CleanupPolicy, expected)
		}
	}

	cfg := config.DefaultConfig()
	cfg.CleanupPolicy = "unknown"
	cfg.Normalize()
	if cfg.CleanupPolicy != config.CleanupPolicyDelete {
		t.Fatalf("invalid cleanup policy normalized to %q", cfg.CleanupPolicy)
	}
}

func TestConfig_Normalize(t *testing.T) {
	cfg := &config.Config{}
	cfg.BrokerPort = -1
	cfg.Normalize()
	if cfg.BrokerPort != 9000 {
		t.Errorf("Normalize should have reset BrokerPort to 9000, got %d", cfg.BrokerPort)
	}
}
