package config_test

import (
	"os"
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
	os.Setenv("BROKER_PORT", "9999")
	os.Setenv("LOG_RETENTION_HOURS", "24")
	defer os.Unsetenv("BROKER_PORT")
	defer os.Unsetenv("LOG_RETENTION_HOURS")

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

func TestConfig_Normalize(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.BrokerPort = -1
	cfg.Normalize()
	if cfg.BrokerPort != 9000 {
		t.Errorf("Normalize should have reset BrokerPort to 9000, got %d", cfg.BrokerPort)
	}
}
