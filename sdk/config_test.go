package sdk

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewDefaultPublisherConfig(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	if cfg == nil {
		t.Fatal("expected non-nil PublisherConfig")
	}

	if len(cfg.BrokerAddrs) == 0 {
		t.Error("expected at least one broker address")
	}
	if cfg.BrokerAddrs[0] != "localhost:9000" {
		t.Errorf("expected broker addr localhost:9000, got %s", cfg.BrokerAddrs[0])
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("expected MaxRetries=3, got %d", cfg.MaxRetries)
	}
	if cfg.BatchSize != 100 {
		t.Errorf("expected BatchSize=100, got %d", cfg.BatchSize)
	}
	if cfg.AckTimeoutMS != 5000 {
		t.Errorf("expected AckTimeoutMS=5000, got %d", cfg.AckTimeoutMS)
	}
	if cfg.WriteTimeoutMS != 5000 {
		t.Errorf("expected WriteTimeoutMS=5000, got %d", cfg.WriteTimeoutMS)
	}
	if cfg.FlushTimeoutMS != 30000 {
		t.Errorf("expected FlushTimeoutMS=30000, got %d", cfg.FlushTimeoutMS)
	}
	if cfg.Topic != "default-topic" {
		t.Errorf("expected Topic=default-topic, got %s", cfg.Topic)
	}
	if cfg.Partitions != 1 {
		t.Errorf("expected Partitions=1, got %d", cfg.Partitions)
	}
	if cfg.LingerMS != 50 {
		t.Errorf("expected LingerMS=50, got %d", cfg.LingerMS)
	}
	if cfg.BufferSize != 1024 {
		t.Errorf("expected BufferSize=1024, got %d", cfg.BufferSize)
	}
	if cfg.MaxInflightRequests != 5 {
		t.Errorf("expected MaxInflightRequests=5, got %d", cfg.MaxInflightRequests)
	}
	if cfg.Acks != "1" {
		t.Errorf("expected Acks=1, got %s", cfg.Acks)
	}
	if cfg.CompressionType != "none" {
		t.Errorf("expected CompressionType=none, got %s", cfg.CompressionType)
	}
	if cfg.LeaderStaleness != 30*time.Second {
		t.Errorf("expected LeaderStaleness=30s, got %v", cfg.LeaderStaleness)
	}
	if cfg.LogLevel != LogLevelInfo {
		t.Errorf("expected LogLevel=Info, got %d", cfg.LogLevel)
	}
}

func TestNewDefaultConsumerConfig(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	if cfg == nil {
		t.Fatal("expected non-nil ConsumerConfig")
	}

	if len(cfg.BrokerAddrs) == 0 {
		t.Error("expected at least one broker address")
	}
	if cfg.BrokerAddrs[0] != "localhost:9000" {
		t.Errorf("expected broker addr localhost:9000, got %s", cfg.BrokerAddrs[0])
	}
	if cfg.GroupID != "default-group" {
		t.Errorf("expected GroupID=default-group, got %s", cfg.GroupID)
	}
	if cfg.ConsumerID == "" {
		t.Error("expected non-empty ConsumerID")
	}
	if cfg.BatchSize != 100 {
		t.Errorf("expected BatchSize=100, got %d", cfg.BatchSize)
	}
	if cfg.PollInterval != 500*time.Millisecond {
		t.Errorf("expected PollInterval=500ms, got %v", cfg.PollInterval)
	}
	if cfg.PollTimeoutMS != 30000 {
		t.Errorf("expected PollTimeoutMS=30000, got %d", cfg.PollTimeoutMS)
	}
	if cfg.MaxPollRecords != 500 {
		t.Errorf("expected MaxPollRecords=500, got %d", cfg.MaxPollRecords)
	}
	if cfg.SessionTimeoutMS != 30000 {
		t.Errorf("expected SessionTimeoutMS=30000, got %d", cfg.SessionTimeoutMS)
	}
	if cfg.HeartbeatIntervalMS != 3000 {
		t.Errorf("expected HeartbeatIntervalMS=3000, got %d", cfg.HeartbeatIntervalMS)
	}
	if !cfg.EnableAutoCommit {
		t.Error("expected EnableAutoCommit=true")
	}
	if cfg.AutoCommitInterval != 5*time.Second {
		t.Errorf("expected AutoCommitInterval=5s, got %v", cfg.AutoCommitInterval)
	}
	if cfg.MaxCommitRetries != 5 {
		t.Errorf("expected MaxCommitRetries=5, got %d", cfg.MaxCommitRetries)
	}
	if cfg.Mode != ModePolling {
		t.Errorf("expected Mode=polling, got %s", cfg.Mode)
	}
	if cfg.CompressionType != "none" {
		t.Errorf("expected CompressionType=none, got %s", cfg.CompressionType)
	}
	if cfg.LeaderStaleness != 30*time.Second {
		t.Errorf("expected LeaderStaleness=30s, got %v", cfg.LeaderStaleness)
	}
	if cfg.LogLevel != LogLevelInfo {
		t.Errorf("expected LogLevel=Info, got %d", cfg.LogLevel)
	}
}

func TestLoadConfig_JSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pub.json")

	cfg := PublisherConfig{Topic: "my-topic", Partitions: 8, Acks: "all"}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	var loaded PublisherConfig
	if err := LoadConfig(path, &loaded); err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if loaded.Topic != "my-topic" {
		t.Errorf("expected Topic=my-topic, got %s", loaded.Topic)
	}
}

func TestLoadConfig_YAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "consumer.yaml")

	yamlData := `group_id: test-group
batch_size: 50`
	if err := os.WriteFile(path, []byte(yamlData), 0644); err != nil {
		t.Fatal(err)
	}

	var loaded ConsumerConfig
	if err := LoadConfig(path, &loaded); err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if loaded.GroupID != "test-group" {
		t.Errorf("expected GroupID=test-group, got %s", loaded.GroupID)
	}
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	var cfg PublisherConfig
	err := LoadConfig("/nonexistent/path.json", &cfg)
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestLoadConfig_ConsumerDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.json")
	if err := os.WriteFile(path, []byte(`{"group_id":"override"}`), 0644); err != nil {
		t.Fatal(err)
	}

	var loaded ConsumerConfig
	if err := LoadConfig(path, &loaded); err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if loaded.GroupID != "override" {
		t.Errorf("expected GroupID=override, got %s", loaded.GroupID)
	}
	if loaded.BatchSize != 100 {
		t.Errorf("expected default BatchSize=100, got %d", loaded.BatchSize)
	}
}
