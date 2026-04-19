package sdk

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

func TestNewDefaultPublisherConfig_AllDefaults(t *testing.T) {
	cfg := NewDefaultPublisherConfig()

	assert.Equal(t, 100, cfg.RetryBackoffMS)
	assert.Equal(t, 2000, cfg.MaxBackoffMS)
	assert.Equal(t, 0, cfg.PublishDelayMS)
	assert.False(t, cfg.UseTLS)
	assert.Equal(t, "", cfg.TLSCertPath)
	assert.Equal(t, "", cfg.TLSKeyPath)
	assert.False(t, cfg.EnableMetrics)
	assert.False(t, cfg.EnableIdempotence)
	assert.False(t, cfg.EnableBenchmark)
	assert.Equal(t, "", cfg.BenchTopicName)
	assert.Equal(t, 0, cfg.MessageSize)
	assert.Equal(t, 0, cfg.NumMessages)
	assert.Equal(t, 0, cfg.CurrentBrokerIndex)
	assert.False(t, cfg.AutoCreateTopics)
}

func TestNewDefaultConsumerConfig_AllDefaults(t *testing.T) {
	cfg := NewDefaultConsumerConfig()

	assert.Equal(t, 1000, cfg.WorkerChannelSize)
	assert.Equal(t, 500*time.Millisecond, cfg.CommitRetryBackoff)
	assert.Equal(t, 2*time.Second, cfg.CommitRetryMaxBackoff)
	assert.Equal(t, 1*time.Second, cfg.StreamingCommitInterval)
	assert.Equal(t, 100, cfg.StreamingCommitBatchSize)
	assert.Equal(t, 300000, cfg.StreamingReadDeadlineMS)
	assert.Equal(t, 1000, cfg.StreamingRetryIntervalMS)
	assert.False(t, cfg.UseTLS)
	assert.Equal(t, "", cfg.TLSCertPath)
	assert.Equal(t, "", cfg.TLSKeyPath)
	assert.False(t, cfg.EnableMetrics)
	assert.False(t, cfg.EnableBenchmark)
	assert.False(t, cfg.EnableCorrectness)
	assert.Equal(t, 0, cfg.NumMessages)
	assert.Equal(t, 0, cfg.CurrentBrokerIndex)
	assert.False(t, cfg.EnableImmediateCommit)
	assert.Equal(t, 0, cfg.MaxConnectRetries)
	assert.Equal(t, 0, cfg.ConnectRetryBackoffMS)
}

func TestConsumerConfig_TLSFields(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	cfg.UseTLS = true
	cfg.TLSCertPath = "/path/to/cert.pem"
	cfg.TLSKeyPath = "/path/to/key.pem"

	assert.True(t, cfg.UseTLS)
	assert.Equal(t, "/path/to/cert.pem", cfg.TLSCertPath)
	assert.Equal(t, "/path/to/key.pem", cfg.TLSKeyPath)
}

func TestPublisherConfig_TLSFields(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.UseTLS = true
	cfg.TLSCertPath = "/path/to/cert.pem"
	cfg.TLSKeyPath = "/path/to/key.pem"

	assert.True(t, cfg.UseTLS)
	assert.Equal(t, "/path/to/cert.pem", cfg.TLSCertPath)
	assert.Equal(t, "/path/to/key.pem", cfg.TLSKeyPath)
}

func TestConsumerConfig_EnableMetrics(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	assert.False(t, cfg.EnableMetrics)
	cfg.EnableMetrics = true
	assert.True(t, cfg.EnableMetrics)
}

func TestPublisherConfig_EnableMetrics(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	assert.False(t, cfg.EnableMetrics)
	cfg.EnableMetrics = true
	assert.True(t, cfg.EnableMetrics)
}

func TestLoadConfig_PublisherDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p.json")
	if err := os.WriteFile(path, []byte(`{"topic":"override-topic"}`), 0644); err != nil {
		t.Fatal(err)
	}

	var loaded PublisherConfig
	if err := LoadConfig(path, &loaded); err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	assert.Equal(t, "override-topic", loaded.Topic)
	assert.Equal(t, 3, loaded.MaxRetries)
	assert.Equal(t, 100, loaded.RetryBackoffMS)
	assert.Equal(t, 5000, loaded.AckTimeoutMS)
	assert.Equal(t, 1, loaded.Partitions)
	assert.Equal(t, 30*time.Second, loaded.LeaderStaleness)
}

func TestLoadConfig_JSONOverridesYAMLExt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	yamlData := `topic: yaml-topic
partitions: 4
use_tls: true
tls_cert_path: /cert
tls_key_path: /key
enable_metrics: true`
	if err := os.WriteFile(path, []byte(yamlData), 0644); err != nil {
		t.Fatal(err)
	}

	var loaded PublisherConfig
	if err := LoadConfig(path, &loaded); err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	assert.Equal(t, "yaml-topic", loaded.Topic)
	assert.Equal(t, 4, loaded.Partitions)
	assert.True(t, loaded.UseTLS)
	assert.Equal(t, "/cert", loaded.TLSCertPath)
	assert.Equal(t, "/key", loaded.TLSKeyPath)
	assert.True(t, loaded.EnableMetrics)
}

func TestLoadConfig_ConsumerYAMLWithTLSAndMetrics(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	yamlData := `group_id: tls-group
use_tls: true
tls_cert_path: /consumer/cert
tls_key_path: /consumer/key
enable_metrics: true`
	if err := os.WriteFile(path, []byte(yamlData), 0644); err != nil {
		t.Fatal(err)
	}

	var loaded ConsumerConfig
	if err := LoadConfig(path, &loaded); err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	assert.Equal(t, "tls-group", loaded.GroupID)
	assert.True(t, loaded.UseTLS)
	assert.Equal(t, "/consumer/cert", loaded.TLSCertPath)
	assert.Equal(t, "/consumer/key", loaded.TLSKeyPath)
	assert.True(t, loaded.EnableMetrics)
	assert.Equal(t, 100, loaded.BatchSize)
}

func TestConsumerMode_Constants(t *testing.T) {
	assert.Equal(t, ConsumerMode("polling"), ModePolling)
	assert.Equal(t, ConsumerMode("streaming"), ModeStreaming)
}

func TestLoadConfig_UnknownType_UsesYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.txt")
	yamlData := `topic: txt-topic`
	if err := os.WriteFile(path, []byte(yamlData), 0644); err != nil {
		t.Fatal(err)
	}

	var loaded PublisherConfig
	err := LoadConfig(path, &loaded)
	assert.NoError(t, err)
	assert.Equal(t, "txt-topic", loaded.Topic)
}
