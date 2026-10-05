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
	if cfg.RaftSnapshotIntervalMS != 120000 {
		t.Errorf("Expected default RaftSnapshotIntervalMS 120000, got %d", cfg.RaftSnapshotIntervalMS)
	}
	if cfg.RaftSnapshotThreshold != 8192 {
		t.Errorf("Expected default RaftSnapshotThreshold 8192, got %d", cfg.RaftSnapshotThreshold)
	}
	if cfg.RaftTrailingLogs != 10240 {
		t.Errorf("Expected default RaftTrailingLogs 10240, got %d", cfg.RaftTrailingLogs)
	}
	if cfg.BootstrapSoleVoter {
		t.Error("Expected BootstrapSoleVoter to default to false")
	}
	if cfg.TransactionCoordinatorShards != 50 {
		t.Errorf("Expected default TransactionCoordinatorShards 50, got %d", cfg.TransactionCoordinatorShards)
	}
	if cfg.TransactionRecoveryBatchSize != 256 {
		t.Errorf("Expected default TransactionRecoveryBatchSize 256, got %d", cfg.TransactionRecoveryBatchSize)
	}
	if cfg.ClientRequestTimeoutMS != 30000 {
		t.Errorf("Expected default ClientRequestTimeoutMS 30000, got %d", cfg.ClientRequestTimeoutMS)
	}
	if cfg.MaxTopics != 10000 || cfg.MaxPartitionsPerTopic != 1024 || cfg.MaxPartitions != 100000 {
		t.Errorf("unexpected default topic limits: topics=%d per_topic=%d total=%d", cfg.MaxTopics, cfg.MaxPartitionsPerTopic, cfg.MaxPartitions)
	}
	if cfg.MaxTransactions != 100000 || cfg.MaxTransactionRecords != 10000 || cfg.MaxTransactionBytes != 64*1024*1024 || cfg.MaxTransactionOffsets != 10000 {
		t.Errorf("unexpected default transaction limits: transactions=%d records=%d bytes=%d offsets=%d", cfg.MaxTransactions, cfg.MaxTransactionRecords, cfg.MaxTransactionBytes, cfg.MaxTransactionOffsets)
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	t.Setenv("BROKER_PORT", "9999")
	t.Setenv("LOG_RETENTION_HOURS", "24")
	t.Setenv("RAFT_SNAPSHOT_INTERVAL_MS", "250")
	t.Setenv("RAFT_SNAPSHOT_THRESHOLD", "16")
	t.Setenv("RAFT_TRAILING_LOGS", "0")
	t.Setenv("BOOTSTRAP_SOLE_VOTER", "true")
	t.Setenv("TRANSACTION_COORDINATOR_SHARDS", "17")
	t.Setenv("TRANSACTION_RECOVERY_BATCH_SIZE", "19")
	t.Setenv("CLIENT_REQUEST_TIMEOUT_MS", "45000")
	t.Setenv("MAX_TOPICS", "123")
	t.Setenv("MAX_PARTITIONS_PER_TOPIC", "17")
	t.Setenv("MAX_PARTITIONS", "456")
	t.Setenv("MAX_TRANSACTIONS", "789")
	t.Setenv("MAX_TRANSACTION_RECORDS", "21")
	t.Setenv("MAX_TRANSACTION_BYTES", "8192")
	t.Setenv("MAX_TRANSACTION_OFFSETS", "22")

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
	if cfg.RaftSnapshotIntervalMS != 250 {
		t.Errorf("Expected RaftSnapshotIntervalMS 250 from env, got %d", cfg.RaftSnapshotIntervalMS)
	}
	if cfg.RaftSnapshotThreshold != 16 {
		t.Errorf("Expected RaftSnapshotThreshold 16 from env, got %d", cfg.RaftSnapshotThreshold)
	}
	if cfg.RaftTrailingLogs != 0 {
		t.Errorf("Expected RaftTrailingLogs 0 from env, got %d", cfg.RaftTrailingLogs)
	}
	if !cfg.BootstrapSoleVoter {
		t.Error("Expected BootstrapSoleVoter true from env")
	}
	if cfg.TransactionCoordinatorShards != 17 {
		t.Errorf("Expected TransactionCoordinatorShards 17 from env, got %d", cfg.TransactionCoordinatorShards)
	}
	if cfg.TransactionRecoveryBatchSize != 19 {
		t.Errorf("Expected TransactionRecoveryBatchSize 19 from env, got %d", cfg.TransactionRecoveryBatchSize)
	}
	if cfg.ClientRequestTimeoutMS != 45000 {
		t.Errorf("Expected ClientRequestTimeoutMS 45000 from env, got %d", cfg.ClientRequestTimeoutMS)
	}
	if cfg.MaxTopics != 123 || cfg.MaxPartitionsPerTopic != 17 || cfg.MaxPartitions != 456 {
		t.Errorf("unexpected topic limits from env: topics=%d per_topic=%d total=%d", cfg.MaxTopics, cfg.MaxPartitionsPerTopic, cfg.MaxPartitions)
	}
	if cfg.MaxTransactions != 789 || cfg.MaxTransactionRecords != 21 || cfg.MaxTransactionBytes != 8192 || cfg.MaxTransactionOffsets != 22 {
		t.Errorf("unexpected transaction limits from env: transactions=%d records=%d bytes=%d offsets=%d", cfg.MaxTransactions, cfg.MaxTransactionRecords, cfg.MaxTransactionBytes, cfg.MaxTransactionOffsets)
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
	if cfg.TransactionCoordinatorShards != 50 {
		t.Errorf("Normalize should have reset TransactionCoordinatorShards to 50, got %d", cfg.TransactionCoordinatorShards)
	}
	if cfg.TransactionRecoveryBatchSize != 256 {
		t.Errorf("Normalize should have reset TransactionRecoveryBatchSize to 256, got %d", cfg.TransactionRecoveryBatchSize)
	}
	if cfg.MaxTopics != 10000 || cfg.MaxPartitionsPerTopic != 1024 || cfg.MaxPartitions != 100000 {
		t.Errorf("Normalize should restore topic limits, got topics=%d per_topic=%d total=%d", cfg.MaxTopics, cfg.MaxPartitionsPerTopic, cfg.MaxPartitions)
	}
	if cfg.MaxTransactions != 100000 || cfg.MaxTransactionRecords != 10000 || cfg.MaxTransactionBytes != 64*1024*1024 || cfg.MaxTransactionOffsets != 10000 {
		t.Errorf("Normalize should restore transaction limits, got transactions=%d records=%d bytes=%d offsets=%d", cfg.MaxTransactions, cfg.MaxTransactionRecords, cfg.MaxTransactionBytes, cfg.MaxTransactionOffsets)
	}
}
