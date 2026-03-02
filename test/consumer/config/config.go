package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cursus-io/cursus/util"
	"gopkg.in/yaml.v3"
)

type ConsumerMode string

const (
	ModePolling   ConsumerMode = "polling"
	ModeStreaming ConsumerMode = "streaming"
)

type ConsumerConfig struct {
	BrokerAddrs        []string `yaml:"broker_addrs" json:"broker_addrs"`
	CurrentBrokerIndex int      `yaml:"-" json:"-"`

	Topic      string `yaml:"topic" json:"topic"`
	GroupID    string `yaml:"group_id" json:"group_id"`
	ConsumerID string `yaml:"consumer_id" json:"consumer_id"`

	EnableBenchmark   bool         `yaml:"enable_benchmark" json:"enable_benchmark"`
	EnableCorrectness bool         `yaml:"enable_correctness" json:"enable_correctness"`
	NumMessages       int          `yaml:"num_messages" json:"num_messages"`
	Mode              ConsumerMode `yaml:"mode" json:"mode"`

	WorkerChannelSize int `yaml:"worker_channel_size" json:"worker_channel_size"`

	PollInterval  time.Duration `yaml:"poll_interval" json:"poll_interval"`
	PollTimeoutMS int           `yaml:"poll_timeout_ms" json:"poll_timeout_ms"`
	BatchSize     int           `yaml:"batch_size" json:"batch_size"`

	SessionTimeoutMS         int `yaml:"session_timeout_ms" json:"session_timeout_ms"`
	MaxPollRecords           int `yaml:"max_poll_records" json:"max_poll_records"`
	MaxConnectRetries        int `yaml:"max_connect_retries" json:"max_connect_retries"`
	ConnectRetryBackoffMS    int `yaml:"connect_retry_backoff_ms" json:"connect_retry_backoff_ms"`
	HeartbeatIntervalMS      int `yaml:"heartbeat_interval_ms" json:"heartbeat_interval_ms"`
	StreamingReadDeadlineMS  int `yaml:"streaming_read_deadline_ms" json:"streaming_read_deadline_ms"`
	StreamingRetryIntervalMS int `yaml:"streaming_retry_interval_ms" json:"streaming_retry_interval_ms"`

	EnableAutoCommit   bool          `yaml:"enable_auto_commit" json:"enable_auto_commit"`
	AutoCommitInterval time.Duration `yaml:"auto_commit_interval" json:"auto_commit_interval"`

	MaxCommitRetries      int           `yaml:"max_commit_retries" json:"max_commit_retries"`
	CommitRetryBackoff    time.Duration `yaml:"commit_retry_backoff" json:"commit_retry_backoff"`
	CommitRetryMaxBackoff time.Duration `yaml:"commit_retry_max_backoff" json:"commit_retry_max_backoff"`

	StreamingCommitInterval  time.Duration `yaml:"streaming_commit_interval" json:"streaming_commit_interval"`
	EnableImmediateCommit    bool          `yaml:"enable_immediate_commit" json:"enable_immediate_commit"`
	StreamingCommitBatchSize int           `yaml:"streaming_commit_batch_size" json:"streaming_commit_batch_size"`

	LeaderStaleness time.Duration `yaml:"leader_staleness" json:"leader_staleness"`

	CompressionType string `yaml:"compression_type" json:"compression.type"` // "none", "gzip", "snappy", "lz4"

	LogLevel util.LogLevel `yaml:"log_level" json:"log_level"`
}

func LoadConsumerConfig(explicitPath string) (*ConsumerConfig, error) {
	cfg := &ConsumerConfig{}

	flag.Func("broker-addr", "Comma-separated broker addresses", func(val string) error {
		parts := strings.Split(val, ",")
		cfg.BrokerAddrs = make([]string, 0, len(parts))
		for _, addr := range parts {
			trimmed := strings.TrimSpace(addr)
			if trimmed != "" {
				cfg.BrokerAddrs = append(cfg.BrokerAddrs, trimmed)
			}
		}
		return nil
	})

	flag.StringVar(&cfg.ConsumerID, "consumer-id", "consumer-1", "Consumer ID")
	flag.StringVar(&cfg.GroupID, "group-id", "default-group", "Consumer group ID")
	flag.StringVar(&cfg.Topic, "topic", "", "Topic to consume")

	flag.IntVar(&cfg.WorkerChannelSize, "worker-channel-size", 1000, "Capacity of the internal message channel")

	flag.DurationVar(&cfg.PollInterval, "poll-interval", 500*time.Millisecond, "Poll interval")
	flag.IntVar(&cfg.PollTimeoutMS, "poll-timeout-ms", 30000, "Maximum time in milliseconds to wait for new messages in a poll (Long Polling)")

	flag.IntVar(&cfg.BatchSize, "batch-size", 100, "Batch size for consuming")
	flag.IntVar(&cfg.MaxPollRecords, "max-poll-records", 500, "Max records per poll")

	flag.BoolVar(&cfg.EnableAutoCommit, "enable-auto-commit", true, "Enable auto commit")
	flag.DurationVar(&cfg.AutoCommitInterval, "auto-commit-interval", 5*time.Second, "Auto commit interval")

	flag.IntVar(&cfg.MaxCommitRetries, "max-commit-retries", 5, "Max retries for offset commit")
	flag.DurationVar(&cfg.CommitRetryBackoff, "commit-retry-backoff", 500*time.Millisecond, "Backoff time between commit retries")
	flag.DurationVar(&cfg.CommitRetryMaxBackoff, "commit-retry-max-backoff", 2*time.Second, "Max backoff time for commit retries")

	flag.DurationVar(&cfg.LeaderStaleness, "leader-staleness", 30*time.Second, "Maximum age of leader info before considering it stale (e.g., 30s, 1m)")

	flag.DurationVar(&cfg.StreamingCommitInterval, "streaming-commit-interval", 1*time.Second, "Streaming commit interval")
	flag.BoolVar(&cfg.EnableImmediateCommit, "enable-immediate-commit", false, "Enable immediate commit after each message")
	flag.IntVar(&cfg.StreamingCommitBatchSize, "streaming-commit-batch-size", 100, "Batch size for streaming commits")

	flag.IntVar(&cfg.SessionTimeoutMS, "session-timeout-ms", 30000, "Session timeout in milliseconds")
	flag.StringVar(&cfg.CompressionType, "compression-type", "none", "Compression type (none, gzip, snappy, lz4)")

	benchmarkFlag := flag.Bool("benchmark", false, "Enable benchmark mode with detailed metrics")
	correctnessFlag := flag.Bool("correctness", false, "Enable correctness mode with detailed metrics")
	flag.IntVar(&cfg.NumMessages, "num-messages", 10, "Number of messages to publish")

	configPath := flag.String("config", "/config.yaml", "Path to YAML/JSON config file")
	logLevelStr := flag.String("log-level", "info", "Log level")

	flag.Parse()

	switch strings.ToLower(*logLevelStr) {
	case "debug":
		cfg.LogLevel = util.LogLevelDebug
	case "warn", "warning":
		cfg.LogLevel = util.LogLevelWarn
	case "error":
		cfg.LogLevel = util.LogLevelError
	default:
		cfg.LogLevel = util.LogLevelInfo
	}

	finalPath := explicitPath
	if finalPath == "" {
		finalPath = *configPath
	}

	if finalPath != "" {
		data, err := os.ReadFile(finalPath)
		if err != nil {
			if os.IsNotExist(err) {
				util.Warn("Config file %s not found, using flag defaults", finalPath)
				return cfg, nil
			}
			return nil, fmt.Errorf("failed to read config file %s: %w", finalPath, err)
		}
		if strings.HasSuffix(finalPath, ".json") {
			if err := json.Unmarshal(data, cfg); err != nil {
				return nil, err
			}
		} else {
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return nil, err
			}
		}
	}

	if len(cfg.BrokerAddrs) == 0 {
		cfg.BrokerAddrs = []string{"localhost:9000"}
	}
	if cfg.WorkerChannelSize <= 0 {
		cfg.WorkerChannelSize = 1000
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.PollTimeoutMS == 0 {
		cfg.PollTimeoutMS = 30000
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.AutoCommitInterval == 0 {
		cfg.AutoCommitInterval = 5 * time.Second
	}
	if cfg.MaxCommitRetries == 0 {
		cfg.MaxCommitRetries = 5
	}
	if cfg.CommitRetryBackoff == 0 {
		cfg.CommitRetryBackoff = 500 * time.Millisecond
	}
	if cfg.CommitRetryMaxBackoff == 0 {
		cfg.CommitRetryMaxBackoff = 2 * time.Second
	}
	if cfg.CommitRetryMaxBackoff < cfg.CommitRetryBackoff {
		cfg.CommitRetryMaxBackoff = cfg.CommitRetryBackoff * 10
	}
	if cfg.StreamingCommitInterval == 0 {
		cfg.StreamingCommitInterval = 1 * time.Second
	}
	if cfg.StreamingCommitBatchSize <= 0 {
		cfg.StreamingCommitBatchSize = 100
	}
	if cfg.StreamingCommitInterval > cfg.AutoCommitInterval {
		util.Warn("StreamingCommitInterval (%v) exceeds AutoCommitInterval (%v), capping to %v", cfg.StreamingCommitInterval, cfg.AutoCommitInterval, cfg.AutoCommitInterval/2)
		cfg.StreamingCommitInterval = cfg.AutoCommitInterval / 2
	}
	if cfg.MaxPollRecords == 0 {
		cfg.MaxPollRecords = 500
	}
	if cfg.Mode == "" {
		cfg.Mode = ModePolling
	}
	if cfg.MaxConnectRetries == 0 {
		cfg.MaxConnectRetries = 5
	}
	if cfg.ConnectRetryBackoffMS == 0 {
		cfg.ConnectRetryBackoffMS = 1000
	}
	if cfg.HeartbeatIntervalMS == 0 {
		cfg.HeartbeatIntervalMS = cfg.SessionTimeoutMS / 2
	}

	if cfg.LeaderStaleness == 0 {
		cfg.LeaderStaleness = 30 * time.Second
	}

	if cfg.StreamingReadDeadlineMS == 0 {
		cfg.StreamingReadDeadlineMS = 5 * 60 * 1000 // 5min
	}
	if cfg.StreamingRetryIntervalMS == 0 {
		cfg.StreamingRetryIntervalMS = 1000
	}

	if *benchmarkFlag {
		cfg.EnableBenchmark = true
	}
	if *correctnessFlag {
		cfg.EnableCorrectness = true
	}

	if !cfg.EnableBenchmark {
		if cfg.EnableCorrectness {
			util.Warn("enable_correctness is ignored because enable_benchmark is false")
		}
		cfg.EnableCorrectness = false
		cfg.NumMessages = 0
	} else {
		if cfg.NumMessages <= 0 {
			cfg.NumMessages = 10
		}
	}

	util.SetLevel(cfg.LogLevel)

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		util.Error("Failed to marshal config: %v", err)
	} else {
		util.Info("Configuration:\n%s", string(data))
	}
	return cfg, nil
}
