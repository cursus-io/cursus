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

type PublisherConfig struct {
	BrokerAddrs        []string `yaml:"broker_addrs" json:"broker_addrs"`
	CurrentBrokerIndex int      `yaml:"-" json:"-"`

	MaxRetries     int `yaml:"max_retries" json:"max_retries"`
	RetryBackoffMS int `yaml:"retry_backoff_ms" json:"retry_backoff_ms"`
	AckTimeoutMS   int `yaml:"ack_timeout_ms" json:"ack_timeout_ms"`

	Topic            string `yaml:"topic" json:"topic"`
	AutoCreateTopics bool   `yaml:"auto_create_topics" json:"auto_create_topics"`
	Partitions       int    `yaml:"partitions" json:"partitions"`

	LeaderStaleness time.Duration `yaml:"leader_staleness" json:"leader_staleness"`

	PublishDelayMS      int `yaml:"publish_delay_ms" json:"publish_delay_ms"`
	MaxInflightRequests int `yaml:"max_inflight_requests" json:"max_inflight_requests"`
	MaxBackoffMS        int `yaml:"max_backoff_ms" json:"max_backoff_ms"`
	WriteTimeoutMS      int `yaml:"write_timeout_ms" json:"write_timeout_ms"`

	Acks              string `yaml:"acks" json:"acks"`
	EnableIdempotence bool   `yaml:"enable_idempotence" json:"enable_idempotence"`
	BatchSize         int    `yaml:"batch_size" json:"batch_size"`
	BufferSize        int    `yaml:"buffer_size" json:"buffer_size"`
	LingerMS          int    `yaml:"linger_ms" json:"linger_ms"`

	UseTLS      bool   `yaml:"use_tls" json:"use_tls"`
	TLSCertPath string `yaml:"tls_cert_path" json:"tls_cert_path"`
	TLSKeyPath  string `yaml:"tls_key_path" json:"tls_key_path"`

	CompressionType string `yaml:"compression_type" json:"compression.type"` // "none", "gzip", "snappy", "lz4"

	EnableBenchmark bool   `yaml:"enable_benchmark" json:"enable_benchmark"`
	BenchTopicName  string `yaml:"bench_topic_name" json:"bench_topic_name"`
	MessageSize     int    `yaml:"benchmark_message_size" json:"benchmark_message_size"`
	NumMessages     int    `yaml:"num_messages" json:"num_messages"`

	FlushTimeoutMS int `yaml:"flush_timeout_ms" json:"flush_timeout_ms"`

	LogLevel util.LogLevel `yaml:"log_level" json:"log_level"`
}

func LoadPublisherConfig(explicitPath string) (*PublisherConfig, error) {
	cfg := &PublisherConfig{}

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

	flag.IntVar(&cfg.MaxRetries, "max-retries", 3, "Maximum retry attempts")
	flag.IntVar(&cfg.RetryBackoffMS, "retry-backoff-ms", 100, "Initial backoff time in milliseconds")
	flag.IntVar(&cfg.AckTimeoutMS, "ack-timeout-ms", 5000, "ACK timeout in milliseconds")

	flag.StringVar(&cfg.Topic, "topic", "my-topic", "Topic name")
	flag.BoolVar(&cfg.AutoCreateTopics, "auto-create-topics", cfg.AutoCreateTopics, "Auto-create topics")
	flag.IntVar(&cfg.Partitions, "partitions", 4, "Number of partitions")
	flag.IntVar(&cfg.NumMessages, "num-messages", 10, "Number of messages to publish")

	flag.DurationVar(&cfg.LeaderStaleness, "leader-staleness", 30*time.Second, "Leader staleness duration")

	flag.IntVar(&cfg.PublishDelayMS, "publish-delay-ms", 0, "Delay between messages in milliseconds")
	flag.IntVar(&cfg.MaxInflightRequests, "max-inflight", 5, "Max inflight requests")
	flag.IntVar(&cfg.BatchSize, "batch-size", 100, "Batch size")
	flag.IntVar(&cfg.BufferSize, "buffer-size", 1024, "Buffer size")
	flag.IntVar(&cfg.LingerMS, "linger-ms", 50, "Linger time in milliseconds")
	flag.IntVar(&cfg.MaxBackoffMS, "max-backoff-ms", 2000, "Maximum backoff time in ms")
	flag.IntVar(&cfg.WriteTimeoutMS, "write-timeout-ms", 5000, "Write timeout in ms")

	flag.IntVar(&cfg.FlushTimeoutMS, "flush-timeout-ms", 30000, "Timeout to flush in ms")

	flag.BoolVar(&cfg.UseTLS, "use-tls", false, "Enable TLS")
	flag.StringVar(&cfg.TLSCertPath, "tls-cert", "", "TLS cert path")
	flag.StringVar(&cfg.TLSKeyPath, "tls-key", "", "TLS key path")

	flag.StringVar(&cfg.CompressionType, "compression-type", "none", "Compression type (none, gzip, snappy, lz4)")

	benchmarkFlag := flag.Bool("benchmark", false, "Enable benchmark mode with detailed metrics")
	flag.StringVar(&cfg.BenchTopicName, "bench-topic-name", "bench-topic", "Topic used in benchmark mode")
	flag.IntVar(&cfg.MessageSize, "benchmark_message_size", 100, "Message size in bytes (benchmark mode only)")

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

	finalPath := *configPath
	if explicitPath != "" {
		finalPath = explicitPath
	}

	if finalPath != "" {
		data, err := os.ReadFile(finalPath)
		if err != nil {
			if os.IsNotExist(err) {
				util.Error("Config file %s not found, using flag defaults", finalPath)
			} else {
				return nil, fmt.Errorf("failed to read config file %s: %w", finalPath, err)
			}
		} else {
			if strings.HasSuffix(finalPath, ".json") {
				if err := json.Unmarshal(data, cfg); err != nil {
					util.Warn("Failed to parse JSON config: %v", err)
				}
			} else {
				if err := yaml.Unmarshal(data, cfg); err != nil {
					util.Warn("Failed to parse YAML config: %v", err)
				}
			}
		}
	}

	if len(cfg.BrokerAddrs) == 0 {
		cfg.BrokerAddrs = []string{"localhost:9000"}
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	if cfg.RetryBackoffMS <= 0 {
		cfg.RetryBackoffMS = 100
	}
	if cfg.AckTimeoutMS <= 0 {
		cfg.AckTimeoutMS = 5000
	}
	if cfg.MaxInflightRequests <= 0 {
		cfg.MaxInflightRequests = 5
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1024
	}
	if cfg.LingerMS <= 0 {
		cfg.LingerMS = 50
	}
	if cfg.Partitions <= 0 {
		cfg.Partitions = 1
	}
	if cfg.PublishDelayMS < 0 {
		cfg.PublishDelayMS = 0
	}
	if cfg.MaxBackoffMS <= 0 {
		cfg.MaxBackoffMS = 2000
	}
	if cfg.WriteTimeoutMS <= 0 {
		cfg.WriteTimeoutMS = 5000
	}
	if cfg.FlushTimeoutMS < 0 {
		cfg.FlushTimeoutMS = 30000
	}
	if cfg.Acks != "0" && cfg.Acks != "1" && cfg.Acks != "all" {
		cfg.Acks = "1"
	}
	if cfg.Acks == "all" {
		cfg.Acks = "-1"
	}
	if cfg.EnableIdempotence && cfg.Acks == "0" {
		util.Warn("Idempotence requires acks >= 1, setting acks=1")
		cfg.Acks = "1"
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		cfg.Topic = "default-topic"
	}

	if cfg.LeaderStaleness <= 0 {
		cfg.LeaderStaleness = 30 * time.Second
	}

	if cfg.UseTLS && (cfg.TLSCertPath == "" || cfg.TLSKeyPath == "") {
		return nil, fmt.Errorf("TLS enabled but cert/key paths not provided")
	}

	if *benchmarkFlag {
		cfg.EnableBenchmark = true
	}

	if cfg.EnableBenchmark {
		if cfg.Topic == "my-topic" {
			cfg.Topic = cfg.BenchTopicName
		}
		if cfg.MessageSize <= 0 {
			cfg.MessageSize = 1024
		}
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
