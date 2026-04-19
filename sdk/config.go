package sdk
import (
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
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

	CompressionType string `yaml:"compression_type" json:"compression_type"` // "none", "gzip", "snappy", "lz4"

	EnableMetrics bool `yaml:"enable_metrics" json:"enable_metrics"`

	EnableBenchmark bool   `yaml:"enable_benchmark" json:"enable_benchmark"`
	BenchTopicName  string `yaml:"bench_topic_name" json:"bench_topic_name"`
	MessageSize     int    `yaml:"benchmark_message_size" json:"benchmark_message_size"`
	NumMessages     int    `yaml:"num_messages" json:"num_messages"`

	FlushTimeoutMS int `yaml:"flush_timeout_ms" json:"flush_timeout_ms"`

	LogLevel LogLevel `yaml:"log_level" json:"log_level"`
}

func NewDefaultPublisherConfig() *PublisherConfig {
	return &PublisherConfig{
		BrokerAddrs:         []string{"localhost:9000"},
		MaxRetries:          3,
		RetryBackoffMS:      100,
		AckTimeoutMS:        5000,
		Topic:               "default-topic",
		Partitions:          1,
		LeaderStaleness:     30 * time.Second,
		MaxInflightRequests: 5,
		BatchSize:           100,
		BufferSize:          1024,
		LingerMS:            50,
		MaxBackoffMS:        2000,
		WriteTimeoutMS:      5000,
		FlushTimeoutMS:      30000,
		Acks:                "1",
		CompressionType:     "none",
		LogLevel:            LogLevelInfo,
	}
}

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

	UseTLS      bool   `yaml:"use_tls" json:"use_tls"`
	TLSCertPath string `yaml:"tls_cert_path" json:"tls_cert_path"`
	TLSKeyPath  string `yaml:"tls_key_path" json:"tls_key_path"`

	LeaderStaleness time.Duration `yaml:"leader_staleness" json:"leader_staleness"`

	CompressionType string `yaml:"compression_type" json:"compression_type"` // "none", "gzip", "snappy", "lz4"

	EnableMetrics bool `yaml:"enable_metrics" json:"enable_metrics"`

	LogLevel LogLevel `yaml:"log_level" json:"log_level"`
}

func NewDefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		BrokerAddrs:              []string{"localhost:9000"},
		ConsumerID:               "consumer-" + uuid.New().String()[:8],
		GroupID:                  "default-group",
		WorkerChannelSize:        1000,
		PollInterval:             500 * time.Millisecond,
		PollTimeoutMS:            30000,
		BatchSize:                100,
		MaxPollRecords:           500,
		EnableAutoCommit:         true,
		AutoCommitInterval:       5 * time.Second,
		MaxCommitRetries:         5,
		CommitRetryBackoff:       500 * time.Millisecond,
		CommitRetryMaxBackoff:    2 * time.Second,
		StreamingCommitInterval:  1 * time.Second,
		StreamingCommitBatchSize: 100,
		SessionTimeoutMS:         30000,
		HeartbeatIntervalMS:      3000,
		CompressionType:          "none",
		LeaderStaleness:          30 * time.Second,
		StreamingReadDeadlineMS:  300000,
		StreamingRetryIntervalMS: 1000,
		Mode:                     ModePolling,
		LogLevel:                 LogLevelInfo,
	}
}

func LoadConfig(path string, cfg interface{}) error {
	// Seed default values before unmarshalling if possible
	if c, ok := cfg.(*ConsumerConfig); ok {
		*c = *NewDefaultConsumerConfig()
	} else if p, ok := cfg.(*PublisherConfig); ok {
		*p = *NewDefaultPublisherConfig()
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if strings.HasSuffix(path, ".json") {
		return json.Unmarshal(data, cfg)
	}
	return yaml.Unmarshal(data, cfg)
}
