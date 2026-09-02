package sdk

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/ackpolicy"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type PublisherConfig struct {
	BrokerAddrs []string `yaml:"broker_addrs" json:"broker_addrs"`

	MaxRetries     int `yaml:"max_retries" json:"max_retries"`
	RetryBackoffMS int `yaml:"retry_backoff_ms" json:"retry_backoff_ms"`
	AckTimeoutMS   int `yaml:"ack_timeout_ms" json:"ack_timeout_ms"`

	Topic            string `yaml:"topic" json:"topic"`
	AutoCreateTopics bool   `yaml:"auto_create_topics" json:"auto_create_topics"`
	Partitions       int    `yaml:"partitions" json:"partitions"`

	LeaderStaleness time.Duration `yaml:"leader_staleness" json:"leader_staleness"`

	MaxBackoffMS   int `yaml:"max_backoff_ms" json:"max_backoff_ms"`
	WriteTimeoutMS int `yaml:"write_timeout_ms" json:"write_timeout_ms"`

	Acks              string `yaml:"acks" json:"acks"`
	EnableIdempotence bool   `yaml:"enable_idempotence" json:"enable_idempotence"`
	BatchSize         int    `yaml:"batch_size" json:"batch_size"`
	BufferSize        int    `yaml:"buffer_size" json:"buffer_size"`
	LingerMS          int    `yaml:"linger_ms" json:"linger_ms"`

	UseTLS      bool   `yaml:"use_tls" json:"use_tls"`
	TLSCertPath string `yaml:"tls_cert_path" json:"tls_cert_path"`
	TLSKeyPath  string `yaml:"tls_key_path" json:"tls_key_path"`

	Principal string `yaml:"principal" json:"principal"`
	AuthToken string `yaml:"auth_token" json:"auth_token"`

	HandshakeTimeoutMS int    `yaml:"handshake_timeout_ms" json:"handshake_timeout_ms"`
	CompressionType    string `yaml:"compression_type" json:"compression_type"` // "none", "gzip", "snappy", "lz4"

	EnableMetrics bool `yaml:"enable_metrics" json:"enable_metrics"`

	EnableBenchmark bool `yaml:"enable_benchmark" json:"enable_benchmark"`

	FlushTimeoutMS int `yaml:"flush_timeout_ms" json:"flush_timeout_ms"`
}

// Validate checks publisher contracts that must fail before network or producer
// sequence state is created.
func (c *PublisherConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("publisher config is required")
	}
	selection, err := ackpolicy.Parse(c.Acks)
	if err != nil {
		return fmt.Errorf("invalid acks %q: %w", c.Acks, err)
	}
	c.Acks = selection.Requested
	if c.EnableIdempotence && !selection.SupportsIdempotence() {
		return fmt.Errorf("enable_idempotence requires acks=all or acks=-1")
	}
	if err := validateSDKTopicName(c.Topic); err != nil {
		return err
	}
	if err := validateWireClientSettings(c.CompressionType, c.Principal, c.AuthToken); err != nil {
		return err
	}
	if c.MaxRetries < 0 || c.RetryBackoffMS < 0 || c.AckTimeoutMS < 0 ||
		c.MaxBackoffMS < 0 || c.WriteTimeoutMS < 0 || c.LingerMS < 0 ||
		c.FlushTimeoutMS < 0 || c.LeaderStaleness < 0 || c.HandshakeTimeoutMS < 0 {
		return fmt.Errorf("publisher durations and limits must not be negative")
	}
	if c.Partitions <= 0 || c.BatchSize <= 0 || c.BufferSize <= 0 {
		return fmt.Errorf("publisher partitions, batch size, and buffer size must be positive")
	}
	if len(c.BrokerAddrs) == 0 {
		return fmt.Errorf("publisher requires at least one broker address")
	}
	for _, address := range c.BrokerAddrs {
		if strings.TrimSpace(address) == "" {
			return fmt.Errorf("publisher broker address must not be empty")
		}
	}
	if err := validateTLSFiles(c.UseTLS, c.TLSCertPath, c.TLSKeyPath); err != nil {
		return err
	}
	return nil
}

func NewDefaultPublisherConfig() *PublisherConfig {
	return &PublisherConfig{
		BrokerAddrs:        []string{"localhost:9000"},
		MaxRetries:         3,
		RetryBackoffMS:     100,
		AckTimeoutMS:       5000,
		Topic:              "default-topic",
		Partitions:         1,
		LeaderStaleness:    30 * time.Second,
		BatchSize:          100,
		BufferSize:         1024,
		LingerMS:           50,
		MaxBackoffMS:       2000,
		WriteTimeoutMS:     5000,
		FlushTimeoutMS:     30000,
		HandshakeTimeoutMS: 5000,
		Acks:               "1",
		CompressionType:    "none",
	}
}

type ConsumerMode string

const (
	ModePolling   ConsumerMode = "polling"
	ModeStreaming ConsumerMode = "streaming"
)

type AutoOffsetResetPolicy string

const (
	AutoOffsetResetEarliest AutoOffsetResetPolicy = "earliest"
	AutoOffsetResetLatest   AutoOffsetResetPolicy = "latest"
	AutoOffsetResetError    AutoOffsetResetPolicy = "error"
)

type ReadIsolation string

const (
	ReadCommitted   ReadIsolation = "read_committed"
	ReadUncommitted ReadIsolation = "read_uncommitted"
)

type ConsumerConfig struct {
	BrokerAddrs []string `yaml:"broker_addrs" json:"broker_addrs"`

	Topic      string `yaml:"topic" json:"topic"`
	GroupID    string `yaml:"group_id" json:"group_id"`
	ConsumerID string `yaml:"consumer_id" json:"consumer_id"`

	Mode ConsumerMode `yaml:"mode" json:"mode"`

	WorkerChannelSize int `yaml:"worker_channel_size" json:"worker_channel_size"`

	PollInterval    time.Duration         `yaml:"poll_interval" json:"poll_interval"`
	PollTimeoutMS   int                   `yaml:"poll_timeout_ms" json:"poll_timeout_ms"`
	BatchSize       int                   `yaml:"batch_size" json:"batch_size"`
	AutoOffsetReset AutoOffsetResetPolicy `yaml:"auto_offset_reset" json:"auto_offset_reset"`
	ReadIsolation   ReadIsolation         `yaml:"read_isolation" json:"read_isolation"`

	MaxPollRecords          int `yaml:"max_poll_records" json:"max_poll_records"`
	MaxConnectRetries       int `yaml:"max_connect_retries" json:"max_connect_retries"`
	ConnectRetryBackoffMS   int `yaml:"connect_retry_backoff_ms" json:"connect_retry_backoff_ms"`
	HeartbeatIntervalMS     int `yaml:"heartbeat_interval_ms" json:"heartbeat_interval_ms"`
	StreamingReadDeadlineMS int `yaml:"streaming_read_deadline_ms" json:"streaming_read_deadline_ms"`

	EnableAutoCommit   bool          `yaml:"enable_auto_commit" json:"enable_auto_commit"`
	AutoCommitInterval time.Duration `yaml:"auto_commit_interval" json:"auto_commit_interval"`

	MaxCommitRetries      int           `yaml:"max_commit_retries" json:"max_commit_retries"`
	CommitRetryBackoff    time.Duration `yaml:"commit_retry_backoff" json:"commit_retry_backoff"`
	CommitRetryMaxBackoff time.Duration `yaml:"commit_retry_max_backoff" json:"commit_retry_max_backoff"`

	UseTLS      bool   `yaml:"use_tls" json:"use_tls"`
	TLSCertPath string `yaml:"tls_cert_path" json:"tls_cert_path"`
	TLSKeyPath  string `yaml:"tls_key_path" json:"tls_key_path"`

	Principal string `yaml:"principal" json:"principal"`
	AuthToken string `yaml:"auth_token" json:"auth_token"`

	HandshakeTimeoutMS int `yaml:"handshake_timeout_ms" json:"handshake_timeout_ms"`

	LeaderStaleness         time.Duration `yaml:"leader_staleness" json:"leader_staleness"`
	MetadataRefreshInterval time.Duration `yaml:"metadata_refresh_interval" json:"metadata_refresh_interval"`

	CompressionType string `yaml:"compression_type" json:"compression_type"` // "none", "gzip", "snappy", "lz4"

	EnableMetrics bool `yaml:"enable_metrics" json:"enable_metrics"`
}

func (c *ConsumerConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("consumer config is required")
	}
	if err := validateWireClientSettings(c.CompressionType, c.Principal, c.AuthToken); err != nil {
		return err
	}
	if err := validateSDKTopicName(c.Topic); err != nil {
		return err
	}
	if strings.TrimSpace(c.GroupID) == "" || strings.ContainsAny(c.GroupID, " \t\r\n") {
		return fmt.Errorf("consumer group ID must be non-empty and contain no whitespace")
	}
	if strings.TrimSpace(c.ConsumerID) == "" || strings.ContainsAny(c.ConsumerID, " \t\r\n") {
		return fmt.Errorf("consumer ID must be non-empty and contain no whitespace")
	}
	if err := validateTLSFiles(c.UseTLS, c.TLSCertPath, c.TLSKeyPath); err != nil {
		return err
	}
	if c.Mode != "" && c.Mode != ModePolling && c.Mode != ModeStreaming {
		return fmt.Errorf("unsupported consumer mode %q", c.Mode)
	}
	if c.AutoOffsetReset != "" && c.AutoOffsetReset != AutoOffsetResetEarliest &&
		c.AutoOffsetReset != AutoOffsetResetLatest && c.AutoOffsetReset != AutoOffsetResetError {
		return fmt.Errorf("unsupported auto offset reset policy %q", c.AutoOffsetReset)
	}
	if c.ReadIsolation != "" && c.ReadIsolation != ReadCommitted && c.ReadIsolation != ReadUncommitted {
		return fmt.Errorf("unsupported read isolation %q", c.ReadIsolation)
	}
	if c.PollInterval < 0 || c.AutoCommitInterval < 0 || c.CommitRetryBackoff < 0 ||
		c.CommitRetryMaxBackoff < 0 || c.LeaderStaleness < 0 ||
		c.MetadataRefreshInterval < 0 || c.PollTimeoutMS < 0 ||
		c.ConnectRetryBackoffMS < 0 || c.HeartbeatIntervalMS < 0 || c.StreamingReadDeadlineMS < 0 ||
		c.HandshakeTimeoutMS < 0 {
		return fmt.Errorf("consumer durations must not be negative")
	}
	if c.BatchSize <= 0 || c.MaxPollRecords <= 0 || c.WorkerChannelSize <= 0 {
		return fmt.Errorf("consumer batch size, max poll records, and worker channel size must be positive")
	}
	if c.MaxConnectRetries < 0 || c.MaxCommitRetries < 0 {
		return fmt.Errorf("consumer retry limits must not be negative")
	}
	if len(c.BrokerAddrs) == 0 {
		return fmt.Errorf("consumer requires at least one broker address")
	}
	for _, address := range c.BrokerAddrs {
		if strings.TrimSpace(address) == "" {
			return fmt.Errorf("consumer broker address must not be empty")
		}
	}
	return nil
}

func validateWireClientSettings(compression, principal, token string) error {
	if _, err := wire.ParseCompression(compression); err != nil {
		return err
	}
	if (principal == "") != (token == "") {
		return fmt.Errorf("principal and auth token must be configured together")
	}
	if strings.ContainsAny(principal, " \t\r\n") || strings.ContainsAny(token, " \t\r\n") {
		return fmt.Errorf("principal and auth token must not contain whitespace")
	}
	return nil
}

func validateTLSFiles(enabled bool, certificate, key string) error {
	if !enabled {
		return nil
	}
	if strings.TrimSpace(certificate) == "" || strings.TrimSpace(key) == "" {
		return fmt.Errorf("TLS certificate and key paths are required when TLS is enabled")
	}
	return nil
}

func NewDefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		BrokerAddrs:             []string{"localhost:9000"},
		Topic:                   "default-topic",
		ConsumerID:              "consumer-" + uuid.New().String()[:8],
		GroupID:                 "default-group",
		WorkerChannelSize:       1000,
		PollInterval:            500 * time.Millisecond,
		PollTimeoutMS:           30000,
		BatchSize:               100,
		AutoOffsetReset:         AutoOffsetResetEarliest,
		ReadIsolation:           ReadCommitted,
		MaxPollRecords:          500,
		EnableAutoCommit:        true,
		AutoCommitInterval:      5 * time.Second,
		MaxCommitRetries:        5,
		CommitRetryBackoff:      500 * time.Millisecond,
		CommitRetryMaxBackoff:   2 * time.Second,
		HeartbeatIntervalMS:     3000,
		HandshakeTimeoutMS:      5000,
		CompressionType:         "none",
		LeaderStaleness:         30 * time.Second,
		StreamingReadDeadlineMS: 300000,
		Mode:                    ModePolling,
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
		err = json.Unmarshal(data, cfg)
	} else {
		err = yaml.Unmarshal(data, cfg)
	}
	if err != nil {
		return err
	}
	if publisher, ok := cfg.(*PublisherConfig); ok {
		return publisher.Validate()
	}
	if consumer, ok := cfg.(*ConsumerConfig); ok {
		return consumer.Validate()
	}
	return nil
}
