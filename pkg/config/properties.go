package config

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/util"
	"gopkg.in/yaml.v3"
)

var (
	defaultConfig *Config
	defaultOnce   sync.Once
)

type ConsumerGroupConfig struct {
	Name            string         `yaml:"name" json:"name"`
	ConsumerCount   int            `yaml:"consumer_count" json:"consumer_count"`
	Topics          []string       `yaml:"topics" json:"topics"`
	TopicPartitions map[string]int `yaml:"topic_partitions" json:"topic_partitions"`
}

type Config struct {
	BrokerPort      int           `yaml:"broker_port" json:"broker.port"`
	HealthCheckPort int           `yaml:"health_check_port" json:"health.check.port"`
	EnableExporter  bool          `yaml:"enable_exporter" json:"enable.exporter"`
	ExporterPort    int           `yaml:"exporter_port" json:"exporter.port"`
	LogLevel        util.LogLevel `yaml:"log_level" json:"log_level"`

	// disk storage
	LogDir              string `yaml:"log_dir" json:"log.dir"`
	DiskFlushBatchSize  int    `yaml:"disk_flush_batch_size" json:"disk.flush.batch.size"`
	DiskFlushIntervalMS int    `yaml:"disk_flush_interval_ms" json:"disk.flush.interval.ms"`
	DiskWriteTimeoutMS  int    `yaml:"disk_write_timeout_ms" json:"disk.write.timeout.ms"`
	LingerMS            int    `yaml:"linger_ms" json:"linger.ms"`
	CompressionType     string `yaml:"compression_type" json:"compression.type"` // "none", "gzip", "snappy", "lz4"

	// log segment
	CleanupInterval           int     `yaml:"log_cleanup_interval" json:"log.cleanup.interval"`
	SegmentSize               uint64  `yaml:"log_segment_bytes" json:"log.segment.bytes"`
	SegmentRollTimeMS         int     `yaml:"log_segment_roll_ms" json:"log.segment.roll.ms"`
	IndexSize                 uint64  `yaml:"log_index_size_bytes" json:"log.index.size.bytes"`
	IndexIntervalBytes        int     `yaml:"log_index_interval_bytes" json:"log.index.interval.bytes"`
	CleanupPolicy             string  `yaml:"log_cleanup_policy" json:"log.cleanup.policy"` // delete, compact
	RetentionHours            int     `yaml:"log_retention_hours" json:"log.retention.hours"`
	RetentionBytes            int64   `yaml:"log_retention_bytes" json:"log.retention.bytes"`
	RetentionCheckIntervalMS  int     `yaml:"log_retention_check_interval_ms" json:"log.retention.check.interval.ms"`
	CompactionCheckIntervalMS int     `yaml:"log_compaction_check_interval_ms" json:"log.compaction.check.interval.ms"`
	MinCleanableDirtyRatio    float64 `yaml:"log_min_cleanable_dirty_ratio" json:"log.min.cleanable.dirty.ratio"`

	// internal channels
	ChannelBufferSize          int `yaml:"channel_buffer_size" json:"channel.buffer.size"`
	PartitionChannelBufSize    int `yaml:"partition_channel_buffer_size" json:"partition.channel.buffer.size"`
	ConsumerChannelBufSize     int `yaml:"consumer_channel_buffer_size" json:"consumer.channel.buffer.size"`
	BroadcastChannelBufferSize int `yaml:"broadcast_channel_buffer_size" json:"broadcast.channel.buffer.size"`

	// distributed cluster
	EnabledDistribution  bool     `yaml:"enabled_distribution" json:"distribution.enabled"`
	RaftPort             int      `yaml:"raft_port" json:"distribution.raft.port"`
	DiscoveryPort        int      `yaml:"discovery_port" json:"distribution.discovery.port"`
	RaftPeers            []string `yaml:"raft_peers" json:"distribution.raft.peers"`
	StaticClusterMembers []string `yaml:"static_cluster_members" json:"distribution.static_cluster_members"`
	BootstrapCluster     bool     `yaml:"bootstrap_cluster" json:"distribution.bootstrap"`

	AdvertisedHost            string   `yaml:"advertised_host" json:"distribution.advertised_host"`
	MinInSyncReplicas         int      `yaml:"min_insync_replicas" json:"min.insync.replicas"`
	DefaultReplicationFactor  int      `yaml:"default_replication_factor" json:"default.replication.factor"`

	// idempotency
	EnableIdempotence bool `yaml:"enable_idempotence" json:"enable.idempotence"`

	// consumer
	ConsumerSessionTimeoutMS int                   `yaml:"consumer_session_timeout_ms" json:"consumer.session.timeout.ms"`
	ConsumerHeartbeatCheckMS int                   `yaml:"consumer_heartbeat_check_ms" json:"consumer.heartbeat.check.ms"`
	StaticConsumerGroups     []ConsumerGroupConfig `yaml:"static_consumer_groups" json:"static_consumer_groups"`

	// stream
	MaxStreamConnections    int           `yaml:"max_stream_connections" json:"max.stream.connections"`
	StreamTimeout           time.Duration `yaml:"stream_timeout" json:"stream.timeout"`
	StreamHeartbeatInterval time.Duration `yaml:"stream_heartbeat_interval" json:"stream.heartbeat.interval"`
	StreamCommitInterval    time.Duration `yaml:"stream_commit_interval" json:"stream.commit.interval"`

	// security
	UseTLS      bool `yaml:"use_tls" json:"tls.enable"`
	TLSCert     tls.Certificate
	TLSCertPath string `yaml:"tls_cert_path" json:"tls.cert_path"`
	TLSKeyPath  string `yaml:"tls_key_path" json:"tls.key_path"`
}

func DefaultConfig() *Config {
	defaultOnce.Do(func() {
		defaultConfig = &Config{
			BrokerPort:      9000,
			HealthCheckPort: 9080,
			EnableExporter:  true,
			ExporterPort:    9100,
			LogLevel:        util.LogLevelInfo,

			// disk storage
			LogDir:              "broker-logs",
			DiskFlushBatchSize:  50,
			DiskFlushIntervalMS: 500,
			DiskWriteTimeoutMS:  10,
			LingerMS:            50,
			CompressionType:     "none",

			// log segment & retention
			CleanupInterval:           300,
			SegmentSize:               1 * 1024 * 1024 * 1024,  // 1GB
			SegmentRollTimeMS:         7 * 24 * 60 * 60 * 1000, // 7day
			IndexSize:                 10 * 1024 * 1024,        // 10MB
			IndexIntervalBytes:        4096,
			CleanupPolicy:             "delete", // delete, compact
			RetentionHours:            168,
			RetentionBytes:            -1,
			RetentionCheckIntervalMS:  300000,
			CompactionCheckIntervalMS: 300000,
			MinCleanableDirtyRatio:    0.5,

			// internal channels
			ChannelBufferSize:          1024,
			PartitionChannelBufSize:    10000,
			ConsumerChannelBufSize:     1000,
			BroadcastChannelBufferSize: 10000,

			// distributed cluster
			EnabledDistribution:  false,
			RaftPort:             9001,
			DiscoveryPort:        8000,
			RaftPeers:            []string{},
			StaticClusterMembers: []string{},
			BootstrapCluster:     false,
			AdvertisedHost:       "localhost",
			MinInSyncReplicas:         2,
			DefaultReplicationFactor:  3,

			// idempotency
			EnableIdempotence: false,

			// consumer
			ConsumerSessionTimeoutMS: 10000,
			ConsumerHeartbeatCheckMS: 5000,

			// stream
			MaxStreamConnections:    1000,
			StreamTimeout:           30 * time.Minute,
			StreamHeartbeatInterval: 3 * time.Second,
			StreamCommitInterval:    5 * time.Second,
		}
	})

	cfgCopy := *defaultConfig
	if defaultConfig.RaftPeers != nil {
		cfgCopy.RaftPeers = make([]string, len(defaultConfig.RaftPeers))
		copy(cfgCopy.RaftPeers, defaultConfig.RaftPeers)
	}
	if defaultConfig.StaticClusterMembers != nil {
		cfgCopy.StaticClusterMembers = make([]string, len(defaultConfig.StaticClusterMembers))
		copy(cfgCopy.StaticClusterMembers, defaultConfig.StaticClusterMembers)
	}
	if defaultConfig.StaticConsumerGroups != nil {
		cfgCopy.StaticConsumerGroups = make([]ConsumerGroupConfig, len(defaultConfig.StaticConsumerGroups))
		copy(cfgCopy.StaticConsumerGroups, defaultConfig.StaticConsumerGroups)
	}
	return &cfgCopy
}

func LoadConfig() (*Config, error) {
	cfg := DefaultConfig()
	configPath := flag.String("config", "", "Path to YAML/JSON config file")

	flag.IntVar(&cfg.BrokerPort, "port", cfg.BrokerPort, "Broker port")
	flag.IntVar(&cfg.HealthCheckPort, "health-port", cfg.HealthCheckPort, "Health port")
	flag.BoolVar(&cfg.EnableExporter, "exporter", cfg.EnableExporter, "Enable exporter")
	flag.IntVar(&cfg.ExporterPort, "exporter-port", cfg.ExporterPort, "Exporter port")
	logLevelStr := flag.String("log-level", "info", "Log level")

	// disk storage
	flag.StringVar(&cfg.LogDir, "log-dir", cfg.LogDir, "Log directory")
	flag.IntVar(&cfg.DiskFlushBatchSize, "disk-flush-batch", cfg.DiskFlushBatchSize, "Disk flush batch")
	flag.IntVar(&cfg.DiskFlushIntervalMS, "disk-flush-interval-ms", cfg.DiskFlushIntervalMS, "Disk sync interval in milliseconds")
	flag.IntVar(&cfg.DiskWriteTimeoutMS, "disk-write-timeout", cfg.DiskWriteTimeoutMS, "Disk write timeout")
	flag.IntVar(&cfg.LingerMS, "linger-ms", cfg.LingerMS, "Linger ms")
	flag.StringVar(&cfg.CompressionType, "compression-type", "none", "Compression type (none, gzip, snappy, lz4)")

	// log segment & retention
	flag.IntVar(&cfg.CleanupInterval, "cleanup-interval", cfg.CleanupInterval, "Cleanup seconds")
	var segmentSizeInt64 int64
	flag.Int64Var(&segmentSizeInt64, "segment-size", int64(cfg.SegmentSize), "Segment size")
	flag.IntVar(&cfg.SegmentRollTimeMS, "segment-roll-time-ms", cfg.SegmentRollTimeMS, "Segment roll time")
	var indexSizeInt64 int64
	flag.Int64Var(&indexSizeInt64, "index-size", int64(cfg.IndexSize), "Max index file size")
	flag.IntVar(&cfg.IndexIntervalBytes, "index-interval-bytes", cfg.IndexIntervalBytes, "Index interval bytes")
	flag.StringVar(&cfg.CleanupPolicy, "cleanup-policy", cfg.CleanupPolicy, "Cleanup policy (delete/compact)")
	flag.IntVar(&cfg.RetentionHours, "retention-hours", cfg.RetentionHours, "Retention hours")
	flag.Int64Var(&cfg.RetentionBytes, "retention-bytes", cfg.RetentionBytes, "Retention bytes")
	flag.IntVar(&cfg.RetentionCheckIntervalMS, "retention-check-interval", cfg.RetentionCheckIntervalMS, "Retention check interval")
	flag.IntVar(&cfg.CompactionCheckIntervalMS, "compaction-check-interval", cfg.CompactionCheckIntervalMS, "Compaction check interval ms")
	flag.Float64Var(&cfg.MinCleanableDirtyRatio, "min-cleanable-dirty-ratio", cfg.MinCleanableDirtyRatio, "Min cleanable dirty ratio (0.1 ~ 0.9)")

	// internal channels
	flag.IntVar(&cfg.ChannelBufferSize, "channel-buffer", cfg.ChannelBufferSize, "Channel buffer")
	flag.IntVar(&cfg.PartitionChannelBufSize, "partition-ch-buffer", cfg.PartitionChannelBufSize, "Partition buffer")
	flag.IntVar(&cfg.ConsumerChannelBufSize, "consumer-ch-buffer", cfg.ConsumerChannelBufSize, "Consumer buffer")
	flag.IntVar(&cfg.BroadcastChannelBufferSize, "broadcast-ch-buffer", cfg.BroadcastChannelBufferSize, "Broadcast channel buffer size")

	// distributed cluster
	flag.BoolVar(&cfg.EnabledDistribution, "enable-distribution", cfg.EnabledDistribution, "Enable distributed clustering")
	flag.IntVar(&cfg.RaftPort, "raft-port", cfg.RaftPort, "Raft port for replication")
	flag.IntVar(&cfg.DiscoveryPort, "discovery-port", cfg.DiscoveryPort, "Discovery service port")
	raftPeersFlag := flag.String("raft-peers", "", "Raft peer addresses (comma-separated)")
	flag.BoolVar(&cfg.BootstrapCluster, "bootstrap-cluster", cfg.BootstrapCluster, "Bootstrap Raft cluster")
	flag.StringVar(&cfg.AdvertisedHost, "advertised-host", cfg.AdvertisedHost, "Advertised host for discovery")
	flag.IntVar(&cfg.MinInSyncReplicas, "min-insync-replicas", cfg.MinInSyncReplicas, "Minimum in-sync replicas for writes")
	flag.IntVar(&cfg.DefaultReplicationFactor, "default-replication-factor", cfg.DefaultReplicationFactor, "Default replication factor for new topics")

	// idempotency
	flag.BoolVar(&cfg.EnableIdempotence, "enable-idempotence", cfg.EnableIdempotence, "Enable producer idempotency")

	// consumer
	flag.IntVar(&cfg.ConsumerSessionTimeoutMS, "consumer-session-timeout", cfg.ConsumerSessionTimeoutMS, "Session timeout")
	flag.IntVar(&cfg.ConsumerHeartbeatCheckMS, "consumer-heartbeat-check", cfg.ConsumerHeartbeatCheckMS, "Heartbeat check")

	// stream
	flag.IntVar(&cfg.MaxStreamConnections, "max-stream-connections", cfg.MaxStreamConnections, "Max stream connections")
	flag.DurationVar(&cfg.StreamTimeout, "stream-timeout", cfg.StreamTimeout, "Stream timeout")
	flag.DurationVar(&cfg.StreamHeartbeatInterval, "stream-heartbeat-interval", cfg.StreamHeartbeatInterval, "Stream heartbeat")
	flag.DurationVar(&cfg.StreamCommitInterval, "stream-commit-interval", cfg.StreamCommitInterval, "Stream commit interval")

	// security
	flag.BoolVar(&cfg.UseTLS, "tls", cfg.UseTLS, "Enable TLS")
	flag.StringVar(&cfg.TLSCertPath, "tls-cert", cfg.TLSCertPath, "TLS cert")
	flag.StringVar(&cfg.TLSKeyPath, "tls-key", cfg.TLSKeyPath, "TLS key")

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

	if env := os.Getenv("CONFIG_PATH"); env != "" && *configPath == "" {
		*configPath = env
	}

	if *configPath != "" {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			if os.IsNotExist(err) {
				util.Warn("Config file %s not found, using flag defaults", *configPath)
			} else {
				return nil, fmt.Errorf("failed to read config file %s: %w", *configPath, err)
			}
		} else {
			if strings.HasSuffix(*configPath, ".json") {
				if err := json.Unmarshal(data, cfg); err != nil {
					return nil, err
				}
			} else {
				if err := yaml.Unmarshal(data, cfg); err != nil {
					return nil, err
				}
			}
		}
	}

	if *raftPeersFlag != "" {
		parts := strings.Split(*raftPeersFlag, ",")
		cfg.RaftPeers = make([]string, 0, len(parts))
		for _, s := range parts {
			if s = strings.TrimSpace(s); s != "" {
				cfg.RaftPeers = append(cfg.RaftPeers, s)
			}
		}
	}

	if segmentSizeInt64 <= 0 {
		cfg.SegmentSize = DefaultConfig().SegmentSize
	} else {
		cfg.SegmentSize = uint64(segmentSizeInt64)
	}
	if indexSizeInt64 <= 0 {
		cfg.IndexSize = DefaultConfig().IndexSize
	} else {
		cfg.IndexSize = uint64(indexSizeInt64)
	}

	overrideEnvInt(&cfg.BrokerPort, "BROKER_PORT")
	overrideEnvInt(&cfg.HealthCheckPort, "HEALTH_CHECK_PORT")
	overrideEnvBool(&cfg.EnableExporter, "ENABLE_EXPORTER")
	overrideEnvInt(&cfg.ExporterPort, "EXPORTER_PORT")

	overrideEnvInt(&cfg.DiskFlushBatchSize, "DISK_FLUSH_BATCH")
	overrideEnvInt(&cfg.DiskFlushIntervalMS, "DISK_FLUSH_INTERVAL_MS")
	overrideEnvInt(&cfg.LingerMS, "LINGER_MS")
	overrideEnvString(&cfg.CompressionType, "COMPRESSION_TYPE")

	overrideEnvUint64(&cfg.SegmentSize, "LOG_SEGMENT_BYTES")
	overrideEnvUint64(&cfg.IndexSize, "LOG_INDEX_SIZE_BYTES")
	overrideEnvInt(&cfg.IndexIntervalBytes, "LOG_INDEX_INTERVAL_BYTES")
	overrideEnvString(&cfg.CleanupPolicy, "LOG_CLEANUP_POLICY")
	overrideEnvInt(&cfg.RetentionHours, "LOG_RETENTION_HOURS")
	overrideEnvInt64(&cfg.RetentionBytes, "LOG_RETENTION_BYTES")
	overrideEnvInt(&cfg.RetentionCheckIntervalMS, "LOG_RETENTION_CHECK_INTERVAL_MS")
	overrideEnvInt(&cfg.CompactionCheckIntervalMS, "LOG_COMPACTION_CHECK_INTERVAL_MS")
	overrideEnvFloat64(&cfg.MinCleanableDirtyRatio, "LOG_MIN_CLEANABLE_DIRTY_RATIO")

	overrideEnvInt(&cfg.PartitionChannelBufSize, "PARTITION_CH_BUFFER")
	overrideEnvInt(&cfg.ConsumerChannelBufSize, "CONSUMER_CH_BUFFER")
	overrideEnvInt(&cfg.BroadcastChannelBufferSize, "BROADCAST_CH_BUFFER")

	overrideEnvBool(&cfg.EnabledDistribution, "ENABLE_DISTRIBUTION")
	overrideEnvString(&cfg.AdvertisedHost, "ADVERTISED_HOST")
	overrideEnvInt(&cfg.RaftPort, "RAFT_PORT")
	overrideEnvInt(&cfg.DiscoveryPort, "DISCOVERY_PORT")
	overrideEnvStringSlice(&cfg.RaftPeers, "RAFT_PEERS")
	overrideEnvStringSlice(&cfg.StaticClusterMembers, "STATIC_CLUSTER_MEMBERS")
	overrideEnvBool(&cfg.BootstrapCluster, "BOOTSTRAP_CLUSTER")
	overrideEnvInt(&cfg.MinInSyncReplicas, "MIN_INSYNC_REPLICAS")
	overrideEnvInt(&cfg.DefaultReplicationFactor, "DEFAULT_REPLICATION_FACTOR")

	overrideEnvBool(&cfg.EnableIdempotence, "ENABLE_IDEMPOTENCE")

	overrideEnvInt(&cfg.ConsumerSessionTimeoutMS, "CONSUMER_SESSION_TIMEOUT")
	overrideEnvInt(&cfg.ConsumerHeartbeatCheckMS, "CONSUMER_HEARTBEAT_CHECK")

	cfg.Normalize()
	util.SetLevel(cfg.LogLevel)

	if cfg.UseTLS {
		if cfg.TLSCertPath == "" || cfg.TLSKeyPath == "" {
			return nil, fmt.Errorf("TLS enabled but missing cert/key path")
		}
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS cert: %w", err)
		}
		cfg.TLSCert = cert
	}

	return cfg, nil
}
