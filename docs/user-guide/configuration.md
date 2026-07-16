# Configuration

This document provides a comprehensive guide to configuring cursus. 

It covers all available configuration parameters, their default values, configuration sources, and the precedence order when configuration is specified in multiple locations.

## Purpose and Scope

### Configuration in cursus controls:

- Network settings (ports, TLS)
- Performance tuning (buffer sizes, batch sizes, timeouts)
- Operational behavior (cleanup intervals, metrics exporters)
- Disk persistence parameters (flush batching, linger times)
- Message handling (compression, channel capacities)

The configuration system uses a hierarchical approach where settings can be specified via multiple sources with a defined precedence order.

## Configuration Sources and Precedence

cursus supports three configuration sources with the following precedence order (highest to lowest):

- **Command-line flags** - Highest precedence
- **Configuration file** (YAML or JSON) - Specified via `--config` flag or `CONFIG_PATH` environment variable
- **Built-in defaults** - Lowest precedence

## Configuration File Format

Configuration files can be in either YAML or JSON format. The format is detected automatically based on the file extension.

### YAML Format

The standard configuration format used in cursus is YAML. Here's the complete structure:

```
broker:
  port: 9000
  health_check_port: 9080
  log_dir: "broker-logs"
  cleanup_interval: 60
  enable_exporter: true
  exporter_port: 9100
  enable_benchmark: false
  
  # TLS Configuration
  use_tls: false
  tls_cert_path: "certs/server.crt"
  tls_key_path: "certs/server.key"
  internal_broker_port: 19000
  internal_use_tls: false
  internal_tls_cert_path: "certs/broker.crt"
  internal_tls_key_path: "certs/broker.key"
  internal_tls_ca_path: "certs/ca.crt"
  internal_tls_server_name: "broker.internal"
  enable_sasl: false
  sasl_users:
    - principal: "game-server"
      token: "change-me"
      permissions: ["topic.read", "topic.write", "group", "transaction"]
  
  # Compression
  enable_gzip: false
  
  # Performance Tuning
  channel_buffer_size: 10000
  disk_flush_batch_size: 500
  linger_ms: 100
  disk_write_timeout_ms: 200
  
  # Partition/Consumer Tuning
  partition_channel_buffer_size: 10000
  consumer_channel_buffer_size: 1000
```

### JSON Format

The same configuration can be expressed in JSON format:

```
{
  "broker.port": 9000,
  "health.check.port": 9080,
  "log.dir": "broker-logs",
  "enable.exporter": true,
  "exporter.port": 9100,
  "enable.benchmark": false,
  "cleanup.interval": 60,
  "tls.enable": false,
  "tls.cert_path": "certs/server.crt",
  "tls.key_path": "certs/server.key",
  "gzip.enable": false,
  "disk.flush.batch.size": 500,
  "linger.ms": 100,
  "channel.buffer.size": 10000,
  "partition.channel.buffer.size": 10000,
  "consumer.channel.buffer.size": 1000
}
```

## Configuration Structure

The configuration is represented by the Config struct in the codebase, which organizes parameters into logical categories.


### Common Parameters

| Parameter           | Type       | Default        | Description                                      |
|--------------------|------------|----------------|--------------------------------------------------|
| `broker_port`        | int        | 9000           | Main broker TCP port for client connections     |
| `health_check_port`  | int        | 9080           | HTTP port for `/live`, `/ready`, and `/health`             |
| `log_dir`            | string     | "broker-logs"  | Directory path for persistent log segments      |
| `enable_exporter`    | bool       | true           | Enable Prometheus metrics exporter              |
| `exporter_port`      | int        | 9100           | HTTP port for the Prometheus `/metrics` endpoint       |
| `enable_benchmark`   | bool       | false          | Enable benchmark mode for testing               |
| `cleanup_interval`   | int        | 300            | Log cleanup interval (seconds)                 |

In standalone mode, `log_dir` also contains `__transaction_state.journal`. The broker fsyncs this append-only coordinator journal before acknowledging transaction state transitions and repairs only a torn or checksum-corrupt final record during startup. One encoded snapshot record is limited to 32 MiB, so transaction batches must remain bounded. Include the journal in backup and restore procedures.

The health and metrics listeners are unauthenticated operations endpoints. Restrict both ports to a trusted network. `/live` reports process liveness, while `/ready` and the compatible `/health` endpoint include storage and distributed leader checks. See [Broker Observability](../reference/observability.md).

# Security and Compression

| Parameter       | Type   | Default | Description                                  |
|----------------|--------|---------|----------------------------------------------|
| `use_tls`        | bool   | false   | Enable TLS for TCP connections               |
| `tls_cert_path`  | string | ""      | Path to TLS certificate file                 |
| `tls_key_path`  | string | ""      | Path to TLS private key file                 |
| `internal_broker_port` | int | 0 | Optional dedicated broker-to-broker command port |
| `internal_use_tls` | bool | false | Require mutual TLS on the internal broker listener |
| `internal_tls_cert_path` | string | "" | Broker certificate for internal mTLS |
| `internal_tls_key_path` | string | "" | Broker private key for internal mTLS |
| `internal_tls_ca_path` | string | "" | CA used to verify peer broker certificates |
| `internal_tls_server_name` | string | "" | Server name used by broker-to-broker mTLS clients |
| `enable_sasl` | bool | false | Enable SASL-PLAIN-style token authentication for text commands |
| `sasl_users` | list | [] | Principal/token/permissions entries accepted by `AUTH` and inline authentication |
| `enable_gzip`    | bool   | false   | Enable gzip compression for messages        |

When `use_tls` is enabled and certificate paths are provided, the broker loads the certificate using `tls.LoadX509KeyPair()` during initialization. In distributed mode, `internal_broker_port` moves broker-to-broker text commands away from the public client listener. If `internal_use_tls` is enabled, the internal listener requires client certificates signed by `internal_tls_ca_path`, and peer routers dial the internal port with mTLS using `internal_tls_server_name` for certificate verification.

When `enable_sasl` is enabled, protected commands require `AUTH principal=<principal> token=<token>` or inline `principal=<principal> auth_token=<token>`. A user can declare `permissions` from `admin`, `topic.read`, `topic.write`, `group`, `transaction`, and `*`. `CONSUME`/`STREAM` require both `topic.read` and `group`; `TXN_PUBLISH` requires `transaction` and `topic.write`; `SEND_OFFSETS_TO_TXN` requires `transaction` and `group`. Topic `auth_policy=acl` is evaluated after the coarse permission check. Omitting `permissions` preserves the legacy authenticated-user access model; declare an explicit list for least privilege. The `SASL_USERS=principal:token` environment form creates legacy users without a restricted permission list, so use YAML or JSON configuration when least privilege is required.

# DiskHandler Performance Tuning

These parameters directly affect the write path performance and batching behavior described in DiskHandler and Write Path.

| Parameter             | Type | Default | Description                                                |
|----------------------|------|---------|------------------------------------------------------------|
| `disk_flush_batch_size` | int  | 50      | Number of messages to batch before flushing to disk       |
| `linger_ms`             | int  | 50      | Maximum time to wait before flushing (milliseconds)       |
| `channel_buffer_size`   | int  | 1024    | Buffer size for DiskHandler's writeCh channel             |
| `disk_write_timeout_ms` | int  | 10      | Timeout while enqueueing an asynchronous write (ms)       |
| `disk_flush_interval_ms`| int  | 500     | Periodic fsync interval (milliseconds)                    |
| `log_segment_bytes`     | uint64 | 1073741824 | Maximum segment file size (1GB default)                |
| `log_index_size_bytes`  | uint64 | 10485760   | Maximum index file size (10MB default)                 |
| `log_index_interval_bytes` | int | 4096    | Index entry interval in bytes                            |
| `log_retention_hours`   | int  | 168     | Log retention period in hours (7 days default)            |
| `log_retention_bytes`   | int64 | -1     | Retained byte limit; `-1` means unlimited                 |
| `log_segment_roll_ms`   | int  | 604800000 | Time-based roll interval (7 days)                       |
| `log_cleanup_policy`    | string | "delete" | Only implemented cleanup policy; compaction is unsupported |
| `log_retention_check_interval_ms` | int | 300000 | Retention evaluation interval                          |
| `compression_type`      | string | "none" | Compression type: "none", "gzip", "snappy", "lz4"       |


Trade-offs:

- Higher `disk_flush_batch_size`: Better throughput, higher queueing latency, and more records waiting for flush/sync
- Lower `linger_ms`: Lower latency, more frequent I/O operations, reduced throughput
- Larger `channel_buffer_size`: Better handling of burst traffic, higher memory usage

# Partition and Consumer Channel Tuning

These parameters control the in-memory channel buffer sizes for message distribution within the topic management system.

| Parameter                     | Type | Default | Description                                      |
|-------------------------------|------|---------|--------------------------------------------------|
| `partition_channel_buffer_size` | int  | 10000   | Buffer size for each Partition's input channel  |
| `consumer_channel_buffer_size`  | int  | 1000    | Buffer size for each Consumer's message channel |

# Broker And Cluster Parameters

These values participate in active broker behavior:

| Parameter | Default | Purpose |
|---|---:|---|
| `bootstrap_servers` | empty | Initial broker addresses for distributed discovery. |
| `acks` | empty/default path | Broker publish acknowledgement selection. |
| `min_insync_replicas` | 2 | Minimum in-sync replicas required by quorum writes. |
| `replication_factor` | 3 | Requested topic replica count when distribution is enabled. |
| `internal_broker_port` | 0 | Dedicated broker-to-broker command listener; configure in production clusters. |
| `internal_auth_token` | empty | Shared internal command credential; required unless mTLS identity is authoritative. |
| `internal_use_tls` | false | Enables broker-internal TLS and client-certificate verification. |
| `transactional_id_expiration_ms` | 604800000 | Retention for completed transaction payloads. Epoch tombstones remain for fencing; active transactions are not expired. |
| `producer_state_ttl_ms` | 1800000 | In-memory producer state cleanup window; durable records/checkpoints remain recovery sources. |

Distribution is disabled by default. Production clusters should use a dedicated internal listener, mTLS, least-privilege client users, and explicit advertised addresses.
# Using Configuration in Different Scenarios

## Scenario 1: Development with Defaults

For local development, you can run the broker with built-in defaults:

```
./bin/cursus
```

This uses:

- Port: 9000
- Health check port: 9080
- Exporter port: 9100
- Log directory: `./broker-logs`


## Scenario 2: Using a Configuration File

Create a configuration file and specify it:

```
./bin/cursus --config /path/to/config.yaml

// Or using the environment variable:
// export CONFIG_PATH=/path/to/config.yaml
// ./bin/cursus
```

The environment variable approach is checked in `pkg/config/properties.go`

## Scenario 3: Docker Deployment

In docker-compose deployments, configuration is typically mounted as a volume and referenced via environment variable:

```
services:
  broker:
    volumes:
      - ./config.yaml:/root/config.yaml
    environment:
      - CONFIG_PATH=/root/config.yaml
    ports:
      - "9000:9000"
      - "9100:9100"
      - "9080:9080"
```

## Scenario 4: Override Specific Parameters via CLI

You can use a configuration file for most settings and override specific values:

```
./bin/cursus --config config.yaml --port 9001 --exporter-port 9101
```

CLI flags take precedence over configuration file values.

## Scenario 5: High-Throughput Configuration

For maximum throughput at the cost of latency:

```
broker:
  disk_flush_batch_size: 1000    # Batch more messages
  linger_ms: 200                  # Wait longer before flush
  channel_buffer_size: 20000      # Larger write buffer
  partition_channel_buffer_size: 20000  # Larger partition buffers
  consumer_channel_buffer_size: 5000    # Larger consumer buffers
```

## Scenario 6: Low-Latency Configuration

For minimum latency at the cost of throughput:

```
broker:
  disk_flush_batch_size: 50      # Flush more frequently
  linger_ms: 10                  # Minimal wait time
  channel_buffer_size: 1024      # Smaller buffers
  partition_channel_buffer_size: 5000
  consumer_channel_buffer_size: 500
```

# Configuration Parameter Mapping

The Config struct uses both YAML and JSON tags to support both formats. Here's how parameter names map between different formats:

| Go Field Name             | YAML Key                     | JSON Key                      | CLI Flag                  |
|---------------------------|------------------------------|-------------------------------|---------------------------|
| BrokerPort                | `broker_port`                | `broker.port`                 | --port                   |
| HealthCheckPort           | `health_check_port`          | `health.check.port`           | --health-port            |
| LogDir                    | `log_dir`                    | `log.dir`                     | --log-dir                |
| EnableExporter            | `enable_exporter`            | `enable.exporter`             | --exporter               |
| ExporterPort              | `exporter_port`              | `exporter.port`               | --exporter-port          |
| EnableBenchmark           | `enable_benchmark`           | `enable.benchmark`            | --benchmark              |
| CleanupInterval           | `cleanup_interval`           | `cleanup.interval`            | --cleanup-interval       |
| UseTLS                    | `use_tls`                    | `tls.enable`                  | --tls                    |
| TLSCertPath               | `tls_cert_path`              | `tls.cert_path`               | --tls-cert               |
| TLSKeyPath                | `tls_key_path`               | `tls.key_path`                | --tls-key                |
| EnableGzip                | `enable_gzip`                | `gzip.enable`                 | --gzip                   |
| InternalBrokerPort        | `internal_broker_port`       | `distribution.internal_broker_port` | --internal-broker-port |
| InternalUseTLS            | `internal_use_tls`           | `internal_tls.enable`         | --internal-tls           |
| InternalTLSCertPath        | `internal_tls_cert_path`     | `internal_tls.cert_path`      | --internal-tls-cert      |
| InternalTLSKeyPath         | `internal_tls_key_path`      | `internal_tls.key_path`       | --internal-tls-key       |
| InternalTLSCAPath          | `internal_tls_ca_path`       | `internal_tls.ca_path`        | --internal-tls-ca        |
| InternalTLSServerName      | `internal_tls_server_name`   | `internal_tls.server_name`    | --internal-tls-server-name |
| EnableSASL                 | `enable_sasl`                | `sasl.enable`                 | --enable-sasl            |
| ProducerStateTTLMS        | `producer_state_ttl_ms`      | `producer.state.ttl.ms`       | --producer-state-ttl-ms  |
| TransactionalIDExpirationMS | `transactional_id_expiration_ms` | `transactional.id.expiration.ms` | --transactional-id-expiration-ms |
| DiskFlushBatchSize        | `disk_flush_batch_size`      | `disk.flush.batch.size`       | --disk-flush-batch       |
| LingerMS                  | `linger_ms`                  | `linger.ms`                   | --linger-ms              |
| ChannelBufferSize         | `channel_buffer_size`        | `channel.buffer.size`         | --channel-buffer         |
| DiskWriteTimeoutMS        | `disk_write_timeout_ms`      | `disk.write.timeout.ms`       | --disk-write-timeout     |
| PartitionChannelBufSize   | `partition_channel_buffer_size` | `partition.channel.buffer.size` | --partition-ch-buffer |
| ConsumerChannelBufSize    | `consumer_channel_buffer_size` | `consumer.channel.buffer.size` | --consumer-ch-buffer   |
| SegmentSize              | `log_segment_bytes`          | `log.segment.bytes`            | --segment-size         |
| SegmentRollTimeMS        | `log_segment_roll_ms`        | `log.segment.roll.ms`          | --segment-roll-time-ms |
| IndexSize                | `log_index_size_bytes`       | `log.index.size.bytes`         | --index-size           |
| CleanupPolicy            | `log_cleanup_policy`         | `log.cleanup.policy`           | --cleanup-policy       |
| RetentionHours           | `log_retention_hours`        | `log.retention.hours`          | --retention-hours      |
| RetentionBytes           | `log_retention_bytes`        | `log.retention.bytes`          | --retention-bytes      |

# Special Configuration Handling

## TLS Certificate Loading

When TLS is enabled, certificates are loaded during configuration initialization:

```
if cfg.UseTLS && cfg.TLSCertPath != "" && cfg.TLSKeyPath != "" {
    cert, err := tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath)
    if err != nil {
        return nil, err
    }
    cfg.TLSCert = cert
}
```

The loaded certificate is stored in the TLSCert field of the Config struct and used by the server when establishing TLS connections.

## Bootstrap Servers Parsing

The bootstrap_servers field supports comma-separated values in a single string, which are automatically split:

```
if len(cfg.BootstrapServers) == 1 && strings.Contains(cfg.BootstrapServers[0], ",") {
    cfg.BootstrapServers = strings.Split(cfg.BootstrapServers[0], ",")
}
```

This allows configuration like:

```
bootstrap_servers: "broker1:9000,broker2:9000,broker3:9000"
```

## SDK Client Configuration

### Consumer TLS

The Go SDK consumer now supports TLS connections, matching the producer's TLS capabilities. Add the following fields to `ConsumerConfig`:

| Parameter      | Type   | Default | Description                          |
|---------------|--------|---------|--------------------------------------|
| `use_tls`      | bool   | false   | Enable TLS for consumer connections  |
| `tls_cert_path`| string | ""      | Path to TLS certificate file         |
| `tls_key_path` | string | ""      | Path to TLS private key file         |

```yaml
consumer:
  broker_addrs: ["broker1:9000"]
  topic: "orders"
  group_id: "my-group"
  use_tls: true
  tls_cert_path: "certs/client.crt"
  tls_key_path: "certs/client.key"
```

When `use_tls` is enabled, both producer and consumer use `tls.DialWithDialer()` with TLS 1.2 minimum.

### SDK Metrics (Prometheus)

Both `PublisherConfig` and `ConsumerConfig` support an `enable_metrics` field to opt in to Prometheus runtime metrics.

| Parameter       | Type | Default | Description                              |
|----------------|------|---------|------------------------------------------|
| `enable_metrics`| bool | false   | Enable Prometheus runtime metric collection |
| `auto_offset_reset` | string | `earliest` | Missing/out-of-range offset policy: `earliest`, `latest`, or `error` |
| `read_isolation` | string | `read_committed` | Consumer visibility: `read_committed` or `read_uncommitted` |

When enabled, the SDK registers the following metrics in a dedicated Prometheus registry:

**Producer Metrics:**

| Metric                                    | Type      | Labels  | Description                        |
|------------------------------------------|-----------|---------|-------------------------------------|
| `cursus_producer_messages_sent_total`     | Counter   | topic   | Messages successfully sent          |
| `cursus_producer_send_errors_total`       | Counter   | topic   | Send errors                         |
| `cursus_producer_batch_latency_seconds`   | Histogram | topic   | Batch send latency                  |

**Consumer Metrics:**

| Metric                                     | Type      | Labels       | Description                     |
|-------------------------------------------|-----------|--------------|----------------------------------|
| `cursus_consumer_messages_received_total`  | Counter   | topic, group | Messages received                |
| `cursus_consumer_commit_total`             | Counter   | topic, group | Offset commits                   |
| `cursus_consumer_commit_errors_total`      | Counter   | topic, group | Commit errors                    |
| `cursus_consumer_poll_latency_seconds`     | Histogram | topic, group | Poll operation latency           |
| `cursus_consumer_rebalance_total`          | Counter   | topic, group | Rebalance events                 |

To expose metrics via HTTP:

```go
import "github.com/cursus-io/cursus/sdk"

http.Handle("/metrics", sdk.MetricsHandler())
log.Fatal(http.ListenAndServe(":2112", nil))
```

## Configuration Validation

`Config.Normalize()` applies safe fallbacks for invalid or non-positive values, including write batching, sync intervals, segment/index sizes, retention intervals, channel capacities, replica settings, and transaction/producer retention. TLS certificate loading still fails startup when configured files are invalid.

Unsupported `log_cleanup_policy` values, including `compact`, normalize to `delete` with a warning because compaction is not implemented. Operators should treat normalization warnings as configuration errors in production and verify the effective startup configuration.

Missing values fall back to defaults in `pkg/config/properties.go`. Configuration precedence is defaults, file, environment, then CLI overrides where a flag is exposed.