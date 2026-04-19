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
| `health_check_port`  | int        | 9080           | HTTP port for health check endpoint             |
| `log_dir`            | string     | "broker-logs"  | Directory path for persistent log segments      |
| `enable_exporter`    | bool       | true           | Enable Prometheus metrics exporter              |
| `exporter_port`      | int        | 9100           | HTTP port for Prometheus metrics endpoint       |
| `enable_benchmark`   | bool       | false          | Enable benchmark mode for testing               |
| `cleanup_interval`   | int        | 300            | Deduplication map cleanup interval (seconds)   |


# Security and Compression

| Parameter       | Type   | Default | Description                                  |
|----------------|--------|---------|----------------------------------------------|
| `use_tls`        | bool   | false   | Enable TLS for TCP connections               |
| `tls_cert_path`  | string | ""      | Path to TLS certificate file                 |
| `tls_key_path`  | string | ""      | Path to TLS private key file                 |
| `enable_gzip`    | bool   | false   | Enable gzip compression for messages        |

When `use_tls` is enabled and certificate paths are provided, the broker loads the certificate using `tls.LoadX509KeyPair()` during initialization 

# DiskHandler Performance Tuning

These parameters directly affect the write path performance and batching behavior described in DiskHandler and Write Path.

| Parameter             | Type | Default | Description                                                |
|----------------------|------|---------|------------------------------------------------------------|
| `disk_flush_batch_size` | int  | 50      | Number of messages to batch before flushing to disk       |
| `linger_ms`             | int  | 50      | Maximum time to wait before flushing (milliseconds)       |
| `channel_buffer_size`   | int  | 1024    | Buffer size for DiskHandler's writeCh channel             |
| `disk_write_timeout_ms` | int  | 5       | Timeout for synchronous writes when channel is full       |


Trade-offs:

- Higher `disk_flush_batch_size`: Better throughput, higher latency, more data loss risk on crash
- Lower `linger_ms`: Lower latency, more frequent I/O operations, reduced throughput
- Larger `channel_buffer_size`: Better handling of burst traffic, higher memory usage

# Partition and Consumer Channel Tuning

These parameters control the in-memory channel buffer sizes for message distribution within the topic management system.

| Parameter                     | Type | Default | Description                                      |
|-------------------------------|------|---------|--------------------------------------------------|
| `partition_channel_buffer_size` | int  | 10000   | Buffer size for each Partition's input channel  |
| `consumer_channel_buffer_size`  | int  | 1000    | Buffer size for each Consumer's message channel |

# Broker-Specific Parameters

These parameters are available in the Config struct but are primarily used in client/producer contexts rather than the broker itself:

| Parameter                      | Type      | Default | Description                                        |
|--------------------------------|-----------|---------|----------------------------------------------------|
| `bootstrap_servers`              | []string  | nil     | List of broker addresses for client connections   |
| `acks`                           | string    | ""      | Acknowledgment mode (client-side setting)        |
| `min_insync_replicas`            | int       | 0       | Minimum replicas for writes (future replication support) |
| `buffer_size`                    | int       | 0       | Generic buffer size                               |
| `batch_size`                     | int       | 0       | Batch size for producers                           |
| `max_inflight_requests_per_conn` | int       | 0       | Maximum concurrent requests per connection        |

These parameters are defined in the struct for future extensibility but are not actively used by the current broker implementation.

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
| DiskFlushBatchSize        | `disk_flush_batch_size`      | `disk.flush.batch.size`       | --disk-flush-batch       |
| LingerMS                  | `linger_ms`                  | `linger.ms`                   | --linger-ms              |
| ChannelBufferSize         | `channel_buffer_size`        | `channel.buffer.size`         | --channel-buffer         |
| DiskWriteTimeoutMS        | `disk_write_timeout_ms`      | `disk.write.timeout.ms`       | --disk-write-timeout     |
| PartitionChannelBufSize   | `partition_channel_buffer_size` | `partition.channel.buffer.size` | --partition-ch-buffer |
| ConsumerChannelBufSize    | `consumer_channel_buffer_size` | `consumer.channel.buffer.size` | --consumer-ch-buffer   |

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

The current implementation performs minimal validation during configuration loading. The following validations are implicit:

- **Port numbers**: Must be valid integers
- **Boolean flags**: Must parse as boolean values
- **File paths**: Checked only when TLS is enabled and certificate loading is attempted
- **Numeric parameters**: Must parse as integers

Missing configuration values fall back to defaults defined in `pkg/config/properties.go`