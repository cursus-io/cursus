# Monitoring and Observability

## Purpose and Scope

This document describes the monitoring and observability capabilities built into cursus, including health check endpoints, Prometheus metrics, and structured logging. 

These features enable operators to monitor broker health, track performance metrics, and diagnose issues in production environments.

## Overview

cursus provides three primary observability mechanisms:

| Component            | Port       | Protocol          | Purpose                                                      |
|---------------------|-----------|-----------------|--------------------------------------------------------------|
| Health Check Server  | 9080      | HTTP             | Load balancer health checks, liveness probes                |
| Metrics Exporter     | 9100      | HTTP (Prometheus)| Performance metrics, resource utilization                   |
| Structured Logging   | N/A       | stdout/stderr    | Request tracing, command auditing, error diagnostics        |

**Note:** All components can be independently configured and integrate with standard monitoring infrastructure (Prometheus, Grafana, ELK stack, etc.).

## Health Check System

### Implementation

The health check system is implemented as a lightweight HTTP server running on a separate port from the main broker listener. This separation ensures health checks don't compete with broker traffic.

### Key Components:

- **`startHealthCheckServer()`**: Initializes HTTP server
- **brokerReady** : Atomic boolean flag tracking broker readiness
- **Health Handler** : HTTP request handler

## Readiness Flow

### Endpoints

| Endpoint | Method | Response Codes | Response Body                                               |
|----------|--------|----------------|-------------------------------------------------------------|
| /health  | GET    | 200, 503        | "OK" or "Broker not ready: Main listener not active"       |
| /        | GET    | 200, 503        | Same as /health                                            |

**Note:** Both endpoints return the same health status. The root endpoint(`/`) allows simple `curl` commands without specifying a path.

### HTTP Status Codes:

- **200 OK**: Broker is ready and accepting connections on port 9000
- **503 Service Unavailable**: Main TCP listener has not started successfully

### Configuration


# manifests/config.yaml
```
broker:
  port: 9000
  # Health check configuration
  # Defaults to 9080 if not specified
```

The health check port is configured via:
- **YAML/JSON config file**: `health_check_port` field
- **Command-line flag**: --health-port 
- **Default value**: 9080 

## Prometheus Metrics System

### Architecture

The Prometheus metrics exporter runs as a separate HTTP server on port 9100, exposing metrics in the standard Prometheus text format. The exporter can be toggled via configuration.

### Metric Categories

While the pkg/metrics package implementation isn't shown in the provided files, the architecture suggests these metric categories based on the system design:

| Category          | Metric Examples                                  | Description                        |
|------------------|-------------------------------------------------|------------------------------------|
| Topic Metrics     | Topics created, deleted, active topics         | Topic lifecycle tracking            |
| Partition Metrics | Messages per partition, partition queue depth  | Partition-level performance         |
| Disk Metrics      | Bytes written, flush operations, segment rotations | Storage subsystem performance       |
| Consumer Metrics  | Active consumers, messages delivered           | Consumer tracking                   |
| Server Metrics    | Active connections, commands processed         | Server-level operations             |

The TopicManager includes a metrics field [pkg/topic] for tracking deduplication and message flow.

### Configuration

manifests/config.yaml
```
broker:
  enable_exporter: true
  exporter_port: 9100

prometheus:
  scrape_interval: "5s"
  scrape_timeout: "3s"
```

### Configuration Options:

| Parameter       | Type    | Default | Description                             |
|-----------------|--------|---------|-----------------------------------------|
| enable_exporter | boolean | true    | Enable/disable Prometheus exporter      |
| exporter_port   | integer | 9100    | Port for metrics HTTP server            |

**Command-line Flags:**

- `--exporter`: Enable exporter (boolean)
- `--exporter-port`: Port number (integer)

### Prometheus Integration

To scrape cursus metrics, configure Prometheus with:


prometheus.yml
```
scrape_configs:
  - job_name: 'cursus'
    scrape_interval: 5s
    scrape_timeout: 3s
    static_configs:
      - targets: ['localhost:9100']
```

For Docker deployments, use the service name:

```
static_configs:
  - targets: ['cursus:9100']
```

## Structured Logging

### Logging Strategy

cursus implements structured logging throughout the codebase using Go's standard log package with prefixed message categories. All logs are written to `stdout/stderr` for compatibility with container log aggregation systems.

### Log Format Examples

Request Logging 
```
[REQ] [192.168.1.10:52341] Received request. Topic: 'orders', Payload: 'CREATE orders 4'
```

Command Result Logging 

```
[CMD] SUCCESS | Command: [CREATE orders 4] | Response: ✅ Topic 'orders' now has 4 partitions
[CMD] FAILURE | Command: [DELETE nonexistent] | Response: ERROR: topic 'nonexistent' not found
```

Streaming Logging 

```
[STREAM] Completed streaming 342 messages for command [CONSUME orders 0 0]
```

Error Logging 

```
[CONSUME_ERR] Error streaming data for command [CONSUME orders 0 0]: failed to read messages from disk
```

Operational Logging 

```
📈 Prometheus exporter started on port 9100
🧩 Broker listening on :9000 (TLS=false, Gzip=false)
🩺 Health check endpoint started on port 9080
```

Log Categories Detail

| Prefix         | Source                  | Level | Description                           |
|----------------|------------------------|-------|---------------------------------------|
| `[REQ]`        | HandleConnection        | INFO  | Incoming request metadata             |
| `[INPUT_WARN]` | HandleConnection        | WARN  | Malformed input                        |
| `[CMD]`        | CommandHandler          | INFO  | Command execution results (SUCCESS/FAILURE) |
| `[STREAM]`     | HandleConsumeCommand    | INFO  | Message streaming completion status    |
| `[CONSUME_ERR]`| HandleConsumeCommand    | ERROR | Errors during message consumption      |

## Client Address Tracking

All connection-related logs include the remote client address for request tracing:

```
clientAddr := conn.RemoteAddr().String()
log.Printf("[%s] Received request. Topic: '%s', Payload: '%s'", 
    clientAddr, topicName, payload)
```

This enables:
- Correlating multiple requests from the same client
- Identifying problematic clients
- Request-level tracing for debugging

## Configuration Reference

Complete Configuration Example

manifests/config.yaml
```
broker:
  # Network Ports
  port: 9000                    # Main broker TCP port
  health_check_port: 9080       # Health check HTTP port (optional, defaults to 9080)
  
  # Observability
  enable_exporter: true         # Enable Prometheus metrics
  exporter_port: 9100          # Prometheus exporter port
  
  # System Configuration
  cleanup_interval: 60         # Deduplication cleanup interval (seconds)
  use_tls: false               # TLS encryption
  enable_gzip: false           # Message compression
  
  # Performance Tuning
  channel_buffer_size: 10000   # Partition channel buffer
  disk_flush_batch_size: 500   # Messages per disk flush
  linger_ms: 100               # Max wait before flush (ms)

# Prometheus Scrape Configuration
prometheus:
  scrape_interval: "5s"
  scrape_timeout: "3s"
```

## Configuration Loading Priority

Priority Order (highest to lowest):
- Command-line flags (--exporter-port=9100)
- Config file values (via CONFIG_PATH env var or --config flag)
- Built-in defaults

### Docker Deployment Configuration

manifests/docker-compose.yml
```
services:
  broker:
    ports:
      - "9000:9000"   # Main broker port
      - "9100:9100"   # Metrics exporter
      - "9080:9080"   # Health check
    environment:
      - CONFIG_PATH=/root/config.yaml
    volumes:
      - ./config.yaml:/root/config.yaml
```

All three ports must be exposed for full observability:

- 9000: Client connections (publishers/consumers)
- 9080: Health checks (load balancers, Kubernetes probes)
- 9100: Metrics (Prometheus scraping)

## Monitoring Best Practices

### Health Check Integration

Load Balancer Configuration:

Configure load balancers to use the health endpoint for backend health checks:

```
Health Check URL: http://broker:9080/health
Expected Status: 200
Check Interval: 10s
Timeout: 3s
Unhealthy Threshold: 3
Kubernetes Liveness Probe:

livenessProbe:
  httpGet:
    path: /health
    port: 9080
  initialDelaySeconds: 10
  periodSeconds: 10
  timeoutSeconds: 3
  failureThreshold: 3
```

### Metrics Collection

Recommended Prometheus Alerts:

| Alert           | Condition                                | Severity  | Description                     |
|-----------------|------------------------------------------|-----------|---------------------------------|
| BrokerDown      | `up{job="cursus"} == 0`                 | Critical  | Broker unreachable             |
| HighDiskUsage   | Disk segment count growing rapidly       | Warning   | May need compaction            |
| ConsumerLag     | Consumer offset far behind latest        | Warning   | Processing bottleneck          |
| HighErrorRate   | Command failure rate > 5%                | Warning   | Application issues             |

### Log Aggregation

Recommended Setup:

- **Docker Logs**: Use `docker logs -f cursus` for development
- **Production**: Ship logs to centralized system (ELK, Splunk, Datadog)
- **Retention**: Keep at least 7 days of logs for incident investigation
- **Parsing**: Index logs by category prefix ([CMD], [REQ], etc.)

### Example Filebeat Configuration:

```
filebeat.inputs:
  - type: container
    paths:
      - '/var/lib/docker/containers/*/*.log'
    processors:
      - add_docker_metadata: ~
      - decode_json_fields:
          fields: ["message"]
          target: "json"

output.elasticsearch:
  hosts: ["elasticsearch:9200"]
  index: "cursus-logs-%{+yyyy.MM.dd}"
```

## Troubleshooting

### Health Check Issues

Symptom: Health check returns 503

Diagnosis:

```
curl http://localhost:9080/health
# Response: "Broker not ready: Main listener not active"
```

Possible Causes:
- Main listener failed to bind to port 9000 (port already in use)
- Network configuration issues
- Broker crashed after health server started

Resolution:

- Check broker logs for TCP listener errors
- Verify port 9000 is available: `netstat -an | grep 9000`
- Check firewall rules

### Metrics Not Available

Symptom: Prometheus cannot scrape metrics

Diagnosis:

```
curl http://localhost:9100/metrics
# Connection refused or timeout
```

Possible Causes:

- Exporter disabled in configuration
- Port 9100 not accessible
- Metrics server failed to start

Resolution:

- Verify enable_exporter: true in config
- Check Docker port mapping: `docker ps`
- Verify metrics server startup in logs: `📈 Prometheus exporter started`

### Missing Logs

Symptom: Expected log messages not appearing

Possible Causes:

- Log verbosity insufficient
- Docker logging driver not configured
- Log aggregator not collecting from container

Resolution:

```
# View container logs directly
docker logs -f cursus

# Check Docker logging driver
docker inspect cursus | grep LogConfig

# Verify log output
docker exec cursus ls -la /proc/1/fd/1
```

## SDK Client Metrics

In addition to broker-side metrics, the Go SDK provides built-in Prometheus metrics for producer and consumer clients. These are opt-in via the `enable_metrics` configuration field.

### Enabling SDK Metrics

```go
cfg := sdk.NewDefaultPublisherConfig()
cfg.EnableMetrics = true

// Expose metrics endpoint
http.Handle("/metrics", sdk.MetricsHandler())
go http.ListenAndServe(":2112", nil)
```

### Available SDK Metrics

**Producer:**

| Metric | Type | Description |
|--------|------|-------------|
| `cursus_producer_messages_sent_total` | Counter | Total messages successfully acknowledged |
| `cursus_producer_send_errors_total` | Counter | Total send failures (network errors, partial failures) |
| `cursus_producer_batch_latency_seconds` | Histogram | End-to-end batch send latency |

**Consumer:**

| Metric | Type | Description |
|--------|------|-------------|
| `cursus_consumer_messages_received_total` | Counter | Total messages received (polling + streaming) |
| `cursus_consumer_commit_total` | Counter | Successful offset commits |
| `cursus_consumer_commit_errors_total` | Counter | Failed offset commit attempts |
| `cursus_consumer_poll_latency_seconds` | Histogram | Poll round-trip latency |
| `cursus_consumer_rebalance_total` | Counter | Consumer group rebalance events |

### Prometheus Scrape Configuration for SDK

```yaml
scrape_configs:
  - job_name: 'cursus-sdk'
    scrape_interval: 10s
    static_configs:
      - targets: ['app-host:2112']
```

### Recommended Alerts for SDK Metrics

| Alert | Condition | Severity |
|-------|-----------|----------|
| HighSendErrorRate | `rate(cursus_producer_send_errors_total[5m]) > 0.1` | Warning |
| ConsumerLag | `rate(cursus_consumer_messages_received_total[5m]) == 0` | Warning |
| FrequentRebalances | `rate(cursus_consumer_rebalance_total[5m]) > 0.5` | Warning |
| HighCommitFailure | `rate(cursus_consumer_commit_errors_total[5m]) > 0.05` | Warning |

# Summary

cursus provides comprehensive observability through three independent systems:

- **Health Checks** (:9080): Binary ready/not-ready status for load balancers
- **Prometheus Metrics** (:9100): Time-series performance data for monitoring
- **Structured Logs** (stdout): Request tracing and diagnostic information

All three systems are production-ready, configurable, and integrate with standard monitoring infrastructure. The separation of concerns (different ports, different protocols) ensures that monitoring operations don't interfere with broker functionality.
