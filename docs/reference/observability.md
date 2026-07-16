# Broker Observability

## Contract

Cursus exposes health and Prometheus endpoints on HTTP listeners separate from
the client protocol listener.

| Default port | Endpoint | Response | Purpose |
|---|---|---|---|
| `9080` | `GET /live` | JSON | Process liveness |
| `9080` | `GET /ready` | JSON | Broker and dependency readiness |
| `9080` | `GET /health` | Plain text | Backward-compatible readiness check |
| `9080` | `GET /` | Plain text | Alias of `/health` |
| `9100` | `GET /metrics` | Prometheus/OpenMetrics | Broker metrics when the exporter is enabled |

Only `GET` and `HEAD` are accepted by health endpoints. Other methods return
`405 Method Not Allowed`.

The HTTP listeners bind before broker readiness is published. A health or
metrics bind failure fails broker startup instead of being logged as a
background-only failure.

## Liveness And Readiness

`/live` returns `200` while the health HTTP server can execute requests. It does
not check topic storage, consumer groups, or cluster quorum.

```json
{"status":"live"}
```

`/ready` returns `200` only after the client listener, command handler, worker
pool, and enabled HTTP services have initialized and all dynamic checks pass.

Standalone response:

```json
{"status":"ready","checks":{"storage":"ok"}}
```

In distributed mode, readiness also requires a resolvable cluster leader. A
broker process can therefore remain live while returning `503` from `/ready`
during election or loss of cluster leadership.

```json
{
  "status": "not_ready",
  "checks": {
    "cluster_leader": "no cluster leader available",
    "storage": "ok"
  }
}
```

`/health` remains compatible with existing container checks:

```text
HTTP 200: OK
HTTP 503: Broker not ready: <failed-check>=<reason>
```

Recommended Kubernetes probes:

```yaml
livenessProbe:
  httpGet:
    path: /live
    port: 9080
readinessProbe:
  httpGet:
    path: /ready
    port: 9080
```

Do not use `/live` for traffic admission. It intentionally remains successful
when the broker cannot safely serve client work.

## Metrics Semantics

Runtime gauges are generated from a point-in-time broker snapshot on every
scrape. They do not retain labels for deleted topics, departed groups, or old
partition leaders.

### Broker Traffic

| Metric | Type | Meaning |
|---|---|---|
| `cursus_broker_client_connections_total` | Counter | Accepted client TCP connections |
| `cursus_broker_client_connections_active` | Gauge | Connections currently handled |
| `cursus_broker_commands_total{command,result}` | Counter | Completed text command dispatches |
| `cursus_broker_command_duration_seconds{command}` | Histogram | Command dispatch latency |
| `cursus_broker_command_errors_total{command,code}` | Counter | Wire errors by bounded command and error code |
| `broker_messages_processed_total` | Counter | Messages accepted by the topic manager |
| `broker_message_latency_seconds` | Histogram | Topic manager publish latency |
| `broker_seqnum_gap_total{topic,partition,producer_id}` | Counter | Detected producer sequence gaps |
| `broker_seqnum_duplicate_total{topic,partition}` | Counter | Detected duplicate producer sequences |

Command duration ends when a streaming command is accepted. It does not include
the lifetime or payload transfer time of a stream.

### Topic And Storage State

| Metric | Meaning |
|---|---|
| `cursus_broker_ready` | Current readiness result (`1` or `0`) |
| `cursus_broker_topics` | Topics loaded on this broker |
| `cursus_broker_partitions` | Partitions loaded on this broker |
| `cursus_partition_log_start_offset{topic,partition}` | Earliest retained offset |
| `cursus_partition_log_end_offset{topic,partition}` | Next allocated offset |
| `cursus_partition_high_watermark{topic,partition}` | Next offset visible to committed readers |
| `cursus_streams_active` | Registered streaming consumers |
| `cursus_storage_handlers` | Open partition storage handlers |
| `cursus_storage_segments` | Segment files represented by open handlers |
| `cursus_storage_bytes` | Segment and offset-index bytes represented by open handlers |
| `cursus_storage_pending_writes` | Messages queued for disk writes |
| `cursus_storage_active_readers` | Current segment readers |
| `cursus_storage_stat_failures` | Files that could not be inspected during the scrape |

`cursus_storage_bytes` covers open topic handlers. It is not filesystem capacity
or free-space telemetry; collect those values with the node or container
runtime exporter.

### Consumer Groups

| Metric | Meaning |
|---|---|
| `cursus_consumer_group_members{group,topic}` | Active group members |
| `cursus_consumer_group_generation{group,topic}` | Current group generation |
| `cursus_consumer_group_assigned_partitions{group,topic}` | Assignments held by active members |
| `cursus_consumer_group_committed_offset{group,topic,partition}` | Durable next offset |
| `cursus_consumer_group_lag{group,topic,partition}` | `max(HWM - committedNextOffset, 0)` |
| `cursus_consumer_group_offset_out_of_range{group,topic,partition}` | Commit is below log start or above the high watermark |

Lag uses the high watermark rather than the local log end so uncommitted replica
tail data is not reported as consumable work. For an exact-topic group with no
stored offset, the collector applies the broker's earliest default of `0`.
Wildcard groups expose partitions for which broker offset state exists.

`broker_consumer_lag{topic,partition,group}` remains as a deprecated alias with
the same scrape-time HWM semantics. New dashboards should use
`cursus_consumer_group_lag`.

### Cluster State

| Metric | Meaning |
|---|---|
| `cursus_distribution_enabled` | Distributed mode is enabled |
| `cursus_cluster_brokers` | Brokers in replicated metadata |
| `cursus_cluster_has_leader` | This broker resolves a cluster leader |
| `cursus_cluster_is_leader` | This broker is the current cluster leader |
| `cursus_cluster_offline_partitions` | Partitions without a leader assignment |
| `cursus_cluster_under_replicated_partitions` | Partitions where ISR size is below replica count |
| `cursus_cluster_partition_replicas{topic,partition}` | Configured replicas |
| `cursus_cluster_partition_in_sync_replicas{topic,partition}` | Current ISR size |
| `cursus_cluster_partition_leader_epoch{topic,partition}` | Current leader epoch |
| `cursus_cluster_partition_leader{topic,partition,broker_id}` | Current leader identity (`1`) |
| `cluster_replication_lag_seconds{topic,partition,broker}` | Successful follower acknowledgement latency |
| `cluster_quorum_operations_total{operation,result}` | Instrumented quorum operation results |

The historical `cluster_*` vectors that are not listed above are compatibility
surfaces and may have no series unless their corresponding operation occurs.
Use the `cursus_cluster_*` scrape-time metrics for current topology and health.

### Transaction State

Transaction-state gauges are not exported yet. Operators can inspect a known
transaction with `TXN_STATUS`, and command failures are counted by
`cursus_broker_command_errors_total{command,code}`. Until bounded transaction
state, recovery, and age metrics are added, alerting cannot directly detect a
transaction that remains in `committing`; recovery logs and command errors are
the current operational signals.

## Configuration

```yaml
health_check_port: 9080
enable_exporter: true
exporter_port: 9100
```

Equivalent environment variables are `HEALTH_CHECK_PORT`, `ENABLE_EXPORTER`,
and `EXPORTER_PORT`.

Prometheus example:

```yaml
scrape_configs:
  - job_name: cursus
    scrape_interval: 15s
    static_configs:
      - targets: ["broker-1:9100", "broker-2:9100", "broker-3:9100"]
```

## Alert Baseline

```promql
# Scrape target unavailable
up{job="cursus"} == 0

# Process is reachable but cannot serve client work
cursus_broker_ready == 0

# No cluster leader
cursus_distribution_enabled == 1 and cursus_cluster_has_leader == 0

# Replication safety degraded
cursus_cluster_under_replicated_partitions > 0

# Group commit no longer points into retained data
cursus_consumer_group_offset_out_of_range == 1

# Sustained consumer backlog
max_over_time(cursus_consumer_group_lag[10m]) > 10000

# Storage writer backlog
cursus_storage_pending_writes > 0
```

Tune lag and pending-write thresholds to topic throughput and retention. A
single non-zero sample can be normal during bursts.

## Security Boundary

Health and metrics endpoints do not perform client authentication. Bind and
publish these ports only on a trusted operations network, or place them behind
an authenticated reverse proxy. Metrics include topic, consumer group, and
broker identifiers.

Do not expose the metrics listener as a public application endpoint.

## Validation

```bash
curl -fsS http://localhost:9080/live
curl -fsS http://localhost:9080/ready
curl -fsS http://localhost:9100/metrics | grep '^cursus_'
```

The compatibility probe remains:

```bash
curl -fsS http://localhost:9080/health
```
