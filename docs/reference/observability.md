# Broker Observability

## Contract

Cursus exposes health and Prometheus endpoints on HTTP listeners separate from
the client protocol listener.

| Default port | Endpoint | Response | Purpose |
|---|---|---|---|
| `9080` | `GET /live` | JSON | Process liveness |
| `9080` | `GET /ready` | JSON | Broker and dependency readiness |
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
Consumer group member counts are not readiness checks. A registered group with
zero members leaves the broker ready; only broker-owned consumer metadata
recovery participates in readiness.

Standalone response:

```json
{"status":"ready","checks":{"consumer_metadata":"ok","storage":"ok","topic_metadata":"ok"}}
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

Only `/live` and `/ready` are registered. `/health` and `/` return HTTP 404.

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
| `cursus_broker_publish_acknowledgements_total{ack_mode,result}` | Counter | Publish requests by normalized acknowledgement mode and bounded result |
| `cursus_broker_async_replication_failures_total{topic,error_class}` | Counter | Follower failures after an `acks=1` leader acknowledgement |
| `cursus_broker_log_compaction_runs_total{result,reason}` | Counter | Completed, skipped, or failed compaction passes with bounded reasons |
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
| `cursus_topic_metadata_restored_topics` | Topics restored from the durable standalone manifest during startup |
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
| `cursus_storage_segment_cache_entries` | Memory-mapped segment readers retained by open handlers |
| `cursus_storage_segment_cache_hits` | Cache hits accumulated by open handlers |
| `cursus_storage_segment_cache_misses` | Cache misses accumulated by open handlers |
| `cursus_storage_segment_cache_evictions` | Cache evictions accumulated by open handlers |
| `cursus_wire_protocol_failures_total{reason}` | Rejected Wire v2 frames by bounded protocol reason |
| `cursus_wire_decompression_rejections_total{reason}` | Rejected compressed payloads by bounded reason |

`cursus_storage_bytes` covers open topic handlers. It is not filesystem capacity
or free-space telemetry; collect those values with the node or container
runtime exporter.

### Consumer Metadata Recovery

| Metric | Meaning |
|---|---|
| `cursus_consumer_metadata_recovery_ready{phase}` | `1` only after durable group/offset replay completes without error |
| `cursus_consumer_metadata_restored_groups` | Groups materialized during startup |
| `cursus_consumer_metadata_restored_offsets` | Committed `(group,topic,partition)` next-offset keys restored |
| `cursus_consumer_metadata_replayed_records` | Active internal log records scanned |
| `cursus_consumer_metadata_orphan_records` | Records fenced or superseded by authoritative lifecycle/revision state |
| `cursus_consumer_metadata_corrupt_records` | Malformed or inconsistent records that stopped recovery |

In diagnostics-only mode, `/ready` includes the retained `consumer_metadata` failure and `cursus_consumer_metadata_recovery_ready` remains `0`; the client command listener is not opened. Use the restored counts together with the offline [standalone storage recovery inventory](../standalone-storage-recovery.md).

### Consumer Groups

| Metric | Type / unit | Meaning |
|---|---|---|
| `cursus_consumer_group_members{topic,group}` | Gauge / members | Current member count, emitted only by the authoritative coordinator |
| `cursus_consumer_group_state{topic,group,state}` | Gauge / boolean | One-hot authoritative state: `stable` or `empty` |
| `cursus_consumer_group_coordinator_up{topic,group}` | Gauge / boolean | `1` only on the broker that successfully resolves itself as current coordinator; every broker emits `0` or `1` |
| `cursus_consumer_group_last_activity_timestamp_seconds{topic,group}` | Gauge / Unix seconds | Latest accepted heartbeat or group lifecycle activity known by the authoritative coordinator; `0` means unknown, including after standalone durable recovery |
| `cursus_consumer_group_last_rebalance_timestamp_seconds{topic,group}` | Gauge / Unix seconds | Latest completed membership rebalance known by the authoritative coordinator; `0` means unavailable or no rebalance has completed since recovery |
| `cursus_consumer_group_observation_failures_total{topic,group,reason}` | Counter / failures | Per-broker observation failures; reason is one of `coordinator_lookup`, `group_lookup`, or `topic_lookup`, and a series appears only after its first failure |
| `cursus_consumer_group_generation{group,topic}` | Gauge / generation | Current group generation |
| `cursus_consumer_group_assigned_partitions{group,topic}` | Gauge / partitions | Assignments held by active members |
| `cursus_consumer_group_committed_offset{group,topic,partition}` | Gauge / offsets | Durable next offset |
| `cursus_consumer_group_lag{group,topic,partition}` | Gauge / messages | `max(HWM - committedNextOffset, 0)` |
| `cursus_consumer_group_offset_out_of_range{group,topic,partition}` | Gauge / boolean | Commit is below log start or above the high watermark |

In standalone mode the local coordinator is authoritative. In distributed
mode, replicated membership can remain present on a broker that no longer owns
the group, while heartbeat activity is coordinator-local. Such a broker emits
`coordinator_up = 0` and omits the authoritative lifecycle gauges. It does not
present its retained snapshot as healthy.

For a converged three-broker scrape, exactly one target emits authoritative
lifecycle gauges and the sum of `coordinator_up` is one. Gate each `max` by
that exact authority count; an unguarded `max` can retain an old positive value
during an overlapping coordinator view. Use `sum` for the per-broker failure
counters:

```promql
max by (topic, group) (cursus_consumer_group_members)
and on (topic, group)
sum by (topic, group) (cursus_consumer_group_coordinator_up) == 1

max by (topic, group, state) (cursus_consumer_group_state)
and on (topic, group)
sum by (topic, group) (cursus_consumer_group_coordinator_up) == 1

max by (topic, group) (
  cursus_consumer_group_last_activity_timestamp_seconds
)
and on (topic, group)
sum by (topic, group) (cursus_consumer_group_coordinator_up) == 1

max by (topic, group) (
  cursus_consumer_group_last_rebalance_timestamp_seconds
)
and on (topic, group)
sum by (topic, group) (cursus_consumer_group_coordinator_up) == 1

sum by (topic, group) (cursus_consumer_group_coordinator_up)

sum by (topic, group, reason) (
  rate(cursus_consumer_group_observation_failures_total[5m])
)
```

`sum` of an authoritative lifecycle gauge is also exact after coordinator ring
convergence because only one broker emits it. A `coordinator_up` sum of zero
means no scraped target currently claims authority; a value above one reveals
an overlapping ring view. Both cases remove the gated lifecycle value instead
of allowing a stale value to mask a member-count decrease.

Heartbeat and rebalance timestamps are runtime lifecycle observations, not a
durable heartbeat journal. Cluster snapshots can carry them across coordinator
materialization, but standalone metadata recovery intentionally restores no
members and reports `0` until a new lifecycle event occurs. Persisting every
heartbeat would add storage write amplification to the consumer hot path.

An unknown group has no Cursus series. Cursus does not fabricate a zero-valued
group because it does not know which application groups are required. Use an
external cluster-config catalog to fill that missing actual state with zero.
See the [consumer lifecycle alert runbook](../operations/consumer-lifecycle-alerts.md)
for the catalog contract, alert rules, and a `1 -> 0 -> 1` canary procedure.

Lag uses the high watermark rather than the local log end so uncommitted replica
tail data is not reported as consumable work. For an exact-topic group with no
stored offset, the collector applies the broker's earliest default of `0`.
Wildcard groups expose partitions for which broker offset state exists.

Wire v2 exposes only `cursus_consumer_group_lag`; the former
`broker_consumer_lag` alias is not registered.

The in-process Go SDK exposes separate counters for physical anomalies and
expected compacted holes. `cursus_consumer_offset_gap_total{topic,group}`
increments only when a non-compacted topic jumps forward unexpectedly.
`cursus_consumer_compacted_offsets_skipped_total{topic,group}` counts logical
offsets removed by a cleanup policy that includes `compact`.

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
| `cursus_cluster_isr_catchup_proofs_total{outcome,reason}` | ISR catch-up proofs accepted or rejected by bounded reason |
| `cluster_replication_lag_seconds{topic,partition,broker}` | Successful follower acknowledgement latency |

`cluster_replication_lag_seconds` is the only operation-time cluster metric.
Use the `cursus_cluster_*` scrape-time metrics for current topology and health.

### Transaction State

| Metric | Meaning |
|---|---|
| `cursus_transaction_recovery_ready` | Transaction state recovery completed before serving |
| `cursus_transactions{state}` | Transactions retained by coordinator state |
| `cursus_transactions_expired` | Expired transaction identities awaiting replacement or compaction |
| `cursus_transaction_oldest_active_seconds` | Age of the oldest open or committing transaction |

Use these gauges with `cursus_broker_command_errors_total{command,code}` and the
[transaction alert runbook](../operations/transaction-alerts.md).

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

# The converged broker set has no single authoritative group coordinator
sum by (topic, group) (cursus_consumer_group_coordinator_up) != 1

# Sustained consumer backlog
max_over_time(cursus_consumer_group_lag[10m]) > 10000

# Storage writer backlog
cursus_storage_pending_writes > 0

# Compaction errors (inspect the bounded reason label)
increase(cursus_broker_log_compaction_runs_total{result="error"}[10m]) > 0
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
