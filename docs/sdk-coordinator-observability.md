# Go SDK consumer coordinator observability

## Scope and current routing behavior

The Go consumer resolves a group coordinator with `FIND_COORDINATOR` through
`ConsumerClient.ConnectWithFailoverContext`. That bootstrap connection may be
made to any configured broker. A successful response supplies the broker
address used by group commands; the bootstrap or Kubernetes ClusterIP is not
the coordinator route after successful discovery.

The legacy consumer uses the coordinator route for `JOIN_GROUP`, `SYNC_GROUP`,
`HEARTBEAT`, `FETCH_OFFSET`, `COMMIT_OFFSET`, `BATCH_COMMIT`, and
`LEAVE_GROUP`. Heartbeat and batched commit each reuse their own connection;
the remaining legacy paths normally open a bounded request connection. The
cooperative consumer also keeps separate membership and operation connections.
`GROUP_STATUS` is not emitted by the production Go consumer. It appears in
broker-facing integration fixtures, so a broker-side `GROUP_STATUS`
`NOT_COORDINATOR` series cannot by itself be attributed to this SDK.

Before this change, `NOT_COORDINATOR` with `host` and `port` overwrote
`Consumer.coordinatorAddr`, including when the returned address was identical
to the stale address. An address-less response left the stale cache untouched.
The surrounding command paths had different bounds: offset fetch used five
attempts, group join used 30 attempts with backoff, batch commit had its
existing bounded transport retry and commit queue, and heartbeat retried on its
normal periodic lifecycle. This made a same-address or address-less redirect
capable of recurring without evidence that the cache ever converged.

The cache contract is now:

- Initial startup resolves before joining. A group-command redirect is not
  expected when that lookup succeeds and ownership stays stable.
- A redirect to a different usable address atomically refreshes the cache. The
  next existing command attempt is classified as `retry`; no extra discovery
  request is added.
- A redirect to the same address, or one without a usable address, invalidates
  the cache. The next existing bounded attempt performs `FIND_COORDINATOR`.
- A late redirect from an already superseded connection cannot roll the cache
  back to its old address.
- A transport reconnect first reuses the cached address. A failed dial causes
  invalidation and rediscovery. Reconnect does not clear a healthy cache.
- The first successful command on a `retry` or `reconnect` route promotes that
  route to the normal `cache` state.

`CoordinatorAddr` remains an optional initial/pinned deployment override, but
a broker redirect can replace it. Production Kubernetes brokers advertise
their stable StatefulSet DNS names, so a ClusterIP is suitable for bootstrap
only. If a deployment advertises a load-balanced ClusterIP as every broker's
identity, direct coordinator routing is impossible and repeated redirects are
expected until the advertised-address configuration is corrected.

## Apache Kafka comparison

The comparison baseline is Apache Kafka 4.1.0, using the versioned
[`AbstractCoordinator`](https://github.com/apache/kafka/blob/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java),
[`ConsumerCoordinator`](https://github.com/apache/kafka/blob/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java),
and the [4.1 protocol error table](https://kafka.apache.org/41/design/protocol/#protocol_error_codes).
Kafka treats `NOT_COORDINATOR` as retriable, marks the coordinator unknown for
heartbeat, sync, offset fetch, and offset commit responses, and performs
rediscovery inside the caller's timer and retry backoff.

Cursus adopts the safety property that an invalid coordinator is not kept as a
valid cache entry and that retries remain deadline/budget bounded. It differs
by retaining Cursus Wire v2's redirect-address optimization: a different
broker address in the structured error can refresh the cache without a second
lookup. This avoids extra recovery work while preserving rediscovery for
missing or self-referential redirects. No wire protocol or public method
signature changed.

Recovery work is bounded by the existing command budget. A usable redirect
costs one failed command response, one replacement connection, and one command
retry. A missing or same-address redirect additionally costs one framed
`FIND_COORDINATOR` request/response on the next existing attempt. The exact
byte count depends on group-name length, Wire v2 framing, authentication, and
compression; the change does not add background polling, payload reads, or
unbounded recovery requests. Recovery bytes therefore grow with at most that
single bounded lookup per invalidated attempt, not with topic data volume.

## Metrics

All metrics use the SDK-owned Prometheus registry returned by `MetricsHandler`.
Prometheus target labels such as `job`, `service`, `namespace`, and `pod`
identify the application deployment and are intentionally not duplicated as
SDK labels.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `cursus_consumer_coordinator_commands_total` | Counter | `command`, `client_role`, `route_source`, `result` | Completed coordinator command attempts, including success and failure. This is the denominator for redirect ratios. |
| `cursus_consumer_coordinator_redirects_total` | Counter | `command`, `client_role`, `route_source` | Structured `NOT_COORDINATOR` responses. |
| `cursus_consumer_coordinator_resolution_total` | Counter | `result`, `reason`, `client_role` | Coordinator lookup or classified reconnect outcomes. |
| `cursus_consumer_coordinator_cache_refresh_total` | Counter | `result`, `reason`, `client_role` | Cache refresh outcomes. A missing/same-address redirect is counted when its subsequent lookup succeeds or fails. |
| `cursus_consumer_coordinator_retry_total` | Counter | `command`, `result`, `client_role` | Outcome of commands sent on a route learned from a redirect. |
| `cursus_consumer_coordinator_resolution_duration_seconds` | Histogram | `result`, `reason`, `client_role` | Lookup or reconnect latency. |

Bounded `command` values are `join_group`, `sync_group`, `leave_group`,
`heartbeat`, `fetch_offset`, `commit_offset`, `batch_commit`, `group_status`,
and `other`. `route_source` is one of `bootstrap`, `cache`, `retry`, or
`reconnect`. `result` is `success` or `failure`; `reason` is `initial`,
`redirect`, `reconnect`, or `cache_miss`.

`ConsumerConfig.ClientRole` is optional and defaults to `consumer`. Its public
bounded values are `consumer`, `cooperative_consumer`, `processor`, `saga`,
and `observer`; configuration rejects other values. Use the scrape target's
`job`/`service` label for the application and `client_role` only when one
process runs multiple bounded roles.

Remote IP/port, broker address, client ID, request ID, payload, token, group,
member, topic, and retry number are not Prometheus labels. There is no
per-client cache-valid gauge because multiple clients sharing one process and
role would overwrite one another; the refresh, retry, and command counters
retain correct aggregate semantics.

## PromQL

Application and role redirect rate:

```promql
sum by (job, service, client_role) (
  rate(cursus_consumer_coordinator_redirects_total[5m])
)
```

Command redirect rate:

```promql
sum by (job, service, client_role, command) (
  rate(cursus_consumer_coordinator_redirects_total[5m])
)
```

Redirect ratio over all completed coordinator commands:

```promql
sum by (job, service, client_role) (
  rate(cursus_consumer_coordinator_redirects_total[5m])
)
/
clamp_min(
  sum by (job, service, client_role) (
    rate(cursus_consumer_coordinator_commands_total[5m])
  ),
  1e-9
)
```

Share of cache-routed commands that redirect:

```promql
sum by (job, service, client_role) (
  rate(cursus_consumer_coordinator_redirects_total{route_source="cache"}[5m])
)
/
clamp_min(
  sum by (job, service, client_role) (
    rate(cursus_consumer_coordinator_commands_total{route_source="cache"}[5m])
  ),
  1e-9
)
```

Redirect-triggered resolution success ratio:

```promql
sum by (job, service, client_role) (
  rate(cursus_consumer_coordinator_resolution_total{reason="redirect",result="success"}[5m])
)
/
clamp_min(
  sum by (job, service, client_role) (
    rate(cursus_consumer_coordinator_resolution_total{reason="redirect"}[5m])
  ),
  1e-9
)
```

Outcome of commands retried after a redirect:

```promql
sum by (job, service, client_role, command, result) (
  rate(cursus_consumer_coordinator_retry_total[5m])
)
```

Reconnect activity and redirects after reconnect:

```promql
sum by (job, service, client_role, result) (
  rate(cursus_consumer_coordinator_resolution_total{reason="reconnect"}[5m])
)

sum by (job, service, client_role, command) (
  rate(cursus_consumer_coordinator_redirects_total{route_source="reconnect"}[5m])
)
```

An alert should combine a sustained ratio, a minimum coordinator command rate,
and a `for` duration. Do not ship an arbitrary absolute redirects-per-second
default. Measure the deployment baseline first, then substitute its reviewed
`ratio_threshold`, `minimum_request_rate`, and duration into both conditions:

```promql
(
  sum by (job, service, client_role) (rate(cursus_consumer_coordinator_redirects_total[5m]))
  /
  clamp_min(sum by (job, service, client_role) (rate(cursus_consumer_coordinator_commands_total[5m])), 1e-9)
) > ratio_threshold
and
sum by (job, service, client_role) (rate(cursus_consumer_coordinator_commands_total[5m])) > minimum_request_rate
```

## Interpretation and structured logs

A redirect is a routing correction, not proof that publish, consume, or commit
ultimately failed. A small burst during coordinator failover can be healthy if
the cache-refresh or resolution success counter increases once, retry success
follows, and the redirect ratio returns to baseline. Redirects that continue on
`route_source="cache"`, repeated retry failures, or repeated same-address
rediscovery after traffic has stabilized indicate stale or unusable advertised
routing.

The previously observed aggregate of about 4 redirects/s cannot be classified
as normal initial discovery from the broker counter alone. Successful Go SDK
startup resolves before its first group command, and the production Go SDK does
not send `GROUP_STATUS`. A sustained warm-state rate across `FETCH_OFFSET`,
`GROUP_STATUS`, and `HEARTBEAT` therefore points to multiple applications, an
older/custom client, a pinned stale address, or an advertised address that
load-balances back to non-coordinators. The new SDK metrics can prove or exclude
this Go process by `job`/`service`, `client_role`, command, and route source.

Rate-limited key/value logs supplement metrics with `event`, `command`,
`client_role`, `route_source`, `reason`, `result`, `group`, `topic`, `member`,
`old_coordinator`, `new_coordinator`, `same_coordinator`, `retry_attempt`, and
`retry_limit`. These fields are for targeted diagnosis only. Logs never include
credentials, tokens, payloads, request IDs, or TLS material.
