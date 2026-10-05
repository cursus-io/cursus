# Production incident runbook

This runbook covers the alerts shipped by the Helm charts. Preserve the first
failure, broker logs, `/ready` response, metrics snapshot, image digest, and
configuration before restarting anything. Pause deploys and storage changes
until the failure is classified.

## First response

1. Check `/live` and `/ready` on every broker. A live but unready broker is in
   diagnostics-only or admission-blocked state and must not receive clients.
2. Capture `CLUSTER_STATUS`, Pods, PVCs, events, and the Grafana **Cursus
   production overview** dashboard for the incident window.
3. Stop automated rollouts. In a three-member cluster, do not remove or restart
   a second member while one member is unavailable.
4. If writes may be unsafe, drain producers before attempting recovery. Keep
   the original PVC or data directory for forensics.

## Broker not ready

Correlate `cursus_broker_ready` with the other red dashboard panels and the
reason returned by `/ready`. Startup recovery, missing leadership, storage
headroom, and corrupt durable metadata require different actions. Do not loop
restart an unready broker: repeated startup can hide the first useful error and
consume the remaining disk reserve.

If recovery reports durable-data corruption, stop the affected broker. Validate
an immutable copy with:

```bash
cursus-storage backup validate --log-dir /backup/cursus-logs
```

Restore only from one validated backup generation. Never edit journal, segment,
index, manifest, or Raft files in place.

## Storage headroom exhausted

`CursusStorageDiskHeadroomLow` means new writes are being rejected before the
filesystem reaches zero free bytes. Drain producers, identify filesystem
growth, and expand the volume. For the three-member Helm chart, use:

```bash
scripts/expand-helm-cluster-storage.sh RELEASE NAMESPACE NEW_SIZE
```

Wait for every member to become ready and for
`cursus_storage_filesystem_headroom_ready` to return to `1`. Do not delete
segments manually; retention, compaction, indexes, offsets, and transaction
markers form one consistency boundary.

## Request admission saturated

Compare `cursus_broker_requests_inflight`,
`cursus_broker_request_bytes_inflight`, waiters, rejection rate, request
latency, and client retry rate. Reduce or rate-limit ingress first. Look for
oversized batches, stalled clients, and retry storms. Raise
`max_inflight_requests` or `max_inflight_request_bytes` only after measuring
peak broker heap plus filesystem cache and preserving node memory headroom.
Apply a configuration change one broker at a time after the cluster is healthy.

## Writes stalled or responses failing

For `CursusStorageWritesStalled`, check disk latency, pending writes, free
space, and the first storage error. Stop traffic if the queue continues to
grow. For `CursusResponseWriteFailures`, separate client disconnects and
timeouts from storage or replication errors. Clients may retry acknowledged
modes with the same idempotent identity. `acks=0` has no delivery guarantee;
do not infer loss or success from a missing response.

## Replication unsafe or leader unavailable

Stop voluntary restarts and upgrades. Check active brokers, leader, assignments,
ISR, effective minimum ISR, and network/TLS/auth failures. Keep writes drained
when minimum ISR cannot be satisfied. Recover one member and wait for ISR to
converge before touching another. If Raft membership or multiple volumes are
lost, stop the cluster and follow the coordinated restore procedure in
[Upgrade and recovery](upgrade-and-recovery.md). Never combine PVCs from
different backup generations.

## Transaction recovery not ready

Keep transactional producers drained. Capture the oldest active transaction,
state counts, coordinator ownership, and journal validation output. Do not
delete the transaction journal or consumer-offset data to bypass fencing. A
pending offset reservation intentionally blocks stable reads and ordinary
offset commits until the durable commit or abort decision is resolved. Restore
the journal together with topic, offset, event snapshot, and partition data.

## Consumer offset out of range or sustained lag

Verify the topic log start, high watermark, committed next offset, group
generation, and coordinator. An out-of-range commit requires an explicit
application decision to reset or restore retained data; never silently advance
it. For lag, confirm coordinator health and assignment stability before adding
consumers. Compacted-topic holes are expected only when the topic metadata
declares compaction.

## Closing the incident

Confirm all brokers are ready, cluster leader and ISR are stable, filesystem
headroom is restored, request waiters are zero, and recovery metrics are ready.
Publish and consume an acknowledged canary, verify a known payload and
committed offset, and exercise one transaction before returning full traffic.
Record the backup generation, image digest, configuration, root cause, and the
exact validation commands used.
