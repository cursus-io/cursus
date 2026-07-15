# Cursus Wire Protocol Specification

> Version: 1.0
> Target audience: SDK implementors (C++, Java, Python, Go)

---

## 1. Transport Layer

### TCP Connection

| Property | Value |
|----------|-------|
| Transport | TCP (IPv4/IPv6) |
| Default port | 9000 (configurable) |
| TLS | Optional, TLS 1.2+ required when enabled |
| Health check | HTTP GET `/health` on port 9080 (configurable) |
| Max concurrent connections | 1000 |
| Idle timeout | 5 seconds (server re-reads on timeout, does NOT disconnect) |

### Connection Lifecycle

```
Client              Broker
  |--- TCP connect --->|
  |                    | (worker assigned)
  |--- command/batch ->| (length-prefixed frame)
  |<-- response -------|
  |--- command ------->|
  |<-- response -------|
  |    ...             |
  |--- EXIT ---------->| (or close socket)
```

A single TCP connection handles all commands sequentially. For parallel operations, open multiple connections.

---

## 2. Framing

Every message (request and response) uses a **4-byte Big Endian length prefix**.

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                    Body Length (uint32 BE)                     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
|                     Body (N bytes)                            |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- **Max body size**: 64 MB (`67,108,864` bytes)
- **Byte order**: Big Endian (network order) throughout the protocol
- Length field does NOT include itself (only body size)

### Compression

Body may be compressed before framing. Algorithm is configured per-broker (not negotiated per-connection).

| Algorithm | ID |
|-----------|----|
| none | `""` or `"none"` |
| gzip | `"gzip"` |
| snappy | `"snappy"` (xerial format) |
| lz4 | `"lz4"` |

```
Send:  body → compress(body) → [4-byte len][compressed body]
Recv:  [4-byte len][compressed body] → decompress → body
```

---

## 3. Message Types

The broker distinguishes two message types by inspecting the first 2 bytes of the body:

| Magic | Type | Format |
|-------|------|--------|
| `0xBA 0x7C` | Binary batch | Structured binary (Section 5) |
| Other | Text command | UTF-8 string (Section 4) |

---

## 4. Text Commands

### Encoding

Text commands use an optional topic+payload envelope:

```
 0                   1
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      Topic Length (uint16 BE) |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|        Topic (T bytes)        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     Command Payload (UTF-8)   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

Alternatively, raw command strings (without the topic envelope) are accepted if they match a known command keyword.

### Parameter Parsing Rules

- Parameters use `key=value` syntax, separated by whitespace
- **Parameter order is flexible** — the broker parses key=value pairs regardless of order
- The `message=` parameter is special: it captures the entire remainder of the line
- Command names are case-insensitive

### Text Response Contract

Text command responses are machine-readable.

- Success responses MUST be `OK` or `OK key=value ...` unless the command returns a documented JSON envelope with `"status":"OK"`.
- Failure responses MUST be `ERROR: <code> [key=value ...]`.
- Clients SHOULD branch on the `OK` / JSON status / `ERROR:` contract instead of matching natural-language phrases.
- Legacy bare responses, such as the old CREATE phrase or plain integer offsets, are deprecated and should only be accepted by clients as narrow backward-compatible fallbacks.

### Command Reference

#### Topic Management

**CREATE**
```
CREATE topic=<name> [partitions=<N>] [idempotent=<true|false>] [replication_factor=<N>] [retention_hours=<N>] [retention_bytes=<N>] [partitioner=<hash_key|round_robin>] [auth_policy=<open|deny_write|deny_read|acl>] [read_acl=<principal[,principal]>] [write_acl=<principal[,principal]>]
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| partitions | No | 4 | Number of partitions |
| idempotent | No | false | Enable producer dedup |
| retention_hours | No | 0 | Per-topic retention hours override metadata; 0 means broker default |
| retention_bytes | No | 0 | Per-topic retention bytes override metadata; 0 means broker default |
| partitioner | No | hash_key | `hash_key` uses message key hash, `round_robin` ignores keys |
| auth_policy | No | open | `open`, `deny_write`, `deny_read`, or `acl` topic policy |
| read_acl | No | - | Comma-separated principals allowed to read when `auth_policy=acl`; `*` allows any authenticated principal |
| write_acl | No | - | Comma-separated principals allowed to write when `auth_policy=acl`; `*` allows any authenticated principal |
| replication_factor | No | 3 | Replica count (distributed mode) |

Response: `OK topic=<name> partitions=<N> partitioner=<hash_key|round_robin> auth_policy=<open|deny_write|deny_read|acl> read_acl=<csv> write_acl=<csv> retention_hours=<N> retention_bytes=<N>`

**DELETE**
```
DELETE topic=<name>
```
Response: `OK topic=<name> deleted=true`

**LIST**
```
LIST
```
Response: `OK count=<N> topics=<comma-separated-topic-names>`. Empty brokers return `OK count=0 topics=`.

**HELP**
```
HELP
```
Response: `OK commands=<comma-separated-command-names>`.

**DESCRIBE**
```
DESCRIBE topic=<name>
```
Response (JSON):
```json
{
  "status": "OK",
  "topic": "mytopic",
  "partitions": [
    {
      "id": 0,
      "leader": "broker-1:9000",
      "replicas": ["broker-1", "broker-2"],
      "isr": ["broker-1", "broker-2"],
      "leo": 1024,
      "hwm": 1020
    }
  ]
}
```

#### Publishing

**PUBLISH**
```
PUBLISH topic=<name> acks=<0|1|-1|all> producerId=<id> [partition=<N>] [seqNum=<N>] [epoch=<N>] [isIdempotent=<true|false>] message=<text>
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Target topic |
| acks | No | 1 | Durability level |
| producerId | Yes | - | Unique producer ID |
| message | Yes | - | Message payload (captures rest of line) |
| partition | No | round-robin/key policy | Explicit target partition for text PUBLISH. Idempotent producers should use per-partition sequence numbers. |
| seqNum | No | 0 | Sequence number (for idempotent mode) |
| epoch | No | 0 | Producer epoch |
| isIdempotent | No | false | Enable dedup for this message |

Because `message=` captures the rest of the line, optional parameters such as `partition`, `seqNum`, `epoch`, and `isIdempotent` must appear before `message=` in text commands.

Transaction metadata fields such as `transactional_id`, `transaction_state`, and `transaction_marker` are broker-internal on `PUBLISH`. Clients must use `INIT_PRODUCER_ID`, `BEGIN_TXN`, `TXN_PUBLISH`, and `END_TXN`; direct metadata injection is rejected with `ERROR: transaction_metadata_forbidden command=PUBLISH`.

Response (JSON — `AckResponse`):
```json
{
  "status": "OK",
  "last_offset": 42,
  "producer_id": "producer-1",
  "producer_epoch": 0,
  "seq_start": 1,
  "seq_end": 1,
  "leader": "broker-1:9000"
}
```

**Acks Semantics**:

| Value | Behavior |
|-------|----------|
| `0` | Fire-and-forget. Broker returns `OK` immediately |
| `1` | Leader writes to local disk, then responds |
| `-1` / `all` | Leader waits for `min_in_sync_replicas` acknowledgments |

#### Cluster Discovery

**FIND_COORDINATOR**
```
FIND_COORDINATOR group=<name>
```
| Param | Required | Description |
|-------|----------|-------------|
| group | Yes | Consumer group name |

Response: `OK coordinator_id=<broker_id> host=<host> port=<port>`

Any broker can answer this command. The coordinator is determined by consistent hashing of the group name across active brokers.

> In cluster mode, group commands (`JOIN_GROUP`, `SYNC_GROUP`, `LEAVE_GROUP`, `HEARTBEAT`, `COMMIT_OFFSET`, `FETCH_OFFSET`) must be sent to the coordinator. If sent to a non-coordinator broker, the response will be:
> `ERROR: NOT_COORDINATOR host=<coordinator_host> port=<coordinator_port>`

**METADATA**
```
METADATA topic=<name>
```
| Param | Required | Description |
|-------|----------|-------------|
| topic | Yes | Topic name |

Response: `OK topic=<name> partitions=<N> leaders=<host:port>,<host:port>,...`

Returns the client-facing address of each partition's leader broker, in partition order (P0, P1, P2, ...).

Any broker can answer this command. Addresses are the advertised client addresses from the FSM broker registry.

> In cluster mode, `CONSUME` and `STREAM` should be sent to the partition leader. If sent to a non-leader broker, the response will be:
> `ERROR: NOT_LEADER LEADER_IS <host:port>`

#### Consumer Group Coordination

**JOIN_GROUP**
```
JOIN_GROUP topic=<name> group=<name> member=<id>
```
Response: `OK generation=<N> member=<actual-id> assignments=[0,1,2]`

> Broker appends a random 4-digit suffix to the member ID.
> e.g., `member=consumer-1` → actual ID `consumer-1-8374`

**SYNC_GROUP**
```
SYNC_GROUP topic=<name> group=<name> member=<actual-id>
```
Response: `OK assignments=[0,1,2]`

**LEAVE_GROUP**
```
LEAVE_GROUP topic=<name> group=<name> member=<actual-id>
```
Response: `OK group=<name> member=<actual-id> left=true`

**HEARTBEAT**
```
HEARTBEAT topic=<name> group=<name> member=<actual-id> [generation=<N>]
```
Response: `OK member=<actual-id> generation=<N>`

| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| group | Yes | - | Consumer group name |
| member | Yes | - | Consumer member ID (with suffix) |
| generation | No | - | Current generation number; if supplied, stale generations return ERROR: GEN_MISMATCH ... |

Recommended interval: 3 seconds. Server session timeout: configurable (default ~30s).

**GROUP_STATUS**
```
GROUP_STATUS group=<name>
```
Response (JSON):
```json
{
  "group_name": "mygroup",
  "topic_name": "mytopic",
  "state": "Stable",
  "generation": 3,
  "member_count": 2,
  "partition_count": 4,
  "members": [
    {
      "member_id": "consumer-1-8374",
      "last_heartbeat": "2025-01-01T00:00:00Z",
      "assignments": [0, 1]
    }
  ],
  "last_rebalance": "2025-01-01T00:00:00Z"
}
```

#### Consuming

**CONSUME** (single poll)
```
CONSUME topic=<name> partition=<N> offset=<N> member=<id> group=<name> [autoOffsetReset=<earliest|latest>] [batch=<N>] [wait_ms=<N>]
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Topic name (supports `*` and `?` wildcards) |
| partition | Yes | - | Partition ID |
| offset | Yes | - | Starting offset |
| member | Yes | - | Consumer member ID |
| group | No | default-group | Consumer group |
| autoOffsetReset | No | earliest | `earliest` (0) or `latest` (HWM) |
| batch | No | 8192 | Max messages per poll |
| wait_ms | No | 0 | Long-poll timeout in ms |

Response: Binary batch frame (Section 5)

If the broker has a committed offset for `(topic, group, partition)`, `CONSUME`
starts from that committed offset even when the request includes a lower
`offset=` value. If no committed offset exists, the explicit `offset=` value is
used. If the command omits a usable explicit offset in a future protocol
revision, `autoOffsetReset=earliest` starts at `0` and `autoOffsetReset=latest`
starts at the partition high-water mark.

`CONSUME` is a stateless partition-leader read. The
partition leader does not validate consumer group ownership, member liveness, or
generation on the data path. Ownership and generation fencing are enforced by
coordinator commands such as `HEARTBEAT`, `COMMIT_OFFSET`, and `BATCH_COMMIT`.

**STREAM** (continuous push)
```
STREAM topic=<name> partition=<N> member=<id> group=<name> [batch=<N>]
```

Opens a continuous stream. Server pushes binary batches at ~100ms intervals. The connection stays open until the client disconnects, the broker removes the stream, the stream times out, or an unrecoverable stream error occurs. Like `CONSUME`, `STREAM` is a stateless partition-leader data path and does not validate group ownership or generation on every read.

Keepalive: Server sends `[00 00 00 00]` (4 zero bytes as length prefix) when no messages are available. Clients MUST treat zero-length frames as keepalive and continue reading.

Control frames: The broker may send UTF-8 text frames with the prefix `STREAM_CONTROL` on the same length-prefixed connection. Clients MUST inspect text control frames before binary batch decoding.

```text
STREAM_CONTROL type=CLOSE reason=<stopped|removed|timeout|error|offset_out_of_range> offset=<nextOffset>
```

`type=CLOSE` is a graceful stream terminator. `offset` is the broker's next stream offset at close time. `reason=offset_out_of_range` means the requested stream offset is older than the retained log. Clients SHOULD close the socket, keep or refresh their committed offset, and reconnect or rejoin according to the consumer group lifecycle. A broker crash, process kill, or network failure can still close the TCP connection without a terminator; clients MUST treat raw disconnect as retryable and resume from the broker committed offset.

#### Offset Management


**LIST_OFFSETS**

```text
LIST_OFFSETS topic=<name> [partition=<N>]
```

Success response:

```text
OK topic=<name> partitions=<N> offsets=P0:earliest=<N>:latest=<N>:leo=<N>:hwm=<N>,P1:earliest=<N>:latest=<N>:leo=<N>:hwm=<N>
```

`LIST_OFFSETS` returns the broker-side offset range for retained records. `earliest` is the first retained offset. `latest` is the next readable committed offset and is the value clients should use for `autoOffsetReset=latest`. `leo` is the partition log end offset, and `hwm` is the high-water mark before the broker caps reads to the flushed durable tail. A single `partition=` returns only that partition.

Errors:

```text
ERROR: missing_topic command=LIST_OFFSETS
ERROR: topic_not_found topic=<name>
ERROR: invalid_partition command=LIST_OFFSETS
ERROR: partition_not_found partition=<N>
```

**FETCH_OFFSET**
```
FETCH_OFFSET topic=<name> partition=<N> group=<name>
```
Response: `OK offset=<nextOffset>`. If no offset has been committed, the broker returns `OK offset=0` (earliest). Older brokers returned a plain integer; clients may keep that only as a legacy fallback.

The key is `(topic, group, partition)`. Offsets are independent for every group
and every partition.

**COMMIT_OFFSET**
```
COMMIT_OFFSET topic=<name> partition=<N> group=<name> offset=<N> [member=<actual-id> generation=<N>]
```
Response: `OK`

The `offset` value is the next offset to read after the client has processed
records. Commits are durable and monotonic per `(topic, group, partition)`.
Committing the same offset is idempotent. Committing an offset lower than the
current committed offset returns an error and does not move the stored offset
backward.

**BATCH_COMMIT**
```
BATCH_COMMIT topic=<name> group=<name> member=<id> generation=<N> P<partition>:<offset>,P<partition>:<offset>,...
```
Example: `BATCH_COMMIT topic=t1 group=g1 member=m1 generation=3 P0:100,P1:200,P2:150`

Response: `OK batched=<N>`

> The `P` prefix before partition numbers is required.

Batch commits follow the same monotonic rule as `COMMIT_OFFSET` for each
included partition. The broker validates `member` and `generation` before applying
the batch. If any partition is not owned by that member in that generation, the
entire batch is rejected with `ERROR: NOT_OWNER ...`. Stale generations return
`ERROR: GEN_MISMATCH ...`, and unknown members return `ERROR: member_not_found ...`.


#### Transaction Coordinator

Cursus exposes a broker-managed transaction coordinator for consume-process-produce workflows. In distributed mode, transaction commands are routed by `transactional_id` using the coordinator key `txn:<transactional_id>`. Clients can discover the owner with `FIND_COORDINATOR transactional_id=<id>` and must retry on `ERROR: NOT_COORDINATOR host=<host> port=<port>`.

Transaction state is replicated through the Raft FSM as `TXN_SYNC` and included in FSM snapshot version 4. Clients should first call `INIT_PRODUCER_ID` for a `transactional_id`; the broker returns the authoritative `(producerId, epoch)` session and bumps `epoch` on re-initialization to fence older producers. The coordinator fences stale producers by `(transactional_id, producerId, epoch)`: lower epochs are rejected, and staged operations must use the same producer and epoch that opened the transaction. Completed transactional ids are retained for `transactional_id_expiration_ms` so retry/fencing state survives normal restarts, while active `open` and `committing` transactions are not expired by the cleanup path.

**INIT_PRODUCER_ID**

```text
INIT_PRODUCER_ID transactional_id=<id>
```

Success: `OK transactional_id=<id> producerId=<producer-id> epoch=<N>`. Re-initializing the same `transactional_id` returns the same broker-managed `producerId` with a higher `epoch`, aborts any open local staging for that id, and fences commands that still use the previous epoch. If the transaction is already `committing`, the broker rejects reinitialization so the prepared commit can be retried or recovered.

**BEGIN_TXN**

```text
BEGIN_TXN transactional_id=<id> producerId=<producer-id> epoch=<N>
```

Success: `OK transactional_id=<id> state=open producerId=<producer-id> epoch=<N>`.

**TXN_PUBLISH**

```text
TXN_PUBLISH transactional_id=<id> topic=<topic> [partition=<N>] producerId=<producer-id> seqNum=<N> epoch=<N> [key=<key>] message=<payload>
```

Success: `OK transactional_id=<id> staged_messages=1 topic=<topic> partition=<N>`.

The record is staged in the transaction coordinator and is not published until `END_TXN ... result=commit`. `seqNum` is required and must be greater than zero; the broker uses `(producerId, epoch, seqNum)` to make commit recovery idempotent even when the target topic is not globally idempotent. On commit, the broker writes the record with `transactional_id` and `transaction_state=open`, then uses the normal publish path, including partition-leader routing in distributed mode. After records are written and staged offsets are committed, the broker appends a hidden Cursus transaction commit marker to each touched partition; `read_committed` visibility is controlled by a later marker for the same `(transactional_id, epoch)`, so transactional records are not visible if marker append fails before retry/recovery completes, even if they carry transaction metadata or an older epoch marker exists. Uncommitted staged records are not published by this staging path, and abort writes hidden abort markers for touched partitions that already contain transaction records from a retry/recovery path.

**SEND_OFFSETS_TO_TXN**

```text
SEND_OFFSETS_TO_TXN transactional_id=<id> producerId=<producer-id> epoch=<N> topic=<topic> group=<group> member=<member> generation=<N> P<partition>:<nextOffset>,P<partition>:<nextOffset>
```

Success: `OK transactional_id=<id> staged_offsets=<N>`.

The broker validates `member`, `generation`, and partition ownership before staging offsets, then validates them again before commit. Offsets keep the same monotonic rule as `COMMIT_OFFSET`; `END_TXN ... result=commit` fails before publishing records if any staged offset would regress.

**END_TXN**

```text
END_TXN transactional_id=<id> producerId=<producer-id> epoch=<N> result=<commit|abort>
```

Success: `OK transactional_id=<id> state=<committed|aborted> messages=<N> offsets=<N>`. Retrying the same final result with the same `producerId` and `epoch` is idempotent; a lower epoch is rejected as fenced, and trying to abort a committed transaction or commit an aborted transaction returns an error.

**TXN_STATUS**

```text
TXN_STATUS transactional_id=<id>
```

Success: `OK transactional_id=<id> state=<open|committing|committed|aborted> messages=<N> offsets=<N>`.

Current guarantee: a successful transaction commit stages records and offsets behind a broker transaction coordinator, fences stale producer epochs, persists transaction state through the distributed metadata FSM, writes hidden Cursus transaction control markers with durable transaction control-record key/value bytes and Cursus control-batch metadata to touched partition logs, uses the normal partition-leader publish path, appends commit markers before applying staged offsets, commits offsets through the consumer-group coordinator, and makes `ReadCommitted`/`CONSUME` use transaction markers to expose committed transactions, skip aborted transactions, and stop at the first unresolved open transaction as a last-stable-offset boundary. Partitions maintain an in-memory transaction marker index rebuilt from durable logs on startup, so `read_committed` does not need to rediscover every later marker by scanning from the requested offset. `INIT_PRODUCER_ID` provides broker-owned producer epoch allocation for transactional ids. Retried `END_TXN` requests for an already committed or aborted transaction are idempotent for the same producer epoch. On broker start, restored `committing` transactions are recovered by replaying the prepared transaction and finalizing it as committed on the transaction coordinator; producer sequence state is rebuilt from partition logs so replay does not append duplicate transactional records after a lost producer-state checkpoint. The marker contract is stored in Cursus log records (`control_batch_type=transaction`, `control_batch_version=2`, `control_batch_coordinator_epoch=<epoch>`) plus transaction control-record key/value bytes (`key: int16 version, int16 markerType`; `value: int16 version, int32 coordinatorEpoch`). This makes the transaction marker payload schema Cursus-compatible while the surrounding Cursus log segment remains Cursus-owned, not a Cursus broker network-protocol byte stream. Cursus does not make external side effects exactly-once. Clients should continue to use idempotent processors for external systems.

#### Event Sourcing Commands

These commands are available only on topics created with `event_sourcing=true`.

Event-sourcing commands are partition-routed by aggregate `key`. In distributed mode, `APPEND_STREAM`, `READ_STREAM`, `STREAM_VERSION`, `SAVE_SNAPSHOT`, and `READ_SNAPSHOT` must execute on the leader for the aggregate partition. A non-leader broker returns `ERROR: NOT_LEADER LEADER_IS <host:port>`; clients should reconnect to that address and retry. Followers index replicated event-sourcing messages, apply quorum-replicated snapshots, and rebuild local stream indexes from the committed log on restart. Partitions persist their high watermark checkpoint and restore it on broker restart so committed reads resume from the last successful committed tail.

**APPEND_STREAM**
```
APPEND_STREAM topic=<name> key=<aggregate_key> version=<N> event_type=<type> [schema_version=<N>] [metadata=<json>] message=<payload>
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Event-sourcing-enabled topic |
| key | Yes | - | Aggregate ID (determines partition routing) |
| version | Yes | - | Expected next version (must equal current version + 1) |
| event_type | No | "" | Event type name (e.g., `OrderCreated`) |
| schema_version | No | 1 | Schema version for upcasting |
| metadata | No | "" | Arbitrary JSON metadata |
| message | Yes | - | Event payload (captures rest of line) |

Success response: `OK version=<N> offset=<N> partition=<N>`

Internal broker catch-up commands: `LIST_SNAPSHOTS topic=<name> partition=<N>`, `FETCH_SNAPSHOT topic=<name> partition=<N> key=<aggregate_key>`, and `CATCHUP_SNAPSHOTS topic=<name> partition=<N> [leader=<host:port>]`. These commands are for broker-to-broker recovery only. In distributed mode, internal broker commands require `internal_token=<shared-token>` and brokers must be configured with `internal_auth_token`; clients and SDKs must not send these commands directly. Operators can additionally set `internal_broker_port` to move broker-to-broker command forwarding off the public client listener. When `internal_use_tls=true`, that internal listener requires mutual TLS using `internal_tls_cert_path`, `internal_tls_key_path`, and `internal_tls_ca_path`; the router dials peer brokers on the internal port with the same CA trust. The shared internal token remains a command-level guard, but mTLS/internal listener separation is the stronger network boundary.

Error responses:
- `ERROR: version_conflict current=<N> expected=<N>` — optimistic concurrency failure
- `ERROR: event_sourcing_not_enabled topic=<name>` - topic not created with `event_sourcing=true`
- `ERROR: invalid_schema_version` - `schema_version` was not an unsigned integer
- `ERROR: NOT_LEADER LEADER_IS <host:port>` - command reached a non-leader broker in distributed mode

**READ_STREAM**
```
READ_STREAM topic=<name> key=<aggregate_key> [from_version=<N>]
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| key | Yes | - | Aggregate ID |
| from_version | No | 1 (or snapshot version + 1 if snapshot exists) | Starting version |

Response: Two length-prefixed frames sent sequentially.

Frame 1 — JSON envelope:
```json
{
  "topic": "orders",
  "key": "order-123",
  "partition": 2,
  "count": 5,
  "snapshot": {"version": 500, "payload": "..."}
}
```

The `snapshot` field is included only when a snapshot exists at or after `from_version`. `count` reflects the number of events in Frame 2 (not including the snapshot).

Frame 2 — Binary batch (standard 0xBA7C format) containing the events. If no events exist after the snapshot, the batch has message count 0.

**STREAM_VERSION**
```
STREAM_VERSION topic=<name> key=<aggregate_key>
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| key | Yes | - | Aggregate ID |

Response: `OK version=<N>` (for example, `OK version=6`). Returns `OK version=0` if the aggregate does not exist. Older brokers returned a plain integer; clients may keep that only as a legacy fallback.

**SAVE_SNAPSHOT**
```
SAVE_SNAPSHOT topic=<name> key=<aggregate_key> version=<N> message=<payload>
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| key | Yes | - | Aggregate ID |
| version | Yes | - | Aggregate version this snapshot represents (must be <= current version) |
| message | Yes | - | Serialized aggregate state (captures rest of line) |

Success response: `OK version=<N> partition=<N>`

In distributed mode, success means the leader stored the snapshot and replicated it to the configured in-sync replica quorum through the internal `REPLICATE_SNAPSHOT internal_token=<shared-token> payload=<json>` broker-to-broker command. Clients should not send `REPLICATE_SNAPSHOT` directly. Brokers reject internal commands without the configured token with `ERROR: internal_command_unauthorized ...`, and reject distributed-mode internal commands when no token is configured with `ERROR: internal_auth_not_configured ...`.

Error responses:
- `ERROR: snapshot_version_exceeds_stream version=<N> current=<N>`
- `ERROR: snapshot_replicate_failed reason="..."`

**READ_SNAPSHOT**
```
READ_SNAPSHOT topic=<name> key=<aggregate_key>
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| key | Yes | - | Aggregate ID |

Success response: `OK snapshot=<json>`
```text
OK snapshot={"version":500,"payload":"..."}
```

Not found response: `OK snapshot=null`. Older brokers returned `NULL`; clients may keep that only as a legacy fallback.

---

## 5. Binary Batch Format

### Header

```
Offset  Size  Type     Field
------  ----  -------  --------------------------------
0       2     uint16   Magic (0xBA7C)
2       2     uint16   Topic name length (T)
4       T     string   Topic name (UTF-8)
4+T     4     int32    Partition ID
8+T     1     uint8    Acks string length (A)
9+T     A     string   Acks value ("0", "1", "-1", "all")
9+T+A   1     uint8    IsIdempotent (0x00 or 0x01)
10+T+A  8     uint64   Batch start sequence number
18+T+A  8     uint64   Batch end sequence number
26+T+A  4     int32    Message count (M)
30+T+A  ...   Message  M message records follow
```

### Message Record

```
Offset  Size  Type     Field
------  ----  -------  --------------------------------
0       8     uint64   Offset (0 on send, assigned by broker)
8       8     uint64   Sequence number
16      2     uint16   ProducerID length (P)
18      P     string   ProducerID (UTF-8)
18+P    2     uint16   Key length (K)
20+P    K     string   Partition key (UTF-8, optional)
20+P+K  8     int64    Epoch
28+P+K  4     uint32   Payload length (L)
32+P+K  L     bytes    Payload
```

### Batch Response

Server responds with the same binary batch format for CONSUME. PUBLISH with `acks=1` or `acks=all` returns JSON `AckResponse` with `status="OK"`; PUBLISH with `acks=0` returns `OK`.

---

## 6. Consumer Group Protocol

### Lifecycle

```
1. JOIN_GROUP   → receive generation, member ID, initial assignments
2. SYNC_GROUP   → receive confirmed partition assignments
3. Loop:
   a. HEARTBEAT      (every 3s)
   b. CONSUME/STREAM (fetch messages)
   c. COMMIT_OFFSET  (periodically)
4. LEAVE_GROUP  → graceful exit
```

### Rebalancing

Rebalance is triggered when:
- A new consumer joins
- An existing consumer leaves or times out
- Partitions are added to the topic

Detection:
- `HEARTBEAT` returns `ERROR: GEN_MISMATCH ...`, `ERROR: member_not_found ...`, or another `ERROR:` response
- `COMMIT_OFFSET` or `BATCH_COMMIT` returns `ERROR: NOT_OWNER ...`, `ERROR: GEN_MISMATCH ...`, or `ERROR: member_not_found ...`

Client action:
1. Stop consuming.
2. Commit already processed offsets only while the member still owns those partitions.
3. Re-execute `JOIN_GROUP` then `SYNC_GROUP`.
4. Fetch broker committed offsets for the new assignments.
5. Resume consuming from those offsets.
### Offset Resolution Priority

```
1. Saved offset from coordinator for (topic, group, partition)
2. Explicit offset from request parameter
3. autoOffsetReset = "latest" → partition `LIST_OFFSETS latest` value
4. autoOffsetReset = "earliest" → 0
```

### SDK Offset and Group Contract

All SDKs should implement the same consumer group resume behavior:

- After `JOIN_GROUP` and `SYNC_GROUP`, fetch `FETCH_OFFSET` for every assigned partition before consuming.
- Treat the broker committed offset as the source of truth on reconnect, rejoin, and broker restart. SDK-local offset caches must not override a broker committed offset.
- Commit only after records have been processed. The committed value is `lastProcessedOffset + 1`.
- Send `member=<actual-id>` and `generation=<N>` on `COMMIT_OFFSET` when the SDK has group membership state.
- Send `BATCH_COMMIT` entries as `P<partition>:<nextOffset>` and include `member` plus `generation`.
- Treat `ERROR: offset_regression ...` as a failed commit; do not update local committed state from it.
- Treat `ERROR: GEN_MISMATCH ...`, `ERROR: NOT_OWNER ...`, and `ERROR: member_not_found ...` from `HEARTBEAT`, `COMMIT_OFFSET`, or `BATCH_COMMIT` as group membership failures that require stopping consumption and rejoining.
- On `ERROR: OFFSET_OUT_OF_RANGE ...`, apply the SDK `auto_offset_reset` policy: `earliest` resumes from the broker-reported earliest retained offset, `latest` resumes from the broker-reported latest offset, and `error` fails the consumer instead of silently skipping or replaying data.
### Offset Lifecycle and Delivery Guarantees

Consumer group offsets are stored by the broker in the internal
`__consumer_offsets` topic and loaded again when the broker starts. In
distributed mode, offset updates are also applied through the Raft FSM and
included in FSM snapshots. A successful `COMMIT_OFFSET` or `BATCH_COMMIT`
therefore survives broker restart according to the configured broker storage
durability.

Recommended client loop:

1. Fetch or join/sync assignments.
2. Consume from the broker-selected resume offset.
3. Process the returned records.
4. Commit `lastProcessedOffset + 1` using `COMMIT_OFFSET ... member=<id> generation=<N>` or `BATCH_COMMIT ... P<partition>:<nextOffset>`.

This gives at-least-once delivery when the client commits after processing. If a
client commits before processing and then crashes, it may skip unprocessed
records, which is at-most-once behavior for that client. Cursus provides broker-managed transaction commands for consume-process-produce workflows, including producer fencing, durable transaction state, idempotent finalization, hidden partition transaction markers with durable transaction control-record key/value bytes and Cursus control-batch metadata, startup recovery for prepared commits, and read-committed filtering with a last-stable-offset style boundary for unresolved open transactions. It is still not exactly-once for external effects, so applications that need exactly-once external effects must still make their processors idempotent or transactional outside the broker.

Example for a game server such as wargame-IOCP:

```
JOIN_GROUP topic=match-events group=wargame-iocp member=game-01
SYNC_GROUP topic=match-events group=wargame-iocp member=game-01-1234
FETCH_OFFSET topic=match-events partition=0 group=wargame-iocp
# broker returns: OK offset=<nextOffset>
CONSUME topic=match-events partition=0 offset=<nextOffset> group=wargame-iocp member=game-01-1234 batch=128
COMMIT_OFFSET topic=match-events partition=0 group=wargame-iocp offset=<lastProcessedOffset+1>
```

The game server does not need its own Postgres table for Cursus offsets once it
uses this API. On reconnect or broker restart, the same group resumes from the
last successful broker commit.

---

## 7. Error Handling

### Error Response Format

```
ERROR: <code> [key=value ...]
```

### Error Codes

| Error | Cause | Client Action |
|-------|-------|---------------|
| `topic_not_found topic=<X>` | Topic not created | CREATE topic first |
| `PARTITION_NOT_FOUND <N>` | Invalid partition ID | Check DESCRIBE for valid IDs |
| `TOPIC_NOT_FOUND <X>` | Topic not found | CREATE topic first |
| `NOT_AUTHORIZED_FOR_PARTITION <T>:<P>` | Not partition leader | Redirect to correct leader |
| `NOT_LEADER LEADER_IS <addr>` | Not Raft leader | Reconnect to specified address |
| `group_not_found group=<X>` | Group not registered | JOIN_GROUP or REGISTER_GROUP |
| `topic_not_assigned_to_group` | Topic mismatch | Verify group registration |
| `GEN_MISMATCH current=N requested=N group=<G> member=<M>` | Stale generation | Re-join group |
| `NOT_OWNER partition=N member=<M> group=<G> generation=N` | Assignment changed | Re-join group |
| `member_not_found member=<M> group=<G>` | Member is no longer active | Re-join group |
| `offset_regression reason="..."` | Commit lower than current stored offset | Treat commit as failed; refetch offset |
| `stale_producer_epoch producer=<id> current=<N> got=<N>` | Idempotent producer request uses an older fenced epoch | Treat as fatal for that producer instance; create a new producer session |
| `OFFSET_OUT_OF_RANGE requested=N earliest=N latest=N` | Requested offset is older than retained log or beyond available range | Treat as data loss or reset according to policy |
| `no_valid_offsets` | BATCH_COMMIT parse failure | Check format: `P<N>:<offset>` |
| `version_conflict current=N expected=N` | Optimistic concurrency failure | Reload aggregate and retry |
| `event_sourcing_not_enabled topic=<X>` | ES command on non-ES topic | CREATE topic with `event_sourcing=true` |
| `snapshot_version_exceeds_stream version=N current=N` | Invalid snapshot version | Use version <= current stream version |

### Leader Redirect

In distributed mode, if a request reaches a non-leader broker:

```
ERROR: NOT_LEADER LEADER_IS 192.168.1.10:9000
```

Client should reconnect to the specified address and retry.

---

## 8. Topic Policy Contract

### Authorization

The wire protocol exposes per-topic `auth_policy` metadata: `open`, `deny_write`, `deny_read`, and `acl`. When `enable_sasl=true`, all protected admin, topic, group, and transaction commands require connection authentication with `AUTH principal=<principal> token=<token>` or inline `principal=<principal> auth_token=<token>`.

Configured users may receive `admin`, `topic.read`, `topic.write`, `group`, `transaction`, or wildcard `*` permissions. Commands that cross boundaries require every applicable permission: `CONSUME`/`STREAM` require `topic.read` plus `group`, `TXN_PUBLISH` requires `transaction` plus `topic.write`, and `SEND_OFFSETS_TO_TXN` requires `transaction` plus `group`. Missing authentication returns `ERROR: authentication_required command=<COMMAND>`; insufficient coarse permission returns `ERROR: NOT_AUTHORIZED_FOR_OPERATION command=<COMMAND> permission=<permission>`.

After coarse authorization, `auth_policy=acl` checks `read_acl` for topic reads and `write_acl` for topic writes, returning `ERROR: NOT_AUTHORIZED_FOR_TOPIC topic=<T> operation=<read|write>` on denial. Internal broker contexts bypass client permissions but remain subject to the separate internal-listener/token boundary. This is a token authentication contract, not a mechanism-specific SASL byte protocol; use TLS/mTLS and network controls across trust boundaries. An omitted `permissions` list retains legacy full access for an authenticated user and should be avoided in least-privilege deployments.

### Retention

Topics store `retention_hours` and `retention_bytes` policy metadata. A value of `0` means the broker-level default applies. Enforcement is still performed by the broker storage/retention loop, and retention-gap reads return `ERROR: OFFSET_OUT_OF_RANGE requested=<N> earliest=<N> latest=<N>` instead of an empty batch. Clients should treat this as data loss for that group and recover according to application policy, such as alerting, resetting to `earliest`, or rebuilding from another source.

### Partition Keys

`partitioner=hash_key` routes keyed messages with FNV-1a 64-bit hash modulo the topic partition count and routes unkeyed messages round-robin. `partitioner=round_robin` ignores message keys and routes every publish by round-robin. Increasing partition count can remap future records for an existing key.

---

## 9. Idempotent Producer

### Enable

Set `isIdempotent=true` on PUBLISH or in binary batch header.

### Requirements

- Each idempotent message must have a unique `(producerId, epoch, seqNum)` tuple within a partition
- `seqNum` must be monotonically increasing within the current producer epoch for each partition
- A new `(producerId, epoch)` sequence starts at `seqNum=1`; starting above 1 is rejected as a gap
- A higher `epoch` fences the previous producer session and may restart `seqNum` from 1
- A lower `epoch` is rejected as stale producer state
- `seqNum` = 0 disables dedup for non-transactional publish messages; transactional `TXN_PUBLISH` requires `seqNum > 0`
- Broker rejects duplicates silently (returns OK)

### Sequence Tracking

- Broker tracks the last seen `(epoch, seqNum)` per `(producerId)` per partition
- Disk-backed partitions persist producer sequence checkpoints, rebuild producer state from partition logs on broker restart, and use that state to make transactional commit recovery idempotent
- Distributed FSM snapshots also include producer sequence state for replicated message commands
- FSM snapshots that encode producer epochs use snapshot version 3. Do not run mixed-version rolling upgrades across brokers that cannot decode producer-epoch snapshot state; upgrade the cluster together or drain snapshots before introducing older binaries.
- Producer state expires from memory after `producer_state_ttl_ms` of inactivity (default 30 minutes); durable checkpoints retain the last persisted sequence until the partition data is removed

---

## 10. SDK Implementation Checklist

### Required

- [ ] TCP connection with length-prefixed framing (4-byte BE)
- [ ] Binary batch encoding/decoding (0xBA7C magic)
- [ ] Text command formatting
- [ ] JSON response parsing (AckResponse, DESCRIBE, GROUP_STATUS)
- [ ] Consumer group lifecycle (JOIN → SYNC → HEARTBEAT → CONSUME → COMMIT)
- [ ] Error response parsing
- [ ] Leader redirect handling

### Recommended

- [ ] TLS 1.2+ support
- [ ] Compression (gzip/snappy/lz4)
- [ ] Connection pooling
- [ ] Exponential backoff on reconnect
- [ ] Async offset commits (BATCH_COMMIT)
- [ ] Idempotent producer with sequence numbers
- [x] Transaction coordinator commands (BEGIN_TXN, TXN_PUBLISH, SEND_OFFSETS_TO_TXN, END_TXN)
- [ ] Wildcard topic subscription
- [ ] Stream mode (continuous push)
- [ ] Read/write buffer tuning (2MB recommended)
- [ ] Event sourcing: APPEND_STREAM with optimistic concurrency
- [ ] Event sourcing: READ_STREAM two-frame response parsing
- [ ] Event sourcing: Snapshot save/read
- [ ] Event sourcing: Auto-snapshot after N events (recommended: 500)
