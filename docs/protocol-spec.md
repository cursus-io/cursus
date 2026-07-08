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
CREATE topic=<name> [partitions=<N>] [idempotent=<true|false>] [replication_factor=<N>]
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| partitions | No | 4 | Number of partitions |
| idempotent | No | false | Enable producer dedup |
| replication_factor | No | 3 | Replica count (distributed mode) |

Response: `OK topic=<name> partitions=<N>`

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
PUBLISH topic=<name> acks=<0|1|-1|all> producerId=<id> message=<text> [seqNum=<N>] [epoch=<N>] [isIdempotent=<true|false>]
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Target topic |
| acks | No | 1 | Durability level |
| producerId | Yes | - | Unique producer ID |
| message | Yes | - | Message payload (captures rest of line) |
| seqNum | No | 0 | Sequence number (for idempotent mode) |
| epoch | No | 0 | Producer epoch |
| isIdempotent | No | false | Enable dedup for this message |

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
Response: `OK`

| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| group | Yes | - | Consumer group name |
| member | Yes | - | Consumer member ID (with suffix) |
| generation | No | - | Current generation number (for future validation) |

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

**STREAM** (continuous push)
```
STREAM topic=<name> partition=<N> member=<id> group=<name> [batch=<N>]
```

Opens a continuous stream. Server pushes binary batches at ~100ms intervals. Connection stays open until client disconnects.

Keepalive: Server sends `[00 00 00 00]` (4 zero bytes as length prefix) when no messages are available.

#### Offset Management

**FETCH_OFFSET**
```
FETCH_OFFSET topic=<name> partition=<N> group=<name>
```
Response: `OK offset=<nextOffset>`. If no offset has been committed, the broker returns `OK offset=0` (earliest). Older brokers returned a plain integer; clients may keep that only as a legacy fallback.

The key is `(topic, group, partition)`. Offsets are independent for every group
and every partition.

**COMMIT_OFFSET**
```
COMMIT_OFFSET topic=<name> partition=<N> group=<name> offset=<N>
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
included partition.

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

Internal broker catch-up commands: LIST_SNAPSHOTS topic=<name> partition=<N>, FETCH_SNAPSHOT topic=<name> partition=<N> key=<aggregate_key>, and CATCHUP_SNAPSHOTS topic=<name> partition=<N> [leader=<host:port>]. These commands are for broker-to-broker recovery only.

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

In distributed mode, success means the leader stored the snapshot and replicated it to the configured in-sync replica quorum through the internal `REPLICATE_SNAPSHOT` broker-to-broker command. Clients should not send `REPLICATE_SNAPSHOT` directly.

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
- HEARTBEAT returns `ERROR` or generation changes
- CONSUME returns ownership validation failure

Client action:
1. Stop consuming
2. Commit current offsets
3. Re-execute JOIN_GROUP → SYNC_GROUP
4. Resume consuming with new assignments

### Offset Resolution Priority

```
1. Saved offset from coordinator for (topic, group, partition)
2. Explicit offset from request parameter
3. autoOffsetReset = "latest" → partition HWM
4. autoOffsetReset = "earliest" → 0
```

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
4. Commit `lastProcessedOffset + 1`.

This gives at-least-once delivery when the client commits after processing. If a
client commits before processing and then crashes, it may skip unprocessed
records, which is at-most-once behavior for that client. Cursus does not provide
exactly-once processing semantics; applications that need exactly-once effects
must make their processing idempotent or transactional outside the broker.

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
| `group_not_found: <X>` | Group not registered | JOIN_GROUP or REGISTER_GROUP |
| `topic_not_assigned_to_group` | Topic mismatch | Verify group registration |
| `generation mismatch` | Stale generation | Re-join group |
| `not partition owner` | Assignment changed | Re-join group |
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

## 8. Idempotent Producer

### Enable

Set `isIdempotent=true` on PUBLISH or in binary batch header.

### Requirements

- Each message must have a unique `(producerId, seqNum)` pair
- `seqNum` must be monotonically increasing per producer
- `seqNum` = 0 disables dedup for that message
- Broker rejects duplicates silently (returns OK)

### Sequence Tracking

- Broker tracks last seen sequence per `(producerId)` per partition
- State survives across batches but not broker restarts (tracked in memory + FSM for distributed)
- Producer state expires after 30 minutes of inactivity

---

## 9. SDK Implementation Checklist

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
- [ ] Wildcard topic subscription
- [ ] Stream mode (continuous push)
- [ ] Read/write buffer tuning (2MB recommended)
- [ ] Event sourcing: APPEND_STREAM with optimistic concurrency
- [ ] Event sourcing: READ_STREAM two-frame response parsing
- [ ] Event sourcing: Snapshot save/read
- [ ] Event sourcing: Auto-snapshot after N events (recommended: 500)
