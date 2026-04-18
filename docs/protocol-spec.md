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

Response: `Topic '<name>' now has <N> partitions`

**DELETE**
```
DELETE topic=<name>
```
Response: `Topic '<name>' deleted`

**LIST**
```
LIST
```
Response: `topic1, topic2, topic3` or `(no topics)`

**DESCRIBE**
```
DESCRIBE topic=<name>
```
Response (JSON):
```json
{
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
Response: `Left group '<name>'`

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
Response: `<offset>` (plain integer) or `0` if not found

**COMMIT_OFFSET**
```
COMMIT_OFFSET topic=<name> partition=<N> group=<name> offset=<N>
```
Response: `OK`

**BATCH_COMMIT**
```
BATCH_COMMIT topic=<name> group=<name> member=<id> generation=<N> P<partition>:<offset>,P<partition>:<offset>,...
```
Example: `BATCH_COMMIT topic=t1 group=g1 member=m1 generation=3 P0:100,P1:200,P2:150`

Response: `OK batched=<N>`

> The `P` prefix before partition numbers is required.

#### Event Sourcing Commands

These commands are available only on topics created with `event_sourcing=true`.

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

Error responses:
- `ERROR: version_conflict current=<N> expected=<N>` — optimistic concurrency failure
- `ERROR: topic '<name>' is not event-sourcing enabled` — topic not created with `event_sourcing=true`

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

Response: Plain integer (e.g., `6`). Returns `0` if the aggregate does not exist.

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

Error response: `ERROR: snapshot version <N> exceeds stream version <N>`

**READ_SNAPSHOT**
```
READ_SNAPSHOT topic=<name> key=<aggregate_key>
```
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| key | Yes | - | Aggregate ID |

Success response (JSON):
```json
{"version": 500, "payload": "..."}
```

Not found response: `NULL`

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

Server responds with the same binary batch format (for CONSUME) or JSON `AckResponse` (for PUBLISH).

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
1. Saved offset from coordinator (via FETCH_OFFSET)
2. Explicit offset from request parameter
3. autoOffsetReset = "latest" → partition HWM
4. autoOffsetReset = "earliest" → 0
```

---

## 7. Error Handling

### Error Response Format

```
ERROR: <description>
```

### Error Codes

| Error | Cause | Client Action |
|-------|-------|---------------|
| `topic '<X>' does not exist` | Topic not created | CREATE topic first |
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
| `topic '<X>' is not event-sourcing enabled` | ES command on non-ES topic | CREATE topic with `event_sourcing=true` |
| `snapshot version N exceeds stream version N` | Invalid snapshot version | Use version <= current stream version |

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
