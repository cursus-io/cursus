# Event Sourcing

## Overview

Event sourcing is a persistence pattern where state changes are captured as an immutable, append-only sequence of domain events. Instead of storing only the current state, the system stores every event that led to it. The current state is derived by replaying events from the beginning (or from a snapshot).

Cursus provides native event sourcing support at the broker level. Aggregates are modeled as key-based logical streams within existing topics. The broker handles stream indexing, optimistic concurrency control, and snapshot management. Projections and schema evolution remain the responsibility of client applications.

### Why Native Support Matters

Without broker-level support, applications must implement concurrency control, version tracking, and stream indexing on top of a general-purpose message log. This leads to duplicated effort across SDKs and introduces race conditions that are difficult to resolve without coordination at the storage layer. By moving these concerns into the broker, Cursus guarantees linearizable writes per aggregate and eliminates an entire class of concurrency bugs.

### Design Principles

- **Reuse existing primitives.** Events are stored as regular messages in the append-only partition log. Consumer groups, replication, retention, and all existing features apply without modification.
- **Opt-in per topic.** The `event_sourcing=true` flag on CREATE enables event sourcing behavior. Non-event-sourcing topics are completely unaffected.
- **Broker owns storage, SDK owns domain logic.** The broker handles versioning, indexing, and snapshots. Schema evolution (upcasting) is the SDK's responsibility.

---

## Quick Start

### 1. Create an Event-Sourcing Topic

```
CREATE topic=orders partitions=4 event_sourcing=true
```

Response:

```
Topic 'orders' now has 4 partitions
```

The `event_sourcing=true` flag tells the broker to maintain a per-partition stream index for aggregate-level version tracking.

### 2. Append Events

Append events to an aggregate stream using `APPEND_STREAM`. The `key` parameter is the aggregate ID, and `version` is the expected next version (optimistic concurrency).

```
APPEND_STREAM topic=orders key=order-123 version=1 event_type=OrderCreated message={"customer":"alice","total":49.98}
```

Response:

```
OK version=1 offset=0 partition=2
```

Append a second event:

```
APPEND_STREAM topic=orders key=order-123 version=2 event_type=ItemAdded message={"sku":"GADGET-7","qty":1}
```

Response:

```
OK version=2 offset=1 partition=2
```

### 3. Check Stream Version

```
STREAM_VERSION topic=orders key=order-123
```

Response:

```
2
```

Returns `0` if the aggregate does not exist.

### 4. Read a Stream

`READ_STREAM` returns all events for an aggregate. The response consists of two length-prefixed frames: a JSON envelope followed by a binary batch containing the events.

```
READ_STREAM topic=orders key=order-123
```

Frame 1 (JSON envelope):

```json
{
  "topic": "orders",
  "key": "order-123",
  "partition": 2,
  "count": 2
}
```

Frame 2: Binary batch in standard 0xBA7C format containing the two events.

To read events starting from a specific version:

```
READ_STREAM topic=orders key=order-123 from_version=2
```

---

## Optimistic Concurrency

Every `APPEND_STREAM` call includes a `version` parameter that specifies the expected next version of the aggregate. The broker checks this against the current version before writing.

### Version Semantics

| version value | Meaning |
|---------------|---------|
| `1` | Expect this to be a new aggregate (current version must be 0) |
| `N` | Expect the current version to be `N-1` |

The broker rejects the write if the expected version does not match:

```
APPEND_STREAM topic=orders key=order-123 version=2 event_type=ItemAdded message={"sku":"X"}
```

If the current version is already 3:

```
ERROR: version_conflict current=3 expected=2
```

### Handling VERSION_CONFLICT

The standard pattern for handling conflicts is optimistic retry:

1. Reload the aggregate by reading the stream again.
2. Re-apply the domain command against the updated state.
3. Re-attempt the append with the correct version.
4. Repeat up to a maximum retry count (3 is recommended).

```
// Pseudocode
for retries := 0; retries < 3; retries++ {
    aggregate := loadFromStream("orders", "order-123")
    newEvent  := aggregate.handle(command)
    result    := appendStream("orders", "order-123", aggregate.version+1, newEvent)
    if result.ok {
        break
    }
    // VERSION_CONFLICT: loop retries with fresh state
}
```

Two concurrent writers for the same aggregate are serialized by the broker. Exactly one succeeds; the other receives VERSION_CONFLICT and must retry.

---

## Snapshots

As the number of events in a stream grows, replaying all events on every load becomes expensive. Snapshots solve this by persisting the aggregate state at a specific version. On subsequent reads, the broker returns the snapshot plus only the events that occurred after it.

### Saving a Snapshot

```
SAVE_SNAPSHOT topic=orders key=order-123 version=3 message={"customer":"alice","items":[...],"total":69.97,"status":"shipped"}
```

Response:

```
OK version=3 partition=2
```

Constraints:
- The `version` must be less than or equal to the current stream version.
- A new snapshot for the same aggregate overwrites the previous one.

### Reading a Snapshot

```
READ_SNAPSHOT topic=orders key=order-123
```

Response (JSON):

```json
{
  "version": 3,
  "payload": "{\"customer\":\"alice\",\"items\":[...],\"total\":69.97,\"status\":\"shipped\"}"
}
```

If no snapshot exists, the response is:

```
NULL
```

### Automatic Use in READ_STREAM

When a snapshot exists, `READ_STREAM` automatically uses it. The JSON envelope includes the snapshot, and the binary batch contains only events after the snapshot version:

```
READ_STREAM topic=orders key=order-123
```

Frame 1:

```json
{
  "topic": "orders",
  "key": "order-123",
  "partition": 2,
  "count": 2,
  "snapshot": {
    "version": 3,
    "payload": "..."
  }
}
```

Frame 2: Binary batch with events at versions 4 and 5 only.

### Snapshot Frequency Recommendation

Save a snapshot every 500 events. This keeps aggregate load times bounded while avoiding excessive snapshot writes. SDKs should implement auto-snapshot logic:

```
// Pseudocode
if aggregate.version % 500 == 0 {
    saveSnapshot(topic, key, aggregate.version, serialize(aggregate))
}
```

### Snapshot Locality

In distributed mode, snapshots are stored locally on each node. They are not replicated. If a node lacks a snapshot, it replays events from version 1. This is by design: snapshots are a performance optimization, not a correctness requirement.

---

## Schema Evolution

Events are immutable. Once written, their payload never changes. Schema evolution is handled at read time by the client application.

### Event Type and Schema Version

Each event carries two fields for schema management:

| Field | Description |
|-------|-------------|
| `event_type` | The event type name (e.g., `OrderCreated`, `ItemAdded`) |
| `schema_version` | An integer version for the event's schema (default: 1) |

Example:

```
APPEND_STREAM topic=orders key=order-123 version=4 event_type=OrderCreated schema_version=2 message={"customer":"alice","email":"alice@example.com","total":49.98}
```

### Upcasting in the SDK

When reading a stream, the SDK should check each event's `schema_version` and transform older schemas to the current version. This process is called upcasting.

```
// Pseudocode: upcaster chain
func upcast(eventType string, schemaVersion int, payload map[string]any) map[string]any {
    if eventType == "OrderCreated" && schemaVersion == 1 {
        // v1 -> v2: add email field with default
        payload["email"] = "unknown"
        schemaVersion = 2
    }
    return payload
}
```

Register upcasters per event type in the SDK. When `readStream` returns events, apply the upcaster chain before passing events to the domain model.

---

## Projections

Cursus does not provide a built-in projection engine. Instead, projections are built using the existing `CONSUME` and `STREAM` commands with consumer groups.

### Pattern

1. Create a consumer group on the event-sourcing topic.
2. Consume events using `CONSUME` or `STREAM` (continuous push mode).
3. For each event, update the read model (database, cache, search index).
4. Commit offsets after processing.

```
JOIN_GROUP topic=orders group=order-projections member=projector-1
CONSUME topic=orders partition=0 offset=0 member=projector-1-8374 group=order-projections
```

Because event-sourcing topics are regular Cursus topics, all consumer group features work: rebalancing, offset management, wildcard subscriptions, and batch commits.

### Multiple Projections

Different consumer groups can maintain independent projections of the same event stream:

- `order-summary-projection` -- updates a summary dashboard
- `order-search-projection` -- indexes orders into a search engine
- `order-analytics-projection` -- feeds an analytics pipeline

Each group tracks its own offsets and processes events independently.

---

## Command Reference

### APPEND_STREAM

Append an event to an aggregate stream with optimistic concurrency control.

```
APPEND_STREAM topic=<name> key=<aggregate_key> version=<N> event_type=<type> [schema_version=<N>] [metadata=<json>] message=<payload>
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| topic | Yes | - | Event-sourcing-enabled topic |
| key | Yes | - | Aggregate ID (used for partition routing) |
| version | Yes | - | Expected next version (must equal current + 1) |
| event_type | No | "" | Event type name |
| schema_version | No | 1 | Schema version for upcasting |
| metadata | No | "" | Arbitrary JSON metadata |
| message | Yes | - | Event payload (captures rest of line) |

Success response:

```
OK version=<N> offset=<N> partition=<N>
```

Error responses:

```
ERROR: version_conflict current=<N> expected=<N>
ERROR: topic '<name>' does not exist
ERROR: topic '<name>' is not event-sourcing enabled
```

### READ_STREAM

Read events from an aggregate stream. Returns two length-prefixed frames.

```
READ_STREAM topic=<name> key=<aggregate_key> [from_version=<N>]
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| key | Yes | - | Aggregate ID |
| from_version | No | 1 (or snapshot version + 1) | Starting version |

Frame 1 -- JSON envelope:

```json
{
  "topic": "...",
  "key": "...",
  "partition": 0,
  "count": 5,
  "snapshot": {"version": 500, "payload": "..."}
}
```

The `snapshot` field is present only when a snapshot exists at or after `from_version`.

Frame 2 -- Binary batch (0xBA7C format) containing the events.

### STREAM_VERSION

Get the current version of an aggregate stream.

```
STREAM_VERSION topic=<name> key=<aggregate_key>
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| key | Yes | - | Aggregate ID |

Response: Plain integer (e.g., `3`). Returns `0` if the aggregate does not exist.

### SAVE_SNAPSHOT

Save an aggregate snapshot at a specific version.

```
SAVE_SNAPSHOT topic=<name> key=<aggregate_key> version=<N> message=<payload>
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| key | Yes | - | Aggregate ID |
| version | Yes | - | Version this snapshot represents (must be <= current version) |
| message | Yes | - | Serialized aggregate state (captures rest of line) |

Success response:

```
OK version=<N> partition=<N>
```

Error responses:

```
ERROR: snapshot version <N> exceeds stream version <N>
ERROR: topic '<name>' does not exist
```

### READ_SNAPSHOT

Read the latest snapshot for an aggregate.

```
READ_SNAPSHOT topic=<name> key=<aggregate_key>
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| topic | Yes | - | Topic name |
| key | Yes | - | Aggregate ID |

Success response (JSON):

```json
{"version": 500, "payload": "..."}
```

Not found response:

```
NULL
```

---

## Best Practices

### Aggregate Design

- **One stream per aggregate instance.** Use the aggregate ID as the `key`. For example, each order gets its own stream keyed by `order-<uuid>`.
- **Keep events small.** Store only what changed, not the full state. State reconstruction is the job of the aggregate's `apply` method.
- **Use descriptive event type names.** `OrderCreated`, `ItemAdded`, `PaymentReceived` -- not `Update` or `Change`.
- **Include causation and correlation IDs in metadata.** This enables tracing command-to-event chains across aggregates.

### Snapshot Frequency

- Save a snapshot every 500 events as a starting point.
- Adjust based on aggregate load time requirements. If aggregates rarely exceed 100 events, snapshots may not be necessary.
- Never skip snapshots entirely for high-traffic aggregates. Without snapshots, loading an aggregate with 100,000 events requires replaying all of them.

### Partition Key Choice

- The `key` field determines partition routing. All events for the same aggregate always land on the same partition.
- Choose keys with good cardinality. UUIDs work well. Sequential integers can cause hot partitions if the hash distribution is poor.
- If you need cross-aggregate ordering guarantees, use a shared key (e.g., `customer-<id>`). But note this means all aggregates with that key share a single partition, which limits write throughput.

### Concurrency and Retries

- Always use optimistic concurrency (the `version` parameter). Appending without version checks defeats the purpose of event sourcing.
- Limit retries to 3 attempts. If a write still fails after 3 retries, the aggregate is under heavy contention. Consider redesigning the aggregate boundaries.
- In systems with eventual consistency, prefer idempotent event handlers over exactly-once delivery guarantees.

### Projections

- Use separate consumer groups for each projection. This allows independent progress and failure isolation.
- Projections should be rebuildable. If a projection gets corrupted, reset its consumer group offsets to zero and replay from the beginning.
- For time-sensitive projections, use `STREAM` (continuous push mode) instead of polling with `CONSUME`.
