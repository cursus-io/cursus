# API Reference

This document summarizes the Cursus TCP command API. The canonical response contract is defined in [Protocol Specification](../protocol-spec.md); this page is a command-oriented quick reference.

Cursus uses a length-prefixed TCP protocol. Every command request and text response is encoded as:

```text
[4-byte big-endian length][payload]
```

`CONSUME`, `STREAM`, and `READ_STREAM` are data-plane commands and can return one or more length-prefixed binary frames. Other control-plane commands return a single text or JSON response frame.

## Response Contract

Control-plane commands use one of these response forms:

```text
OK
OK key=value [key=value ...]
{"status":"OK", ...}
ERROR: <code> [key=value ...]
```

Clients should treat `OK`, `OK ...`, and JSON responses with `status:"OK"` as success. Clients should treat every `ERROR:` response as failure and branch on the machine-readable error code immediately after the prefix.

Legacy natural-language responses such as `Topic '<name>' now has <N> partitions`, `(no topics)`, or plain integer offsets are deprecated. SDKs may keep narrow fallback parsers for older brokers, but new client code should use the structured contract above.

## Topic Commands

### CREATE

```text
CREATE topic=<name> [partitions=<N>] [idempotent=<bool>] [event_sourcing=<bool>] [replication_factor=<N>] [retention_hours=<N>] [retention_bytes=<N>] [partitioner=<hash_key|round_robin>] [auth_policy=<open|deny_write|deny_read>]
```

Creates a topic or increases its partition count when the topic already exists.

Success:

```text
OK topic=<name> partitions=<N>
```

Common errors:

```text
ERROR: missing_topic expected="CREATE topic=<name> [partitions=<N>]"
ERROR: invalid_partitions reason="must be a positive integer"
ERROR: invalid_replication_factor reason="must be a positive integer"
ERROR: create_topic_failed reason="..."
```

### DELETE

```text
DELETE topic=<name>
```

Success:

```text
OK topic=<name> deleted=true
```

Common errors:

```text
ERROR: missing_topic expected="DELETE topic=<name>"
ERROR: topic_not_found topic=<name>
ERROR: delete_topic_failed reason="..."
```

### LIST

```text
LIST
```

Success:

```text
OK count=<N> topics=<comma-separated-topic-names>
```

When no topics exist, the broker returns:

```text
OK count=0 topics=
```

### DESCRIBE

```text
DESCRIBE topic=<name>
```

Success is a JSON topic metadata object with `status:"OK"`.

Common errors:

```text
ERROR: missing_topic expected="DESCRIBE topic=<name>"
ERROR: topic_not_found topic=<name>
ERROR: marshal_metadata_failed reason="..."
```

### HELP

```text
HELP
```

Success:

```text
OK commands=<comma-separated-command-names>
```

## Publish Commands

### PUBLISH

```text
PUBLISH topic=<name> [partition=<N>] [key=<routing-key>] [producerId=<id>] [seqNum=<N>] [epoch=<N>] [isIdempotent=<bool>] [acks=0|1|all] message=<payload>
```

Because `message=` captures the rest of the line, put optional parameters before `message=`.

For `acks=1` or `acks=all`, success is a JSON ack response with `status:"OK"`. Text `PUBLISH` may include `partition=<N>` to target a partition explicitly; otherwise the topic partition policy selects the partition. Idempotent publish uses `(producerId, epoch, seqNum)` per partition: each new `(producerId, epoch)` sequence starts at `seqNum=1`, higher epochs fence older producer sessions, lower epochs are rejected as stale, and `seqNum=0` disables dedup for that message. Distributed FSM snapshots that include producer epochs use snapshot version 3; avoid mixed-version rolling upgrades with binaries that cannot decode that snapshot state. For `acks=0`, success is:

```text
OK
```

Common errors:

```text
ERROR: missing_topic command=PUBLISH
ERROR: missing_message command=PUBLISH
ERROR: topic_not_found topic=<name>
ERROR: partition_not_found partition=<N>
ERROR: invalid_acks value=<value>
ERROR: invalid_seq_num reason="..."
ERROR: invalid_epoch reason="..."
ERROR: stale_producer_epoch reason="..."
```

### REPLICATE_MESSAGE

Internal replication command used between brokers.

Success:

```text
OK
```

Common errors:

```text
ERROR: missing_payload command=REPLICATE_MESSAGE
ERROR: unmarshal_failed reason="..."
ERROR: topic_not_found topic=<name>
ERROR: partition_not_found partition=<N>
ERROR: replica_append_failed reason="..."
```

## Consume Commands

### CONSUME

```text
CONSUME topic=<name> group=<group> partition=<N> offset=<N> member=<member-id> [batch=<N>]
```

`CONSUME` returns binary message frames. For consumer groups, the broker uses the committed offset for `(topic, group, partition)` as the authoritative resume point when one exists; otherwise the earliest offset policy is `0`. `CONSUME` is a stateless partition-leader read: ownership, liveness, and generation fencing are enforced by coordinator commands, not on the data path.

Common errors:

```text
ERROR: invalid_consume_syntax
ERROR: missing_topic command=CONSUME
ERROR: missing_partition command=CONSUME
ERROR: missing_offset command=CONSUME
ERROR: missing_member command=CONSUME
ERROR: invalid_partition
ERROR: invalid_offset
ERROR: NOT_LEADER LEADER_IS <host:port>
ERROR: OFFSET_OUT_OF_RANGE requested=<N> earliest=<N> latest=<N>
```

### STREAM

Continuous push-mode consume command.

```text
STREAM topic=<name> group=<group> partition=<N> offset=<N> member=<member-id>
```

`STREAM` returns one or more length-prefixed frames:

```text
[binary batch frame]
[00 00 00 00]                                  # zero-length keepalive
STREAM_CONTROL type=CLOSE reason=<stopped|removed|timeout|error|offset_out_of_range> offset=<nextOffset>
```

Clients must treat zero-length frames as keepalive. Like `CONSUME`, `STREAM` is a stateless partition-leader data path and does not validate group ownership or generation on every read. A `STREAM_CONTROL type=CLOSE` frame is a graceful terminator; `reason=offset_out_of_range` means the requested stream offset is older than the retained log. Clients should close the socket and resume through the consumer group offset contract or reset according to policy. Raw TCP disconnect without a close control frame remains possible on broker crash or network failure and should be treated as retryable.

Common errors:

```text
ERROR: invalid_stream_syntax
ERROR: missing_topic command=STREAM
ERROR: missing_partition command=STREAM
ERROR: missing_offset command=STREAM
ERROR: missing_member command=STREAM
ERROR: NOT_LEADER LEADER_IS <host:port>
```

## Consumer Group Commands

### REGISTER_GROUP

```text
REGISTER_GROUP topic=<name> group=<group>
```

Success:

```text
OK group=<group> topic=<name> registered=true
```

### JOIN_GROUP

```text
JOIN_GROUP topic=<name> group=<group> member=<member-id>
```

Success:

```text
OK member=<assigned-member-id> generation=<N>
```

### SYNC_GROUP

```text
SYNC_GROUP topic=<name> group=<group> member=<assigned-member-id>
```

Success:

```text
OK member=<assigned-member-id> generation=<N> assignments=<partition-list>
```

### HEARTBEAT

```text
HEARTBEAT topic=<name> group=<group> member=<assigned-member-id> [generation=<N>]
```

Success:

```text
OK member=<assigned-member-id> generation=<N>
```

### LEAVE_GROUP

```text
LEAVE_GROUP topic=<name> group=<group> member=<assigned-member-id>
```

Success:

```text
OK group=<group> member=<assigned-member-id> left=true
```


### LIST_OFFSETS

```text
LIST_OFFSETS topic=<name> [partition=<N>]
```

Returns retained and readable offset bounds for all partitions or one partition.

```text
OK topic=<name> partitions=<N> offsets=P0:earliest=<N>:latest=<N>:leo=<N>:hwm=<N>,P1:earliest=<N>:latest=<N>:leo=<N>:hwm=<N>
```

`latest` is the next readable committed offset and is the value SDKs should use for `auto_offset_reset=latest`. `leo` is the log end offset, and `hwm` is the high-water mark before the broker caps reads to the flushed durable tail.

Errors:

```text
ERROR: missing_topic command=LIST_OFFSETS
ERROR: topic_not_found topic=<name>
ERROR: invalid_partition command=LIST_OFFSETS
ERROR: partition_not_found partition=<N>
```

### FETCH_OFFSET

```text
FETCH_OFFSET topic=<name> group=<group> partition=<N>
```

Success:

```text
OK offset=<nextOffset>
```

When no offset has been committed, the broker returns `OK offset=0`.

### COMMIT_OFFSET

```text
COMMIT_OFFSET topic=<name> group=<group> partition=<N> offset=<nextOffset> [member=<member-id> generation=<N>]
```

The offset is the next offset to read after successful processing. Commits are monotonic per `(topic, group, partition)`: a commit lower than the current offset fails and does not rewind the group. When `member` or `generation` is supplied, both must be present and the member must own the partition in that generation. Legacy clients may omit both fields, but group-aware SDKs should send them.

Success:

```text
OK
```

Common errors:

```text
ERROR: invalid_offset
ERROR: invalid_generation command=COMMIT_OFFSET
ERROR: offset_regression reason="..."
ERROR: GEN_MISMATCH current=<N> requested=<N> group=<group> member=<member-id>
ERROR: NOT_OWNER partition=<N> member=<member-id> group=<group> generation=<N>
ERROR: member_not_found member=<member-id> group=<group>
ERROR: offset_manager_not_available
ERROR: commit_offset_failed reason="..."
```
### BATCH_COMMIT

```text
BATCH_COMMIT topic=<name> group=<group> member=<member-id> generation=<N> P0:<nextOffset>,P1:<nextOffset>
```

Success:

```text
OK batched=<N>
```

The `P` prefix in each partition entry is required. The broker validates `member` and `generation` before applying the batch and rejects the whole batch if any partition is no longer owned by that member.

Common errors:

```text
ERROR: invalid_batch_commit_format
ERROR: invalid_generation command=BATCH_COMMIT
ERROR: no_valid_offsets
ERROR: offset_regression reason="..."
ERROR: GEN_MISMATCH current=<N> requested=<N> group=<group> member=<member-id>
ERROR: NOT_OWNER partition=<N> member=<member-id> group=<group> generation=<N>
ERROR: member_not_found member=<member-id> group=<group>
ERROR: offset_manager_not_available
ERROR: bulk_commit_failed reason="..."
```
### GROUP_STATUS

```text
GROUP_STATUS group=<group>
```

Success is a JSON group status response with `status:"OK"`.

### FIND_COORDINATOR

```text
FIND_COORDINATOR group=<group>
```

Success:

```text
OK host=<host> port=<port>
```

In distributed mode, non-coordinator brokers can return:

```text
ERROR: NOT_COORDINATOR host=<host> port=<port>
```

## Cluster Commands

### METADATA

```text
METADATA topic=<name>
```

Success is a JSON metadata response with `status:"OK"`.

Common errors:

```text
ERROR: missing_topic command=METADATA
ERROR: topic_not_found topic=<name>
ERROR: fsm_not_available
ERROR: distribution_not_enabled
```

### RAFT_APPLY

Internal replication command used by distributed brokers.

Success:

```text
OK
```

Common errors:

```text
ERROR: missing_required_params command=RAFT_APPLY params=type,payload
ERROR: empty_required_params command=RAFT_APPLY params=type,payload
ERROR: distribution_required command=RAFT_APPLY
ERROR: invalid_payload_json reason="..."
ERROR: raft_apply_failed reason="..."
```

## Event Sourcing Commands

Event-sourcing commands are routed by aggregate `key`. In distributed mode, the broker handling the command must be the leader for the aggregate partition. A non-leader broker returns:

```text
ERROR: NOT_LEADER LEADER_IS <host:port>
```

Clients and SDKs should reconnect to that leader and retry. Followers index replicated event-sourcing records, apply quorum-replicated snapshots, and can pull missing snapshots from the partition leader with internal catch-up commands after restart. Partitions restore a synced high-watermark checkpoint with durable-tail clamping, so committed reads remain bounded by the last successful committed tail.



### BEGIN_TXN

```text
BEGIN_TXN transactional_id=<id> producerId=<producer-id> [epoch=<N>]
```

Starts a broker-managed transaction. In distributed mode, route transaction commands to `FIND_COORDINATOR transactional_id=<id>`; the coordinator key is `txn:<id>`. Success: `OK transactional_id=<id> state=open producerId=<producer-id> epoch=<N>`.

### TXN_PUBLISH

```text
TXN_PUBLISH transactional_id=<id> topic=<topic> [partition=<N>] producerId=<producer-id> seqNum=<N> epoch=<N> [key=<key>] message=<payload>
```

Stages one record in the transaction. `seqNum` is required and must be greater than zero; Cursus uses `(producerId, epoch, seqNum)` to make commit recovery idempotent even on non-idempotent topics. The record is not published until `END_TXN ... result=commit` succeeds. Committed records are stamped with transaction metadata before they enter the normal publish path, and commit/abort writes hidden Cursus transaction control markers to touched partition logs. The producer and epoch must match `BEGIN_TXN`; stale epochs are fenced.

### SEND_OFFSETS_TO_TXN

```text
SEND_OFFSETS_TO_TXN transactional_id=<id> producerId=<producer-id> epoch=<N> topic=<topic> group=<group> member=<member> generation=<N> P<partition>:<nextOffset>,P<partition>:<nextOffset>
```

Stages consumer offsets in the transaction. The broker validates group member, generation, partition ownership, and monotonic offsets before commit.

### END_TXN

```text
END_TXN transactional_id=<id> producerId=<producer-id> epoch=<N> result=<commit|abort>
```

Commits or aborts staged records and offsets. Transaction state is replicated in the metadata FSM and included in snapshots, committed records use the normal partition-leader publish path with forced idempotent sequence validation, finalization retries are idempotent for the same producer epoch, hidden Cursus transaction markers are appended to touched partition logs, startup recovery finalizes restored `committing` transactions, producer sequence state is rebuilt from partition logs, and committed reads hide transaction marker records plus open or aborted transactional records. Cursus still does not provide Kafka-compatible partition-log control batches or exactly-once external side effects.

### TXN_STATUS

```text
TXN_STATUS transactional_id=<id>
```

Returns transaction state and staged operation counts.

### APPEND_STREAM

```text
APPEND_STREAM topic=<name> key=<aggregate-key> version=<N> [event_type=<type>] [schema_version=<N>] [metadata=<json>] message=<payload>
```

Success:

```text
OK version=<N> offset=<N> partition=<N>
```

Common errors:

```text
ERROR: missing_topic
ERROR: missing_key
ERROR: missing_version
ERROR: invalid_version
ERROR: missing_message
ERROR: topic_not_found topic=<name>
ERROR: event_sourcing_not_enabled topic=<name>
ERROR: version_conflict current=<N> expected=<N>
ERROR: append_stream_failed reason="..."
```

### READ_STREAM

```text
READ_STREAM topic=<name> key=<aggregate-key> [from_version=<N>]
```

Success returns a JSON envelope frame with `status:"OK"`, followed by a binary batch frame containing committed events. Error envelopes use JSON with `status:"ERROR"`.

### STREAM_VERSION

```text
STREAM_VERSION topic=<name> key=<aggregate-key>
```

Success:

```text
OK version=<N>
```

### SAVE_SNAPSHOT

```text
SAVE_SNAPSHOT topic=<name> key=<aggregate-key> version=<N> message=<payload>
```

Success:

```text
OK version=<N> partition=<N>
```

Common errors:

```text
ERROR: snapshot_version_exceeds_stream version=<N> current=<N>
ERROR: snapshot_save_failed reason="..."
```

### READ_SNAPSHOT

```text
READ_SNAPSHOT topic=<name> key=<aggregate-key>
```

Success when a snapshot exists:

```text
OK snapshot={"version":500,"payload":"..."}
```

Success when no snapshot exists:

```text
OK snapshot=null
```


## Topic Policy Notes

- Minimal per-topic authorization policy is part of topic metadata: `auth_policy=open|deny_write|deny_read`. It rejects unauthorized topic reads/writes with `ERROR: NOT_AUTHORIZED_FOR_TOPIC ...`, but it is not caller identity-aware ACL/SASL yet. Use TLS and external network/application controls for authentication boundaries.
- Topics expose `retention_hours` and `retention_bytes` policy metadata. `0` means broker default. Reads before the earliest retained offset fail with `ERROR: OFFSET_OUT_OF_RANGE requested=<N> earliest=<N> latest=<N>`. SDKs should apply `auto_offset_reset` (`earliest`, `latest`, or `error`) to decide whether to reset or fail; `latest` should use `LIST_OFFSETS latest`, the next readable committed offset.
- `partitioner=hash_key` uses FNV-1a 64-bit hash modulo partition count for keyed messages and round-robin for missing keys. `partitioner=round_robin` ignores keys. Increasing partition count can remap future records for an existing key.

## Server-Level Errors

Malformed frames and handler failures also use the same error prefix:

```text
ERROR: decode_failed reason="..."
ERROR: malformed_input reason=missing_topic_or_payload
ERROR: command_failed reason="..."
ERROR: empty_command_response
ERROR: unknown_command command=<name>
ERROR: empty_command
```
