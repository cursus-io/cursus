# Command Interface

Cursus clients use 4-byte big-endian length-prefixed frames over TCP. Most requests contain a UTF-8 text command with `key=value` parameters. Successful text responses start with `OK`; failures start with `ERROR: <code>`. Binary consume/stream frames are documented separately in the [wire protocol specification](../protocol-spec.md).

## Command Map

| Area | Commands | Route |
|---|---|---|
| Protocol and auth | `PROTOCOL_INFO`, `NEGOTIATE`, `AUTH` | Any broker |
| Topic admin | `CREATE`, `DELETE`, `LIST`, `DESCRIBE`, `HELP` | Any broker; distributed mutations use metadata consensus |
| Produce and read | `PUBLISH`, `CONSUME`, `STREAM`, `LIST_OFFSETS` | Partition leader |
| Consumer groups | `REGISTER_GROUP`, `FIND_COORDINATOR`, `JOIN_GROUP`, `SYNC_GROUP`, `HEARTBEAT`, `LEAVE_GROUP`, `GROUP_STATUS`, `FETCH_OFFSET`, `COMMIT_OFFSET`, `BATCH_COMMIT` | Group coordinator except discovery |
| Transactions | `INIT_PRODUCER_ID`, `BEGIN_TXN`, `TXN_PUBLISH`, `SEND_OFFSETS_TO_TXN`, `END_TXN`, `TXN_STATUS` | Transaction coordinator selected by `transactional_id` |
| Event sourcing | `APPEND_STREAM`, `READ_STREAM`, `STREAM_VERSION`, `SAVE_SNAPSHOT`, `READ_SNAPSHOT` | Aggregate partition leader |
| Cluster admin | `METADATA`, `CLUSTER_STATUS`, `ELECT_LEADER` | Any broker or current metadata leader as documented |

## Routing Rules

1. Discover group coordinators with `FIND_COORDINATOR group=<group>` and transaction coordinators with `FIND_COORDINATOR transactional_id=<id>`.
2. Discover partition leaders with `METADATA topic=<topic>`.
3. On `ERROR: NOT_COORDINATOR host=<host> port=<port>`, update the coordinator cache and retry only when the error registry marks the operation retryable.
4. On `ERROR: NOT_LEADER LEADER_IS <host:port>`, update partition metadata and retry the leader-routed command.
5. Treat fencing, validation, and authorization errors as terminal until client state or credentials change.

## Examples

```text
CREATE topic=orders partitions=12
PUBLISH topic=orders key=customer-42 message={"orderId":"o-1"}
FIND_COORDINATOR group=order-workers
JOIN_GROUP topic=orders group=order-workers member=worker-1
FETCH_OFFSET topic=orders group=order-workers partition=0
CONSUME topic=orders group=order-workers partition=0 offset=0 member=<member-id> generation=<N> isolation=read_committed batch=128
COMMIT_OFFSET topic=orders group=order-workers partition=0 offset=<lastProcessedOffset+1> member=<member-id> generation=<N>
```

`COMMIT_OFFSET` values are next offsets, not last processed offsets. Stored offsets are monotonic and authoritative for resume. Refer to the [API reference](api-reference.md) for complete parameters and responses; internal replication commands are intentionally excluded from the client interface.
