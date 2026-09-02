# Command Interface

Cursus clients use Wire v2 `CRS2` frames over TCP after a required binary handshake. The frame header carries the command ID and request ID; most payloads use the deterministic `CRQ2` field schema. The text forms in this document are readable representations of those fields. Binary batches and stream frames are documented in the [wire protocol specification](../protocol-spec.md).

## Operator CLI

The container image includes `/app/cursusctl` for one explicit Wire v2 command
per invocation. It is intended for approved operational actions such as catalog
creation, status inspection, and an explicit `REGISTER_GROUP`; it does not
persist credentials or execute a command file.

```text
/app/cursusctl --broker cursus:9000 LIST
/app/cursusctl --broker cursus:9000 CREATE topic=orders partitions=3
/app/cursusctl --broker cursus:9000 REGISTER_GROUP topic=orders group=workers
```

When SASL is enabled, pass `--principal` plus `--auth-token-env NAME`. The
token is read only from `NAME` and is never accepted as a command-line value.

## Command Map

| Area | Commands | Route |
|---|---|---|
| Connection and auth | Wire v2 binary handshake, `AUTH` | Any broker |
| Topic admin | `CREATE`, `DELETE`, `TRUNCATE`, `LIST`, `DESCRIBE`, `HELP` | Any broker; distributed mutations use metadata consensus |
| Produce and read | `PUBLISH`, `CONSUME`, `STREAM`, `LIST_OFFSETS` | Partition leader |
| Consumer groups | `REGISTER_GROUP`, `FIND_COORDINATOR`, `JOIN_GROUP`, `SYNC_GROUP`, `HEARTBEAT`, `LEAVE_GROUP`, `GROUP_STATUS`, `FETCH_OFFSET`, `COMMIT_OFFSET`, `BATCH_COMMIT` | Group coordinator except discovery |
| Transactions | `INIT_PRODUCER_ID`, `BEGIN_TXN`, `TXN_PUBLISH`, `SEND_OFFSETS_TO_TXN`, `END_TXN`, `TXN_STATUS` | Transaction coordinator selected by `transactional_id` |
| Event sourcing | `APPEND_STREAM`, `READ_STREAM`, `STREAM_VERSION`, `SAVE_SNAPSHOT`, `READ_SNAPSHOT` | Aggregate partition leader |
| Cluster admin | `METADATA`, `CLUSTER_STATUS`, `ELECT_LEADER` | Any broker or current metadata leader as documented |

## Routing Rules

1. Discover group coordinators with `FIND_COORDINATOR group=<group>` and transaction coordinators with `FIND_COORDINATOR transactional_id=<id>`.
2. Discover partition leaders with `METADATA topic=<topic>`.
3. On `ERROR: NOT_COORDINATOR host=<host> port=<port>`, update the coordinator cache and retry only when the error registry marks the operation retryable.
4. On `ERROR: NOT_LEADER leader=<host:port>`, update partition metadata and retry the leader-routed command.
5. Treat fencing, validation, and authorization errors as terminal until client state or credentials change.

## Examples

```text
CREATE topic=orders partitions=12
CREATE topic=orders retention_hours=168
DELETE topic=retired-orders if_exists=true
TRUNCATE topic=test-orders expected_revision=7
PUBLISH topic=orders key=customer-42 message={"orderId":"o-1"}
FIND_COORDINATOR group=order-workers
JOIN_GROUP topic=orders group=order-workers member=worker-1
FETCH_OFFSET topic=orders group=order-workers partition=0
CONSUME topic=orders group=order-workers partition=0 offset=0 member=<member-id> generation=<N> isolation=read_committed batch=128
COMMIT_OFFSET topic=orders group=order-workers partition=0 offset=<lastProcessedOffset+1> member=<member-id> generation=<N>
```

The second `CREATE` is a patch: omitted fields retain their current values. Explicit `retention_hours=0`, `idempotent=false`, or `read_acl=` are distinct from omission. Immutable mode or replication changes are rejected, while effective mutable changes advance the definition revision returned by `CREATE`, `METADATA`, and `DESCRIBE`.

`DELETE` is admin-only and `if_exists=true` is an explicit idempotency choice for approved retries. Do not derive it from a topic missing from desired state or use delete-and-create as a reset substitute. `TRUNCATE` is also admin-only; it requires the current definition revision and returns a new revision and lifecycle epoch. Active groups or transactions block it, and old-epoch writes are fenced after it commits.

`COMMIT_OFFSET` values are next offsets, not last processed offsets. Stored offsets are monotonic and authoritative for resume. Refer to the [API reference](api-reference.md) for complete parameters and responses; internal replication commands are intentionally excluded from the client interface.
