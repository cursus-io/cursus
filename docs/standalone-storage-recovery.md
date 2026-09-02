# Standalone Storage Recovery

This runbook covers offline inspection and explicit migration of standalone topic, consumer-group, and committed-offset storage. The broker runtime accepts only the current manifest and versioned metadata formats; older pre-manifest data must be converted on a stopped copy or replaced by a clean bootstrap. Run migration commands only while every broker process that can write the target directory is stopped. Take a volume snapshot or immutable copy first.

## Recovery contract

Standalone startup is ordered and fail-closed:

1. Load and validate the version-3 `{log_dir}/__topic_metadata.json`.
2. Restore every declared topic and validate the broker-owned `__consumer_offsets` topic.
3. Replay durable group registrations and tombstones by lifecycle epoch.
4. Replay complete committed-next-offset snapshots by revision.
5. Enable broker readiness and the command listener.

The command listener is not opened if any earlier step fails. `/live` remains available in diagnostics-only mode, `/ready` returns `503`, and the failed check explains whether topic metadata, storage, or consumer metadata recovery failed. A missing/corrupt manifest, an omitted persisted topic, an unreadable path, a corrupt/truncated internal record, an invalid record key, a conflicting epoch/revision, or an offset regression is never converted into an empty healthy broker.

A successful standalone `REGISTER_GROUP`, `COMMIT_OFFSET`, `BATCH_COMMIT`, or group deletion means the corresponding internal record was synchronously appended and the authoritative data file crossed the filesystem sync boundary. A process or container kill after that acknowledgement therefore replays the acknowledged state. Lower offsets remain rejected before and after restart.

## Consumer metadata format

`__consumer_offsets` remains a normal partition-log encoding at the disk layer, but its payload and key have a broker-owned versioned contract. New records use JSON version `1`:

```json
{
  "version": 1,
  "type": "group_registration",
  "group": "wargame-production-iocp-match-events-topic",
  "topic": "match.events.topic",
  "partition_count": 4,
  "epoch": 1,
  "timestamp": "2026-08-13T00:00:00Z"
}
```

```json
{
  "version": 1,
  "type": "offset_snapshot",
  "group": "wargame-production-iocp-match-events-topic",
  "topic": "match.events.topic",
  "epoch": 1,
  "revision": 7,
  "offsets": [
    {"partition": 0, "offset": 128},
    {"partition": 1, "offset": 91}
  ],
  "timestamp": "2026-08-13T00:00:01Z"
}
```

```json
{
  "version": 1,
  "type": "group_tombstone",
  "group": "wargame-production-iocp-match-events-topic",
  "topic": "match.events.topic",
  "epoch": 2,
  "timestamp": "2026-08-13T00:00:02Z"
}
```

Registration and tombstone records share a stable `cursus.consumer.group.v1.<sha256(group)>` compaction key. Offset snapshots use `cursus.consumer.offset.v1.<sha256(group NUL topic)>`. A snapshot is the complete set of committed keys currently known for that group/topic, not a delta. Lifecycle epochs fence records from a deleted/re-created group; revisions order snapshots even when internal-topic partition expansion changes physical replay order. Repeating the same registration is idempotent. A tombstone is written before in-memory deletion, so older registration and offset records cannot revive the group.

The internal topic is always forced to compact cleanup with unlimited time/size retention. Broker defaults and application `CREATE` policy cannot enable delete retention, set a retention limit, change its mode, or delete the topic. Compaction must retain the latest lifecycle record and latest complete offset snapshot for every semantic key.

The broker runtime decoder rejects earlier single-offset and bulk-offset JSON payloads. The read-only storage inspector can identify those records, and the offline migration command can copy only operator-selected values into a versioned migration authority file. When no versioned lifecycle exists, the migration tool accepts a group only when its selected records identify exactly one topic; the operator must still supply the authoritative partition count, including for groups that never committed an offset.

## Read-only inventory

The production image contains `/root/cursus-storage`. These commands do not create a topic, group, offset, index, checkpoint, or manifest:

```sh
cursus-storage manifest inspect --log-dir /var/lib/cursus/logs > inventory.json
cursus-storage consumer-metadata inspect --log-dir /var/lib/cursus/logs > consumer-records.json
```

The full manifest inventory reports every topic and partition, active and `.deleted` log segments, the active segment marker, segment base/first/last offsets and record counts, log start/end, HWM, and decoded active/deleted consumer metadata records. Problems such as truncation, malformed frames, path mismatches, non-contiguous active logs, or an HWM beyond log end are reported rather than repaired.

A `.log.deleted` consumer-offset segment is never renamed or automatically returned to the live log. Without an explicit migration selection, its presence prevents the internal handler from opening. Selecting one of its records copies only the operator-approved semantic value into the migration authority file; the `.deleted` evidence remains unchanged.

## Migrating a pre-manifest directory

Do this on an offline snapshot or with all writers stopped.

1. Save the full inventory and review every reported problem. Do not proceed while `problems` is non-empty.
2. Independently establish each intended group-to-topic mapping and exact partition count. Do not infer an offset that is absent from the inventory.
3. Create a strict selection file. Every selected record is addressed by segment state, internal log partition, segment base, and logical record offset:

```json
{
  "version": 1,
  "groups": [
    {
      "group": "wargame-production-iocp-match-events-topic",
      "topic": "match.events.topic",
      "partition_count": 4,
      "records": [
        {
          "segment_state": "active",
          "log_partition": 1,
          "segment_base": 0,
          "record_offset": 42
        }
      ]
    },
    {
      "group": "wargame-production-iocp-chat-message",
      "topic": "chat.message",
      "partition_count": 2,
      "records": []
    }
  ]
}
```

An empty `records` list deliberately restores a registered group with no commit; `FETCH_OFFSET` will return `OK offset=0`. To select an operator-reviewed record from retained evidence, use `"segment_state":"deleted"`; this is the only path by which a deleted record can influence migration. To preserve a known deletion, use `"deleted":true` with an empty record list, which creates a lifecycle tombstone.

4. Validate without writing:

```sh
cursus-storage consumer-metadata migrate \
  --log-dir /var/lib/cursus/logs \
  --selection consumer-selection.json \
  --dry-run > consumer-migration-dry-run.json
```

The result includes the complete inventory, selected versioned records, and an inventory SHA-256. Compare it to the saved inventory.

5. Commit the selection:

```sh
cursus-storage consumer-metadata migrate \
  --log-dir /var/lib/cursus/logs \
  --selection consumer-selection.json
```

This exclusively creates `{log_dir}/__consumer_metadata_migration.json`, syncs its contents and parent directory, and never overwrites an existing migration authority. Repeating the same selection is idempotent; a different selection fails.

6. Prepare a version-2 topic definition file that covers every persisted topic directory, including `__consumer_offsets`. The internal definition must be non-idempotent, non-event-sourcing, and compact; the migration command canonicalizes its retention to unlimited:

```json
{
  "version": 2,
  "topics": [
    {
      "name": "__consumer_offsets",
      "revision": 1,
      "partitions": 4,
      "replication_factor": 3,
      "idempotent": false,
      "event_sourcing": false,
      "policy": {
        "cleanup_policy": "compact",
        "partitioner": "hash_key",
        "auth_policy": "open",
        "retention_hours": 0,
        "retention_bytes": 0
      }
    }
  ]
}
```

Include the exact policy and partition count for every application topic as well.

7. Validate, then exclusively create the topic manifest:

```sh
cursus-storage manifest create \
  --log-dir /var/lib/cursus/logs \
  --definitions topic-definitions.json \
  --dry-run > manifest-dry-run.json

cursus-storage manifest create \
  --log-dir /var/lib/cursus/logs \
  --definitions topic-definitions.json
```

The command performs a second inventory comparison immediately before publication, writes and syncs a same-directory temporary file, installs the manifest without overwrite, and syncs the directory. An existing manifest is never replaced. Repeating an identical migration is idempotent; different definitions fail.

8. Re-run both inspect commands. Confirm the manifest, migration inventory fingerprint, topic/partition ranges, selected records, and untouched `.deleted` files before starting a broker.

## Post-migration validation

First validate the migrated copy with the exact candidate image and the same mount path/configuration used in production. Keep `WARGAME_BROKER_ALLOW_NEW_CONSUMER_GROUP_BOOTSTRAP=0`; Cursus recovery does not read or depend on that Wargame QA flag.

Check:

- `/ready` becomes `200` only after topic and consumer metadata recovery;
- `LIST_GROUPS` contains the expected groups;
- `GROUP_STATUS group=<group>` preserves the topic and partition count;
- `FETCH_OFFSET topic=<topic> partition=<N> group=<group>` returns each selected committed next offset, or `0` for an explicitly registration-only group;
- `LIST_OFFSETS topic=<topic>` reports the expected earliest/latest/LEO/HWM boundaries;
- `cursus_consumer_metadata_recovery_ready{phase="ready"}` is `1`;
- restored group/offset/record counts match the reviewed selection;
- orphan and corrupt record metrics are understood and corrupt records are `0`.

Then stop the broker, restart it against the same copy, and repeat every query. Also perform a container/VM restart test and verify that an acknowledged test offset remains exact and a lower commit is rejected. The four query commands above are read-only; they do not bootstrap groups or append offsets.

Relevant metrics are:

- `cursus_topic_metadata_restored_topics`
- `cursus_consumer_metadata_recovery_ready{phase=...}`
- `cursus_consumer_metadata_restored_groups`
- `cursus_consumer_metadata_restored_offsets`
- `cursus_consumer_metadata_replayed_records`
- `cursus_consumer_metadata_orphan_records`
- `cursus_consumer_metadata_corrupt_records`

Do not start production from the original PVC until the dry-run artifacts, explicit record selection, manifest definitions, readiness result, query output, and repeated-restart result have been reviewed. Never reset/guess an offset or rename a `.deleted` segment as part of this procedure.
