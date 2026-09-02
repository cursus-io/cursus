# Standalone Clean-Bootstrap Recovery

This runbook describes the only supported recovery boundary for storage created
before the current Cursus formats: a complete clean bootstrap. Cursus does not
convert, import, select, or preserve pre-manifest topics, unversioned consumer
offsets, older transaction journals, or partial copies of broker state.

Do not remove production data while a broker can still write it. Stop every
writer first, verify the exact Cursus-owned paths, and take an immutable backup
when forensic retention is required.

## Current recovery contract

Standalone startup is ordered and fail-closed:

1. Load and strictly validate the version-3 `{log_dir}/__topic_metadata.json`.
2. Restore every declared topic and verify there is no undeclared persisted
   topic directory.
3. Validate the broker-owned `__consumer_offsets` topic and replay only
   version-1 metadata records with their required stable keys.
4. Replay the current checksummed transaction journal.
5. Reconcile partition checkpoints and expose the client listener only after
   every recovery dependency is ready.

The runtime rejects an older or missing manifest beside persisted topic logs,
unversioned single/bulk offset JSON, an unsupported journal, malformed record
keys, conflicting lifecycle epochs or revisions, offset regression, unknown
fields, partial state, and corrupt non-tail data. It never guesses defaults or
turns unsupported state into an empty healthy broker. `/live` remains available
for diagnostics while `/ready` returns `503`.

Successful standalone group registration and offset commits synchronously cross
the authoritative filesystem durability boundary. Current compacted
`__consumer_offsets` logs may begin above physical offset zero; replay validates
the retained latest registration, tombstone, and complete offset-snapshot
records directly. No external migration-authority file is recognized.

## Read-only inspection

The `cursus-storage` binary exposes inspection and orphan-archive commands only:

```sh
cursus-storage manifest inspect --log-dir /var/lib/cursus/logs > inventory.json
cursus-storage consumer-metadata inspect --log-dir /var/lib/cursus/logs > consumer-records.json
cursus-storage orphan inspect --log-dir /var/lib/cursus/logs > orphans.json
```

Inspection does not create a topic, group, offset, index, checkpoint, manifest,
or recovery authorization. The consumer metadata inspector accepts only current
versioned records and reports unversioned payloads as clean-bootstrap problems.
The orphan archive command moves one manifest-omitted directory to a verified
location outside `log_dir`; it cannot make that data runnable by the broker.

## Clean-bootstrap procedure

The following is an operator-owned destructive transition, not an automatic
broker action:

1. Stop the broker and every application that can write its storage.
2. Resolve and record the exact standalone Cursus `log_dir` and any separate
   transaction, checkpoint, or producer-state paths configured for that broker.
3. Confirm that each resolved target belongs only to this Cursus instance. Do
   not use a filesystem root, home directory, unresolved environment variable,
   wildcard, or shared application volume as a deletion target.
4. Take a volume snapshot or move the verified state to a read-only forensic
   archive when retention is required.
5. Remove the complete Cursus persistence unit: topic manifest, every topic
   partition directory including `__consumer_offsets`, transaction journal,
   HWM checkpoints, producer state, event indexes/snapshots, and temporary
   recovery artifacts. Partial deletion is unsupported.
6. Start the current binary against the empty verified storage root and create
   topics and consumer groups through the current Wire v2 APIs.
7. Confirm `/ready == 200`, expected topic definitions, exact partition counts,
   empty initial offsets, and zero recovery errors before resuming producers or
   consumers.
8. Commit a test offset, restart the broker, and verify the exact committed-next
   offset and topic policies are restored before production traffic resumes.

Never rename a `.deleted` segment back into the active log, manufacture a
manifest, copy a selected historical offset into current state, or retain only
one component of the old persistence unit.

## Current consumer metadata format

`__consumer_offsets` stores version-1 `group_registration`, complete
`offset_snapshot`, and `group_tombstone` JSON records inside the current segment
format. Registration and tombstone records use
`cursus.consumer.group.v1.<sha256(group)>`; offset snapshots use
`cursus.consumer.offset.v1.<sha256(group NUL topic)>`. Lifecycle epochs fence
group deletion/re-creation and revisions order complete offset snapshots.

The internal topic is always compacted with unlimited time/size retention.
Application `CREATE`, broker defaults, and restored manifests cannot weaken its
policy, enable idempotent/event-sourcing mode, or delete it.

Relevant recovery metrics include:

- `cursus_topic_metadata_restored_topics`
- `cursus_consumer_metadata_recovery_ready{phase=...}`
- `cursus_consumer_metadata_restored_groups`
- `cursus_consumer_metadata_restored_offsets`
- `cursus_consumer_metadata_replayed_records`
- `cursus_consumer_metadata_orphan_records`
- `cursus_consumer_metadata_corrupt_records`
