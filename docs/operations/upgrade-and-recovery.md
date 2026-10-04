# Upgrade And Recovery

## Support Boundary

| Persistent artifact | Current writer | Older reader support | Upgrade rule |
|---|---:|---|---|
| Topic manifest | 1 | pre-manifest storage requires offline migration | Do not start a current broker against undeclared persisted topics. |
| Transaction journal | 1 | bare legacy snapshots are readable | Preserve the journal with all partition and offset data. |
| Consumer metadata records | 3 | earlier record forms are replayed | Upgrade readers before enabling a newer writer contract. |
| Raft FSM snapshot | 8 | versions 0-7 are restored with documented defaults | Do not run binaries that cannot decode the persisted snapshot version. |

Mixed-version rolling upgrades across a format boundary are unsupported. A
normal restart of the same compatible release is supported; a format-changing
upgrade is a coordinated whole-cluster operation.

Each broker exclusively locks its log directory before opening metadata or
recovering partitions. A second broker using the same directory fails startup;
each partition also rejects a second storage handler. The filesystem must
support the operating system's exclusive file locks. Locks are released on
normal shutdown or process exit, including a crash. The `.cursus.lock` and
`partition_<id>.lock` files remain on disk and are safe to retain in backups.
Do not delete or replace them while a broker is running: the file's existence
does not indicate a stale lock, and removing it can defeat mutual exclusion.

## Preflight

Validate a copied backup before using it for a rollback or recovery. The command is read-only: it verifies the explicit topic manifest against the persisted partition layout, scans topic and consumer metadata records, and validates any transaction-journal framing and checksums without starting a broker.

```bash
cursus-storage backup validate --log-dir /backup/cursus-logs
```

It exits non-zero if the manifest is missing or if validation finds a storage problem. Preserve its JSON output with the backup record; it is the operator evidence that the copy was restorable at the time it was made.

For the fixed three-member Kubernetes topology, use the separate [Kubernetes cluster runbook](kubernetes-cluster.md). Its StatefulSet keeps one PVC per member and requires a one-member-at-a-time restart; never combine PVCs from different backup generations.

Before changing any binary or configuration, stop writes and record the target
release, `git`/image digest, configuration checksum, member list, leader, ISR,
and available disk space. Take one immutable backup generation containing the
topic manifest, transaction journal, consumer-offset logs, every `.log`, its
matching `.index`, and each `.log.compacted-<size>` sidecar.

Run these read-only checks against the copy, not the production volume:

```sh
cursus-storage manifest inspect --log-dir /var/lib/cursus/logs > inventory.json
cursus-storage consumer-metadata inspect --log-dir /var/lib/cursus/logs > consumer-records.json
```

Do not proceed if either output reports problems, orphaned topic directories,
or an invalid compaction sidecar. Keep the inventory and the backup generation
identifier with the change record.

## Coordinated Upgrade

1. Drain clients and confirm a healthy quorum/ISR.
2. Validate the immutable backup generation on a separate copy.
3. Stop every broker; do not leave old writers running.
4. Deploy the candidate binaries and exact reviewed configuration to all
   members.
5. Start a quorum, then wait for leader election, ISR recovery, and readiness.
6. Validate topics, groups, committed offsets, transaction recovery, and an
   acknowledged test publish/consume before restoring client traffic.

## Rollback And Restore

If the candidate cannot read or safely operate on the persisted format, stop
the complete cluster. Do not live-downgrade binaries. Restore the validated
single backup generation and start the previously compatible release as a
quorum. Re-run the readiness and read-only checks above before admitting
clients. A partial file restore, a missing compaction sidecar, or mixing files
from generations is not a supported recovery procedure.
