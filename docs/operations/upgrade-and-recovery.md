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

## Preflight

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
