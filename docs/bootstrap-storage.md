# Bootstrap storage and publication

Bootstrap storage replaces a partition's retained log when ordinary replication
cannot recover a usable prefix. These APIs provide local staging, restart recovery,
and publication fences. The caller must validate cluster membership and leadership;
the persisted fence alone does not authorize a replica to join the ISR.

## Staging and installation

`ExportBootstrapContext` captures canonical records and a bounded snapshot catalog.
The logical start and end are independent of the physical record count: a marked
compacted range may contain holes or no records. Export preserves control records
and transaction metadata needed to rebuild local indexes.

`StageBootstrap` accepts two descriptors with fixed names and roles: `records`
(`canonical-log`) and `catalog` (`snapshot-catalog`). It rejects invalid identities,
bounds, checksums, and sizes before creating staging files. Admission accounts for
incoming bytes and derived indexes. Current admission settings do not invalidate
an already persisted installation intent.

Chunks must arrive in order. An exact duplicate byte range is accepted; conflicting
duplicates and gaps are rejected. A completed download is not yet an installation.
Installation checks file sizes and hashes, record framing, topic and partition
identity, logical bounds, and permitted offset gaps before writing durable intent.
A checksum-valid file can still fail these semantic checks.

Before durable intent, a failed installation preserves the old log and can be
aborted. After durable intent, the partition remains pending until replacement and
upper-layer recovery complete. Reopening validates the complete replacement and
deletion inventory before replaying it. Malformed intent fails recovery without
deleting either partition-owned files or neighboring partition files.

## Read visibility and completion

`BootstrapSession` drains existing read views and holds exclusive publication
access. Ordinary reads cannot observe a partially replaced partition. The caller
detaches derived file handles before installation, then reconstructs event-source,
producer, transaction, and checkpoint state before completion releases the pending
barrier. Installation success alone is insufficient to publish recovered state.

`WithBootstrapSource` captures one local generation without marking the source
pending. Its read view stops at the captured committed and flushed end even when
the physical log contains a speculative tail. The exported files remain usable
after the callback returns; the view and source callback object do not. Read views
also expire when callbacks return an error. Callbacks must finish local work and
detach results before making network or Raft calls.

## Regression checks

The storage, partition, and event-source suites cover interrupted replacement and
reopen, missing or corrupted staging files, invalid persistent inventories,
checksum-valid malformed records, compacted gaps, transaction reconstruction,
checkpoint failure and retry, callback lifetime, and committed source bounds.

These storage checks do not establish complete broker activation or mixed-version
recovery. Production capability activation and end-to-end recovery acceptance are
separate integration requirements.
