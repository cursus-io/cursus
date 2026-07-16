# Segment Management

Cursus partitions use base-offset-named data and sparse index files. Segment lifecycle is owned by `DiskHandler`; `DiskManager` only registers and closes handlers.

## Naming

```text
{log_dir}/{topic}/partition_{partition}_segment_{baseOffset:020d}.log
{log_dir}/{topic}/partition_{partition}_segment_{baseOffset:020d}.index
```

`baseOffset` is the first logical record offset assigned to the segment. Sorted filenames therefore preserve logical segment order.

## Roll Conditions

The active segment rolls before a write when any condition is true:

- data bytes would exceed `log_segment_bytes` (default 1 GiB),
- the next sparse entry would exceed `log_index_size_bytes` (default 10 MiB),
- the segment has reached `log_segment_roll_ms` (default seven days).

Rotation flushes and syncs the old data file, flushes/closes its index, resets byte/index positions, creates the new base-offset files, and records the new segment in the ordered list. A single record may exceed the nominal remaining space only when its validated size requires a fresh empty segment; records larger than the broker maximum are rejected.

## Startup

On open, the handler:

1. removes completed deletion tombstones,
2. lists and sorts segment files,
3. parses base offsets from filenames,
4. scans and repairs the active tail,
5. opens the active data and index files,
6. restores next offset from valid records.

Malformed filenames are skipped with an error log; an invalid active tail is truncated to the last valid record and synced.

## Read Safety

Reads copy the current ordered segment list and mmap each selected file. Active-reader accounting and the retention deletion lifecycle prevent a segment from disappearing while a read is using it. Sparse index entries map logical offsets to byte positions, after which the reader validates contiguous offsets.

## Retention

Retention evaluates closed segments by age and total retained bytes. The active segment is never selected. Deletion is the only supported cleanup policy; log compaction is not implemented.

After retention, `GetFirstOffset` exposes the new earliest base offset. A client requesting older data receives `OFFSET_OUT_OF_RANGE` and must follow its configured reset/error policy.

## Concurrency

- `mu` protects segment metadata and active file ownership.
- `ioMu` serializes buffered writer and file operations.
- `indexMu` protects index writer/mapper lifecycle.
- lock order for write/rotation paths is metadata, then I/O; index lifecycle uses its dedicated lock.

## Operational Checks

Monitor segment count, bytes, active readers, pending writes, earliest/latest offsets, and retention-gap metrics. Back up closed segments and coordinator metadata consistently. In standalone mode this includes the internal consumer offset topic and `__transaction_state.journal`; copying only topic segment `.log` files does not preserve coordinator authority.
