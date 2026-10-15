# Retained bootstrap admission

`DiskHandler.SetBootstrapLimits(BootstrapLimits{MaxTransferBytes: ..., MaxTotalBytes: ...})`
sets positive byte limits for new exports and receives. `BootstrapLimits()` returns
the effective settings. Defaults are 8 GiB per transfer and 32 GiB across active
process-wide reservations, so a normal 1 GiB segment fits. Receiving reserves
space for the incoming records/catalog and locally generated indexes. Existing
filesystem headroom checks still apply independently.

Broker configuration wiring should apply the same settings to every disk handler
before exposing recovery endpoints, including handlers created after startup.
`MaxTotalBytes` must be at least `MaxTransferBytes`. The transfer maximum cannot
exceed the stable format bound, `math.MaxInt64 / 32`; that bound leaves room for
checked byte/index arithmetic and does not allocate a buffer. A broker should
choose limits from its expected retained history and available disk capacity.

Each admitted transfer captures its limits. Lowering settings rejects new work
that exceeds them but does not revoke already admitted transfers or durable
pending installations. Startup replay, matching pending/completed retries, and
completion validate the persistent format independently of admission settings.
This separation is required for recovery after an operator lowers limits.

Records are streamed in at most 256 KiB chunks and canonicalized one record at a
time. Raising byte limits does not allocate a transfer-sized memory buffer. The
catalog remains capped at 16 MiB; eight active transfers and a five-minute transfer
TTL remain fixed. Limit failures return `BootstrapLimitError` with resource,
requested bytes/count, and limit; they never trigger reset or truncation.

Network source capture should call `ExportBootstrapContext(ctx, start, end,
fence, catalog)` with its request deadline. It checks cancellation between records
and 256 KiB checksum reads, retires canceled staging for cleanup, and never returns a partial
export. `ExportBootstrap` remains a background-context compatibility wrapper.
Mutex acquisition and individual filesystem calls, especially an in-flight fsync,
are synchronous. Cancellation is checked after lock acquisition and flushing, so
this bounds ongoing copy/hash work rather than guaranteeing a strict lock-wait
deadline.

## Transient ownership and failed cleanup

Before durable intent, each transfer owns one active-transfer token and a separate
reservation for its staging bytes. The byte reservation covers the admitted
estimate and any growth reserved before a write; it is not a measurement of live
file lengths. A failed or partial deletion retains the entire reservation and the
active token until deletion succeeds. Closing retires the handle immediately, so
reads, writes, and installation cannot reuse partially removed staging. Repeated
close/abort calls retry cleanup.

Failed cleanup owners remain reachable in a process-wide deduplicated queue.
The eight-active-transfer limit also bounds this queue, including zero-byte
stages. Each new admission attempts at most one queued cleanup in round-robin
order before checking filesystem headroom. It skips a busy transfer without waiting for its lock, because admission
may already hold partition locks that installation needs. Filesystem deletion
runs outside the shared accounting and queue locks. A single filesystem call can
still block; no extra retry worker or timer is created. Eight undeletable stages
therefore reject new work instead of accumulating uncharged files. This trades
availability during persistent deletion failures for bounded staging ownership;
recovery retries deletion work and keeps reserved bytes charged conservatively.

Once a pending manifest exists, its StageID and catalog descriptor remain the
authoritative recovery inputs. Ownership passes to durable installation exactly
once; the transient cleanup queue must never delete those files. Existing replay
and completion rules, including lower-limit retries and successful completion
despite post-commit garbage-collection failure, remain unchanged. Startup orphan
cleanup still fails admission when deletion fails and preserves the pending
StageID under the partition's exclusive storage ownership.

Regression checks cover deletion failure followed by explicit retry, retired
handle rejection, TTL failure reachability, concurrent close/TTL/admission,
zero-byte saturation and recovery, busy-lock admission, constructor failure,
unchanged growth/overflow limits, and durable handoff with lower-limit restart
and preserved catalog bytes. Accounting checks compare active tokens and reserved
bytes before and after each operation; partial deletion must not reduce charges.

## Detached canonical staging

`NewDetachedBootstrapRoot(dir, limits, minFreeBytes, minFreePercent)` opens a
private ephemeral root with its own `.cursus.lock`. A future caller can place it
under the broker log directory, for example `.recovery-metadata`. The root has no
partition handler or worker. Startup removes only directories named
`.detached.bootstrap-<32 lowercase hexadecimal digits>`; unknown names and
partition bootstrap pending/completed files are preserved. Invalid recognized
entries or failed orphan deletion fail opening the root.

`root.Stage(topic, partition, metadata)` checks identity and the stable bootstrap
format, requires a zero-size catalog with the SHA-256 of empty bytes, and reserves
incoming bytes plus the full sparse-index upper bound before creating files.
It uses the same process-wide active-transfer and byte limits and the same
filesystem headroom guard as bootstrap transfers. A private child identity is
registered before filesystem creation, so closing the root cannot miss work in
progress. Chunk writes accept exact duplicates and reject holes, conflicting
bytes, overlapping extensions, and out-of-bounds requests.

`stage.Prepare(ctx)` verifies completeness, regular files, checksums, canonical
record identity/order, fixed logical bounds, and gap authority, then syncs the
files and builds the immutable on-disk sparse index. Durable installation and
preparation share these checks. Preparation alone creates no live partition
files, tail index, compaction marker, pending manifest, or HWM. An empty detached
index uses zero bytes; durable installation retains its live-index preallocation.
Preparation checks cancellation between checksum chunks and before frame reads
and decoding; individual filesystem operations remain synchronous.

Preparation moves the same child and reservation to `DetachedRawStore` and stops
its transfer TTL. A callback that was already running must observe the ownership
transition under the transfer mutex. Old stage abort, write, and prepare handles
cannot delete or mutate the prepared store. This first primitive retains its
active-transfer token until physical deletion, including while leases exist.
There is no metadata cache adoption or production provider integration yet.

`store.Acquire()` opens a `DetachedRawLease` with its own records/index handles,
fixed logical start/end, fixed raw byte length, and fixed index extent. Acquisition
performs no content scan. `lease.ReadRaw(ctx, start, maxRecords, maxBytes)` returns
all canonical `DiskMessage` fields, including transaction and control data, plus
`NextOffset`, `Exhausted`, and payload `DecodedBytes`. Page limits count payload
bytes, excluding four-byte frame headers. A first record exceeding the byte limit
returns `ErrRetainedPageLimit`; a later omitted frame may be decoded once and
retained for the next call. Every frame allocation is bounded by `MaxMessageSize`.
Errors and cancellation return no partial page.

Sequential pages reuse the lease cursor. Non-contiguous requests binary-search
16-byte on-disk index entries and decode only from the preceding sparse anchor.
Logical holes advance toward the fixed end without filtering physical records.
`lease.Work()` reports cumulative actual raw read bytes, index read bytes, and
decoded records, separately from the conservative staging-byte reservation.
Immutable ownership assumes no out-of-band file mutation; it is not a tampering
proof.

Store close retires new leases while existing leases remain readable. Root close
retires new work, skips busy child locks, and returns
`ErrDetachedBootstrapPending` while work, pins, or failed cleanup remain. Closing
is repeatable; individual deletion operations can still block. The nested kernel
lock remains held until an explicit root-close retry observes every child
physically removed. Failed cleanup enters the existing bounded process-wide
queue only after the last lease closes. Successful admission-triggered cleanup
also removes the root's child bookkeeping, without filesystem I/O under the root,
queue, or budget mutex. Neither reserved bytes nor active tokens are released
while pinned or unsuccessfully deleted files remain.

Regression coverage includes shared install/preparation rejection cases,
field-preserving sparse pages, actual read/decode costs, cancellation, a running
TTL callback during ownership transfer, creation/preparation versus root close,
lease survival after retirement, cleanup failure and admission retry, recognized
startup cleanup, and partial handle-open failure.
