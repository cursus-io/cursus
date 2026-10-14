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
and 256 KiB checksum reads, removes canceled staging, and never returns a partial
export. `ExportBootstrap` remains a background-context compatibility wrapper.
Mutex acquisition and individual filesystem calls, especially an in-flight fsync,
are synchronous. Cancellation is checked after lock acquisition and flushing, so
this bounds ongoing copy/hash work rather than guaranteeing a strict lock-wait
deadline.
