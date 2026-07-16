# Platform-Specific Storage Paths

Cursus uses build-tagged implementations only where the operating system exposes a useful primitive. The persistent segment and index format is identical across supported platforms.

## Linux

`pkg/disk/flush_linux.go` implements `SendCurrentSegmentToConn` with `unix.Sendfile` when the destination is a `*net.TCPConn`. The handler flushes the buffered writer, obtains the raw socket descriptor through `SyscallConn`, and loops until the current segment bytes are transferred or an error occurs.

For a non-TCP `net.Conn`, Linux falls back to `io.Copy`.

## Non-Linux

`pkg/disk/flush_default.go` uses `io.Copy` after flushing and seeking the current segment to the beginning. This path covers Windows and other non-Linux builds.

## Directory Durability

Checkpoint replacement uses platform-specific parent-directory sync helpers. Unix builds call directory `Sync`; Windows uses the available portable behavior. The checkpoint file itself is written through a temporary file, synced, closed, and renamed before the parent-directory step.

## Explicit Non-Guarantees

The current code does not use `O_DIRECT`, universal `O_SYNC`, or a documented `fadvise` path. Do not rely on those behaviors when reasoning about durability or benchmark results. Periodic `file.Sync`, explicit flush/rotation/shutdown sync, HWM checkpoints, and replication decisions define the implemented contract.

## When Sendfile Helps

Zero-copy transfer can reduce CPU and userspace copying for whole current-segment transfers on Linux TCP sockets. Normal `CONSUME` reads use the sparse index and mmap decode path, so sendfile does not accelerate every consumer request. Measure the actual command path before attributing throughput differences to platform code.

## Validation

Run storage and network tests on every supported target because build tags compile different functions:

```bash
go test ./pkg/disk/...
GOOS=linux GOARCH=amd64 go test ./pkg/disk/...
GOOS=windows GOARCH=amd64 go test ./pkg/disk/...
```

Cross-compiled tests may only compile unless a matching runner is available. Use native restart/recovery tests to validate filesystem semantics.
