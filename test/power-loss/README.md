# Standalone guest power-loss check

Run from the repository root on a Docker host:

```sh
docker build -f test/power-loss/Dockerfile -t cursus-power-loss .
mkdir -p power-loss-results
docker run --rm -v "$PWD/power-loss-results:/results" cursus-power-loss
```

The container needs neither privileged mode nor host devices. QEMU software
emulation boots a disposable ext4 disk with `data=ordered`. The broker test
creates real records, group offsets, transaction participants, producer sequence
state, an event index and a snapshot through TCP commands. A test-only marker
stops the broker at one of the six journal rewrite phases. The driver then kills
QEMU without guest shutdown, sync or unmount and boots the same disk. Recovery
must complete the original reset, retain unrelated transaction references and
the producer epoch floor, and preserve fresh data on receipt replay.

This removes the guest kernel and its page cache. QEMU `cache=writeback` honors
guest flushes; `cache=unsafe` and `-snapshot` are intentionally not used. The host
kernel and physical device stay powered. This is not certification of a physical
controller's cache, sector atomicity or power-loss protection. The driver logs
QEMU, kernel-package and filesystem-tool versions; per-phase serial transcripts
are saved under `power-loss-results` even on failure.

The ordinary process-crash version runs without QEMU:

```sh
go test -tags=e2e_faults -race -count=1 ./cmd/broker \
  -run '^TestStandaloneBrokerTruncateJournalCrashRecovery$'
```

The fault functions are compiled only with `e2e_faults` and also require
`CURSUS_E2E_FAULTS=1`. Production builds contain no marker-triggered exit or
metadata-read failure. CI runs both the broker process check and the guest check
under the regular workflow filters.

References: [QEMU cache modes](https://qemu-project.gitlab.io/qemu/system/invocation.html)
and [Linux ext4 ordered-data mode](https://www.kernel.org/doc/html/v6.7/admin-guide/ext4.html).
