# Acknowledged storage recovery rehearsal

## Design and acceptance contract

Kafka 4.1's [flush guidance](https://kafka.apache.org/41/operations/hardware-and-os/)
distinguishes replication durability from per-write filesystem synchronization.
Its versioned [LogLoader](https://github.com/apache/kafka/blob/4.1.0/storage/src/main/java/org/apache/kafka/storage/internals/log/LogLoader.java)
rebuilds missing or corrupt derived indexes from the log during recovery.
Cursus adopts the distinction between authoritative records and derived indexes,
but retains its own standalone synchronous acknowledgement contract: complete
checksummed record corruption must fail closed, not be treated as a partial tail
that can be discarded. This trades availability for preserving acknowledged data
and forensic evidence. No storage format or acknowledgement policy changes here.

The [Kafka upgrade procedure](https://kafka.apache.org/41/getting-started/upgrade/)
separates binary rollout from feature activation. Cursus still requires a
coordinated upgrade across incompatible persistent formats. A same-binary restore
rehearsal cannot establish mixed-version compatibility or authorize a downgrade.

The regression boundaries are defined before implementation:

- Write 32 identified records over the public broker protocol and acknowledge a
  consumer offset. Kill the ext4 guest immediately after the final ACK, without
  a test-side fsync, shutdown or unmount. Boot the same disk, check every payload
  and offset through the public consumer, retry the last producer identity, and
  require a new append to advance exactly once.
- Stop a seeded standalone broker, copy its entire persistence unit into one
  backup generation, compare every file digest, and run the read-only backup
  validator without changing that generation.
- Corrupt a complete acknowledged payload in a writable clone. Require startup
  rejection and byte-for-byte preservation of the damaged log. Restore from a
  separate validated clone, never edit the damaged original into a healthy state.
- Damage the derived index in another writable clone. Require reconstruction
  from the authoritative log and the same complete public readback.
- Compare group registrations and committed offsets together with application
  records; preserve the immutable generation after every rehearsal.
- For the distributed rehearsal, stop all three writers before copying any
  member. Compare source-volume and copied-file SHA-256 inventories, restore all
  members into three fresh volumes from that single generation, and start the
  same recorded image/configuration. Require exact replica/public readback,
  the saved consumer commit and a new acknowledged write. Retain and recheck
  the backup inventory after recovery; never run the standalone validator on
  distributed member volumes.

Record copied/validated file bytes, payload bytes, record count and recovery
elapsed time. Copy and hash bytes are logical test work, not physical device I/O.
Guest power loss covers QEMU/ext4 page-cache loss with flush-respecting writeback;
it does not certify a physical controller, host power loss, XFS or multi-rack loss.
Standalone validation does not validate a distributed Raft/PVC backup.

## Execution evidence

### Stale standalone visibility checkpoint

The first hosted guest run retained all 32 acknowledged records but restarted
with HWM 26 and LEO 32. The asynchronous HWM checkpoint had lagged the durable
log; a duplicate retry did not advance that stale visibility boundary.

Kafka 4.1 [ReplicaManager](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/server/ReplicaManager.scala)
checkpoints HW periodically, while [Partition](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/cluster/Partition.scala)
recomputes it from leader/ISR state on leadership changes, including an ISR of
one. Cursus should likewise distinguish a checkpoint from the authority that
determines current visibility. Its standalone log has no replication commit
decision: every recovered complete append can advance HWM. Distributed mode
must retain its separate committed-watermark authority and never promote an
uncommitted local tail merely because it survived.

The correction restores standalone HWM from the recovered durable end,
after the existing pending-bootstrap guard. Transaction visibility still requires
its recovered markers and coordinator decision. This avoids an additional
checkpoint fsync per ACK or a second log scan: recovery adds only a scalar
boundary assignment, with zero additional log bytes read. A complete append
whose response was lost may also survive, as already allowed by retry semantics.
Before implementation, regression checks require stale versioned and legacy
checkpoints to recover standalone visibility, the same distributed checkpoints
to preserve their boundary/fencing, open transactions to remain invisible, and
the unchanged guest power-loss oracle to read all acknowledged records.

The deterministic stale-checkpoint test failed before the correction for both
versioned and legacy standalone checkpoints. After the correction, the focused
checkpoint/bootstrap/committed-read suite passed in 103.405 seconds and the
native process-crash/backup suite passed in 37.385 seconds. The checkpoint crash
test still checks the exact old-or-new file image separately from reconstructed
standalone visibility. The hosted guest power-loss rerun remains required.

On 2026-11-07 UTC, the native standalone process-crash and backup suite passed
in 10.144 seconds. It preserved all 32 acknowledged records (8,768 payload bytes),
the committed offset 13 and the empty group's offset 0. Retrying producer sequence
32 kept end 32; sequence 33 advanced end to 33. The backup contained 24 files and
52,444,653 bytes, including preallocated index files. Complete record corruption
kept `/ready` at 503 with the checksum error, left the client port closed and
preserved the damaged log. Index reconstruction and validated-clone restore both
passed complete public readback.

The guest campaign passed all seven boundaries: acknowledged prefix, journal
sync/rename/directory sync, and manifest write/sync/directory sync. It used QEMU
7.2.22, Debian Linux 6.1.190, e2fsprogs 1.47.0 and ext4 `data=ordered` with QEMU
`cache=writeback`. The acknowledged-prefix reboot/readback phase completed in
1.60 seconds inside the guest; host boot time is separate. The transcript contains
the final ACK cut marker and all verification assertions. These timings describe
this small fixture and are not recovery-time objectives for production volumes.

The full-cluster rehearsal copied 157,699,845 bytes across three stopped members.
Source, backup and fresh-volume file inventories matched. Each member restarted
with its own recorded image ID; per-service builds need not have identical image
IDs. Restore, the stable-ISR window and full readback completed in 72.987 seconds.
All 32 original acknowledged records, consumer offset 13 and the new 33rd record
were preserved, and the backup inventories remained unchanged. The first local
rehearsal stopped at an overly strict cross-member image equality assertion; it
was corrected to pin and compare each member's own image before the successful
run. No mixed-version upgrade was exercised.

## Reproduction

```sh
go test -v -count=1 ./cmd/broker \
  -run '^TestStandaloneBroker(AcknowledgedPrefixPowerRecovery|BackupCorruptionRestore)$'

docker build -f test/power-loss/Dockerfile -t cursus-power-rehearsal .
mkdir -p power-results
docker run --rm -v "$PWD/power-results:/results" cursus-power-rehearsal

RUN_E2E_ACK_FAULTS=1 CURSUS_ACK_FAULT_RESULTS=/absolute/path/to/results \
  go test -v -count=1 -timeout 20m ./test/e2e-cluster \
  -run '^TestAcknowledgedClusterBackupRestore$'
```

Run only one acknowledged-write fixture at a time. It owns a dedicated Compose
project and ports; on Linux reserve ports 42001–42003 from ephemeral allocation,
as the workflow does. Restore helper containers use `--rm` and no network.
The test destroys only its own containers and original/cloned data volumes after
capturing results. It never copies live writers or reuses a production volume.
