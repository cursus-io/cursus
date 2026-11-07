# Seeded compound-fault campaigns

## Comparison and checks defined before implementation

[Kafka 4.1.0 replication system tests](https://github.com/apache/kafka/blob/4.1.0/tests/kafkatest/tests/core/replication_test.py)
run producers and consumers around repeated broker bounces and verify that
acknowledged messages survive. Adopt reproducible fault schedules and external
write identities rather than counts alone. Cursus retains its push replication,
separate Raft watermark commits and the existing three-broker fault fixture.
The test oracle covers one retained, nontransactional, uncompacted partition;
transactions, deletion, physical power loss and independent failure domains
retain their separate contracts.

Extend the current harness with a seeded permutation of three compound faults:

1. Withhold a real publish ACK, kill its leader, and retry the same identity on
   the surviving majority while a separate producer continues sending traffic.
2. Isolate the old leader from both peers, require no fresh ACK from it, kill a
   majority peer, require no fresh ACK without quorum, then restore the peer and heal
   the network. Probes must reach the client transport; a missing reply remains
   an unknown outcome, not proof that no append occurred. Availability is
   required after healing, not without a majority.
3. Build a follower backlog, pause after a real durable recovery append, kill
   that receiver, and restart it again from its retained volume. Re-arm only
   after removing old reached/release markers so stale evidence cannot pass.

Every batch covers all three faults; the seed controls their order, selected
peer and bounded no-quorum interval. Record the resolved schedule as well as the
seed. No injected fault counts unless its activation was observed. Background
publishing records every attempt without hidden retries, preserving identity and
payload until ACK. Fatal assertions belong to the test goroutine; background
work reports errors and must stop before history inspection or fixture cleanup.

After each round, stop and drain publishing, require 65 seconds of stable full
ISR, and compare the complete history on all replicas and through the public
consumer. Keep checkpoints compact by recording the verified count and digest;
the durable external request journal retains full attempted payloads. Record
per-process recovery read/write/hash/decode/transfer counters before and after
inspection. Restarted counters are separate intervals. Never add sender and
receiver byte counters as though they were independent network traffic.

The default campaign is bounded. An explicit duration enables a long campaign;
minimum publish spacing, maximum duration and an attempt limit bound memory and
artifact growth. Reaching a limit fails rather than silently truncating history.
A short seed run is not a 24-hour result. Report actual elapsed time, completed
rounds, identities, errors, seed and configuration. Preserve failures and logs,
then immediately remove this fixture's containers and volumes.

Required regressions include deterministic schedule coverage, invalid bounds,
concurrent journal integrity and I/O-error propagation, plus the existing oracle
mutation tests and response-loss proxy tests. Run several short seeds before
the 24-hour campaign. Do not introduce task-branch CI exceptions.

## Execution evidence

Three native Windows driver runs against Linux Docker brokers passed on
2026-11-07 UTC. Each seed completed all three fault rounds, including observed
loss of quorum and a receiver crash at the durable catch-up boundary. Every
round required 65 seconds of stable full ISR and complete replica/public-consumer
readback. The source is the harness implementation `397227d` on broker baseline
`c7384a5`; separate consumer-liveness and lineage-work changes are not included.

| Seed | Campaign seconds | Total test seconds | Completed attempts | Final verified records |
| --- | ---: | ---: | ---: | ---: |
| 3271337 | 350.069 | 553.70 | 113 | 97 |
| 20261107 | 360.869 | 491.26 | 110 | 100 |
| 1 | 408.419 | 460.50 | 143 | 109 |

Total test time includes fixture startup and cleanup. The next background ACK
after each heal request was observed between 101 and 3,629 ms across these nine
rounds. These are fixture observations, not a production availability SLO.
Per-process recovery counters and complete broker logs are retained separately
from the compact verified-history digests. The 24-hour campaign is a separate
acceptance run; no 24-hour result is included in these short-run measurements.

## Running

Run only one ACK fixture at a time; all its tests own the same dedicated Compose
project and shifted ports. Keep results outside the source tree so Docker build
contexts do not absorb growing evidence. On Linux, reserve host ports 42001-42003
from ephemeral client allocation before running (the existing CI job does this).

```sh
RUN_E2E_COMPOUND_FAULTS=1 CURSUS_COMPOUND_SEED=3271337 \
  CURSUS_ACK_FAULT_RESULTS=/absolute/path/to/results \
  go test -v -count=1 -timeout 25m ./test/e2e-cluster \
  -run '^TestAcknowledgedWritesSurviveCompoundFaults$'

RUN_E2E_COMPOUND_FAULTS=1 CURSUS_COMPOUND_SEED=20261107 \
  CURSUS_COMPOUND_DURATION=24h CURSUS_ACK_FAULT_RESULTS=/absolute/path/to/soak \
  go test -v -count=1 -timeout 26h ./test/e2e-cluster \
  -run '^TestAcknowledgedWritesSurviveCompoundFaults$'
```

Duration starts after fixture creation and includes fault execution, convergence,
stable-ISR checks and full readback. The run finishes its current round after
the duration elapses. It is not a claim of 24 uninterrupted hours of publishing:
publishing pauses at each verification checkpoint. Payload spacing is at least
one second per background attempt; backlog faults also add 64 identified records.
The fixed limits are 2,000 rounds and 300,000 attempts. Exceeding either fails.
Duration-based campaigns use broker info logging; short runs retain debug logs.
External journal events and recovery metrics are recorded in both modes.
