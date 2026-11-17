# Seeded compound-fault campaigns

## Comparison and checks defined before implementation

[Kafka 4.1.0 replication system tests](https://github.com/apache/kafka/blob/4.1.0/tests/kafkatest/tests/core/replication_test.py)
run producers and consumers around repeated broker bounces and verify that
acknowledged messages survive. Adopt reproducible fault schedules and external
write identities rather than counts alone. Kafka's accepted
[KIP-101](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/67634337/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation)
and versioned
[replica fetcher](https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/server/ReplicaFetcherThread.scala)
use leader-epoch evidence to distinguish retained history from a divergent tail.
Cursus adopts that proof-before-truncation safety property while retaining its
push replication, separate fenced Raft `PARTITION_COMMIT`, typed uncertain-commit
marker and the existing three-broker fault fixture. It does not add Kafka's pull
fetcher or epoch sequence-file format.

The safety consequence is that an unknown acknowledgement never authorizes
offset reuse or truncation. Availability is required after quorum and authority
recover, not during quorum loss. Recovery-cost evidence records transferred
bytes plus logical read, write, hash and decode work; request counts alone are
insufficient, and counters from restarted processes remain separate intervals.
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

## Current validation evidence

The three-round seed `3271337` campaign passed on current `main` on 2026-11-18.
All three faults completed in 307.761 campaign seconds and 390.84 total test
seconds. The external journal recorded 108 attempts; full readback verified 92
records on every replica and through the public consumer with digest
`92f1e832bf174b3a2b9996e6c7bb8b3707752c9e28948be84d9009eb8bceefa6`.

An earlier harness run exposed a false heal: one Docker `iptables -D` failure was
logged but the network was still marked healed, leaving one receiver unable to
reach its source and four internal offset partitions under-replicated. The
corrected helper retries each removal at most three times and verifies rule
absence with `iptables -C` before recording `network_healed`. Its bounded success
and failure paths have deterministic regressions. The unchanged campaign then
completed the same isolation/quorum-loss round with cluster-wide zero
under-replicated partitions for the full 65-second stability window.

Recovery work below is captured before final oracle readback. Values are
per-process and are not summed across restarts or peers. The interrupted receiver
row is the process killed at the durable catch-up boundary; its replacement is a
new counter interval.

| Process interval | Transfer bytes | Lineage read / write / hash bytes | Lineage decoded frames | Retained read bytes / decoded frames |
| --- | ---: | ---: | ---: | ---: |
| Broker 1 | 3,585 | 249,675 / 4,688 / 181,163 | 184 | 94,749 / 112 |
| Broker 2 before interruption | 108,787 | 10,942 / 488 / 11,786 | 27 | 22,397 / 52 |
| Broker 2 replacement | 6,306 | 191,170 / 608 / 179,602 | 180 | 263,969 / 277 |
| Broker 3 | 0 | 0 / 0 / 0 | 0 | 16,241 / 39 |

These figures are recovery-work evidence for one deterministic run, not a
throughput comparison or production availability SLO. The 24-hour campaign
remains separate and is not implied by this preflight.

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
