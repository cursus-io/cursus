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

The first 24-hour attempt on 2026-11-07 UTC failed after 547.69 total test seconds
in round 1, before complete content readback. After the interrupted receiver
rejoined, all three metadata observations agreed at committed end 110, but the
host's broker-2 metrics requests timed out during the required stable window.
The captured receiver stack showed its metrics listener waiting for connections,
without an active metrics handler or a demonstrated lock cycle. That does not
prove whether the cause was broker availability or the host/Docker network path;
the run is a failure, not evidence of either data loss or successful recovery.

Failure capture now records container state and in-container metrics/readiness
before requesting Go stacks. It does not substitute those probes for the public
path or relax the stability/identity oracle. An immediate three-round rerun of
seed 20261107 with the same info logging passed in 457.88 total seconds (347.449
campaign seconds, 126 attempts). The intermittent first-run timeout remains
unexplained; its artifacts are preserved separately from subsequent campaigns.

A later attempt using the diagnostic harness `ee5b475` started at
2026-11-07 16:38:34 UTC and was stopped at 16:42:19 UTC because the laptop was
about to sleep. Broker logs and container state were captured before removing
the fixture's containers and volumes. Its manifest records
`interrupted_for_user_sleep`; it is neither a product failure nor a completed
duration run. A fresh 24-hour attempt started at 17:16:03 UTC using the same
diagnostic binary (SHA-256
`65ed5f8c23755d607c6e6b6f3b96bede9914e13ec3024d842ef0b7e751312b06`).
Waiting, sleep and earlier attempts do not count toward that campaign. Its
completed duration and final history verification must be recorded separately
before claiming a 24-hour pass.

That attempt stopped after 4,346.26 total test seconds. The first 39 rounds
completed full verification: the final verified history contained 1,355 records
from 1,658 attempts. During round 40, a separate host-side cleanup force-deleted
all three running containers and their data volumes at 18:28:21 UTC. Docker API
events and the cleanup command establish this external interruption before the
test's own failure cleanup; it is not evidence of a broker-originated failure or
a completed 24-hour campaign. The external journal remains available. A fresh
attempt started at 18:43:17 UTC using implementation `1cf4d7d` and driver SHA-256
`7e7f44c1722f4b99ebd7e382f945c0c370265ff340e1b3c262a513f8f1f41338`.
The active fixture's ownership is now recorded for concurrent host tasks, and
broker logs plus Docker events are saved throughout the run. Prior attempts'
elapsed time is not added to this attempt.

That attempt ended at 19:01:38 UTC after 1,090.76 seconds. Nine rounds completed;
round ten stopped when an unresolved Raft HWM commit left the partition's write
fence in place even after a new Raft leader was elected. Unlike the prior
interruption, this was a broker availability failure. The retained tail and
full process logs were preserved. The same failure appeared during cooperative
rolling restart. [Partition commit reconciliation](partition-commit-reconciliation.md)
describes the repair and its focused regressions. A new campaign must validate
the repair; neither interrupted attempt establishes 24-hour reliability.

The later full validation run exposed two failures outside the compound scenario.
Consumer-history replay failed before broker startup because host port 40002 was
already in use. The replay and cooperative jobs now reserve every shifted
listener against Linux ephemeral client allocation, as the scaling and ACK jobs
already do. Separately, cooperative rolling restart timed out with a consumer
metadata replica at LEO/HWM 8,315 while the committed end had reached 8,322.
The replica was advancing under continuing writes; no terminal heartbeat fence
was reported. Port reservation alone is not proof of that failure's cause or
correction. The unchanged cooperative recovery assertions now run three times
per validation job to retain evidence of recurrence.
Failures also retain all broker logs, container state and bounded public
readiness/metrics snapshots before cleanup, with a separate timestamp for each
repetition. This preserves the advancing commit boundary and per-process recovery
work instead of relying only on the generic final log tail.

The next repeated run reached the final drain assertion but failed there once;
the other two repetitions passed. All three brokers were ready and the current
coordinator's six committed offsets matched the respective log ends
(`240,240,240,239,240,240`). The oracle's AdminClient used a partial config with
zero retries, so every query started and stopped at broker 1 even though broker 2
was the coordinator. It now uses the SDK defaults and deliberately starts from
a non-coordinator. A two-broker Wire regression demonstrates the old routing
error and successful bounded retry; the final drain checks retain their original
deadline and ACK requirements and now log offsets, ACK counts and query errors.
This explains that final-drain failure, not the earlier moving-tail readiness
timeout. Production routing and consumer behavior are unchanged.
With the corrected oracle, three consecutive runs passed in 115.99, 57.35 and
52.53 seconds. All six partitions drained to their committed log ends in every
run, and none redelivered within an unchanged grant. The third run observed one
133-byte replay during a member handoff, within the existing one-batch bound.

The repaired implementation passed a 12-round preflight in 1,426.51 test seconds
(1,322.062 campaign seconds), verifying 396 records from 450 attempts. A fresh
24-hour attempt then ran from 2026-11-07 19:57:07 to 22:34:54 UTC and failed after
9,462.83 test seconds. The first 76 rounds completed full verification: 2,627
records from 3,147 attempts, with history digest
`20f22d659e1b501fbbf4b2cfd24f517d6f4f43f8ce5307f526fdfea3448cd505`.

Round 77 healed its isolation/quorum-loss fault, received another ACK and
observed all three replicas in ISR at committed end 2,637. Subsequent public
connections and Docker diagnostics timed out. Docker Desktop's own API log
records `HEAD /_ping` calls taking over ten minutes, completing together at
22:34:29 UTC; some broker messages also reached the Docker log stream roughly
ten minutes late. Container deletion began only during the test's cleanup.
This establishes a Docker/host execution stall during the failure, but does
not establish its trigger or prove final record preservation: round 77 did not
complete readback. The retained kernel log has no OOM-kill report, and the
examined Windows event window has no sleep/restart event. Neither absence rules
out resource pressure or a virtualization fault. This attempt is failed, not a
24-hour pass, and its duration cannot be added to a replacement campaign.

The local replacement runner bounds each evidence command and separately
samples host memory, WSL memory/CPU/I/O counters and container resource usage.
Pressure stall counters are captured when the host kernel provides them.
Failed captures retain their timestamps, elapsed time and timeout status;
successful capture cursors advance independently. These changes improve
diagnosis when Docker is unresponsive. They do not repair a demonstrated
broker defect or relax the campaign's recovery deadline and history oracle.

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
