# Prepared transaction startup with an incomplete ISR

## Design comparison before implementation

The three-broker departed-member reservation test exposed an existing startup
failure at `9b657f1`: a returning transaction coordinator attempts prepared
completion before its replicas have rejoined ISR. The exact failure is
`insufficient_in_sync_replicas current=2 required=3`; startup returns it as fatal
and the process exits, removing the replica needed to restore quorum. A CI run
passed with a different startup ordering, so that success does not refute the
local failure. The durable prepared decision and offset reservation must remain.

Kafka 3.9.0's
[TransactionCoordinator.onElection](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala)
loads transaction state and schedules pending markers. Its
[TransactionMarkerChannelManager.tryAppendToLog](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/coordinator/transaction/TransactionMarkerChannelManager.scala)
requeues unavailable completion appends, including insufficient replicas, and
retains coordinator epoch checks. Adopt the distinction between a durable
decision and temporarily unavailable completion. This follows KIP-98's recovery
principle; it does not equate transaction markers or DeleteRecords with Cursus
TRUNCATE.

Cursus already activates internal replication and registration before startup
transaction recovery. Keep that runtime alive, but keep the public listener and
startup readiness unpublished while retrying the exact insufficient-ISR error.
Use the existing recovery batch, with a one-second pause and cancellation. Do
not lower minISR, release reservations, turn an error into a committed result,
or retry unknown/storage/fencing failures. Every leaf of a joined error must
be the permitted availability error. Preserve standalone startup behavior.

Availability improves when the third replica catches up during startup. If ISR
never recovers, startup remains pending until shutdown; it does not declare
readiness. Each retry can repeat a bounded transaction batch and durable sync;
this is an availability correction, not a claim of zero replay or fsync cost.
No protocol, snapshot, journal or manifest format changes are required.

Regression checks defined before the fix: original one-shot startup fails the
wrapped/joined insufficient-ISR retry case; corrected startup completes when
the same recovery operation later succeeds. Unknown, mixed, non-retryable and
standalone failures must still fail immediately; cancellation must stop retries.
Repeat the existing real three-broker reservation/coordinator-restart scenario
without weakening its minISR=3, lifecycle rejection, exact-once output, full ISR
or readiness assertions. Process restart is the failure model; no device-power
loss or torn-sector guarantee is inferred.

## Results and operation

The original one-shot call, retained in a temporary regression harness, failed
both legacy-text and explicitly classified insufficient-ISR cases, as well as
cancellation. The corrected helper passed those same tests and the immediate
fatal/standalone cases. The entire Linux Go 1.25 server race suite passed in
15.839 seconds; server lint reported zero issues.

The unchanged three-broker scenario that had logged a fatal startup exit passed
with this correction in 303.89 seconds on the shared Docker Desktop host,
including image build/setup, lifecycle rejection, actual coordinator kill,
return, full ISR/readiness and exactly-once output verification. This is a test
duration, not recovery latency; startup ordering is scheduler-dependent, so the
deterministic helper regression supplies the exact insufficient-ISR proof.
The pre-existing full live cleanup profiles remain applicable to their recorded
commit and are not relabeled as measurements of this startup wait.

Install the correction on returning coordinators before relying on recovery
with prepared transactions and a temporarily incomplete ISR. A broker can stay
in startup while peers are unavailable. Restore replicas and inspect the
prepared transaction/ISR state; do not lower minISR or remove reservations.
Unknown errors remain fatal. Downgrading preserves the unchanged format but
reintroduces the fatal-exit behavior. The original standalone behavior and
legacy TRUNCATE/retry contracts are unchanged.
