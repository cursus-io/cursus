# Prefix refinement retry continuity

## Comparison and regression contract

The investigation starts at official main `9c43d398eecc387ae0011bb7900b54a74a0b0d8d`.
The initial PR 312 CI attempt counted 63 exchanges against an unchanged bound
of 52. Its single rerun counted 34. In `advanceReplicaTurn`, every apply error
discarded the original request and all refined prefix candidates. The test
pause occurs before mutation; each such retry repeated the binary search.
This is real repeated recovery work exposed by fault scheduling, not evidence
that the exchange bound should be raised.

Before implementation, inspect Kafka **3.9.0**
[AbstractFetcherThread](https://github.com/apache/kafka/blob/3.9.0/core/src/main/scala/kafka/server/AbstractFetcherThread.scala):
`delayPartitions` retains fetch state and leader epoch; epoch-end responses are
checked against current leadership before truncation. Adopt retained progress
only under unchanged authority. Kafka epoch-based truncation is not Cursus's
authenticated prefix search, nor its administrative full-state TRUNCATE.

Cursus must additionally bind the continuation to the original local partition,
durable content generation, recovery attempt, LEO, topic definition and source/
receiver identity. After an apply error, retain a refined request only if the
existing metadata fence and local witness still validate. Retry the final
challenge over the network; never cache a response as authority for mutation.
Partial writes/rollback, metadata replacement and cancellation invalidate the
continuation. Every actual apply retains all existing fences and disk checks.

Safety: no relaxed admission or new wire/storage format. Availability: existing
backoff remains; permanent failures remain pending. Recovery cost: failed
pre-mutation applies need one final request each, instead of another logarithmic
search; validation adds metadata/content-generation checks without a prefix scan.

Regression checks defined before changing production code: force three
pre-mutation failures after finding exact prefix 1, compare baseline/fix request
counts, preserve the committed seed, invalidate after partial rollback and after
leader change, and rerun the unchanged three-node indexed tail/crash fixture.
Measure bytes and CPU/memory in the integration run; do not turn a request count
into a physical I/O claim. Existing process-crash and guest-power-cut evidence
remains separate from device power-loss guarantees.
