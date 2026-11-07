# Offset snapshot revisions after an ambiguous append

## Design and regression contract

CI found two different offset snapshots with the same group/topic/epoch/revision
after a cooperative consumer's rolling restart. The ordinary write path advances
its revision only after a successful response, although an error can follow a
durable append. A later request can therefore reuse that revision with a different
payload and make strict recovery reject the log.

[Kafka 4.0.0 OffsetMetadataManager](https://github.com/apache/kafka/blob/4.0.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java)
creates per-partition offset records and replays them using their log positions.
Adopt the distinction between an append attempt and an acknowledged state update.
Cursus must retain its complete topic snapshots and strict revision conflict
checks for compatibility; changing replay to accept conflicting revisions would
hide corruption.

Reserve a runtime attempt revision under the existing group lock before calling
the durable writer. Keep that watermark separate from acknowledged revisions and
offsets. Carry it across in-process group replacement only for the same
registration epoch. Recovery after process restart still derives authoritative
state from the committed log. Revision gaps are valid; exhaustion must fail
without writing. This does not alter the record format or expose failed commits.

Before implementation, define checks for durable append followed by a lost reply,
a later changed request, strict replay, stale in-process state replacement,
registration-epoch replacement, and overflow. Cover single, bulk, cooperative,
multi-topic and transaction offset writes. Keep the existing corruption-rejection
checks. Count replayed records and payload bytes, then rerun cooperative fault
recovery. No new recovery scan or record is required solely to reserve a revision;
this correction makes no claim about reduced physical I/O.

## Validation and limits

Focused Linux race tests pass for lost replies across all five write paths,
strict and legacy replay, stale state replacement, epoch replacement, overflow,
multi-topic atomic visibility, and existing conflict rejection. The replay tests
report record counts and payload bytes. This prevents new revision conflicts; it
does not repair previously conflicting records.

Local cooperative E2E reruns encountered repeated Wire timeouts and member
fencing before reaching the CI failure's rolling-restart phase. They do not
establish end-to-end success for this fix; hosted CI remains required.

On commit `39ac3f9`, hosted coordinator race/coverage tests and cooperative E2E
passed. The E2E completed coordinator failover, coordinator restart, and all
three rolling broker restarts, delivering 464 records with zero duplicate
deliveries. This hosted result does not erase the local transport/fencing failures.
