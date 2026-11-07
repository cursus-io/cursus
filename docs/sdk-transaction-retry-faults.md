# SDK transaction retries across response loss

The Kafka comparison and recovery contract were defined before changing tests.
This change adds coverage without changing SDK retry policy or broker formats.

Kafka's producer API treats a transaction completion timeout as an unknown
outcome. Retrying the same operation is allowed; changing commit to abort (or
the reverse) is not a safe way to resolve the uncertainty. Its versioned
transaction manager retains completion state and cached request results.

Sources:
* https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
* https://github.com/apache/kafka/blob/4.1.0/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java

Adopt the same-identity/same-decision recovery rule. Cursus retains explicit
producer ID/epoch, per-transaction epoch renewal, Raft decisions, and its current
Go SDK's explicit caller retries after transport errors. Do not silently import
Kafka's background request queue or claim identical transactional APIs.

The broker fault matrix covers commit/abort at prepare, participant markers,
and final decision, including offset visibility and opposite-decision rejection.
It now invokes these operations through the actual Go SDK producer wrapper and
reuses that same wrapper and session after the coordinator crash. The observed
transaction-specific durable fault marker proves the request reached the broker;
a missing response remains an unknown outcome.

Required cases: END_TXN response lost for commit and abort; same wrapper
retries exactly the original transaction ID, producer ID, epoch and decision;
no automatic INIT_PRODUCER_ID during an unknown outcome; explicit redirect
preserves the command; a terminal producer fence remains an error; the next
transaction renews the epoch only after completion has been acknowledged.
The Wire regressions inspect exact commands across response loss and coordinator
redirection, including both completion decisions and terminal fencing. The six
real-broker cases retain committed-output and consumer-offset verification
through survivors and the restarted broker. A mock response loss alone does not
establish that a transaction was durable; that is the role of the broker fault
markers and retained-volume recovery checks.

Document transport ambiguity separately from broker rejection. An extra retry
may consume another request and recovery work; it must not append duplicate
transaction data or apply offsets twice. Existing byte/work metrics remain
necessary for recovery, and these SDK tests establish no throughput SLO.

The focused native SDK suite passed in 0.780 seconds, and the updated broker
boundary driver compiled. These observations are distinct from execution of
the real-broker matrix and do not claim that its six cases have already run on
this change.
