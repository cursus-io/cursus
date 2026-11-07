# Fault-test admission and protocol checks

## Comparison and regression contract before implementation

[Kafka 4.0.0 AbstractCoordinator](https://github.com/apache/kafka/blob/4.0.0/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java)
backs off on coordinator loading while distinguishing membership fencing and
fatal protocol errors. Its [Errors registry](https://github.com/apache/kafka/blob/4.0.0/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java)
gives loading a defined protocol classification.

Cursus already reports `consumer_metadata_recovering` as retryable availability.
The proposed fixture correction reuses bounded group-registration admission and
the existing group-join helper, which retries only that loading error during sync
with the same member and generation. Cursus retains its own Raft-backed metadata
replay and explicit REGISTER_GROUP operation; Kafka's membership protocol is not
copied. A loading rejection before fault injection is not data-loss evidence.
No post-fault offset, visibility, generation, or recovery assertion is relaxed.

Regression checks: rerun the failed large-history TRUNCATE/DELETE injection suite
and durable coordinator failover test, including actual fault milestones and
their existing convergence deadlines. Large-history verification must still
replicate a member record larger than 1 MiB, converge replica offsets, and retain
the existing per-process I/O measurements. Admission retries are not evidence of
lower recovery bytes or work; no recovery-efficiency claim changes.

The transaction fault hook also emits `transaction_fault_boundary`, which must be
registered explicitly. Plan to classify it as nonretryable internal failure: a
timed-out injected boundary must fail the scenario instead of being hidden by
automatic retries. Regression checks are the static emission inventory and
protocol classification tests, with both ordinary and fault-enabled builds.
Production hooks remain disabled. This classification does not alter transaction
durability or recovery work.

## Linux fixture isolation

An earlier scaling run could not restart a broker because host port 42000 was
already in use. The offset-34000 fixture listeners overlap Linux's automatic
client port range. Reserve its fifteen published ports on the disposable CI
runner before starting the workload, preserving any existing reservations.
[Linux's documented reserved-port setting](https://docs.kernel.org/networking/ip-sysctl.html#ip-variables)
excludes automatic assignments while retaining explicit listener binds. The
scaling test must still complete every restart and retain all recovery byte/work
measurements; neither its workload nor its deadlines change.

The fault-enabled ACK fixture also needs its generated, nonsecret debug config
to be readable by container UID 1000 when the Linux runner uses a different UID.
Give that temporary file read permission for the broker while retaining the
unprivileged broker user; do not relax permissions on production configurations.
The full boundary suite must start all brokers and pass every required scenario
and subtest without skips on Linux.

## Initial validation

The admission changes are implemented using the existing registration and join
helpers. On an isolated local Docker cluster the large-record cleanup suite
passed both TRUNCATE and DELETE (388.58 seconds total), including observed lost
acknowledgements, read faults, retained unrelated data, and byte/work checks.
The coordinator failover test passed twice (126.58 and 35.70 seconds), preserving
its strict post-fault offset and generation assertions. The fixture binary was
built on Windows; Linux CI remains the final integration check.

A disposable Linux container confirmed that a different UID cannot read the
original mode-0600 config but can read mode 0644, and that the proposed reserved
port list is accepted in its own network namespace. No developer-host sysctl or
existing cluster was modified.
