# Authority of consumer offset metrics

## Baseline and Kafka comparison

Deployment revision `9139bf3` and main `14f7a68` exported offsets independently
of lifecycle ownership. A healthy group committed at 5142 could therefore appear
behind or out of range because two non-coordinators retained 1950 and 1844.
The reported cluster state is incident evidence supplied by the operator; no
production scrape, offset mutation or cluster change is part of this fix.

Kafka's [coordinator monitoring documentation](https://kafka.apache.org/40/operations/monitoring/)
distinguishes active, loading and failed coordinator partitions. The versioned
[Kafka 4.1.0 GroupCoordinatorMetrics implementation](https://github.com/apache/kafka/blob/4.1.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/metrics/GroupCoordinatorMetrics.java)
adds/removes metric shards on activation/deactivation and aggregates retained
offset counts from active shards. Cursus adopts that ownership boundary, not
Kafka's metric names or an assertion that Kafka exposes these same per-group
lag gauges. [Accepted KIP-848](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
places group coordination on the broker-side coordinator; it is protocol design,
not a specification of Cursus Prometheus metrics.

Cursus retains its durable offsets-partition routing, recovery fence and local
in-memory observations. `ResolveGroupCoordinators` already rejects unavailable
Raft leadership, missing routes and unrecovered ownership epochs. Sampling uses
one such lifecycle observation to authorize both lifecycle and offset gauges.
It does not replay metadata or contact another broker just to produce a metric.

## Implemented semantics and compatibility

Metric names, types and labels remain unchanged. The committed-offset, lag and
out-of-range gauges now require local coordinator authority, a known stored
commit and a local partition observation. Generation and assignment gauges use
the same authority gate. Non-coordinators, lookup failures, deleted groups and
sources without an ownership contract omit these gauges. Missing commits no
longer fabricate offset zero; a stored commit of zero remains observable.

Standalone coordinators explicitly report local authority. During a distributed
handoff the former owner omits values on its next sample; the new owner cannot
publish until its existing ownership-recovery fence passes. No owner means
absence of offset gauges, not lag/out-of-range zero. `coordinator_up` is zero for
a known non-owner or failed ownership lookup; it may itself be absent when the
group is unknown or recovery admission prevents observation. A successful owner
lookup with unavailable topic data can still report `coordinator_up=1` while
offset gauges are absent and `topic_lookup` failures are counted.

These are sampled observations, not a linearizable cross-broker snapshot. The
existing background sampler normally refreshes each second and expires results
after five seconds; a timed-out sample is invalid. Prometheus scrapes of different
brokers can straddle a transition. Require exactly one observed coordinator and
healthy scrape/runtime observation, and alert separately on missing expected
groups/partitions. This change does not make a coordinator's local replica HWM
a globally simultaneous partition-leader observation.

Existing dashboards which assume every broker emits every group, use absent
series as zero, or depend on default-zero uncommitted offsets must change. During
a mixed-version rollout, old binaries still emit stale values: filter by authority
on the same target **before** aggregating. See the query examples in
[the observability reference](reference/observability.md#consumer-offset-authority-and-query-migration).
Historical range functions can retain pre-upgrade samples; first filter/record
the authoritative instantaneous series and apply windows to that recording rule.

## Safety, availability, recovery work and regression contract

The fix changes observations only: no consumer offset, recovery journal, checkpoint,
routing epoch or TLS setting is written. Added recovery records, recovery bytes
and replay work are zero. Sampling still copies existing in-memory group state;
the authority map is proportional to known groups and the ownership resolver is
not called a second time for offsets. No lower recovery traffic is claimed.
Omitting unavailable data sacrifices apparent coverage to avoid false certainty.

Checks defined before implementation cover the 5142/1950/1844 three-broker
fixture, owner loss, ownership transfer after replay, no-owner lookup failure,
missing authority contract, stored-zero versus missing commits, missing local
partition data, single ownership resolution per sample, wildcard group labels,
and no offset publication during exclusive recovery. Existing asynchronous
snapshot expiry and no-durable-read tests remain part of validation.

Prometheus alert rule rollout and certificate renewal remain separate deployment
work. The examples document required query changes; no deployed alert is changed.
