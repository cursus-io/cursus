# Consumer Lifecycle Alerts

## Responsibility Boundary

Cursus reports the actual lifecycle state held by the current consumer group
coordinator. It does not decide whether a group is required.

A zero-member group is normal when, for example, a lazy consumer has not joined
an empty topic. The same value is an outage when an application contract says
the group must remain active. The cluster-config catalog owns that contract,
and Prometheus owns the comparison and alert duration. Broker `/ready` remains
independent of both values.

## Cluster-config Catalog Contract

Publish application expectations as catalog metrics from cluster-config or its
existing metrics bridge. This repository does not add those group entries.

```text
cursus_required_consumer_group_active{topic="<topic>",group="<group>"} 1
cursus_required_consumer_group_min_members{topic="<topic>",group="<group>"} 1
```

Use `active = 0` to retain a catalog entry without enforcing it. Set
`min_members` to the minimum number of concurrently joined members required by
that application's availability contract.

## Recording And Alert Rules

Collapse the three broker targets to one actual member value:

```promql
max by (topic, group) (cursus_consumer_group_members)
```

A required group might never have been registered, so a direct comparison
would have no left-hand series and would not alert. Fill missing actual values
from the catalog before comparing:

```promql
(
  (
    max by (topic, group) (cursus_consumer_group_members)
    or on (topic, group)
    (0 * cursus_required_consumer_group_min_members)
  )
  < on (topic, group)
  cursus_required_consumer_group_min_members
)
and on (topic, group)
(cursus_required_consumer_group_active == 1)
```

Apply an alert `for` duration longer than the configured consumer session
timeout, heartbeat check interval, normal coordinator movement, and scrape
interval. This prevents a planned rebalance from paging while still bounding
detection time.

Alert separately when a required group's authoritative coordinator is absent
or overlapping:

```promql
(
  (
    sum by (topic, group) (cursus_consumer_group_coordinator_up)
    or on (topic, group)
    (0 * cursus_required_consumer_group_active)
  ) != 1
)
and on (topic, group)
(cursus_required_consumer_group_active == 1)
```

Observation failures are per scraped broker, so aggregate counters with `sum`:

```promql
sum by (topic, group, reason) (
  increase(cursus_consumer_group_observation_failures_total[10m])
) > 0
```

The `reason` label is bounded to `coordinator_lookup`, `group_lookup`, and
`topic_lookup`. Member IDs, client addresses, broker endpoints, and raw errors
are never metric labels.

## Canary Firing And Resolution Check

Use an isolated canary `<topic>` and `<group>` whose catalog contract is
`active = 1` and `min_members = 1`. Do not reuse a production group with other
members, because its aggregate would not reach zero.

1. Confirm all three brokers are ready and the authority sum is one:

   ```promql
   min(cursus_broker_ready) == 1
   sum by (topic, group) (cursus_consumer_group_coordinator_up) == 1
   ```

2. Start one canary consumer. Wait until the authoritative member count is one
   and the state is stable:

   ```promql
   max by (topic, group) (cursus_consumer_group_members) == 1
   max by (topic, group) (cursus_consumer_group_state{state="stable"}) == 1
   ```

3. Stop the canary. Use graceful leave for the fast path, then repeat once with
   an abrupt stop to exercise heartbeat expiry. Wait for member count zero and
   state `empty`:

   ```promql
   max by (topic, group) (cursus_consumer_group_members) == 0
   max by (topic, group) (cursus_consumer_group_state{state="empty"}) == 1
   ```

4. Verify the required-member alert reaches `firing` after its configured
   `for` duration. During this interval, verify every available broker remains
   ready; consumer absence must not change `/ready` or `cursus_broker_ready`.

5. Restart the canary with the same catalog contract. Wait for member count one
   and stable state, then verify the alert resolves.

6. Review both timestamps. Activity should advance after the new join or
   heartbeat, and rebalance time should advance on the transitions to zero and
   back to one:

   ```promql
   max by (topic, group) (
     cursus_consumer_group_last_activity_timestamp_seconds
   )
   max by (topic, group) (
     cursus_consumer_group_last_rebalance_timestamp_seconds
   )
   ```

This procedure validates actual Cursus state and the external expectation join
without adding application-specific group names to the broker configuration.
