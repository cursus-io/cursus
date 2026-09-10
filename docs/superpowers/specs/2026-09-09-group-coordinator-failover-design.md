# Durable Group Coordinator Failover

## Goal

Make group coordinator discovery converge on every surviving broker after a
broker failure. `FIND_COORDINATOR` must use only replicated FSM membership and
must never exclude a node based on a local socket observation. A failed offset
commit must remain fail-closed and give a client a retryable coordinator error.

## Scope

- Broker heartbeats are sent to the current Raft leader, rather than relying on
  peer fan-out as the leader's liveness source.
- The leader maintains an in-memory heartbeat session table and, after a
  bounded election grace period, applies durable `inactive`/`active` membership
  transitions through Raft.
- Registration includes a monotonic broker incarnation epoch. A stale process
  with the same broker ID cannot reactivate itself or supply a valid heartbeat.
- Group coordinator routing rebuilds only from replicated active memberships.
- Group fencing and monotonic offset persistence are unchanged.
- `__consumer_offsets` topology is bootstrapped once through the ordinary Raft
  `TOPIC` command after every current Raft voter has a durable active
  registration. Individual offset records are not put in Raft.

## Non-goals

- Persisting every heartbeat in Raft.
- Replacing the existing group hash-ring algorithm with explicitly persisted
  group ownership.
- Changing transaction coordinator shard ownership or its epoch contract.
- Supporting mixed-version rolling upgrades. The FSM snapshot format changes
  and requires a coordinated binary upgrade.

## Membership model

`BrokerInfo` gains an opaque process `IncarnationID` and its durable monotonic
`IncarnationEpoch`. The Raft FSM is the authoritative source for the token,
epoch, address, client address, and status.

1. A broker starts by registering with its broker ID and a newly generated
   process incarnation token. The Raft leader assigns a strictly increasing
   incarnation epoch for that broker ID and commits an active registration.
2. The broker sends `HEARTBEAT_CLUSTER` to the current Raft leader containing
   its broker ID and process incarnation token. A non-leader responds with a
   retryable not-leader result containing the current leader address; it does
   not update local membership liveness.
3. The leader accepts a heartbeat only if its token equals the active FSM
   registration. It updates a leader-local last-seen table. A successful
   heartbeat of an inactive, current-epoch broker causes a Raft `REGISTER`
   transition back to active.
4. On leadership acquisition, the leader grants all active registrations one
   heartbeat timeout of grace. After grace, it checks sessions at a short,
   deterministic interval. A missing or expired active broker causes a Raft
   `DEREGISTER` apply. The state changes only when that apply succeeds.
5. A recovered broker re-registers only after the preceding incarnation was
   durably fenced inactive. The FSM assigns a higher incarnation epoch,
   records the new endpoint, and commits active status. A registration with a
   different incarnation while another incarnation is active is rejected; old
   processes and heartbeats are therefore fenced.

The session table intentionally remains leader-local, as in Kafka KRaft. Only
the resulting membership state transition is replicated, avoiding heartbeat
write amplification while making every routing decision converge at the same
FSM apply index.

## Consumer offset storage model

The internal topic follows Kafka KRaft's separation of concerns: Raft stores
the `__consumer_offsets` topic definition, partition assignments, replicas,
and leader epochs; the offset records remain data-plane entries in that topic.
Bootstrap waits for all current Raft voters to be durably active before writing
the single `TOPIC` command, so the initial replica set can meet the configured
replication factor and minISR after one broker fails. Application compacted
topics remain unsupported in distributed mode; only this broker-owned internal
topic is the exception.

## Routing and commit behavior

`FindCoordinator` derives its sorted consistent-hash members exclusively from
FSM brokers whose durable status is `active`. It neither consults sockets nor
the leader-local session table. Until `DEREGISTER` applies, the prior
coordinator remains valid in discovery responses; immediately after it applies,
the cached ring is rebuilt and cannot return the inactive broker.

Forwarding a group command to a coordinator can race membership transition. A
failed forward or an owner mismatch returns a structured retryable
`COORDINATOR_NOT_AVAILABLE`/`NOT_COORDINATOR` result, never `OK`. The handler
does not persist or advance an offset on this failure. A client may rediscover,
then retry the same generation/member commit; normal generation fencing and
monotonic-offset checks still decide whether it succeeds or requires rejoin.

## Split-brain and safety

- Only the Raft leader makes liveness-to-membership proposals.
- Only a successful Raft apply changes active membership and the coordinator
  ring.
- Followers may redirect heartbeats but may not independently remove a broker.
- Incarnation epochs fence stale processes after an ID is reused.
- A loss of Raft quorum cannot create a new durable active/inactive result;
  discovery continues from the last committed FSM state and commits fail
  closed.

## Tests

Unit coverage will prove:

1. Three active FSM brokers select a stable group coordinator.
2. A leader heartbeat timeout commits a durable inactive state.
3. After that apply, the same group cannot resolve to the dead broker.
4. Re-registration commits active status with a newer incarnation and rebuilds
   the ring.
5. The leader election grace prevents a premature inactive apply.
6. Stale heartbeat and stale registration epochs are fenced.
7. Coordinator-forward failures do not report commit success or advance an
   offset.
8. Internal-topic bootstrap waits for all Raft voters, commits only durable
   topology, and does not rewrite an existing assignment.

An opt-in Docker three-node E2E will create and sync a group, stop its current
coordinator, wait for a surviving broker to advertise a different coordinator
before the failover deadline, commit with the original fenced generation or
rejoin then commit, verify the durable offset, restart the stopped broker, and
wait for all brokers to become healthy. It will also assert that no surviving
broker reports the stopped address once the durable inactive transition is
visible.

## Operational limits

- Failover latency is bounded by Raft election plus one configurable heartbeat
  timeout and one reconcile tick; it is not instantaneous.
- A network partition that isolates a broker from the Raft leader fences that
  broker even if it can reach another follower. This is intentional: it avoids
  two independently active membership views.
- Legacy broker records without an incarnation token remain readable. Once a
  broker ID has registered with a token, tokenless registrations for that ID
  are rejected; rolling upgrades must therefore upgrade the broker process
  before it attempts to refresh an incarnation-aware registration.
