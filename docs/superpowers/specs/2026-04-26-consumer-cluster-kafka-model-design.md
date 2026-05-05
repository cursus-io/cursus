# Consumer Cluster Connectivity — Kafka Model

**Date:** 2026-04-26
**Status:** Approved
**Scope:** Phase 1 (immediate fix) + Phase 2 (FindCoordinator API)

## Problem

In a 3-node Raft cluster, consumers fail because:

1. **Session affinity**: JOIN_GROUP creates a session on the Raft leader, but subsequent CONSUME/HEARTBEAT may hit a different broker that has no session → `consumer not found`.
2. **Docker address leak**: NOT_LEADER redirect returns Docker-internal addresses (`broker-1:9000`) that external clients cannot resolve.
3. **No coordinator routing**: All group commands funnel to the Raft leader, creating a bottleneck and forwarding loops (coordinator ↔ leader infinite recursion, fixed in prior commit).

Kafka solves these with: `FindCoordinator` API, per-group coordinator ownership, `advertised.listeners`, and partition-leader-based fetch routing.

## Current Architecture

```
Client → any broker → isLeaderAndForward → Raft leader handles everything
                                            (JOIN, CONSUME, HEARTBEAT, COMMIT)
```

- Every broker has a `Coordinator` instance in memory
- Group state is replicated via Raft FSM (`GROUP_SYNC`, `OFFSET_SYNC` log entries)
- `ConsistentHashRing` exists in `ClusterRouter.FindCoordinator()` but is only used for internal forwarding
- `AdvertisedClientHost` / `AdvertisedBrokerPort` config exists but is not set in cluster docker-compose

## Target Architecture (after Phase 2)

```
Client → any broker:  FIND_COORDINATOR group=G1 → coordinator=broker-2:9002
Client → broker-2:    JOIN_GROUP, SYNC_GROUP, HEARTBEAT, COMMIT_OFFSET
Client → broker-2:    NOT_COORDINATOR → re-discover coordinator
Client → partition leader: CONSUME (via NOT_LEADER redirect, phase 3 future)
```

---

## Phase 1 — Make Consumer Work in Cluster

### 1.1 Broker: Advertised Address in Docker Compose

Add environment variables to `test/cluster/docker-compose.yml`:

```yaml
broker-1:
  environment:
    - ADVERTISED_CLIENT_HOST=localhost
    - ADVERTISED_BROKER_PORT=9001

broker-2:
  environment:
    - ADVERTISED_CLIENT_HOST=localhost
    - ADVERTISED_BROKER_PORT=9002

broker-3:
  environment:
    - ADVERTISED_CLIENT_HOST=localhost
    - ADVERTISED_BROKER_PORT=9003
```

`checkLeaderOrRedirect` already uses these values when constructing the `NOT_LEADER LEADER_IS <addr>` response. No broker code change needed.

### 1.2 Python SDK: Leader Redirect Handling

**File:** `cursus-python/src/cursus/consumer.py`

`_send_command` currently ignores NOT_LEADER responses. Change:

```python
def _send_command(self, cmd: str) -> str:
    max_retries = 3
    for attempt in range(max_retries):
        conn = self._connect_to_leader()
        try:
            conn.write_frame(encode_message("", cmd))
            resp = conn.read_frame().decode()
        finally:
            conn.close()

        if "NOT_LEADER LEADER_IS" in resp:
            # Extract leader address and update cached leader
            parts = resp.split()
            for i, p in enumerate(parts):
                if p == "LEADER_IS" and i + 1 < len(parts):
                    self._leader_addr = parts[i + 1]
                    break
            continue  # retry with new leader
        return resp
    return resp  # return last response even if redirect
```

### 1.3 Go SDK: No Change

Go SDK already has `handleLeaderRedirection` + `ConnectWithFailover` with leader caching. Once brokers return reachable advertised addresses, it works.

---

## Phase 2 — FindCoordinator API

### 2.1 New Broker Command: FIND_COORDINATOR

**Protocol:**

```
→ FIND_COORDINATOR group=<group_name>
← OK coordinator_id=<broker_id> host=<advertised_host> port=<advertised_port>
```

Any broker can answer this — the consistent hash ring is deterministic across all nodes (built from the same FSM broker registry).

**Implementation:**

- Add `handleFindCoordinator` to `CommandHandler`
- Uses existing `ClusterRouter.FindCoordinator(groupName)` to get `(brokerID, raftAddr)`
- Resolves broker's advertised address from FSM broker registry
- Returns advertised host/port (external-facing address)

**File changes:**
- `pkg/controller/handler.go` — add command entry `{prefix: "FIND_COORDINATOR ", ...}`
- `pkg/controller/command_handler.go` — add `handleFindCoordinator(cmd)`

### 2.2 Coordinator-Local Group Command Processing

**Current flow (broken):**
```
handleJoinGroup → isLeaderAndForward → Raft leader processes
```

**New flow:**
```
handleJoinGroup →
  if not coordinator: return "NOT_COORDINATOR coordinator_host=<addr> coordinator_port=<port>"
  if coordinator: process locally + applyRaftViaLeader()
```

**Key change**: Group commands (JOIN, SYNC, LEAVE, HEARTBEAT, COMMIT_OFFSET, FETCH_OFFSET, BATCH_COMMIT) no longer use `isLeaderAndForward`. Instead:

1. Check if this broker is the coordinator for the group (via consistent hash ring)
2. If NOT coordinator → return `NOT_COORDINATOR` error with coordinator address
3. If coordinator → execute business logic locally, then apply state via Raft

**NOT_COORDINATOR response format:**
```
ERROR: NOT_COORDINATOR host=<advertised_host> port=<advertised_port>
```

### 2.3 Raft Apply from Non-Leader (applyRaftViaLeader)

`raft.Apply()` returns `ErrNotLeader` on non-leader nodes. The coordinator may not be the Raft leader.

**Solution:** Add `applyViaLeader` method to `CommandHandler`:

```go
func (ch *CommandHandler) applyViaLeader(cmdType string, payload map[string]interface{}) (interface{}, error) {
    // Try local apply first (fast path if we're the leader)
    result, err := ch.applyAndWait(cmdType, payload)
    if err == nil {
        return result, nil
    }

    // If ErrNotLeader, forward the Raft log entry to leader
    if !ch.isDistributed() {
        return nil, err
    }

    data, _ := json.Marshal(payload)
    forwardCmd := fmt.Sprintf("RAFT_APPLY type=%s payload=%s", cmdType, string(data))
    encodedCmd := util.EncodeMessage("", forwardCmd)
    resp, fwdErr := ch.Cluster.Router.ForwardToLeader(string(encodedCmd))
    if fwdErr != nil {
        return nil, fmt.Errorf("forward raft apply to leader: %w", fwdErr)
    }
    if strings.HasPrefix(resp, "ERROR") {
        return nil, fmt.Errorf("leader raft apply: %s", resp)
    }
    return resp, nil
}
```

**New broker command:** `RAFT_APPLY type=<type> payload=<json>`
- Only accepted from internal cluster connections (not exposed to clients)
- Leader receives, calls `applyAndWait`, returns result
- This breaks the coordinator ↔ leader infinite loop because RAFT_APPLY does not re-enter the command handler pipeline

### 2.4 Group Command Handlers — Refactored Pattern

All group commands follow the same pattern:

```go
func (ch *CommandHandler) handleJoinGroup(cmd string, ctx *ClientContext) string {
    // ... parse args ...

    if ch.isDistributed() {
        // Check coordinator ownership
        coordAddr, isCoord := ch.checkCoordinator(groupName)
        if !isCoord {
            return fmt.Sprintf("ERROR: NOT_COORDINATOR host=%s port=%d", coordAddr.Host, coordAddr.Port)
        }
        // We ARE the coordinator — process locally, apply via leader
        joinPayload := map[string]interface{}{...}
        _, err := ch.applyViaLeader("GROUP_SYNC", joinPayload)
        if err != nil {
            return fmt.Sprintf("ERROR: %v", err)
        }
        // ... read result from local coordinator state ...
    } else {
        // ... standalone mode (unchanged) ...
    }
}
```

`checkCoordinator` helper:
```go
func (ch *CommandHandler) checkCoordinator(groupName string) (AdvertisedAddr, bool) {
    if !ch.hasRouter() {
        return AdvertisedAddr{}, true // standalone, always local
    }
    id, _, err := ch.Cluster.Router.FindCoordinator(groupName)
    if err != nil {
        return AdvertisedAddr{}, true // fallback to local
    }
    if id == ch.Cluster.Router.BrokerID() {
        return AdvertisedAddr{}, true // we are the coordinator
    }
    // Resolve advertised address for the coordinator broker
    addr := ch.resolveAdvertisedAddr(id)
    return addr, false
}
```

### 2.5 SDK: FindCoordinator Protocol

**Go SDK (`sdk/consumer.go`):**

```go
func (c *Consumer) Start(handler func(Message) error) error {
    // 1. Find coordinator
    coordAddr, err := c.findCoordinator()
    if err != nil {
        return err
    }
    c.coordinatorAddr = coordAddr

    // 2. JOIN/SYNC via coordinator connection
    gen, mid, assignments, err := c.joinGroupWithRetry() // uses coordinatorAddr
    // ...

    // 3. CONSUME via partition leader (existing getLeaderConn)
    // ...
}

func (c *Consumer) findCoordinator() (string, error) {
    conn, _, err := c.client.ConnectWithFailover() // any broker
    defer conn.Close()
    resp := sendCommand(conn, fmt.Sprintf("FIND_COORDINATOR group=%s", c.config.GroupID))
    // parse "OK coordinator_id=... host=... port=..."
    return fmt.Sprintf("%s:%s", host, port), nil
}
```

- `joinGroupWithRetry`, `syncGroup`, `heartbeatLoop`, `commitOffset`, `fetchOffset` → use `coordinatorConn` instead of `getLeaderConn`
- `consumeLoop` (PartitionConsumer) → continue using `getLeaderConn` (partition leader)

**Python SDK (`cursus-python/src/cursus/consumer.py`):**

```python
def _find_coordinator(self) -> str:
    """Ask any broker for the coordinator of this group."""
    resp = self._send_command(f"FIND_COORDINATOR group={self._config.group_id}")
    # parse "OK coordinator_id=... host=... port=..."
    return f"{host}:{port}"

def _send_coordinator_command(self, cmd: str) -> str:
    """Send command to the group coordinator, with NOT_COORDINATOR retry."""
    for attempt in range(3):
        conn = SyncConnection(self._coordinator_addr)
        conn.connect()
        try:
            conn.write_frame(encode_message("", cmd))
            resp = conn.read_frame().decode()
        finally:
            conn.close()

        if "NOT_COORDINATOR" in resp:
            self._coordinator_addr = self._find_coordinator()
            continue
        return resp
    return resp
```

- `_join_and_sync`, `_heartbeat_loop`, `_send_command` for group ops → use `_send_coordinator_command`
- `_partition_poll_loop` → continue using `_connect_to_leader` (partition leader)

### 2.6 Affected Commands Summary

| Command | Current Target | New Target |
|---|---|---|
| FIND_COORDINATOR | (new) | Any broker |
| JOIN_GROUP | Raft leader | Coordinator |
| SYNC_GROUP | Raft leader | Coordinator |
| LEAVE_GROUP | Raft leader | Coordinator |
| HEARTBEAT | Raft leader | Coordinator |
| COMMIT_OFFSET | Raft leader | Coordinator |
| FETCH_OFFSET | Raft leader | Coordinator |
| BATCH_COMMIT | Raft leader | Coordinator |
| CONSUME/STREAM | Raft leader | Partition leader (unchanged, phase 3) |
| RAFT_APPLY | (new, internal) | Raft leader only |

### 2.7 Files Changed

**Broker:**
- `pkg/controller/handler.go` — add FIND_COORDINATOR and RAFT_APPLY command entries
- `pkg/controller/command_handler.go` — `handleFindCoordinator`, `checkCoordinator`, refactor all group handlers to coordinator pattern
- `pkg/controller/cluster_handler.go` — `applyViaLeader` method, `resolveAdvertisedAddr` helper
- `test/cluster/docker-compose.yml` — add ADVERTISED_CLIENT_HOST/ADVERTISED_BROKER_PORT

**Go SDK:**
- `sdk/consumer.go` — `findCoordinator()`, coordinator connection for group ops
- `sdk/consumer_client.go` — `ConnectToAddr(addr)` helper for coordinator connection

**Python SDK:**
- `src/cursus/consumer.py` — `_find_coordinator()`, `_send_coordinator_command()`, split group ops from consume ops

---

## Future Phases (out of scope)

**Phase 3 — Partition Leader Routing:** `METADATA` command returns per-partition leader addresses. SDK fetches from partition leaders directly instead of Raft leader.

**Phase 4 — advertised.listeners:** Multiple listener names (INTERNAL, EXTERNAL) with separate bind addresses. Broker-to-broker uses INTERNAL, client uses EXTERNAL.
