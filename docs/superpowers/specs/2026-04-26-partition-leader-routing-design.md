# Phase 3 — Partition Leader Routing (Kafka Model)

**Date:** 2026-04-26
**Status:** Approved
**Depends on:** Phase 2 (FindCoordinator) — completed

## Problem

CONSUME/STREAM commands currently route to the Raft leader via `checkLeaderOrRedirect`. This means:
- All consume traffic funnels to one broker (Raft leader)
- Partitions assigned to other brokers still require the Raft leader to proxy reads
- Adding brokers doesn't distribute consume load

Kafka routes FETCH directly to each partition's leader broker. Cursus already does this for PUBLISH (`isPartitionLeaderAndForward`) but not for CONSUME.

## Current Architecture

```
CONSUME → any broker → checkLeaderOrRedirect → Raft leader reads all partitions
PUBLISH → any broker → isPartitionLeaderAndForward → partition leader (already correct)
```

## Target Architecture

```
SDK: METADATA topic=T → {P0: broker-2:9002, P1: broker-3:9003, P2: broker-1:9001}
SDK: PartitionConsumer[P0] → broker-2:9002 (CONSUME directly)
SDK: PartitionConsumer[P1] → broker-3:9003 (CONSUME directly)
SDK: PartitionConsumer[P2] → broker-1:9001 (CONSUME directly)
```

---

## 3.1 Broker: METADATA Command

**Protocol:**

```
→ METADATA topic=<name>
← OK topic=<name> partitions=3 leaders=localhost:9002,localhost:9003,localhost:9001
```

`leaders` is a comma-separated list of `host:port` pairs, one per partition in order (P0, P1, P2, ...). Each address is the partition leader's advertised client address.

Any broker can answer — partition metadata is in the Raft FSM (`GetPartitionMetadata`).

**Implementation:**

- Add `handleMetadata` to `CommandHandler`
- For each partition, resolve the leader broker's advertised address:
  - If leader is this broker → use own `AdvertisedClientHost:AdvertisedBrokerPort`
  - If leader is another broker → forward `METADATA` to that broker (same pattern as `FIND_COORDINATOR`)
  - Simpler alternative: resolve from FSM broker registry + derive client port

Since each broker only knows its own advertised address, the simplest correct approach: the broker answering METADATA looks up each partition's leader ID from FSM, then for each unique leader broker, forwards a single `BROKER_ADDR` request. However, this adds complexity.

**Pragmatic approach:** Use the same pattern as `FIND_COORDINATOR` forwarding — forward the entire METADATA to each partition leader and merge. But this is N network calls.

**Simplest approach:** Since all brokers in a cluster share the same `AdvertisedClientHost` (e.g., `localhost`), derive the address as: `AdvertisedClientHost:clientPort`. The `clientPort` is already known per-broker in the router. For the partition leader's port, use `AdvertisedBrokerPort` from the leader's config — but we don't have it.

**Chosen approach:** Store `AdvertisedBrokerPort` in FSM broker registration. Each broker registers with its advertised port. METADATA reads from FSM.

**File changes:**
- `pkg/cluster/replication/fsm/fsm.go` — add `ClientPort int` to `BrokerInfo`
- `pkg/cluster/controller/controller.go` — include client port in broker registration
- `pkg/controller/command_handler.go` — add `handleMetadata`
- `pkg/controller/handler.go` — register METADATA command
- `pkg/server/main.go` — add METADATA to `isCommand`

## 3.2 Broker: FSM BrokerInfo — Add Client Port

**Current BrokerInfo:**
```go
type BrokerInfo struct {
    ID       string    `json:"id"`
    Addr     string    `json:"addr"`     // Raft address (e.g., broker-1:9001)
    Status   string    `json:"status"`
    LastSeen time.Time `json:"last_seen"`
}
```

**New BrokerInfo:**
```go
type BrokerInfo struct {
    ID         string    `json:"id"`
    Addr       string    `json:"addr"`
    ClientAddr string    `json:"client_addr,omitempty"` // advertised client address (e.g., localhost:9001)
    Status     string    `json:"status"`
    LastSeen   time.Time `json:"last_seen"`
}
```

When a broker registers itself (via `REGISTER` FSM command), it includes its client-facing address: `AdvertisedClientHost:AdvertisedBrokerPort` (or `hostname:BrokerPort` as fallback).

This solves the advertised address problem for both METADATA and FIND_COORDINATOR — any broker can look up any other broker's client address from FSM.

## 3.3 Broker: CONSUME/STREAM — Partition Leader Check

**Current:** `checkLeaderOrRedirect` checks if this broker is the **Raft leader**.

**Change:** Replace with partition-level check. The CONSUME handler already knows the topic and partition. Check if this broker is the **partition leader** (via FSM metadata).

```go
func (ch *CommandHandler) checkPartitionLeaderOrRedirect(conn net.Conn, topicName string, partitionID int) error {
    if !ch.Config.EnabledDistribution || ch.Cluster == nil {
        return nil // standalone, always local
    }

    if ch.Cluster.IsAuthorized(topicName, partitionID) {
        return nil // we are the partition leader
    }

    // Find partition leader's client address from FSM
    addr := ch.resolvePartitionLeaderAddr(topicName, partitionID)
    errResp := fmt.Sprintf("ERROR: NOT_LEADER LEADER_IS %s", addr)
    util.WriteWithLength(conn, []byte(errResp))
    return fmt.Errorf("not partition leader")
}
```

`resolvePartitionLeaderAddr` looks up the partition leader from FSM and returns its `ClientAddr`.

**File changes:**
- `pkg/controller/consume_handler.go` — replace `checkLeaderOrRedirect` calls with `checkPartitionLeaderOrRedirect`
- The old `checkLeaderOrRedirect` can be kept for backward compatibility or removed

## 3.4 Go SDK: Per-Partition Leader Connection

**Current:** `PartitionConsumer` uses `getLeaderConn()` → `ConnectWithFailover()` which connects to cached leader or any broker.

**Change:**
- `Consumer.Start()` calls `METADATA` to get partition leader map
- Each `PartitionConsumer` stores its partition leader address
- `PartitionConsumer.reconnect()` connects to its specific partition leader
- `NOT_LEADER LEADER_IS <addr>` response updates that partition's leader address

```go
type Consumer struct {
    // ... existing fields ...
    partitionLeaders map[int]string // partition ID → "host:port"
    partitionMu      sync.RWMutex
}

func (c *Consumer) fetchMetadata() error {
    // send METADATA topic=<topic> to any broker
    // parse "OK topic=T partitions=N leaders=addr0,addr1,addr2"
    // populate c.partitionLeaders
}
```

`PartitionConsumer` changes:
- `reconnect()` uses `c.consumer.getPartitionLeaderAddr(partitionID)` instead of `ConnectWithFailover()`
- `handleBrokerError` on `NOT_LEADER LEADER_IS` updates `c.consumer.partitionLeaders[partitionID]`

**File changes:**
- `sdk/consumer.go` — add `partitionLeaders`, `fetchMetadata()`, `getPartitionLeaderAddr()`
- `sdk/partition.go` — `reconnect()` uses partition-specific leader address

## 3.5 Python SDK: Per-Partition Leader Connection

**Current:** `_partition_poll_loop` uses `_connect_to_leader()` which connects to any broker.

**Change:**
- `_join_and_sync()` after getting assignments, call `METADATA` to get partition leaders
- `_partition_poll_loop` connects to its specific partition leader
- `NOT_LEADER LEADER_IS` updates that partition's leader

```python
class Consumer:
    def __init__(self, config):
        # ... existing ...
        self._partition_leaders: dict[int, str] = {}  # partition → "host:port"

    def _fetch_metadata(self) -> None:
        resp = self._send_command(f"METADATA topic={self._config.topic}")
        # parse leaders=addr0,addr1,addr2
        # populate self._partition_leaders

    def _connect_to_partition_leader(self, partition: int) -> SyncConnection:
        addr = self._partition_leaders.get(partition, self._leader_addr or self._config.brokers[0])
        conn = SyncConnection(addr)
        conn.connect()
        return conn
```

**File changes:**
- `cursus-python/src/cursus/consumer.py` — add `_partition_leaders`, `_fetch_metadata()`, `_connect_to_partition_leader()`, update `_partition_poll_loop`

## 3.6 Affected Commands Summary

| Command | Before | After |
|---|---|---|
| METADATA | (new) | Any broker → partition leader map |
| CONSUME | Raft leader | Partition leader |
| STREAM | Raft leader | Partition leader |
| PUBLISH | Partition leader (unchanged) | Partition leader (unchanged) |

## 3.7 Files Changed Summary

**Broker:**
- `pkg/cluster/replication/fsm/fsm.go` — `BrokerInfo.ClientAddr` field
- `pkg/cluster/controller/controller.go` — include ClientAddr in registration
- `pkg/controller/command_handler.go` — `handleMetadata`, `resolvePartitionLeaderAddr`
- `pkg/controller/handler.go` — register METADATA command
- `pkg/controller/consume_handler.go` — `checkPartitionLeaderOrRedirect`
- `pkg/server/main.go` — add METADATA to `isCommand`

**Go SDK:**
- `sdk/consumer.go` — `partitionLeaders`, `fetchMetadata()`, `getPartitionLeaderAddr()`
- `sdk/partition.go` — reconnect uses partition leader

**Python SDK:**
- `src/cursus/consumer.py` — `_partition_leaders`, `_fetch_metadata()`, `_connect_to_partition_leader()`
