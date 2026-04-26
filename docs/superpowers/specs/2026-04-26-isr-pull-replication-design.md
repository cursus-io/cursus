# ISR Pull-Based Replication (Kafka Model)

**Date:** 2026-04-26
**Status:** Draft
**Scope:** Replace push-based REPLICATE_MESSAGE with pull-based FETCH_REPLICA

## Current Architecture (Push)

```
Producer → Partition Leader → EnqueueBatchLeader (local write)
                            → ReplicateToFollowers (push TCP to each follower)
                            → FlushDisk + AdvanceHWM → ACK

Follower: receives REPLICATE_MESSAGE → ReplicaAppend (passive)
```

Problems: no ordering guarantee, follower doesn't control pace, no log reconciliation on failover.

## Target Architecture (Pull)

```
Producer → Partition Leader → EnqueueBatchLeader (local write)
                            → wait for HWM to advance → ACK

Follower: FETCH_REPLICA loop → pull from leader's log → ReplicaAppend
Leader:   track each follower's fetch offset → HWM = min(all ISR LEOs)
```

## Design

### D1: FETCH_REPLICA Command (Broker)

New internal command for follower-to-leader replication:

```
→ FETCH_REPLICA topic=<name> partition=<N> offset=<N> broker_id=<id>
← Binary batch (same 0xBA7C format as CONSUME response)
```

- Only accepted from cluster-internal connections
- Leader reads from its local log starting at the requested offset
- Returns up to `replication_batch_size` messages
- Leader updates the follower's known LEO in ISRManager

Handler in `consume_handler.go` or new `replication_handler.go`:
```go
func (ch *CommandHandler) handleFetchReplica(cmd string, conn net.Conn) {
    // Parse topic, partition, offset, broker_id
    // Read from partition log (ReadMessages, not ReadCommitted — replicas read beyond HWM)
    // Update ISRManager: follower's LEO = offset + len(messages)
    // Send binary batch response
}
```

### D2: Follower Fetch Loop (Broker)

Each broker runs a fetch loop for every partition where it's a follower:

```go
type ReplicaFetcher struct {
    topic       string
    partition   int
    leaderAddr  string
    localOffset uint64  // next offset to fetch
}

func (rf *ReplicaFetcher) Run(ctx context.Context) {
    for {
        // Connect to leader
        // Send FETCH_REPLICA topic=T partition=P offset=<localOffset>
        // Receive batch
        // ReplicaAppend locally
        // Update localOffset
        // If empty response, wait 100ms
    }
}
```

Lifecycle:
- Started when broker learns it's a follower for a partition (from FSM partition metadata)
- Stopped when broker becomes leader or partition is reassigned
- Restarts with new leader address on leader change

### D3: HWM = min(ISR LEOs)

Replace `AdvanceHWM()` (sets HWM = LEO) with ISR-based HWM:

```go
func (ch *CommandHandler) computeHWM(topic string, partition int) uint64 {
    localLEO := p.LEO.Load()
    minLEO := localLEO

    for _, replica := range isr {
        if replica == self { continue }
        replicaLEO := isrManager.GetReplicaLEO(replica, topic, partition)
        if replicaLEO < minLEO {
            minLEO = replicaLEO
        }
    }
    return minLEO
}
```

ISRManager tracks each follower's LEO (updated on FETCH_REPLICA):
```go
type ISRManager struct {
    // existing fields...
    replicaLEOs map[string]map[string]uint64  // brokerID -> "topic-partition" -> LEO
}
```

### D4: Produce ACK Waits for HWM

Currently: produce → local write → push replicate → flush → advanceHWM → ACK

New: produce → local write → flush → wait for HWM >= produced offset → ACK

```go
// produce_handler.go
p.EnqueueBatchLeader(msgs)
p.FlushDisk()

if acksLower != "0" {
    // Wait for followers to fetch up to this offset
    deadline := time.Now().Add(ackTimeout)
    for time.Now().Before(deadline) {
        hwm := ch.computeHWM(topic, partition)
        if hwm >= assignedOffset+1 {
            p.SetHWM(hwm)
            break
        }
        time.Sleep(10 * time.Millisecond)
    }
}
```

### D5: Remove ReplicateToFollowers (Push)

Delete:
- `ClusterController.ReplicateToFollowers`
- `handleReplicateMessage`
- `REPLICATE_MESSAGE` command registration

### D6: ISR Determination by Fetch Lag

Replace heartbeat-based ISR with fetch-lag-based:

```go
func (i *ISRManager) ComputeISR(topic string, partition int) []string {
    leaderLEO := getLeaderLEO(topic, partition)

    for _, replica := range metadata.Replicas {
        replicaLEO := i.replicaLEOs[replica][key]
        lag := leaderLEO - replicaLEO
        if lag <= maxLagMessages || timeSinceLastFetch < maxLagTime {
            currentISR = append(currentISR, replica)
        }
    }
}
```

### D7: Partition State Machine

Each partition on each broker has a role:
- **Leader**: accepts produce, serves FETCH_REPLICA to followers, computes HWM
- **Follower**: runs ReplicaFetcher, serves CONSUME from local log up to HWM
- **None**: partition not assigned to this broker

On FSM partition metadata change (leader change):
- Stop old role (fetcher or produce handler)
- Start new role
- Truncate follower log if needed (follower's log diverged from new leader)

## Files Changed

### New files:
- `pkg/controller/replication_handler.go` — FETCH_REPLICA handler
- `pkg/cluster/replication/replica_fetcher.go` — follower fetch loop

### Modified files:
- `pkg/cluster/replication/isr_manager.go` — replicaLEOs tracking, fetch-lag ISR
- `pkg/cluster/controller/controller.go` — remove ReplicateToFollowers, add fetcher lifecycle
- `pkg/controller/produce_handler.go` — HWM-wait ACK instead of push replication
- `pkg/controller/handler.go` — register FETCH_REPLICA
- `pkg/topic/partition.go` — ReadBeyondHWM for replica fetch (reads LEO, not HWM)
- `pkg/server/main.go` — add FETCH_REPLICA to isCommand

### Deleted:
- ReplicateToFollowers method
- handleReplicateMessage handler
- REPLICATE_MESSAGE command entry

## Task Decomposition

| Task | Description | Deps |
|---|---|---|
| 1 | ISRManager: add replicaLEOs tracking + GetReplicaLEO/UpdateReplicaLEO | - |
| 2 | Partition: add ReadBeyondHWM (reads up to LEO, not HWM) | - |
| 3 | FETCH_REPLICA command handler | 1, 2 |
| 4 | ReplicaFetcher: follower fetch loop | 3 |
| 5 | Fetcher lifecycle management (start/stop on leader change) | 4 |
| 6 | Produce handler: HWM-wait ACK + computeHWM | 1 |
| 7 | Remove push replication (ReplicateToFollowers, REPLICATE_MESSAGE) | 4, 6 |
| 8 | ISR: fetch-lag based instead of heartbeat | 1 |
| 9 | Log truncation on follower (diverged log reconciliation) | 4 |
| 10 | Integration test: cluster E2E with pull replication | 7, 8 |
