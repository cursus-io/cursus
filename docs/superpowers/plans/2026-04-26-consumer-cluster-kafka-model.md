# Consumer Cluster Kafka Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable consumer group operations in a 3-node Raft cluster by implementing Kafka-style FindCoordinator routing and advertised addresses.

**Architecture:** Group commands (JOIN/SYNC/LEAVE/HEARTBEAT/COMMIT) route to a per-group coordinator broker determined by consistent hashing. The coordinator applies state changes via Raft (forwarding to the Raft leader internally). SDKs discover the coordinator via `FIND_COORDINATOR` and maintain a dedicated coordinator connection.

**Tech Stack:** Go (broker + Go SDK), Python (Python SDK), Docker Compose (cluster config)

**Spec:** `docs/superpowers/specs/2026-04-26-consumer-cluster-kafka-model-design.md`

---

### Task 1: Docker Compose — Advertised Address Config

**Files:**
- Modify: `test/cluster/docker-compose.yml`

- [ ] **Step 1: Add ADVERTISED_CLIENT_HOST and ADVERTISED_BROKER_PORT to all brokers**

```yaml
# broker-1 environment section — add these two lines:
    - ADVERTISED_CLIENT_HOST=localhost
    - ADVERTISED_BROKER_PORT=9001

# broker-2 environment section — add these two lines:
    - ADVERTISED_CLIENT_HOST=localhost
    - ADVERTISED_BROKER_PORT=9002

# broker-3 environment section — add these two lines:
    - ADVERTISED_CLIENT_HOST=localhost
    - ADVERTISED_BROKER_PORT=9003
```

- [ ] **Step 2: Verify by rebuilding and checking NOT_LEADER response**

Run:
```bash
cd test/cluster && docker compose down && docker compose up -d --build broker-1 broker-2 broker-3
sleep 12
# Send a command to a non-leader broker — the redirect address should show localhost:<port>
docker run --rm --network cluster_network python:3.11-slim python3 -c "
import socket, struct
s = socket.socket(); s.connect(('broker-2', 9000)); s.settimeout(5)
msg = struct.pack('>H', 0) + b'LIST'
s.send(struct.pack('>I', len(msg)) + msg)
rl = s.recv(4); resp = s.recv(struct.unpack('>I', rl)[0])
print(resp.decode())
s.close()
"
```

Expected: Response containing topic list OR `NOT_LEADER LEADER_IS localhost:9001` (with `localhost` not `broker-1`).

- [ ] **Step 3: Commit**

```bash
git add test/cluster/docker-compose.yml
git commit -m "config: add advertised client host/port to cluster docker-compose"
```

---

### Task 2: Broker — FIND_COORDINATOR Command

**Files:**
- Modify: `pkg/controller/handler.go:59-82` (command registry)
- Modify: `pkg/controller/command_handler.go` (add handler function)
- Test: `pkg/controller/handler_test.go`

- [ ] **Step 1: Write the test**

Add to `pkg/controller/handler_test.go`:

```go
func TestHandleFindCoordinator_Standalone(t *testing.T) {
	cfg := config.DefaultConfig()
	sm := stream.NewStreamManager(cfg.MaxStreamConnections, cfg.StreamTimeout, cfg.StreamHeartbeatInterval)
	dm := disk.NewDiskManager(cfg)
	smAdapter, _ := topic.NewStreamManagerAdapter(sm)
	tm := topic.NewTopicManager(cfg, dm, smAdapter)
	cd := coordinator.NewCoordinator(context.Background(), cfg, tm)

	ch := NewCommandHandler(tm, cfg, cd, sm, nil)
	ctx := NewClientContext("default-group", 0)

	resp := ch.HandleCommand("FIND_COORDINATOR group=test-group", ctx)
	// Standalone mode: should return OK with host=localhost port=<broker_port>
	assert.Contains(t, resp, "OK")
	assert.Contains(t, resp, fmt.Sprintf("port=%d", cfg.BrokerPort))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/controller/ -run TestHandleFindCoordinator_Standalone -v`
Expected: FAIL — unknown command

- [ ] **Step 3: Add handleFindCoordinator to command_handler.go**

Add after the `handleBatchCommit` function in `pkg/controller/command_handler.go`:

```go
// handleFindCoordinator processes FIND_COORDINATOR command
func (ch *CommandHandler) handleFindCoordinator(cmd string) string {
	args := parseKeyValueArgs(cmd[17:]) // len("FIND_COORDINATOR ") = 17
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "ERROR: FIND_COORDINATOR requires group parameter"
	}

	host := "localhost"
	port := ch.Config.BrokerPort

	if ch.isDistributed() {
		coordID, coordRaftAddr, err := ch.Cluster.Router.FindCoordinator(groupName)
		if err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		}

		if coordID == ch.Cluster.Router.BrokerID() {
			// We are the coordinator — return our own advertised address
			if ch.Config.AdvertisedClientHost != "" {
				host = ch.Config.AdvertisedClientHost
			}
			if ch.Config.AdvertisedBrokerPort > 0 {
				port = ch.Config.AdvertisedBrokerPort
			}
		} else {
			// Another broker is coordinator — derive client address from Raft address
			if h, _, err := net.SplitHostPort(coordRaftAddr); err == nil {
				host = h
			}
			port = ch.Cluster.Router.ClientPort()
		}

		return fmt.Sprintf("OK coordinator_id=%s host=%s port=%d", coordID, host, port)
	}

	// Standalone: this broker is always the coordinator
	if ch.Config.AdvertisedClientHost != "" {
		host = ch.Config.AdvertisedClientHost
	}
	if ch.Config.AdvertisedBrokerPort > 0 {
		port = ch.Config.AdvertisedBrokerPort
	}
	return fmt.Sprintf("OK coordinator_id=standalone host=%s port=%d", host, port)
}
```

Add `"net"` to the imports in `command_handler.go` if not already present.

- [ ] **Step 4: Register the command in handler.go**

In `pkg/controller/handler.go`, add to the `ch.commands` slice (before the `REPLICATE_MESSAGE` entry):

```go
{prefix: "FIND_COORDINATOR ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleFindCoordinator(cmd) }},
```

- [ ] **Step 5: Add ClientPort() method to ClusterRouter**

In `pkg/cluster/controller/router.go`, add after the `BrokerID()` method:

```go
func (r *ClusterRouter) ClientPort() int {
	return r.clientPort
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `go test ./pkg/controller/ -run TestHandleFindCoordinator_Standalone -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add pkg/controller/handler.go pkg/controller/command_handler.go pkg/controller/handler_test.go pkg/cluster/controller/router.go
git commit -m "feat: add FIND_COORDINATOR command for Kafka-style coordinator discovery"
```

---

### Task 3: Broker — RAFT_APPLY Internal Command

**Files:**
- Modify: `pkg/controller/handler.go:59-82` (command registry)
- Modify: `pkg/controller/cluster_handler.go` (add applyViaLeader + handleRaftApply)
- Test: `pkg/controller/handler_test.go`

- [ ] **Step 1: Write the test**

Add to `pkg/controller/handler_test.go`:

```go
func TestHandleRaftApply_Standalone(t *testing.T) {
	cfg := config.DefaultConfig()
	sm := stream.NewStreamManager(cfg.MaxStreamConnections, cfg.StreamTimeout, cfg.StreamHeartbeatInterval)
	dm := disk.NewDiskManager(cfg)
	smAdapter, _ := topic.NewStreamManagerAdapter(sm)
	tm := topic.NewTopicManager(cfg, dm, smAdapter)
	cd := coordinator.NewCoordinator(context.Background(), cfg, tm)

	ch := NewCommandHandler(tm, cfg, cd, sm, nil)
	ctx := NewClientContext("default-group", 0)

	// Standalone: RAFT_APPLY should return error since Raft is not available
	resp := ch.HandleCommand(`RAFT_APPLY type=GROUP_SYNC payload={"type":"JOIN","group":"g1","member":"m1","topic":"t1"}`, ctx)
	assert.Contains(t, resp, "ERROR")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/controller/ -run TestHandleRaftApply_Standalone -v`
Expected: FAIL — unknown command

- [ ] **Step 3: Add handleRaftApply to cluster_handler.go**

Add to `pkg/controller/cluster_handler.go`:

```go
// handleRaftApply processes RAFT_APPLY command (internal, from coordinator to leader).
func (ch *CommandHandler) handleRaftApply(cmd string) string {
	args := parseKeyValueArgs(cmd[11:]) // len("RAFT_APPLY ") = 11
	cmdType := args["type"]
	if cmdType == "" {
		return "ERROR: RAFT_APPLY requires type parameter"
	}
	payloadStr := args["payload"]
	if payloadStr == "" {
		return "ERROR: RAFT_APPLY requires payload parameter"
	}

	if !ch.isDistributed() {
		return "ERROR: RAFT_APPLY requires distributed mode"
	}

	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(payloadStr), &payload); err != nil {
		return fmt.Sprintf("ERROR: invalid payload JSON: %v", err)
	}

	_, err := ch.applyAndWait(cmdType, payload)
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}
	return "OK"
}
```

- [ ] **Step 4: Add applyViaLeader to cluster_handler.go**

Add to `pkg/controller/cluster_handler.go`:

```go
// applyViaLeader tries local Raft apply first; if not leader, forwards to leader via RAFT_APPLY.
func (ch *CommandHandler) applyViaLeader(cmdType string, payload map[string]interface{}) (interface{}, error) {
	result, err := ch.applyAndWait(cmdType, payload)
	if err == nil {
		return result, nil
	}

	if !ch.isDistributed() || ch.Cluster.Router == nil {
		return nil, err
	}

	data, jsonErr := json.Marshal(payload)
	if jsonErr != nil {
		return nil, fmt.Errorf("marshal payload: %w", jsonErr)
	}

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

- [ ] **Step 5: Register RAFT_APPLY in handler.go**

In `pkg/controller/handler.go`, add to the `ch.commands` slice (after REPLICATE_MESSAGE):

```go
{prefix: "RAFT_APPLY ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleRaftApply(cmd) }},
```

- [ ] **Step 6: Run test to verify it passes**

Run: `go test ./pkg/controller/ -run TestHandleRaftApply_Standalone -v`
Expected: PASS

- [ ] **Step 7: Run all controller tests**

Run: `go test ./pkg/controller/... -count=1 -timeout 60s`
Expected: All PASS

- [ ] **Step 8: Commit**

```bash
git add pkg/controller/handler.go pkg/controller/cluster_handler.go pkg/controller/handler_test.go
git commit -m "feat: add RAFT_APPLY internal command and applyViaLeader for coordinator pattern"
```

---

### Task 4: Broker — Refactor Group Commands to Coordinator Pattern

**Files:**
- Modify: `pkg/controller/command_handler.go` (refactor all 8 group handlers)
- Modify: `pkg/controller/cluster_handler.go` (add checkCoordinator helper)
- Test: Existing tests must continue to pass

- [ ] **Step 1: Add checkCoordinator helper to cluster_handler.go**

Add to `pkg/controller/cluster_handler.go`:

```go
// AdvertisedAddr holds the external-facing address for a coordinator broker.
type AdvertisedAddr struct {
	Host string
	Port int
}

// checkCoordinator checks if this broker is the coordinator for the given group.
// Returns (addr, false) if another broker is coordinator, or (_, true) if we are.
func (ch *CommandHandler) checkCoordinator(groupName string) (AdvertisedAddr, bool) {
	if !ch.hasRouter() {
		return AdvertisedAddr{}, true
	}
	id, raftAddr, err := ch.Cluster.Router.FindCoordinator(groupName)
	if err != nil {
		return AdvertisedAddr{}, true
	}
	if id == ch.Cluster.Router.BrokerID() {
		return AdvertisedAddr{}, true
	}
	host := "localhost"
	if h, _, splitErr := net.SplitHostPort(raftAddr); splitErr == nil {
		host = h
	}
	port := ch.Cluster.Router.ClientPort()
	return AdvertisedAddr{Host: host, Port: port}, false
}

// notCoordinatorResponse builds the NOT_COORDINATOR error response.
func notCoordinatorResponse(addr AdvertisedAddr) string {
	return fmt.Sprintf("ERROR: NOT_COORDINATOR host=%s port=%d", addr.Host, addr.Port)
}
```

Add `"net"` to imports if not present.

- [ ] **Step 2: Refactor handleJoinGroup**

In `pkg/controller/command_handler.go`, replace the distributed block in `handleJoinGroup`:

Find the `if ch.isDistributed() {` block (currently using `isLeaderAndForward`) and replace with:

```go
	if ch.isDistributed() {
		coordAddr, isCoord := ch.checkCoordinator(groupName)
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}

		joinPayload := map[string]interface{}{
			"type":   "JOIN",
			"group":  groupName,
			"member": consumerID,
			"topic":  topicName,
		}

		_, err := ch.applyViaLeader("GROUP_SYNC", joinPayload)
		if err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		}
		assignments = ch.Coordinator.GetMemberAssignments(groupName, consumerID)
	} else {
```

- [ ] **Step 3: Refactor handleSyncGroup**

Replace the distributed block in `handleSyncGroup`:

```go
	if ch.isDistributed() {
		coordAddr, isCoord := ch.checkCoordinator(groupName)
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}
	}
```

Remove the `isLeaderAndForward` call. The rest of the function (reading assignments) stays the same.

- [ ] **Step 4: Refactor handleLeaveGroup**

Replace the distributed block in `handleLeaveGroup`:

```go
	if ch.isDistributed() {
		coordAddr, isCoord := ch.checkCoordinator(groupName)
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}

		payload := map[string]interface{}{
			"type":   "LEAVE",
			"group":  groupName,
			"member": consumerID,
		}

		_, err := ch.applyViaLeader("GROUP_SYNC", payload)
		if err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		}
	} else {
```

- [ ] **Step 5: Refactor handleHeartbeat**

Replace the distributed block in `handleHeartbeat`:

```go
	if ch.isDistributed() {
		coordAddr, isCoord := ch.checkCoordinator(groupName)
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}
	}
```

Remove the `isLeaderAndForward` call. The rest (RecordHeartbeat) stays.

- [ ] **Step 6: Refactor handleFetchOffset**

Replace the distributed block in `handleFetchOffset`:

```go
	if ch.isDistributed() {
		coordAddr, isCoord := ch.checkCoordinator(groupName)
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}
		if !ch.isAuthorizedForPartition(topicName, partition) {
			return fmt.Sprintf("NOT_AUTHORIZED_FOR_PARTITION %s:%d", topicName, partition)
		}
	}
```

- [ ] **Step 7: Refactor handleGroupStatus**

Replace the distributed block in `handleGroupStatus`:

```go
	if ch.isDistributed() {
		coordAddr, isCoord := ch.checkCoordinator(groupName)
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}
	}
```

- [ ] **Step 8: Refactor handleCommitOffset**

Replace the distributed block in `handleCommitOffset`:

```go
	if ch.isDistributed() {
		coordAddr, isCoord := ch.checkCoordinator(groupID)
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}

		if !ch.isAuthorizedForPartition(topicName, partition) {
			return fmt.Sprintf("NOT_AUTHORIZED_FOR_PARTITION %s:%d", topicName, partition)
		}

		payload := map[string]interface{}{
			"type":      "COMMIT",
			"group":     groupID,
			"topic":     topicName,
			"partition": partition,
			"offset":    offset,
		}
		_, err := ch.applyViaLeader("OFFSET_SYNC", payload)
		if err != nil {
			return fmt.Sprintf("ERROR: Offset sync failed: %v", err)
		}
		return "OK"
	}
```

- [ ] **Step 9: Refactor handleBatchCommit**

Replace the distributed block in `handleBatchCommit` — change `isLeaderAndForward` to coordinator check:

```go
	if ch.isDistributed() {
		coordAddr, isCoord := ch.checkCoordinator(groupID)
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}
	}
```

Keep the rest of the batch commit logic unchanged (partition data parsing, authorization checks, `applyViaLeader` for the batch).

For the final Raft apply at the bottom, replace `applyAndWait` with `applyViaLeader`:

```go
		_, err := ch.applyViaLeader("BATCH_OFFSET", batchCommitData)
```

- [ ] **Step 10: Remove unused isCoordinatorAndForward**

Delete the `isCoordinatorAndForward` function from `pkg/controller/cluster_handler.go` — it's no longer used.

- [ ] **Step 11: Build and run all tests**

Run:
```bash
go build ./...
go test ./pkg/controller/... -count=1 -timeout 60s
go test ./pkg/cluster/... -count=1 -timeout 60s
```
Expected: All PASS

- [ ] **Step 12: Commit**

```bash
git add pkg/controller/command_handler.go pkg/controller/cluster_handler.go
git commit -m "feat: refactor group commands to coordinator pattern with NOT_COORDINATOR response"
```

---

### Task 5: Go SDK — FindCoordinator + Coordinator Connection

**Files:**
- Modify: `sdk/consumer.go` (add coordinatorAddr, findCoordinator, getCoordinatorConn)
- Modify: `sdk/consumer_client.go` (add ConnectToAddr)
- Modify: `sdk/consumer_group.go` (heartbeat uses coordinator conn)
- Test: `sdk/consumer_test.go`

- [ ] **Step 1: Add ConnectToAddr to consumer_client.go**

Add to `sdk/consumer_client.go`:

```go
// ConnectToAddr opens a connection to a specific address (e.g., the coordinator).
func (c *ConsumerClient) ConnectToAddr(addr string) (net.Conn, error) {
	return c.Connect(addr)
}
```

- [ ] **Step 2: Add coordinatorAddr field and getCoordinatorConn to consumer.go**

In `sdk/consumer.go`, add to the `Consumer` struct:

```go
	coordinatorAddr string
```

Add helper method:

```go
func (c *Consumer) getCoordinatorConn() (net.Conn, error) {
	c.mu.RLock()
	addr := c.coordinatorAddr
	c.mu.RUnlock()

	if addr != "" {
		conn, err := c.client.ConnectToAddr(addr)
		if err == nil {
			return conn, nil
		}
		LogWarn("Coordinator %s unreachable: %v, rediscovering", addr, err)
	}

	// Fallback: rediscover coordinator
	newAddr, err := c.findCoordinator()
	if err != nil {
		return c.getLeaderConn() // last resort fallback
	}
	c.mu.Lock()
	c.coordinatorAddr = newAddr
	c.mu.Unlock()
	return c.client.ConnectToAddr(newAddr)
}
```

- [ ] **Step 3: Add findCoordinator method**

Add to `sdk/consumer.go`:

```go
func (c *Consumer) findCoordinator() (string, error) {
	conn, _, err := c.client.ConnectWithFailover()
	if err != nil {
		return "", fmt.Errorf("connect for find_coordinator: %w", err)
	}
	defer func() { _ = conn.Close() }()

	cmd := fmt.Sprintf("FIND_COORDINATOR group=%s", c.config.GroupID)
	if err := WriteWithLength(conn, EncodeMessage("", cmd)); err != nil {
		return "", fmt.Errorf("send find_coordinator: %w", err)
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		return "", fmt.Errorf("read find_coordinator: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if !strings.HasPrefix(respStr, "OK") {
		return "", fmt.Errorf("find_coordinator failed: %s", respStr)
	}

	var host, port string
	for _, part := range strings.Fields(respStr) {
		if strings.HasPrefix(part, "host=") {
			host = strings.TrimPrefix(part, "host=")
		} else if strings.HasPrefix(part, "port=") {
			port = strings.TrimPrefix(part, "port=")
		}
	}
	if host == "" || port == "" {
		return "", fmt.Errorf("find_coordinator: missing host/port in response: %s", respStr)
	}
	return net.JoinHostPort(host, port), nil
}
```

- [ ] **Step 4: Add NOT_COORDINATOR handling to group operations**

Add helper to `sdk/consumer.go`:

```go
// handleNotCoordinator checks if response is NOT_COORDINATOR and updates coordinator address.
func (c *Consumer) handleNotCoordinator(respStr string) bool {
	if !strings.Contains(respStr, "NOT_COORDINATOR") {
		return false
	}
	var host, port string
	for _, part := range strings.Fields(respStr) {
		if strings.HasPrefix(part, "host=") {
			host = strings.TrimPrefix(part, "host=")
		} else if strings.HasPrefix(part, "port=") {
			port = strings.TrimPrefix(part, "port=")
		}
	}
	if host != "" && port != "" {
		newAddr := net.JoinHostPort(host, port)
		c.mu.Lock()
		c.coordinatorAddr = newAddr
		c.mu.Unlock()
		LogInfo("Coordinator moved to %s", newAddr)
	}
	return true
}
```

- [ ] **Step 5: Update Start() to discover coordinator first**

In `sdk/consumer.go`, at the beginning of `Start()`, before `joinGroupWithRetry()`:

```go
func (c *Consumer) Start(handler func(Message) error) error {
	c.MessageHandler = handler

	// Discover coordinator for this consumer group
	if coordAddr, err := c.findCoordinator(); err == nil {
		c.coordinatorAddr = coordAddr
		LogInfo("Coordinator for group '%s': %s", c.config.GroupID, coordAddr)
	}

	gen, mid, assignments, err := c.joinGroupWithRetry()
```

- [ ] **Step 6: Update joinGroup to use coordinator connection**

In `sdk/consumer.go`, change `joinGroup()`:

Replace `conn, err := c.getLeaderConn()` with `conn, err := c.getCoordinatorConn()`.

After reading the response, add NOT_COORDINATOR handling:

```go
	respStr := strings.TrimSpace(string(resp))
	if c.handleNotCoordinator(respStr) {
		return 0, "", nil, fmt.Errorf("coordinator moved, retry")
	}
	if !strings.HasPrefix(respStr, "OK") {
```

- [ ] **Step 7: Update syncGroup to use coordinator connection**

In `sdk/consumer.go`, change `syncGroup()`:

Replace `conn, err := c.getLeaderConn()` with `conn, err := c.getCoordinatorConn()`.

- [ ] **Step 8: Update fetchOffset to use coordinator connection**

In `sdk/consumer.go`, change `fetchOffset()`:

Replace `conn, err := c.getLeaderConn()` with `conn, err := c.getCoordinatorConn()`.

- [ ] **Step 9: Update heartbeat to use coordinator connection**

In `sdk/consumer_group.go`, change `getOrDialHeartbeatConn()`:

Replace `newConn, err := c.getLeaderConn()` with `newConn, err := c.getCoordinatorConn()`.

- [ ] **Step 10: Update Close() leave group to use coordinator**

In `sdk/consumer.go`, in `Close()`:

Replace `if conn, err := c.getLeaderConn()` with `if conn, err := c.getCoordinatorConn()`.

- [ ] **Step 11: Update sendBatchCommit to use coordinator connection**

In `sdk/consumer.go`, in `sendBatchCommit()`:

Replace `newConn, err := c.getLeaderConn()` with `newConn, err := c.getCoordinatorConn()`.

- [ ] **Step 12: Update directCommit to use coordinator connection**

In `sdk/consumer.go`, in `directCommit()`:

Replace `conn, err := c.getLeaderConn()` with `conn, err := c.getCoordinatorConn()`.

- [ ] **Step 13: Update handleRebalanceSignal to rediscover coordinator**

In `sdk/consumer_group.go`, in `handleRebalanceSignal()`, before `joinGroupWithRetry()`:

```go
	// Rediscover coordinator (may have changed during rebalance)
	if coordAddr, err := c.findCoordinator(); err == nil {
		c.mu.Lock()
		c.coordinatorAddr = coordAddr
		c.mu.Unlock()
	}

	gen, mid, assignments, err := c.joinGroupWithRetry()
```

- [ ] **Step 14: Build and run tests**

Run:
```bash
go build ./sdk/...
go test ./sdk/... -count=1 -timeout 60s
```
Expected: All PASS

- [ ] **Step 15: Commit**

```bash
git add sdk/consumer.go sdk/consumer_client.go sdk/consumer_group.go
git commit -m "feat(sdk): add FindCoordinator discovery and coordinator connection for group ops"
```

---

### Task 6: Python SDK — FindCoordinator + Coordinator Connection

**Files:**
- Modify: `../cursus-python/src/cursus/consumer.py`
- Test: `../cursus-python/tests/unit/test_consumer.py`

- [ ] **Step 1: Write test for NOT_COORDINATOR parsing**

Add to `../cursus-python/tests/unit/test_consumer.py`:

```python
def test_parse_not_coordinator():
    from cursus.consumer import Consumer
    from cursus.config import ConsumerConfig

    cfg = ConsumerConfig(brokers=["localhost:9000"], topic="t", group_id="g")
    c = Consumer(cfg)

    # Simulate NOT_COORDINATOR response parsing
    resp = "ERROR: NOT_COORDINATOR host=localhost port=9002"
    assert "NOT_COORDINATOR" in resp
    host, port = None, None
    for part in resp.split():
        if part.startswith("host="):
            host = part.split("=", 1)[1]
        elif part.startswith("port="):
            port = part.split("=", 1)[1]
    assert host == "localhost"
    assert port == "9002"
```

- [ ] **Step 2: Run test to verify it passes (parsing logic only)**

Run: `cd ../cursus-python && uv run pytest tests/unit/test_consumer.py::test_parse_not_coordinator -v`
Expected: PASS

- [ ] **Step 3: Add _find_coordinator and _send_coordinator_command to consumer.py**

In `../cursus-python/src/cursus/consumer.py`, add these methods to the `Consumer` class:

```python
    def _find_coordinator(self) -> str:
        """Ask any broker for the coordinator of this consumer group."""
        group = self._config.group_id or "default-group"
        resp = self._send_command(f"FIND_COORDINATOR group={group}")
        if not resp.startswith("OK"):
            raise ConnectionError(f"find coordinator failed: {resp}")
        host, port = None, None
        for part in resp.split():
            if part.startswith("host="):
                host = part.split("=", 1)[1]
            elif part.startswith("port="):
                port = part.split("=", 1)[1]
        if not host or not port:
            raise ConnectionError(f"find coordinator: missing host/port: {resp}")
        return f"{host}:{port}"

    def _send_coordinator_command(self, cmd: str) -> str:
        """Send a command to the group coordinator, with NOT_COORDINATOR retry."""
        for _attempt in range(3):
            addr = self._coordinator_addr or self._leader_addr or self._config.brokers[0]
            conn = SyncConnection(addr)
            try:
                conn.connect()
                conn.write_frame(encode_message("", cmd))
                resp = conn.read_frame().decode()
            except Exception as e:
                raise ConnectionError(f"coordinator command failed: {e}") from e
            finally:
                conn.close()

            if "NOT_COORDINATOR" in resp:
                # Parse new coordinator address
                host, port = None, None
                for part in resp.split():
                    if part.startswith("host="):
                        host = part.split("=", 1)[1]
                    elif part.startswith("port="):
                        port = part.split("=", 1)[1]
                if host and port:
                    self._coordinator_addr = f"{host}:{port}"
                continue
            return resp
        return resp
```

- [ ] **Step 4: Add _coordinator_addr field to __init__**

In `Consumer.__init__`, add:

```python
        self._coordinator_addr: str | None = None
```

- [ ] **Step 5: Update _send_command with NOT_LEADER retry**

Replace the existing `_send_command` method:

```python
    def _send_command(self, cmd: str) -> str:
        for _attempt in range(3):
            conn = self._connect_to_leader()
            try:
                conn.write_frame(encode_message("", cmd))
                resp = conn.read_frame().decode()
            finally:
                conn.close()

            if "NOT_LEADER LEADER_IS" in resp:
                parts = resp.split()
                for i, p in enumerate(parts):
                    if p == "LEADER_IS" and i + 1 < len(parts):
                        self._leader_addr = parts[i + 1]
                        break
                continue
            return resp
        return resp
```

- [ ] **Step 6: Update _join_and_sync to use coordinator**

In `_join_and_sync`, add coordinator discovery at the top and use `_send_coordinator_command` for group ops:

```python
    def _join_and_sync(self) -> None:
        # Discover coordinator
        try:
            self._coordinator_addr = self._find_coordinator()
        except Exception:
            pass  # fallback to leader-based routing

        cmd = CommandBuilder.join_group(
            self._config.topic,
            self._config.group_id or "default-group",
            self._member_id,
        )
        resp = self._send_coordinator_command(cmd)

        if not resp.startswith("OK"):
            raise ConnectionError(f"join group failed: {resp}")

        self._parse_join_response(resp)

        if not self._assignments:
            sync_cmd = CommandBuilder.sync_group(
                self._config.topic,
                self._config.group_id or "default-group",
                self._member_id,
                self._generation,
            )
            sync_resp = self._send_coordinator_command(sync_cmd)
            self._parse_sync_response(sync_resp)

        for pid in self._assignments:
            try:
                fetch_cmd = CommandBuilder.fetch_offset(
                    self._config.topic, pid, self._config.group_id or "default-group"
                )
                resp = self._send_coordinator_command(fetch_cmd)
                self._offsets[pid] = int(resp.strip())
            except (ValueError, ConnectionError):
                self._offsets[pid] = 0
```

- [ ] **Step 7: Update _heartbeat_loop to use coordinator**

In `_heartbeat_loop`, replace `self._send_command(cmd)` with `self._send_coordinator_command(cmd)`.

- [ ] **Step 8: Update close() leave group to use coordinator**

In `close()`, replace `self._send_command(cmd)` with `self._send_coordinator_command(cmd)`.

- [ ] **Step 9: Run tests**

Run:
```bash
cd ../cursus-python && uv run pytest tests/unit/ -v --tb=short
```
Expected: All PASS

- [ ] **Step 10: Commit**

```bash
cd ../cursus-python
git add src/cursus/consumer.py tests/unit/test_consumer.py
git commit -m "feat: add FindCoordinator discovery and coordinator routing for consumer"
```

---

### Task 7: Integration Test — Cluster Consumer E2E

**Files:**
- No new files — test with running cluster

- [ ] **Step 1: Rebuild cluster with all changes**

```bash
cd test/cluster && docker compose down && docker compose up -d --build broker-1 broker-2 broker-3
sleep 15
docker ps --format '{{.Names}} {{.Status}}' | grep broker
```
Expected: All 3 brokers healthy.

- [ ] **Step 2: Test FIND_COORDINATOR from Docker network**

```bash
docker run --rm --network cluster_network python:3.11-slim python3 -c "
import socket, struct
def send(addr, port, cmd):
    s = socket.socket(); s.connect((addr, port)); s.settimeout(10)
    msg = struct.pack('>H', 0) + cmd.encode()
    s.send(struct.pack('>I', len(msg)) + msg)
    rl = s.recv(4); resp = s.recv(struct.unpack('>I', rl)[0])
    s.close(); return resp.decode()
print('FIND_COORDINATOR:', send('broker-1', 9000, 'FIND_COORDINATOR group=test-g1'))
print('FIND_COORDINATOR:', send('broker-2', 9000, 'FIND_COORDINATOR group=test-g1'))
print('FIND_COORDINATOR:', send('broker-3', 9000, 'FIND_COORDINATOR group=test-g1'))
"
```
Expected: All 3 return the same coordinator_id and host/port.

- [ ] **Step 3: Test full consumer flow (CREATE + JOIN + LEAVE) via coordinator**

```bash
docker run --rm --network cluster_network python:3.11-slim python3 -c "
import socket, struct
def send(addr, port, cmd):
    s = socket.socket(); s.connect((addr, port)); s.settimeout(10)
    msg = struct.pack('>H', 0) + cmd.encode()
    s.send(struct.pack('>I', len(msg)) + msg)
    rl = s.recv(4); resp = s.recv(struct.unpack('>I', rl)[0])
    s.close(); return resp.decode()

# Create topic on any broker
print('CREATE:', send('broker-1', 9000, 'CREATE topic=e2e-test partitions=2'))

import time; time.sleep(1)

# Find coordinator
coord_resp = send('broker-1', 9000, 'FIND_COORDINATOR group=e2e-group')
print('COORDINATOR:', coord_resp)

# JOIN via each broker — non-coordinators should return NOT_COORDINATOR
for b in ['broker-1', 'broker-2', 'broker-3']:
    r = send(b, 9000, 'JOIN_GROUP topic=e2e-test group=e2e-group member=c1')
    print(f'JOIN via {b}:', r[:80])
"
```
Expected: One broker returns `OK generation=... member=...`, others return `NOT_COORDINATOR host=... port=...`.

- [ ] **Step 4: Verify no stack overflow on any broker**

```bash
docker logs broker-1 2>&1 | grep -c "stack overflow" && echo "FAIL" || echo "OK"
docker logs broker-2 2>&1 | grep -c "stack overflow" && echo "FAIL" || echo "OK"
docker logs broker-3 2>&1 | grep -c "stack overflow" && echo "FAIL" || echo "OK"
```
Expected: All "OK" (0 occurrences).
