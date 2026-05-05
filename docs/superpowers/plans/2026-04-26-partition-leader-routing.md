# Partition Leader Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route CONSUME/STREAM to partition leaders instead of the Raft leader, enabling distributed consume load across cluster brokers.

**Architecture:** Add `ClientAddr` to FSM broker registration so any broker can resolve any other broker's client-facing address. Add a `METADATA` command that returns per-partition leader addresses. Replace `checkLeaderOrRedirect` (Raft leader check) with `checkPartitionLeaderOrRedirect` (partition leader check). SDKs call `METADATA` to discover partition leaders and connect directly.

**Tech Stack:** Go (broker + Go SDK), Python (Python SDK)

**Spec:** `docs/superpowers/specs/2026-04-26-partition-leader-routing-design.md`

---

### Task 1: FSM — Add ClientAddr to BrokerInfo

**Files:**
- Modify: `pkg/cluster/replication/fsm/fsm.go` (BrokerInfo struct)
- Modify: `pkg/cluster/controller/discovery.go` (Register method)
- Modify: `pkg/server/main.go` (pass client addr to ServiceDiscovery)
- Modify: `pkg/cluster/controller/discovery.go` (serviceDiscovery struct + constructor)

- [ ] **Step 1: Add ClientAddr field to BrokerInfo**

In `pkg/cluster/replication/fsm/fsm.go`, add `ClientAddr` to the `BrokerInfo` struct:

```go
type BrokerInfo struct {
	ID         string    `json:"id"`
	Addr       string    `json:"addr"`
	ClientAddr string    `json:"client_addr,omitempty"`
	Status     string    `json:"status"`
	LastSeen   time.Time `json:"last_seen"`
}
```

- [ ] **Step 2: Add clientAddr to serviceDiscovery**

In `pkg/cluster/controller/discovery.go`, add `clientAddr` field to the struct and update the constructors:

```go
type serviceDiscovery struct {
	rm         RaftManager
	brokerID   string
	addr       string
	clientAddr string
	fsm        *fsm.BrokerFSM
}

func NewServiceDiscoveryImpl(rm RaftManager, brokerID, addr, clientAddr string) *serviceDiscovery {
	sd := &serviceDiscovery{
		rm:         rm,
		brokerID:   brokerID,
		addr:       addr,
		clientAddr: clientAddr,
	}
	if rm != nil {
		sd.fsm = rm.GetFSM()
	}
	return sd
}

func NewServiceDiscovery(rm RaftManager, brokerID, addr, clientAddr string) ServiceDiscovery {
	return NewServiceDiscoveryImpl(rm, brokerID, addr, clientAddr)
}
```

- [ ] **Step 3: Include ClientAddr in Register()**

In `pkg/cluster/controller/discovery.go`, update `Register()`:

```go
func (sd *serviceDiscovery) Register() error {
	broker := &fsm.BrokerInfo{
		ID:         sd.brokerID,
		Addr:       sd.addr,
		ClientAddr: sd.clientAddr,
		Status:     "active",
		LastSeen:   time.Now(),
	}
	// ... rest unchanged
}
```

- [ ] **Step 4: Update caller in main.go**

In `pkg/server/main.go`, compute `clientAddr` and pass it:

```go
	brokerID := fmt.Sprintf("%s-%d", cfg.AdvertisedHost, cfg.BrokerPort)
	localAddr := fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.RaftPort)

	clientHost := cfg.AdvertisedClientHost
	if clientHost == "" {
		clientHost = cfg.AdvertisedHost
	}
	clientPort := cfg.AdvertisedBrokerPort
	if clientPort == 0 {
		clientPort = cfg.BrokerPort
	}
	clientAddr := fmt.Sprintf("%s:%d", clientHost, clientPort)

	sd := clusterController.NewServiceDiscovery(rm, brokerID, localAddr, clientAddr)
```

- [ ] **Step 5: Update ServiceDiscovery interface if needed**

Check if `ServiceDiscovery` is an interface — if the constructor signature changed, update all callers. Search for `NewServiceDiscovery(` in test files and update them to include the `clientAddr` parameter (use `""` for tests).

- [ ] **Step 6: Build and test**

```bash
go build ./...
go test ./pkg/cluster/... -count=1 -timeout 60s
go test ./pkg/server/... -count=1 -timeout 60s
```

- [ ] **Step 7: Commit**

Do NOT commit — user controls git.

---

### Task 2: Broker — METADATA Command

**Files:**
- Modify: `pkg/controller/command_handler.go` (add handleMetadata)
- Modify: `pkg/controller/handler.go` (register command)
- Modify: `pkg/server/main.go` (add to isCommand)

- [ ] **Step 1: Add handleMetadata to command_handler.go**

```go
func (ch *CommandHandler) handleMetadata(cmd string) string {
	args := parseKeyValueArgs(cmd[9:]) // len("METADATA ") = 9
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: METADATA requires topic parameter"
	}

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
	}

	partitionCount := len(t.Partitions)
	leaders := make([]string, partitionCount)

	for i := 0; i < partitionCount; i++ {
		leaders[i] = ch.resolvePartitionLeaderAddr(topicName, i)
	}

	return fmt.Sprintf("OK topic=%s partitions=%d leaders=%s",
		topicName, partitionCount, strings.Join(leaders, ","))
}
```

- [ ] **Step 2: Add resolvePartitionLeaderAddr helper to cluster_handler.go**

In `pkg/controller/cluster_handler.go`:

```go
// resolvePartitionLeaderAddr returns the client-facing address of the partition leader.
func (ch *CommandHandler) resolvePartitionLeaderAddr(topicName string, partitionID int) string {
	// Standalone fallback
	if !ch.isDistributed() {
		host := ch.Config.AdvertisedClientHost
		if host == "" {
			host = "localhost"
		}
		port := ch.Config.AdvertisedBrokerPort
		if port == 0 {
			port = ch.Config.BrokerPort
		}
		return fmt.Sprintf("%s:%d", host, port)
	}

	fsmRef := ch.Cluster.RaftManager.GetFSM()
	if fsmRef == nil {
		return ch.fallbackClientAddr()
	}

	partitionKey := fmt.Sprintf("%s-%d", topicName, partitionID)
	meta := fsmRef.GetPartitionMetadata(partitionKey)
	if meta == nil {
		return ch.fallbackClientAddr()
	}

	broker := fsmRef.GetBroker(meta.Leader)
	if broker == nil {
		return ch.fallbackClientAddr()
	}

	if broker.ClientAddr != "" {
		return broker.ClientAddr
	}

	// Legacy fallback: derive from Raft addr
	if h, _, err := net.SplitHostPort(broker.Addr); err == nil {
		return fmt.Sprintf("%s:%d", h, ch.Cluster.Router.ClientPort())
	}
	return ch.fallbackClientAddr()
}

func (ch *CommandHandler) fallbackClientAddr() string {
	host := ch.Config.AdvertisedClientHost
	if host == "" {
		host = "localhost"
	}
	port := ch.Config.AdvertisedBrokerPort
	if port == 0 {
		port = ch.Config.BrokerPort
	}
	return fmt.Sprintf("%s:%d", host, port)
}
```

Add `"net"` to imports of `cluster_handler.go` if not present.

- [ ] **Step 3: Register METADATA in handler.go**

Add to `ch.commands` slice:

```go
{prefix: "METADATA ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleMetadata(cmd) }},
```

- [ ] **Step 4: Add METADATA to isCommand in main.go**

In `pkg/server/main.go`, add `"METADATA"` to the `isCommand` keywords list.

- [ ] **Step 5: Build and test**

```bash
go build ./...
go test ./pkg/controller/... -count=1 -timeout 60s
```

- [ ] **Step 6: Commit**

Do NOT commit.

---

### Task 3: Broker — checkPartitionLeaderOrRedirect

**Files:**
- Modify: `pkg/controller/consume_handler.go` (add new check, update CONSUME and STREAM handlers)

- [ ] **Step 1: Add checkPartitionLeaderOrRedirect**

In `pkg/controller/consume_handler.go`, add:

```go
// checkPartitionLeaderOrRedirect checks if this broker is the partition leader.
// If not, writes a NOT_LEADER redirect with the partition leader's client address.
func (ch *CommandHandler) checkPartitionLeaderOrRedirect(conn net.Conn, topicName string, partitionID int) error {
	if !ch.Config.EnabledDistribution || ch.Cluster == nil || ch.Cluster.Router == nil {
		return nil
	}

	if ch.Cluster.IsAuthorized(topicName, partitionID) {
		return nil
	}

	leaderAddr := ch.resolvePartitionLeaderAddr(topicName, partitionID)
	errResp := fmt.Sprintf("ERROR: NOT_LEADER LEADER_IS %s", leaderAddr)
	if err := util.WriteWithLength(conn, []byte(errResp)); err != nil {
		return fmt.Errorf("failed to send partition leader redirect: %w", err)
	}
	return fmt.Errorf("not partition leader")
}
```

- [ ] **Step 2: Update HandleConsumeCommand to use partition leader check**

In `HandleConsumeCommand`, the topic and partition are available after parsing. Replace the `checkLeaderOrRedirect` call:

Find:
```go
if err := ch.checkLeaderOrRedirect(conn); err != nil {
```

Replace with:
```go
if err := ch.checkPartitionLeaderOrRedirect(conn, cArgs.TopicName, cArgs.PartitionID); err != nil {
```

Note: `cArgs` is parsed BEFORE this check, so `cArgs.TopicName` and `cArgs.PartitionID` are available. However, looking at the current code, `checkLeaderOrRedirect` is called BEFORE topic matching. Move the check to AFTER `parseCommonArgs`:

Read the current flow carefully and place the partition leader check after `cArgs` is available but before reading messages.

- [ ] **Step 3: Update HandleStreamCommand to use partition leader check**

In `HandleStreamCommand`, similarly replace `checkLeaderOrRedirect`:

Find:
```go
if err := ch.checkLeaderOrRedirect(conn); err != nil {
```

Replace with:
```go
if err := ch.checkPartitionLeaderOrRedirect(conn, cArgs.TopicName, cArgs.PartitionID); err != nil {
```

Again, ensure `cArgs` is parsed before this check.

- [ ] **Step 4: Build and test**

```bash
go build ./...
go test ./pkg/controller/... -count=1 -timeout 60s
```

- [ ] **Step 5: Commit**

Do NOT commit.

---

### Task 4: Go SDK — Per-Partition Leader Connection

**Files:**
- Modify: `sdk/consumer.go` (add partitionLeaders map, fetchMetadata)
- Modify: `sdk/partition.go` (reconnect uses partition-specific leader)

- [ ] **Step 1: Add partitionLeaders and fetchMetadata to consumer.go**

In `Consumer` struct, add:

```go
	partitionLeaders map[int]string // partition ID -> "host:port"
	partitionMu      sync.RWMutex
```

Add `fetchMetadata` method:

```go
func (c *Consumer) fetchMetadata() error {
	conn, _, err := c.client.ConnectWithFailover()
	if err != nil {
		return fmt.Errorf("connect for metadata: %w", err)
	}
	defer func() { _ = conn.Close() }()

	cmd := fmt.Sprintf("METADATA topic=%s", c.config.Topic)
	if err := WriteWithLength(conn, EncodeMessage("", cmd)); err != nil {
		return fmt.Errorf("send metadata: %w", err)
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read metadata: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if !strings.HasPrefix(respStr, "OK") {
		return fmt.Errorf("metadata failed: %s", respStr)
	}

	var leadersStr string
	for _, part := range strings.Fields(respStr) {
		if strings.HasPrefix(part, "leaders=") {
			leadersStr = strings.TrimPrefix(part, "leaders=")
		}
	}
	if leadersStr == "" {
		return fmt.Errorf("metadata: missing leaders in response")
	}

	addrs := strings.Split(leadersStr, ",")
	c.partitionMu.Lock()
	for i, addr := range addrs {
		c.partitionLeaders[i] = addr
	}
	c.partitionMu.Unlock()

	return nil
}

func (c *Consumer) getPartitionLeaderAddr(partitionID int) string {
	c.partitionMu.RLock()
	defer c.partitionMu.RUnlock()
	return c.partitionLeaders[partitionID]
}

func (c *Consumer) updatePartitionLeader(partitionID int, addr string) {
	c.partitionMu.Lock()
	c.partitionLeaders[partitionID] = addr
	c.partitionMu.Unlock()
}
```

- [ ] **Step 2: Initialize partitionLeaders in NewConsumer**

In `NewConsumer`, add:

```go
	c := &Consumer{
		// ... existing fields ...
		partitionLeaders: make(map[int]string),
	}
```

- [ ] **Step 3: Call fetchMetadata in Start() after getting assignments**

In `Start()`, after `joinGroupWithRetry` and getting assignments, add:

```go
	if err := c.fetchMetadata(); err != nil {
		LogWarn("Failed to fetch metadata, will rely on NOT_LEADER redirects: %v", err)
	}
```

- [ ] **Step 4: Update PartitionConsumer.reconnect to use partition leader**

In `sdk/partition.go`, in `reconnect()`, replace `ConnectWithFailover()` with partition-leader-aware connection:

```go
	// Try partition leader first
	leaderAddr := pc.consumer.getPartitionLeaderAddr(pc.partitionID)
	if leaderAddr != "" {
		conn, connectErr := pc.consumer.client.ConnectToAddr(leaderAddr)
		if connectErr == nil {
			pc.mu.Lock()
			if pc.closed {
				_ = conn.Close()
				pc.mu.Unlock()
				return fmt.Errorf("%w", ErrConsumerClosed)
			}
			pc.conn = conn
			pc.mu.Unlock()
			return nil
		}
		LogWarn("Partition [%d] leader %s unreachable: %v", pc.partitionID, leaderAddr, connectErr)
	}

	// Fallback to any broker
	conn, _, connectErr := pc.consumer.client.ConnectWithFailover()
```

- [ ] **Step 5: Update handleBrokerError to update partition leader**

In `sdk/partition.go`, in `handleBrokerError`, update the NOT_LEADER handling:

```go
	if strings.Contains(respStr, "NOT_LEADER") {
		// Extract leader address for this partition
		fields := strings.Fields(respStr)
		for i, f := range fields {
			if f == "LEADER_IS" && i+1 < len(fields) {
				pc.consumer.updatePartitionLeader(pc.partitionID, fields[i+1])
				break
			}
		}
	}
```

Remove or keep the existing `handleLeaderRedirection` call (it updates the global leader cache which is still useful for non-partition connections).

- [ ] **Step 6: Call fetchMetadata in handleRebalanceSignal**

In `sdk/consumer_group.go`, in `handleRebalanceSignal`, after getting new assignments:

```go
	if err := c.fetchMetadata(); err != nil {
		LogWarn("Rebalance: failed to fetch metadata: %v", err)
	}
```

- [ ] **Step 7: Build and test**

```bash
go build ./sdk/...
go test ./sdk/... -count=1 -timeout 60s
```

- [ ] **Step 8: Commit**

Do NOT commit.

---

### Task 5: Python SDK — Per-Partition Leader Connection

**Files:**
- Modify: `../cursus-python/src/cursus/consumer.py`

- [ ] **Step 1: Add _partition_leaders dict and _fetch_metadata**

In `Consumer.__init__`, add:

```python
        self._partition_leaders: dict[int, str] = {}
```

Add method:

```python
    def _fetch_metadata(self) -> None:
        resp = self._send_command(f"METADATA topic={self._config.topic}")
        if not resp.startswith("OK"):
            return
        for part in resp.split():
            if part.startswith("leaders="):
                addrs = part.split("=", 1)[1].split(",")
                for i, addr in enumerate(addrs):
                    self._partition_leaders[i] = addr.strip()
```

- [ ] **Step 2: Add _connect_to_partition_leader**

```python
    def _connect_to_partition_leader(self, partition: int) -> SyncConnection:
        addr = self._partition_leaders.get(partition)
        if not addr:
            return self._connect_to_leader()
        try:
            conn = SyncConnection(addr)
            conn.connect()
            return conn
        except ConnectionError:
            return self._connect_to_leader()
```

- [ ] **Step 3: Update _partition_poll_loop to use partition leader**

In `_partition_poll_loop`, replace `conn = self._connect_to_leader()` with:

```python
                conn = self._connect_to_partition_leader(partition)
```

Also add NOT_LEADER handling to update partition leader:

```python
                    if len(resp_data) > 2:
                        # Check for NOT_LEADER redirect
                        try:
                            resp_str = resp_data.decode()
                            if "NOT_LEADER LEADER_IS" in resp_str:
                                parts = resp_str.split()
                                for i, p in enumerate(parts):
                                    if p == "LEADER_IS" and i + 1 < len(parts):
                                        self._partition_leaders[partition] = parts[i + 1]
                                        break
                                continue
                        except UnicodeDecodeError:
                            pass

                        messages, _, _ = decode_batch(resp_data)
```

- [ ] **Step 4: Call _fetch_metadata after _join_and_sync**

In `_join_and_sync`, at the end (after fetching offsets):

```python
        try:
            self._fetch_metadata()
        except Exception:
            pass
```

- [ ] **Step 5: Run tests**

```bash
cd ../cursus-python && uv run pytest tests/unit/ -v --tb=short
```

- [ ] **Step 6: Commit**

Do NOT commit.

---

### Task 6: Integration Test — Cluster Partition Leader Routing E2E

**Files:** None — test with running cluster

- [ ] **Step 1: Rebuild cluster**

```bash
cd test/cluster && docker compose down && docker compose up -d --build broker-1 broker-2 broker-3
sleep 15
docker ps --format '{{.Names}} {{.Status}}' | grep broker
```

- [ ] **Step 2: Test METADATA command**

```bash
docker run --rm --network cluster_network python:3.11-slim python3 -c "
import socket, struct
def send(addr, port, cmd):
    s = socket.socket(); s.connect((addr, port)); s.settimeout(10)
    msg = struct.pack('>H', 0) + cmd.encode()
    s.send(struct.pack('>I', len(msg)) + msg)
    rl = s.recv(4); rlen = struct.unpack('>I', rl)[0]
    resp = b''
    while len(resp) < rlen: resp += s.recv(rlen - len(resp))
    s.close(); return resp.decode()

print('CREATE:', send('broker-1', 9000, 'CREATE topic=meta-test partitions=3'))
import time; time.sleep(1)
for b in ['broker-1', 'broker-2', 'broker-3']:
    print(f'METADATA via {b}:', send(b, 9000, 'METADATA topic=meta-test'))
"
```

Expected: All 3 brokers return `OK topic=meta-test partitions=3 leaders=<addr0>,<addr1>,<addr2>` with `localhost:900X` addresses. Results should be consistent across brokers.

- [ ] **Step 3: Verify ClientAddr in broker registration**

```bash
docker run --rm --network cluster_network python:3.11-slim python3 -c "
import socket, struct
def send(addr, port, cmd):
    s = socket.socket(); s.connect((addr, port)); s.settimeout(10)
    msg = struct.pack('>H', 0) + cmd.encode()
    s.send(struct.pack('>I', len(msg)) + msg)
    rl = s.recv(4); rlen = struct.unpack('>I', rl)[0]
    resp = b''
    while len(resp) < rlen: resp += s.recv(rlen - len(resp))
    s.close(); return resp.decode()

# LIST_CLUSTER shows broker info from FSM
print(send('broker-1', 9000, 'LIST_CLUSTER'))
"
```

Expected: Each broker entry includes `client_addr` field with `localhost:900X`.

- [ ] **Step 4: Verify no stack overflow, all brokers alive**

```bash
for b in broker-1 broker-2 broker-3; do
    echo -n "$b: "
    docker logs $b 2>&1 | grep -c "stack overflow" | (read n; [ "$n" = "0" ] && echo "OK" || echo "FAIL ($n)")
done
```
