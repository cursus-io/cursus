# Cluster Fixes — Consolidated Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all identified cluster-mode bugs and design issues: remove CONSUME ownership checks (C1), fix batch forwarding (C2), remove Raft-inconsistent local fallbacks (C3), switch to async+flush produce (C4), add periodic metadata refresh (I1), and add producer METADATA integration (I2).

**Architecture:** CONSUME becomes a stateless read like Kafka FETCH — no heartbeat or ownership checks on partition leaders. Produce uses async writes with flush-before-ACK for batching without sacrificing read-after-write consistency. Producer SDK discovers partition leaders via METADATA and sends directly.

**Tech Stack:** Go (broker + Go SDK), Python (Python SDK), Java (Java SDK)

**Spec:** `docs/superpowers/specs/2026-04-26-cluster-fixes-consolidated-design.md`

**Note:** Java SDK (`../cursus-java`) already has `findCoordinator`, `fetchMetadata`, `coordinatorAddr`, `partitionLeaders` implemented. Only periodic metadata refresh (I1) and producer METADATA (I2) are missing.

---

### Task 1: C1 — Remove ownership/heartbeat from CONSUME

**Files:**
- Modify: `pkg/controller/consume_handler.go`

CONSUME should be a stateless read like Kafka's FETCH. The partition leader returns messages for the requested offset — no group membership verification needed.

- [ ] **Step 1: Simplify readFromTopic**

In `pkg/controller/consume_handler.go`, replace the entire `readFromTopic` function body. Remove all heartbeat recording, generation tracking, and ValidateOwnership logic. The function becomes:

```go
func (ch *CommandHandler) readFromTopic(topicName string, cArgs CommonArgs, ctx *ClientContext, batchSize int) ([]types.Message, error) {
	_, p, err := ch.getTopicAndPartition(topicName, cArgs.PartitionID)
	if err != nil {
		return nil, err
	}

	cacheKey := fmt.Sprintf("%s-%d", topicName, cArgs.PartitionID)
	var currentOffset uint64
	if cArgs.HasOffset {
		currentOffset = cArgs.Offset
	} else if cached, ok := ctx.OffsetCache[cacheKey]; ok {
		currentOffset = cached
	} else {
		actualOffset, err := ch.resolveOffset(p, topicName, cArgs)
		if err != nil {
			return nil, err
		}
		currentOffset = actualOffset
	}

	messages, err := p.ReadCommitted(currentOffset, batchSize)
	if err != nil {
		util.Error("Failed to read messages from topic %s: %v", topicName, err)
		return nil, err
	}

	if len(messages) > 0 {
		lastMsg := messages[len(messages)-1]
		ctx.OffsetCache[cacheKey] = lastMsg.Offset + 1
	}

	return messages, nil
}
```

- [ ] **Step 2: Build and test**

```bash
go build ./...
go test ./pkg/controller/... -count=1 -timeout 60s
```

- [ ] **Step 3: Do NOT commit** — user controls git

---

### Task 2: C3 — Remove applyViaLeader local fallback in handleJoinGroup

**Files:**
- Modify: `pkg/controller/command_handler.go`

The local fallback (`AddConsumer` when Raft hasn't propagated) causes Raft/coordinator state inconsistency.

- [ ] **Step 1: Remove local fallback in handleJoinGroup**

In `pkg/controller/command_handler.go`, find the distributed block in `handleJoinGroup`. Remove the fallback block after `applyViaLeader`:

Replace:
```go
		_, err := ch.applyViaLeader("GROUP_SYNC", joinPayload)
		if err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		}
		assignments = ch.Coordinator.GetMemberAssignments(groupName, consumerID)

		// If Raft replication to this node is delayed, apply locally as fallback.
		// The eventual Raft log replay is idempotent.
		if len(assignments) == 0 && ch.Coordinator != nil {
			if ch.Coordinator.GetGroup(groupName) == nil {
				if t := ch.TopicManager.GetTopic(topicName); t != nil {
					_ = ch.Coordinator.RegisterGroup(topicName, groupName, len(t.Partitions))
				}
			}
			assignments, _ = ch.Coordinator.AddConsumer(groupName, consumerID)
		}
```

With:
```go
		_, err := ch.applyViaLeader("GROUP_SYNC", joinPayload)
		if err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		}

		// Wait briefly for Raft to propagate to local FSM
		for i := 0; i < 10; i++ {
			assignments = ch.Coordinator.GetMemberAssignments(groupName, consumerID)
			if len(assignments) > 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
```

This polls for up to 500ms for Raft propagation instead of applying locally. If it still doesn't propagate, assignments will be empty and the SDK will retry via SYNC_GROUP.

- [ ] **Step 2: Build and test**

```bash
go build ./...
go test ./pkg/controller/... -count=1 -timeout 60s
```

- [ ] **Step 3: Do NOT commit**

---

### Task 3: C4 — Async write + flush before ACK

**Files:**
- Modify: `pkg/topic/partition.go` (add FlushDisk method)
- Modify: `pkg/controller/produce_handler.go` (EnqueueBatchSync → EnqueueBatch + FlushDisk)

- [ ] **Step 1: Add FlushDisk to Partition**

In `pkg/topic/partition.go`, add after the `ReadCommitted` method:

```go
// FlushDisk forces all pending async writes to disk.
func (p *Partition) FlushDisk() {
	p.dh.Flush()
}
```

- [ ] **Step 2: Update single PUBLISH (distributed path)**

In `pkg/controller/produce_handler.go`, find the distributed PUBLISH block. Replace `EnqueueBatchSync` with `EnqueueBatch` + `FlushDisk`:

Replace:
```go
		// 1. Append locally (sync write ensures data is on disk before ACK)
		if err := p.EnqueueBatchSync(messageData.Messages); err != nil {
			return ch.errorResponse(fmt.Sprintf("failed to append locally: %v", err))
		}
```

With:
```go
		// 1. Append locally (async batch write)
		if err := p.EnqueueBatch(messageData.Messages); err != nil {
			return ch.errorResponse(fmt.Sprintf("failed to append locally: %v", err))
		}
```

Then AFTER `ReplicateToFollowers` succeeds (or after the replication block), add flush before ACK:

Find the line that builds the `ackResp` and add flush before it:
```go
		// Ensure data is on disk before ACK
		p.FlushDisk()

		ackResp = types.AckResponse{
```

- [ ] **Step 3: Update BATCH PUBLISH (distributed path)**

Same pattern in the batch handler. Replace:
```go
		// 1. Append locally (sync write ensures data is on disk before ACK)
		if err := p.EnqueueBatchSync(batch.Messages); err != nil {
			return ch.errorResponse(fmt.Sprintf("failed to append batch locally: %v", err)), nil
		}
```

With:
```go
		// 1. Append locally (async batch write)
		if err := p.EnqueueBatch(batch.Messages); err != nil {
			return ch.errorResponse(fmt.Sprintf("failed to append batch locally: %v", err)), nil
		}
```

And add `p.FlushDisk()` before the ackResp construction in the batch handler:
```go
		// Ensure data is on disk before ACK
		p.FlushDisk()

		respAck = types.AckResponse{
```

- [ ] **Step 4: Build and test**

```bash
go build ./...
go test ./pkg/controller/... ./pkg/topic/... -count=1 -timeout 60s
```

- [ ] **Step 5: Do NOT commit**

---

### Task 4: C2 — Fix BATCH PUBLISH binary forwarding

**Files:**
- Modify: `pkg/controller/produce_handler.go`

The `isPartitionLeaderAndForward` call for BATCH sends only the text string "BATCH", not the binary data. The subsequent `ForwardDataToPartitionLeader` path already handles binary correctly. Remove the broken text-based forwarding attempt.

- [ ] **Step 1: Remove broken isPartitionLeaderAndForward for BATCH**

In `HandleBatchMessage`, find and remove:

```go
		if _, forwarded, _ := ch.isPartitionLeaderAndForward(batch.Topic, batch.Partition, "BATCH"); forwarded {
			// Note: isPartitionLeaderAndForward doesn't support binary BATCH data currently.
			// Actually HandleBatchMessage is called for binary BATCH data.
			// I need a way to forward binary data to Partition Leader.
			util.Warn("Binary batch forwarding to partition leader is not fully implemented for all cases")
			// For now, let's just forward to Raft leader if not the leader,
			// but better would be to use ForwardDataToPartitionLeader.
		}
```

The `ForwardDataToPartitionLeader` block that follows (checking `!ch.Cluster.IsAuthorized`) already handles binary forwarding correctly. The removed block was dead code that logged a misleading warning.

- [ ] **Step 2: Build and test**

```bash
go build ./...
go test ./pkg/controller/... -count=1 -timeout 60s
```

- [ ] **Step 3: Do NOT commit**

---

### Task 5: I1 — Go SDK periodic METADATA refresh

**Files:**
- Modify: `sdk/consumer.go`
- Modify: `sdk/consumer_group.go`
- Modify: `sdk/config.go`

- [ ] **Step 1: Add MetadataRefreshInterval to ConsumerConfig**

In `sdk/config.go`, find the `ConsumerConfig` struct and add:

```go
	MetadataRefreshInterval time.Duration `yaml:"metadata_refresh_interval" json:"metadata_refresh_interval"`
```

In `NewDefaultConsumerConfig()` (if it exists) or wherever defaults are set, add a default of 30 seconds.

- [ ] **Step 2: Add metadata refresh goroutine to Consumer**

In `sdk/consumer.go`, add a method:

```go
func (c *Consumer) metadataRefreshLoop() {
	interval := c.config.MetadataRefreshInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.doneCh:
			return
		case <-ticker.C:
			if err := c.fetchMetadata(); err != nil {
				LogDebug("Metadata refresh failed: %v", err)
			}
		}
	}
}
```

- [ ] **Step 3: Start the loop in startConsuming/startStreaming**

In `sdk/consumer_group.go`, in `startConsuming()`, add before the partition worker loops:

```go
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.metadataRefreshLoop()
	}()
```

Do the same in `startStreaming()`.

- [ ] **Step 4: Build and test**

```bash
go build ./sdk/...
go test ./sdk/... -count=1 -timeout 60s
```

- [ ] **Step 5: Do NOT commit**

---

### Task 6: I1 — Python SDK periodic METADATA refresh

**Files:**
- Modify: `../cursus-python/src/cursus/consumer.py`
- Modify: `../cursus-python/src/cursus/config.py`

- [ ] **Step 1: Add metadata_refresh_interval_ms to ConsumerConfig**

In `../cursus-python/src/cursus/config.py`, find `ConsumerConfig` and add:

```python
    metadata_refresh_interval_ms: int = 30000
```

- [ ] **Step 2: Add _metadata_refresh_loop to Consumer**

In `../cursus-python/src/cursus/consumer.py`, add:

```python
    def _start_metadata_refresh(self) -> None:
        t = threading.Thread(target=self._metadata_refresh_loop, daemon=True)
        t.start()
        self._workers.append(t)

    def _metadata_refresh_loop(self) -> None:
        interval_s = self._config.metadata_refresh_interval_ms / 1000.0
        while not self._done.is_set():
            self._done.wait(timeout=interval_s)
            if self._done.is_set():
                break
            try:
                self._fetch_metadata()
            except Exception:
                pass
```

- [ ] **Step 3: Call _start_metadata_refresh in _start_partition_workers**

In `_start_partition_workers`, add at the beginning:

```python
    def _start_partition_workers(self) -> None:
        self._start_metadata_refresh()
        for pid in self._assignments:
            # ... existing code
```

- [ ] **Step 4: Run tests**

```bash
cd ../cursus-python && uv run pytest tests/unit/ -v --tb=short
```

- [ ] **Step 5: Do NOT commit**

---

### Task 7: I2 — Go SDK Producer METADATA integration

**Files:**
- Modify: `sdk/producer.go`
- Modify: `sdk/producer_client.go`

- [ ] **Step 1: Add partitionLeaders and fetchMetadata to Producer**

In `sdk/producer.go`, add fields to the `Producer` struct:

```go
	partitionLeaders map[int]string
	partitionMu      sync.RWMutex
```

Initialize in `NewProducer`:
```go
	partitionLeaders: make(map[int]string),
```

Add method:
```go
func (p *Producer) fetchMetadata() error {
	conn, err := p.client.ConnectAny()
	if err != nil {
		return err
	}
	defer conn.Close()

	cmd := fmt.Sprintf("METADATA topic=%s", p.config.Topic)
	if err := WriteWithLength(conn, EncodeMessage("", cmd)); err != nil {
		return err
	}
	resp, err := ReadWithLength(conn)
	if err != nil {
		return err
	}
	respStr := strings.TrimSpace(string(resp))
	if !strings.HasPrefix(respStr, "OK") {
		return fmt.Errorf("metadata failed: %s", respStr)
	}
	for _, part := range strings.Fields(respStr) {
		if strings.HasPrefix(part, "leaders=") {
			addrs := strings.Split(strings.TrimPrefix(part, "leaders="), ",")
			p.partitionMu.Lock()
			for i, addr := range addrs {
				p.partitionLeaders[i] = addr
			}
			p.partitionMu.Unlock()
		}
	}
	return nil
}
```

- [ ] **Step 2: Add ConnectAny to ProducerClient**

In `sdk/producer_client.go`, add:

```go
// ConnectAny opens a temporary connection to any available broker for metadata queries.
func (pc *ProducerClient) ConnectAny() (net.Conn, error) {
	for _, addr := range pc.config.BrokerAddrs {
		conn, err := pc.dial(addr)
		if err == nil {
			return conn, nil
		}
	}
	return nil, fmt.Errorf("no brokers reachable")
}
```

Check if `dial` exists; if not, use `net.DialTimeout("tcp", addr, 5*time.Second)` directly.

- [ ] **Step 3: Use partition leaders in ConnectPartition**

In `sdk/producer_client.go`, update `ConnectPartition` to prefer the partition leader address:

At the start of `ConnectPartition`, before selecting a broker:
```go
func (pc *ProducerClient) ConnectPartition(idx int, addr string) error {
	if addr == "" {
		// Try partition leader if available
		if p, ok := pc.producer; ok && p != nil {
			p.partitionMu.RLock()
			if leaderAddr, exists := p.partitionLeaders[idx]; exists && leaderAddr != "" {
				addr = leaderAddr
			}
			p.partitionMu.RUnlock()
		}
	}
	if addr == "" {
		addr = pc.selectBroker()
	}
	// ... rest unchanged
```

Actually, `ProducerClient` doesn't have a reference to `Producer`. Simpler approach: have `Producer.senderLoop` pass the leader address when reconnecting:

In `Producer`, when the sender needs to connect for partition `i`:
```go
func (p *Producer) getPartitionLeaderAddr(partition int) string {
	p.partitionMu.RLock()
	defer p.partitionMu.RUnlock()
	return p.partitionLeaders[partition]
}
```

Then in the sender reconnect logic, pass this address to `ConnectPartition`.

- [ ] **Step 4: Call fetchMetadata on startup**

In `NewProducer`, after creating the producer, add:
```go
	// Discover partition leaders
	if err := p.fetchMetadata(); err != nil {
		LogWarn("Failed to fetch metadata for producer, will use default routing: %v", err)
	}
```

- [ ] **Step 5: Handle NOT_LEADER in send response**

In the sender loop where ACK responses are parsed, check for `NOT_LEADER LEADER_IS`:
```go
	if strings.Contains(respStr, "NOT_LEADER") {
		fields := strings.Fields(respStr)
		for i, f := range fields {
			if f == "LEADER_IS" && i+1 < len(fields) {
				p.partitionMu.Lock()
				p.partitionLeaders[partitionIdx] = fields[i+1]
				p.partitionMu.Unlock()
			}
		}
	}
```

- [ ] **Step 6: Build and test**

```bash
go build ./sdk/...
go test ./sdk/... -count=1 -timeout 60s
```

- [ ] **Step 7: Do NOT commit**

---

### Task 8: I2 — Python SDK Producer METADATA integration

**Files:**
- Modify: `../cursus-python/src/cursus/producer.py`

- [ ] **Step 1: Add _partition_leaders and _fetch_metadata**

In `Producer.__init__`, add:
```python
        self._partition_leaders: dict[int, str] = {}
```

Add method:
```python
    def _fetch_metadata(self) -> None:
        conn = SyncConnection(self._config.brokers[0])
        try:
            conn.connect()
            conn.write_frame(encode_message("", f"METADATA topic={self._config.topic}"))
            resp = conn.read_frame().decode()
        finally:
            conn.close()
        if not resp.startswith("OK"):
            return
        for part in resp.split():
            if part.startswith("leaders="):
                addrs = part.split("=", 1)[1].split(",")
                for i, addr in enumerate(addrs):
                    self._partition_leaders[i] = addr.strip()
```

- [ ] **Step 2: Call _fetch_metadata on startup**

In `Producer.__init__`, at the end:
```python
        try:
            self._fetch_metadata()
        except Exception:
            pass
```

- [ ] **Step 3: Use partition leader in _sender_loop connection**

In `_sender_loop`, where the connection is established for a partition, prefer the partition leader:

Find the connection creation and update:
```python
        addr = self._partition_leaders.get(part, self._config.brokers[0])
        conn = SyncConnection(addr)
```

Also handle NOT_LEADER in the ACK response:
```python
        if "NOT_LEADER LEADER_IS" in resp_str:
            parts = resp_str.split()
            for i, p in enumerate(parts):
                if p == "LEADER_IS" and i + 1 < len(parts):
                    self._partition_leaders[part] = parts[i + 1]
                    break
```

- [ ] **Step 4: Run tests**

```bash
cd ../cursus-python && uv run pytest tests/unit/ -v --tb=short
```

- [ ] **Step 5: Do NOT commit**

---

### Task 9: I1 — Java SDK periodic METADATA refresh

**Files:**
- Modify: `../cursus-java/cursus-client/src/main/java/io/cursus/client/consumer/CursusConsumer.java`
- Modify: `../cursus-java/cursus-client/src/main/java/io/cursus/client/config/CursusConsumerConfig.java`

Java Consumer already has `fetchMetadata()` called once during `joinGroupAndConsume()`. Add periodic refresh.

- [ ] **Step 1: Add metadataRefreshIntervalMs to CursusConsumerConfig**

In `CursusConsumerConfig.java`, add field with default 30000:

```java
private long metadataRefreshIntervalMs = 30000;

public long getMetadataRefreshIntervalMs() { return metadataRefreshIntervalMs; }
public void setMetadataRefreshIntervalMs(long ms) { this.metadataRefreshIntervalMs = ms; }
```

- [ ] **Step 2: Add metadata refresh scheduled task**

In `CursusConsumer.java`, add a `ScheduledFuture<?>` field:

```java
private volatile ScheduledFuture<?> metadataRefreshFuture;
```

In `joinGroupAndConsume()`, after `fetchMetadata()` (around line 318), add:

```java
    // Start periodic metadata refresh
    if (metadataRefreshFuture != null) {
      metadataRefreshFuture.cancel(false);
    }
    long refreshMs = config.getMetadataRefreshIntervalMs();
    if (refreshMs > 0) {
      metadataRefreshFuture = heartbeatScheduler.scheduleAtFixedRate(
          this::fetchMetadata, refreshMs, refreshMs, TimeUnit.MILLISECONDS);
    }
```

- [ ] **Step 3: Cancel in cancelScheduledTasks**

In `cancelScheduledTasks()`, add:

```java
    if (metadataRefreshFuture != null) {
      metadataRefreshFuture.cancel(false);
      metadataRefreshFuture = null;
    }
```

- [ ] **Step 4: Build and test**

```bash
cd ../cursus-java && ./gradlew :cursus-client:build
```

- [ ] **Step 5: Do NOT commit**

---

### Task 10: I2 — Java SDK Producer METADATA integration

**Files:**
- Modify: `../cursus-java/cursus-client/src/main/java/io/cursus/client/producer/CursusProducer.java`

- [ ] **Step 1: Add partitionLeaders map and fetchMetadata**

In `CursusProducer.java`, add field:

```java
private final Map<Integer, String> partitionLeaders = new ConcurrentHashMap<>();
```

Add method (reuse the `sendPlainSocket` pattern from Consumer, or add to this class):

```java
private void fetchMetadata() {
    try {
      String cmd = CommandBuilder.metadata(config.getTopic());
      String addr = config.getBrokers().get(0);

      // Plain socket send (same as consumer pattern)
      String[] parts = addr.split(":");
      try (java.net.Socket socket = new java.net.Socket(parts[0], Integer.parseInt(parts[1]))) {
        socket.setSoTimeout(5000);
        java.io.OutputStream out = socket.getOutputStream();
        java.io.InputStream in = socket.getInputStream();

        byte[] cmdBytes = cmd.getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[2 + cmdBytes.length];
        System.arraycopy(cmdBytes, 0, payload, 2, cmdBytes.length);

        byte[] frame = new byte[4 + payload.length];
        frame[0] = (byte) (payload.length >> 24);
        frame[1] = (byte) (payload.length >> 16);
        frame[2] = (byte) (payload.length >> 8);
        frame[3] = (byte) (payload.length);
        System.arraycopy(payload, 0, frame, 4, payload.length);
        out.write(frame);
        out.flush();

        byte[] lenBuf = in.readNBytes(4);
        int respLen = ((lenBuf[0] & 0xFF) << 24) | ((lenBuf[1] & 0xFF) << 16)
            | ((lenBuf[2] & 0xFF) << 8) | (lenBuf[3] & 0xFF);
        byte[] respBytes = in.readNBytes(respLen);
        String result = new String(respBytes, StandardCharsets.UTF_8);

        if (result.startsWith("OK")) {
          for (String part : result.split("\\s+")) {
            if (part.startsWith("leaders=")) {
              String[] addrs = part.substring(8).split(",");
              for (int i = 0; i < addrs.length; i++) {
                String leaderAddr = addrs[i].trim();
                if (!leaderAddr.isEmpty()) {
                  partitionLeaders.put(i, leaderAddr);
                }
              }
            }
          }
          log.info("Producer partition leaders: {}", partitionLeaders);
        }
      }
    } catch (Exception e) {
      log.debug("Producer metadata fetch failed (non-critical): {}", e.getMessage());
    }
  }
```

- [ ] **Step 2: Call fetchMetadata in constructor**

In the constructor, after partition buffer setup, add:

```java
    fetchMetadata();
```

- [ ] **Step 3: Use partition leader in sendBatch connection**

In the batch sending logic, when getting a connection for a partition, prefer the partition leader. Find where `connectionManager.sendCommand()` or `connectionManager.getConnection()` is called for batch sends and use `partitionLeaders.get(partitionIdx)` if available.

Also handle NOT_LEADER in ACK response:

```java
    if (ackStr.contains("NOT_LEADER") && ackStr.contains("LEADER_IS")) {
      String[] fields = ackStr.split("\\s+");
      for (int i = 0; i < fields.length - 1; i++) {
        if ("LEADER_IS".equals(fields[i])) {
          partitionLeaders.put(partitionIdx, fields[i + 1]);
          break;
        }
      }
    }
```

- [ ] **Step 4: Build and test**

```bash
cd ../cursus-java && ./gradlew :cursus-client:build
```

- [ ] **Step 5: Do NOT commit**

---

### Task 11: Integration Test — Cluster E2E (All SDKs)

**Files:** None — test with running cluster

- [ ] **Step 1: Rebuild cluster**

```bash
cd test/cluster && docker compose down && docker compose up -d --build broker-1 broker-2 broker-3
sleep 15
docker ps --format '{{.Names}} {{.Status}}' | grep broker
```

- [ ] **Step 2: Verify CONSUME without ownership (C1)**

```bash
docker run --rm --network cluster_network python:3.11-slim python3 -c "
import socket, struct, time
def send(addr, port, cmd):
    s = socket.socket(); s.connect((addr, port)); s.settimeout(10)
    msg = struct.pack('>H', 0) + cmd.encode()
    s.send(struct.pack('>I', len(msg)) + msg)
    rl = s.recv(4); rlen = struct.unpack('>I', rl)[0]
    resp = b''
    while len(resp) < rlen: resp += s.recv(rlen - len(resp))
    s.close(); return resp.decode()

# Create topic and produce without any consumer group setup
send('broker-1', 9000, 'CREATE topic=stateless-test partitions=2')
time.sleep(1)
send('broker-1', 9000, 'PUBLISH topic=stateless-test acks=1 producerId=p1 message=hello')
time.sleep(0.5)

# CONSUME without prior JOIN — should return message (stateless read)
resp = send('broker-1', 9000, 'CONSUME topic=stateless-test partition=0 offset=0 member=anon group=g1 batch=10')
print(f'CONSUME response length: {len(resp)} bytes')
print(f'Has data: {len(resp) > 10}')
"
```

Expected: CONSUME returns data (binary batch) without requiring prior JOIN_GROUP.

- [ ] **Step 3: Verify produce + immediate consume (C4)**

Test that async write + flush guarantees read-after-write:

```bash
docker run --rm --network cluster_network python:3.11-slim python3 -c "
import socket, struct, time, json
def send(addr, port, cmd):
    s = socket.socket(); s.connect((addr, port)); s.settimeout(10)
    msg = struct.pack('>H', 0) + cmd.encode()
    s.send(struct.pack('>I', len(msg)) + msg)
    rl = s.recv(4); rlen = struct.unpack('>I', rl)[0]
    resp = b''
    while len(resp) < rlen: resp += s.recv(rlen - len(resp))
    s.close(); return resp

send('broker-1', 9000, b'CREATE topic=flush-test partitions=1')
time.sleep(1)

# Produce
ack = send('broker-1', 9000, b'PUBLISH topic=flush-test acks=1 producerId=p1 message=test-msg')
print(f'ACK: {ack.decode()[:60]}')

# Immediate consume (same broker, should see the message)
resp = send('broker-1', 9000, b'CONSUME topic=flush-test partition=0 offset=0 member=m1 group=g1 batch=10')
print(f'CONSUME: {len(resp)} bytes (should be >10)')
"
```

- [ ] **Step 4: Verify all brokers alive, no stack overflow**

```bash
for b in broker-1 broker-2 broker-3; do
    n=$(docker logs $b 2>&1 | grep -c "stack overflow")
    echo "$b: $([ $n = 0 ] && echo OK || echo FAIL)"
done
```
