package eventsource

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// Handler processes event sourcing commands (APPEND_STREAM, READ_STREAM, etc.).
type Handler struct {
	tm *topic.TopicManager

	mu        sync.RWMutex
	indexes   map[string]*StreamIndex   // key: "topic:partition"
	snapshots map[string]*SnapshotStore // key: "topic:partition"
}

// NewHandler creates a new event sourcing command handler.
func NewHandler(tm *topic.TopicManager) *Handler {
	return &Handler{
		tm:        tm,
		indexes:   make(map[string]*StreamIndex),
		snapshots: make(map[string]*SnapshotStore),
	}
}

// getIndex returns the StreamIndex for the given topic and partition, creating it lazily.
func (h *Handler) getIndex(topicName string, partitionID int) (*StreamIndex, error) {
	key := fmt.Sprintf("%s:%d", topicName, partitionID)

	h.mu.RLock()
	idx, ok := h.indexes[key]
	h.mu.RUnlock()
	if ok {
		return idx, nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// Double-check after acquiring write lock.
	if idx, ok := h.indexes[key]; ok {
		return idx, nil
	}

	dir := h.tm.GetLogDir(topicName, partitionID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create dir for stream index %s:%d: %w", topicName, partitionID, err)
	}
	idx, err := NewStreamIndex(dir, partitionID)
	if err != nil {
		return nil, fmt.Errorf("open stream index for %s:%d: %w", topicName, partitionID, err)
	}
	h.indexes[key] = idx
	return idx, nil
}

// getSnapshot returns the SnapshotStore for the given topic and partition, creating it lazily.
func (h *Handler) getSnapshot(topicName string, partitionID int) (*SnapshotStore, error) {
	key := fmt.Sprintf("%s:%d", topicName, partitionID)

	h.mu.RLock()
	ss, ok := h.snapshots[key]
	h.mu.RUnlock()
	if ok {
		return ss, nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if ss, ok := h.snapshots[key]; ok {
		return ss, nil
	}

	dir := h.tm.GetLogDir(topicName, partitionID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create dir for snapshot store %s:%d: %w", topicName, partitionID, err)
	}
	ss, err := NewSnapshotStore(dir, partitionID)
	if err != nil {
		return nil, fmt.Errorf("open snapshot store for %s:%d: %w", topicName, partitionID, err)
	}
	h.snapshots[key] = ss
	return ss, nil
}

// HandleAppendStream processes:
//
//	APPEND_STREAM topic=<name> key=<aggregate_key> version=<expected> event_type=<type> message=<payload>
func (h *Handler) HandleAppendStream(cmd string) string {
	args := parseKeyValueArgs(cmd[len("APPEND_STREAM "):])

	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing topic parameter"
	}
	key := args["key"]
	if key == "" {
		return "ERROR: missing key parameter"
	}
	versionStr := args["version"]
	if versionStr == "" {
		return "ERROR: missing version parameter"
	}
	expectedVersion, err := strconv.ParseUint(versionStr, 10, 64)
	if err != nil {
		return "ERROR: invalid version"
	}
	payload := args["message"]
	if payload == "" {
		return "ERROR: missing message parameter"
	}

	t := h.tm.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
	}
	if !t.IsEventSourcing {
		return fmt.Sprintf("ERROR: topic '%s' is not event-sourcing enabled", topicName)
	}

	// Route to partition by key.
	msg := types.Message{
		Key:              key,
		Payload:          payload,
		EventType:        args["event_type"],
		AggregateVersion: expectedVersion,
		Metadata:         args["metadata"],
	}
	if svStr := args["schema_version"]; svStr != "" {
		sv, err := strconv.ParseUint(svStr, 10, 32)
		if err == nil {
			msg.SchemaVersion = uint32(sv)
		}
	}

	partitionID := t.GetPartitionForMessage(msg)
	if partitionID < 0 {
		return "ERROR: no partitions available"
	}

	p, err := t.GetPartition(partitionID)
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}

	idx, err := h.getIndex(topicName, partitionID)
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}

	// EnqueueSync acquires p.mu.Lock internally.
	if err := p.EnqueueSync(msg); err != nil {
		return fmt.Sprintf("ERROR: enqueue failed: %v", err)
	}

	// Atomically check version and append index entry.
	// This prevents two concurrent requests from both passing the version check.
	offset := p.NextOffset() - 1
	ok, current, err := idx.CheckAndAppend(key, expectedVersion, offset, 0)
	if err != nil {
		return fmt.Sprintf("ERROR: index append failed: %v", err)
	}
	if !ok {
		return fmt.Sprintf("ERROR: version_conflict current=%d expected=%d", current, expectedVersion)
	}

	return fmt.Sprintf("OK version=%d offset=%d partition=%d", expectedVersion, offset, partitionID)
}

// HandleReadStream writes event data directly to conn.
// Protocol: two length-prefixed frames — JSON envelope + binary batch.
func (h *Handler) HandleReadStream(cmd string, conn net.Conn) {
	args := parseKeyValueArgs(cmd[len("READ_STREAM "):])

	topicName := args["topic"]
	if topicName == "" {
		writeError(conn, "missing topic parameter")
		return
	}
	key := args["key"]
	if key == "" {
		writeError(conn, "missing key parameter")
		return
	}
	fromVersion := uint64(1)
	if fv := args["from_version"]; fv != "" {
		v, err := strconv.ParseUint(fv, 10, 64)
		if err == nil {
			fromVersion = v
		}
	}

	t := h.tm.GetTopic(topicName)
	if t == nil {
		writeError(conn, fmt.Sprintf("topic '%s' does not exist", topicName))
		return
	}

	// Determine the partition for this key.
	partitionID := t.GetPartitionForMessage(types.Message{Key: key})
	if partitionID < 0 {
		writeError(conn, "no partitions available")
		return
	}

	idx, err := h.getIndex(topicName, partitionID)
	if err != nil {
		writeError(conn, err.Error())
		return
	}

	// Check snapshot: if one exists with version >= fromVersion, use it as base.
	ss, err := h.getSnapshot(topicName, partitionID)
	if err != nil {
		writeError(conn, err.Error())
		return
	}

	snap, err := ss.Read(key)
	if err != nil {
		writeError(conn, fmt.Sprintf("snapshot read error: %v", err))
		return
	}

	actualFromVersion := fromVersion
	if snap != nil && snap.Version >= fromVersion {
		// Start reading from the version after the snapshot.
		actualFromVersion = snap.Version + 1
	}

	entries, err := idx.Lookup(key, actualFromVersion)
	if err != nil {
		writeError(conn, fmt.Sprintf("index lookup error: %v", err))
		return
	}

	p, err := t.GetPartition(partitionID)
	if err != nil {
		writeError(conn, err.Error())
		return
	}

	// Collect messages from the partition for each index entry.
	var msgs []types.Message
	for _, entry := range entries {
		batch, err := p.ReadMessages(entry.Offset, 1)
		if err != nil {
			writeError(conn, fmt.Sprintf("read error at offset %d: %v", entry.Offset, err))
			return
		}
		if len(batch) > 0 {
			msgs = append(msgs, batch[0])
		}
	}

	// Build JSON envelope.
	envelope := struct {
		Topic     string        `json:"topic"`
		Key       string        `json:"key"`
		Partition int           `json:"partition"`
		Count     int           `json:"count"`
		Snapshot  *SnapshotData `json:"snapshot,omitempty"`
	}{
		Topic:     topicName,
		Key:       key,
		Partition: partitionID,
		Count:     len(msgs),
	}
	if snap != nil && snap.Version >= fromVersion {
		envelope.Snapshot = snap
	}

	envJSON, err := json.Marshal(envelope)
	if err != nil {
		writeError(conn, fmt.Sprintf("marshal envelope: %v", err))
		return
	}

	// Frame 1: JSON envelope.
	if err := util.WriteWithLength(conn, envJSON); err != nil {
		return
	}

	// Frame 2: binary batch.
	batchData, err := util.EncodeBatchMessages(topicName, partitionID, "1", false, msgs)
	if err != nil {
		// Envelope already sent; best effort write of empty batch.
		_ = util.WriteWithLength(conn, []byte{})
		return
	}
	_ = util.WriteWithLength(conn, batchData)
}

// HandleSaveSnapshot processes:
//
//	SAVE_SNAPSHOT topic=<name> key=<aggregate_key> version=<N> message=<json_payload>
func (h *Handler) HandleSaveSnapshot(cmd string) string {
	args := parseKeyValueArgs(cmd[len("SAVE_SNAPSHOT "):])

	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing topic parameter"
	}
	key := args["key"]
	if key == "" {
		return "ERROR: missing key parameter"
	}
	versionStr := args["version"]
	if versionStr == "" {
		return "ERROR: missing version parameter"
	}
	version, err := strconv.ParseUint(versionStr, 10, 64)
	if err != nil {
		return "ERROR: invalid version"
	}
	payload := args["message"]
	if payload == "" {
		return "ERROR: missing message parameter"
	}

	t := h.tm.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
	}

	partitionID := t.GetPartitionForMessage(types.Message{Key: key})
	if partitionID < 0 {
		return "ERROR: no partitions available"
	}

	// Validate that version <= current stream version.
	idx, err := h.getIndex(topicName, partitionID)
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}
	currentVersion := idx.GetVersion(key)
	if version > currentVersion {
		return fmt.Sprintf("ERROR: snapshot version %d exceeds stream version %d", version, currentVersion)
	}

	ss, err := h.getSnapshot(topicName, partitionID)
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}

	if err := ss.Save(key, version, payload); err != nil {
		return fmt.Sprintf("ERROR: snapshot save failed: %v", err)
	}

	return fmt.Sprintf("OK version=%d partition=%d", version, partitionID)
}

// HandleReadSnapshot processes:
//
//	READ_SNAPSHOT topic=<name> key=<aggregate_key>
func (h *Handler) HandleReadSnapshot(cmd string) string {
	args := parseKeyValueArgs(cmd[len("READ_SNAPSHOT "):])

	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing topic parameter"
	}
	key := args["key"]
	if key == "" {
		return "ERROR: missing key parameter"
	}

	t := h.tm.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
	}

	partitionID := t.GetPartitionForMessage(types.Message{Key: key})
	if partitionID < 0 {
		return "ERROR: no partitions available"
	}

	ss, err := h.getSnapshot(topicName, partitionID)
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}

	snap, err := ss.Read(key)
	if err != nil {
		return fmt.Sprintf("ERROR: snapshot read failed: %v", err)
	}
	if snap == nil {
		return "NULL"
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Sprintf("ERROR: marshal snapshot: %v", err)
	}
	return string(data)
}

// HandleStreamVersion processes:
//
//	STREAM_VERSION topic=<name> key=<aggregate_key>
func (h *Handler) HandleStreamVersion(cmd string) string {
	args := parseKeyValueArgs(cmd[len("STREAM_VERSION "):])

	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing topic parameter"
	}
	key := args["key"]
	if key == "" {
		return "ERROR: missing key parameter"
	}

	t := h.tm.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
	}

	partitionID := t.GetPartitionForMessage(types.Message{Key: key})
	if partitionID < 0 {
		return "ERROR: no partitions available"
	}

	idx, err := h.getIndex(topicName, partitionID)
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}

	version := idx.GetVersion(key)
	return fmt.Sprintf("%d", version)
}

// writeError writes a JSON error envelope to the connection.
func writeError(conn net.Conn, msg string) {
	errResp, _ := json.Marshal(map[string]string{"error": msg})
	_ = util.WriteWithLength(conn, errResp)
}

// parseKeyValueArgs parses "key=value" pairs from a command argument string.
// The "message" key receives all text after "message=" (preserving spaces).
func parseKeyValueArgs(argsStr string) map[string]string {
	result := make(map[string]string)

	messageIdx := strings.Index(argsStr, "message=")

	if messageIdx != -1 {
		beforeMessage := argsStr[:messageIdx]
		parts := strings.Fields(beforeMessage)
		for _, part := range parts {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				result[kv[0]] = kv[1]
			}
		}
		result["message"] = strings.TrimSpace(argsStr[messageIdx+8:])
	} else {
		parts := strings.Fields(argsStr)
		for _, part := range parts {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				result[kv[0]] = kv[1]
			}
		}
	}
	return result
}
