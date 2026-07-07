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
	closed    bool
	wg        sync.WaitGroup
	indexes   map[string]*StreamIndex   // key: "topic:partition"
	snapshots map[string]*SnapshotStore // key: "topic:partition"
}

type AppendOptions struct {
	LeaderAppend bool
	AfterAppend  func(topic string, partition int, msg types.Message) error
}

type AppendResult struct {
	Topic     string
	Key       string
	Version   uint64
	Offset    uint64
	Partition int
	Message   types.Message
}

type SnapshotResult struct {
	Topic     string
	Key       string
	Version   uint64
	Partition int
	Payload   string
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
	key := topicName + ":" + strconv.Itoa(partitionID)

	h.mu.RLock()
	if h.closed {
		h.mu.RUnlock()
		return nil, fmt.Errorf("handler is closed")
	}
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
	if err := h.RecoverIndexFromLog(topicName, partitionID, idx); err != nil {
		_ = idx.Close()
		return nil, err
	}
	h.indexes[key] = idx
	return idx, nil
}

// getSnapshot returns the SnapshotStore for the given topic and partition, creating it lazily.
func (h *Handler) getSnapshot(topicName string, partitionID int) (*SnapshotStore, error) {
	key := topicName + ":" + strconv.Itoa(partitionID)

	h.mu.RLock()
	if h.closed {
		h.mu.RUnlock()
		return nil, fmt.Errorf("handler is closed")
	}
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

// IndexReplicatedMessages records event-sourcing messages that arrived through
// follower replication. It is idempotent for already-indexed versions.
func (h *Handler) IndexReplicatedMessages(topicName string, partitionID int, messages []types.Message) error {
	idx, err := h.getIndex(topicName, partitionID)
	if err != nil {
		return err
	}
	return h.indexMessages(idx, messages)
}

func (h *Handler) RecoverIndexFromLog(topicName string, partitionID int, idx *StreamIndex) error {
	t := h.tm.GetTopic(topicName)
	if t == nil || !t.IsEventSourcing {
		return nil
	}
	p, err := t.GetPartition(partitionID)
	if err != nil {
		return fmt.Errorf("partition lookup for index recovery topic=%s partition=%d: %w", topicName, partitionID, err)
	}

	latest := p.GetHWM()
	const batchSize = 256
	for offset := uint64(0); offset < latest; {
		msgs, err := p.ReadCommitted(offset, batchSize)
		if err != nil {
			if offset == 0 {
				return nil
			}
			return fmt.Errorf("recover stream index from log offset=%d: %w", offset, err)
		}
		if len(msgs) == 0 {
			break
		}
		if err := h.indexMessages(idx, msgs); err != nil {
			return err
		}
		offset = msgs[len(msgs)-1].Offset + 1
	}
	return nil
}

func (h *Handler) indexMessages(idx *StreamIndex, messages []types.Message) error {
	for _, msg := range messages {
		if msg.Key == "" || msg.AggregateVersion == 0 {
			continue
		}
		current := idx.GetVersion(msg.Key)
		switch {
		case msg.AggregateVersion <= current:
			continue
		case msg.AggregateVersion != current+1:
			return fmt.Errorf("stream index gap key=%s current=%d next=%d", msg.Key, current, msg.AggregateVersion)
		}
		if err := idx.Append(msg.Key, msg.AggregateVersion, msg.Offset, 0); err != nil {
			return err
		}
	}
	return nil
}

// HandleAppendStream processes:
//
//	APPEND_STREAM topic=<name> key=<aggregate_key> version=<expected> event_type=<type> message=<payload>
func (h *Handler) HandleAppendStream(cmd string) string {
	result, errResp := h.AppendStream(cmd, AppendOptions{})
	if errResp != "" {
		return errResp
	}
	return result.Response()
}

func (r *AppendResult) Response() string {
	return fmt.Sprintf("OK version=%d offset=%d partition=%d", r.Version, r.Offset, r.Partition)
}

func (h *Handler) AppendStream(cmd string, opts AppendOptions) (*AppendResult, string) {
	h.wg.Add(1)
	defer h.wg.Done()

	args := parseKeyValueArgs(cmd[len("APPEND_STREAM "):])

	topicName := args["topic"]
	if topicName == "" {
		return nil, "ERROR: missing_topic"
	}
	key := args["key"]
	if key == "" {
		return nil, "ERROR: missing_key"
	}
	versionStr := args["version"]
	if versionStr == "" {
		return nil, "ERROR: missing_version"
	}
	expectedVersion, err := strconv.ParseUint(versionStr, 10, 64)
	if err != nil {
		return nil, "ERROR: invalid_version"
	}
	payload, ok := args["message"]
	if !ok || payload == "" {
		return nil, "ERROR: missing_message"
	}

	t := h.tm.GetTopic(topicName)
	if t == nil {
		return nil, fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if !t.IsEventSourcing {
		return nil, fmt.Sprintf("ERROR: event_sourcing_not_enabled topic=%s", topicName)
	}

	msg := types.Message{
		Key:              key,
		Payload:          payload,
		EventType:        args["event_type"],
		SchemaVersion:    1,
		AggregateVersion: expectedVersion,
		Metadata:         args["metadata"],
	}
	if svStr := args["schema_version"]; svStr != "" {
		sv, err := strconv.ParseUint(svStr, 10, 32)
		if err != nil {
			return nil, "ERROR: invalid_schema_version"
		}
		msg.SchemaVersion = uint32(sv)
	}

	partitionID := t.GetPartitionForMessage(msg)
	if partitionID < 0 {
		return nil, "ERROR: no_partitions_available"
	}

	p, err := t.GetPartition(partitionID)
	if err != nil {
		return nil, fmt.Sprintf("ERROR: partition_lookup_failed partition=%d reason=%q", partitionID, err.Error())
	}

	idx, err := h.getIndex(topicName, partitionID)
	if err != nil {
		return nil, fmt.Sprintf("ERROR: stream_index_failed partition=%d reason=%q", partitionID, err.Error())
	}

	var appendedOffset uint64
	var appendedMsg types.Message
	ok, current, err := idx.CheckEnqueueAndAppend(key, expectedVersion, func() (uint64, error) {
		if opts.LeaderAppend {
			batch := []types.Message{msg}
			if err := p.EnqueueBatchLeader(batch); err != nil {
				return 0, err
			}
			appendedMsg = batch[0]
			appendedOffset = batch[0].Offset
		} else {
			if err := p.EnqueueSync(msg); err != nil {
				return 0, err
			}
			appendedMsg = msg
			appendedOffset = p.NextOffset() - 1
			appendedMsg.Offset = appendedOffset
		}

		if opts.AfterAppend != nil {
			if err := opts.AfterAppend(topicName, partitionID, appendedMsg); err != nil {
				return 0, err
			}
		}

		if opts.LeaderAppend {
			p.FlushDisk()
			p.AdvanceHWM()
		}
		return appendedOffset, nil
	})
	if err != nil {
		return nil, fmt.Sprintf("ERROR: append_stream_failed reason=%q", err.Error())
	}
	if !ok {
		return nil, fmt.Sprintf("ERROR: version_conflict current=%d expected=%d", current, expectedVersion)
	}

	return &AppendResult{Topic: topicName, Key: key, Version: expectedVersion, Offset: appendedOffset, Partition: partitionID, Message: appendedMsg}, ""
}

// HandleReadStream writes event data directly to conn.
// Protocol: two length-prefixed frames — JSON envelope + binary batch.
func (h *Handler) HandleReadStream(cmd string, conn net.Conn) {
	h.wg.Add(1)
	defer h.wg.Done()

	args := parseKeyValueArgs(cmd[len("READ_STREAM "):])

	topicName := args["topic"]
	if topicName == "" {
		writeError(conn, "missing_topic")
		return
	}
	key := args["key"]
	if key == "" {
		writeError(conn, "missing_key")
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
		writeError(conn, fmt.Sprintf("topic_not_found topic=%s", topicName))
		return
	}
	if !t.IsEventSourcing {
		writeError(conn, fmt.Sprintf("event_sourcing_not_enabled topic=%s", topicName))
		return
	}

	// Determine the partition for this key.
	partitionID := t.GetPartitionForMessage(types.Message{Key: key})
	if partitionID < 0 {
		writeError(conn, "no_partitions_available")
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
		writeError(conn, fmt.Sprintf("snapshot_read_failed reason=%q", err.Error()))
		return
	}

	actualFromVersion := fromVersion
	if snap != nil && snap.Version >= fromVersion {
		// Start reading from the version after the snapshot.
		actualFromVersion = snap.Version + 1
	}

	entries, err := idx.Lookup(key, actualFromVersion)
	if err != nil {
		writeError(conn, fmt.Sprintf("index_lookup_failed reason=%q", err.Error()))
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
		batch, err := p.ReadCommitted(entry.Offset, 1)
		if err != nil {
			writeError(conn, fmt.Sprintf("partition_read_failed offset=%d reason=%q", entry.Offset, err.Error()))
			return
		}
		if len(batch) > 0 && batch[0].Key == key {
			msgs = append(msgs, batch[0])
		}
	}

	// Build JSON envelope.
	envelope := struct {
		Status    string        `json:"status"`
		Topic     string        `json:"topic"`
		Key       string        `json:"key"`
		Partition int           `json:"partition"`
		Count     int           `json:"count"`
		Snapshot  *SnapshotData `json:"snapshot,omitempty"`
	}{
		Status:    "OK",
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
	result, errResp := h.SaveSnapshot(cmd, nil)
	if errResp != "" {
		return errResp
	}
	return result.Response()
}

func (r *SnapshotResult) Response() string {
	return fmt.Sprintf("OK version=%d partition=%d", r.Version, r.Partition)
}

func (h *Handler) SaveSnapshot(cmd string, afterSave func(result SnapshotResult) error) (*SnapshotResult, string) {
	h.wg.Add(1)
	defer h.wg.Done()

	args := parseKeyValueArgs(cmd[len("SAVE_SNAPSHOT "):])

	topicName := args["topic"]
	if topicName == "" {
		return nil, "ERROR: missing_topic"
	}
	key := args["key"]
	if key == "" {
		return nil, "ERROR: missing_key"
	}
	versionStr := args["version"]
	if versionStr == "" {
		return nil, "ERROR: missing_version"
	}
	version, err := strconv.ParseUint(versionStr, 10, 64)
	if err != nil {
		return nil, "ERROR: invalid_version"
	}
	payload := args["message"]
	if payload == "" {
		return nil, "ERROR: missing_message"
	}

	t := h.tm.GetTopic(topicName)
	if t == nil {
		return nil, fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if !t.IsEventSourcing {
		return nil, fmt.Sprintf("ERROR: event_sourcing_not_enabled topic=%s", topicName)
	}

	partitionID := t.GetPartitionForMessage(types.Message{Key: key})
	if partitionID < 0 {
		return nil, "ERROR: no_partitions_available"
	}

	idx, err := h.getIndex(topicName, partitionID)
	if err != nil {
		return nil, fmt.Sprintf("ERROR: stream_index_failed partition=%d reason=%q", partitionID, err.Error())
	}
	currentVersion := idx.GetVersion(key)
	if version > currentVersion {
		return nil, fmt.Sprintf("ERROR: snapshot_version_exceeds_stream version=%d current=%d", version, currentVersion)
	}

	result := SnapshotResult{Topic: topicName, Key: key, Version: version, Partition: partitionID, Payload: payload}
	if errResp := h.SaveSnapshotReplica(result); errResp != "" {
		return nil, errResp
	}
	if afterSave != nil {
		if err := afterSave(result); err != nil {
			return nil, fmt.Sprintf("ERROR: snapshot_replicate_failed reason=%q", err.Error())
		}
	}
	return &result, ""
}

func (h *Handler) SaveSnapshotReplica(result SnapshotResult) string {
	t := h.tm.GetTopic(result.Topic)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", result.Topic)
	}
	if !t.IsEventSourcing {
		return fmt.Sprintf("ERROR: event_sourcing_not_enabled topic=%s", result.Topic)
	}
	if _, err := t.GetPartition(result.Partition); err != nil {
		return fmt.Sprintf("ERROR: partition_lookup_failed partition=%d reason=%q", result.Partition, err.Error())
	}
	ss, err := h.getSnapshot(result.Topic, result.Partition)
	if err != nil {
		return fmt.Sprintf("ERROR: snapshot_store_failed partition=%d reason=%q", result.Partition, err.Error())
	}
	if err := ss.Save(result.Key, result.Version, result.Payload); err != nil {
		return fmt.Sprintf("ERROR: snapshot_save_failed reason=%q", err.Error())
	}
	return ""
}

// HandleReadSnapshot processes:
//
//	READ_SNAPSHOT topic=<name> key=<aggregate_key>
func (h *Handler) HandleReadSnapshot(cmd string) string {
	h.wg.Add(1)
	defer h.wg.Done()

	args := parseKeyValueArgs(cmd[len("READ_SNAPSHOT "):])

	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic"
	}
	key := args["key"]
	if key == "" {
		return "ERROR: missing_key"
	}

	t := h.tm.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if !t.IsEventSourcing {
		return fmt.Sprintf("ERROR: event_sourcing_not_enabled topic=%s", topicName)
	}

	partitionID := t.GetPartitionForMessage(types.Message{Key: key})
	if partitionID < 0 {
		return "ERROR: no_partitions_available"
	}

	ss, err := h.getSnapshot(topicName, partitionID)
	if err != nil {
		return fmt.Sprintf("ERROR: snapshot_store_failed partition=%d reason=%q", partitionID, err.Error())
	}

	snap, err := ss.Read(key)
	if err != nil {
		return fmt.Sprintf("ERROR: snapshot_read_failed reason=%q", err.Error())
	}
	if snap == nil {
		return "OK snapshot=null"
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Sprintf("ERROR: marshal_snapshot_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK snapshot=%s", string(data))
}

// HandleStreamVersion processes:
//
//	STREAM_VERSION topic=<name> key=<aggregate_key>
func (h *Handler) HandleStreamVersion(cmd string) string {
	h.wg.Add(1)
	defer h.wg.Done()

	args := parseKeyValueArgs(cmd[len("STREAM_VERSION "):])

	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic"
	}
	key := args["key"]
	if key == "" {
		return "ERROR: missing_key"
	}

	t := h.tm.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if !t.IsEventSourcing {
		return fmt.Sprintf("ERROR: event_sourcing_not_enabled topic=%s", topicName)
	}

	partitionID := t.GetPartitionForMessage(types.Message{Key: key})
	if partitionID < 0 {
		return "ERROR: no_partitions_available"
	}

	idx, err := h.getIndex(topicName, partitionID)
	if err != nil {
		return fmt.Sprintf("ERROR: stream_index_failed partition=%d reason=%q", partitionID, err.Error())
	}

	version := idx.GetVersion(key)
	return fmt.Sprintf("OK version=%d", version)
}

// DeleteTopic closes cached stream indexes and snapshot stores for a deleted topic.
func (h *Handler) DeleteTopic(topicName string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	prefix := topicName + ":"
	var firstErr error
	for key, idx := range h.indexes {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if err := idx.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close index %s: %w", key, err)
		}
		delete(h.indexes, key)
	}
	for key, ss := range h.snapshots {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if err := ss.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close snapshot store %s: %w", key, err)
		}
		delete(h.snapshots, key)
	}
	return firstErr
}

// Close closes all StreamIndex and SnapshotStore instances held by this handler.
// After Close, getIndex and getSnapshot will return errors.
func (h *Handler) Close() error {
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()

	h.wg.Wait()

	h.mu.Lock()
	defer h.mu.Unlock()

	var firstErr error
	for key, idx := range h.indexes {
		if err := idx.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close index %s: %w", key, err)
		}
	}
	for key, ss := range h.snapshots {
		if err := ss.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close snapshot store %s: %w", key, err)
		}
	}

	h.indexes = nil
	h.snapshots = nil
	return firstErr
}

// writeError writes a JSON error envelope to the connection.
func writeError(conn net.Conn, msg string) {
	errResp, _ := json.Marshal(map[string]string{"status": "ERROR", "error": msg})
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
