package eventsource

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

	mu          sync.RWMutex
	indexSyncMu sync.Mutex
	closed      bool
	wg          sync.WaitGroup
	indexes     map[string]*StreamIndex   // key: "topic:partition"
	indexedHWM  map[string]uint64         // committed log tail represented by each index
	snapshots   map[string]*SnapshotStore // key: "topic:partition"
}

type AppendOptions struct {
	LeaderAppend bool
	LeaderEpoch  int64
	AfterAppend  func(topic string, partition int, msg types.Message) error
	AfterCommit  func(topic string, partition int, hwm uint64) error
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
	Topic          string `json:"topic"`
	Key            string `json:"key"`
	Version        uint64 `json:"version"`
	Partition      int    `json:"partition"`
	Payload        string `json:"payload"`
	LifecycleEpoch uint64 `json:"lifecycle_epoch,omitempty"`
}

// NewHandler creates a new event sourcing command handler.
func NewHandler(tm *topic.TopicManager) *Handler {
	return &Handler{
		tm:         tm,
		indexes:    make(map[string]*StreamIndex),
		indexedHWM: make(map[string]uint64),
		snapshots:  make(map[string]*SnapshotStore),
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
	if err := os.MkdirAll(dir, 0o750); err != nil {
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
	t := h.tm.GetTopic(topicName)
	if t == nil {
		_ = idx.Close()
		return nil, fmt.Errorf("topic %s not found during stream index recovery", topicName)
	}
	p, err := t.GetPartition(partitionID)
	if err != nil {
		_ = idx.Close()
		return nil, fmt.Errorf("partition lookup for stream index %s:%d: %w", topicName, partitionID, err)
	}
	h.indexes[key] = idx
	// Recovery reads only the stable committed prefix. Never claim that an
	// unresolved transactional tail has been indexed merely because it is in
	// the partition HWM.
	h.indexedHWM[key] = p.LastStableOffset()
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
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create dir for snapshot store %s:%d: %w", topicName, partitionID, err)
	}
	ss, err := NewSnapshotStore(dir, partitionID)
	if err != nil {
		return nil, fmt.Errorf("open snapshot store for %s:%d: %w", topicName, partitionID, err)
	}
	h.snapshots[key] = ss
	return ss, nil
}

// PrepareCommittedIndex captures the currently indexed committed tail before a
// follower advances its HWM.
func (h *Handler) PrepareCommittedIndex(topicName string, partitionID int) error {
	_, err := h.getIndex(topicName, partitionID)
	return err
}

// CurrentVersion returns the committed aggregate version and its partition.
// Callers that need the latest transactional visibility first advance the
// committed index to the partition HWM with IndexCommittedToHWM.
func (h *Handler) CurrentVersion(topicName, key string) (uint64, int, error) {
	if key == "" {
		return 0, 0, fmt.Errorf("stream key is required")
	}
	t := h.tm.GetTopic(topicName)
	if t == nil {
		return 0, 0, fmt.Errorf("topic %s not found", topicName)
	}
	if !t.IsEventSourcing {
		return 0, 0, fmt.Errorf("event sourcing is not enabled for topic %s", topicName)
	}
	partition := t.GetPartitionForMessage(types.Message{Key: key})
	if partition < 0 {
		return 0, 0, fmt.Errorf("no partitions available")
	}
	idx, err := h.getIndex(topicName, partition)
	if err != nil {
		return 0, 0, err
	}
	return idx.GetVersion(key), partition, nil
}

// IndexCommittedToHWM advances the derived stream index only through records
// visible below the partition's stable committed boundary.
func (h *Handler) IndexCommittedToHWM(topicName string, partitionID int, targetHWM uint64) error {
	h.indexSyncMu.Lock()
	defer h.indexSyncMu.Unlock()

	idx, err := h.getIndex(topicName, partitionID)
	if err != nil {
		return err
	}
	t := h.tm.GetTopic(topicName)
	if t == nil || !t.IsEventSourcing {
		return nil
	}
	p, err := t.GetPartition(partitionID)
	if err != nil {
		return fmt.Errorf("partition lookup for committed index topic=%s partition=%d: %w", topicName, partitionID, err)
	}

	key := topicName + ":" + strconv.Itoa(partitionID)
	h.mu.RLock()
	start := h.indexedHWM[key]
	h.mu.RUnlock()

	scanEnd := targetHWM
	if stable := p.LastStableOffset(); stable < scanEnd {
		scanEnd = stable
	}
	if scanEnd <= start {
		return nil
	}

	const batchSize = 256
	indexedUntil := start
	for offset := start; offset < scanEnd; {
		msgs, err := p.ReadCommitted(offset, batchSize)
		if err != nil {
			return fmt.Errorf("read committed stream index range offset=%d: %w", offset, err)
		}
		if len(msgs) == 0 {
			break
		}

		bounded := msgs[:0]
		for _, msg := range msgs {
			if msg.Offset >= scanEnd {
				break
			}
			bounded = append(bounded, msg)
		}
		if len(bounded) == 0 {
			break
		}
		if err := h.indexMessages(idx, bounded); err != nil {
			return err
		}
		next := bounded[len(bounded)-1].Offset + 1
		if next <= offset {
			return fmt.Errorf("stream index scan did not advance from offset %d", offset)
		}
		offset = next
		indexedUntil = next
	}

	h.mu.Lock()
	if h.indexedHWM[key] < indexedUntil {
		h.indexedHWM[key] = indexedUntil
	}
	h.mu.Unlock()
	return nil
}

func (h *Handler) RecoverIndexFromLog(topicName string, partitionID int, idx *StreamIndex) error {
	if err := idx.Reset(); err != nil {
		return fmt.Errorf("reset stream index topic=%s partition=%d: %w", topicName, partitionID, err)
	}
	t := h.tm.GetTopic(topicName)
	if t == nil || !t.IsEventSourcing {
		return nil
	}
	p, err := t.GetPartition(partitionID)
	if err != nil {
		return fmt.Errorf("partition lookup for index recovery topic=%s partition=%d: %w", topicName, partitionID, err)
	}

	first := p.GetFirstOffset()
	latest := p.LastStableOffset()
	if first > latest {
		return fmt.Errorf("stream retention floor %d exceeds stable tail %d", first, latest)
	}
	if first > 0 {
		// The stream index is disposable. Once retention removes its prefix,
		// application snapshots are the durable proof of each aggregate's
		// preceding version.
		ss, err := NewSnapshotStore(h.tm.GetLogDir(topicName, partitionID), partitionID)
		if err != nil {
			return fmt.Errorf("open snapshot checkpoint for stream index recovery: %w", err)
		}
		records, listErr := ss.List()
		closeErr := ss.Close()
		if err := errors.Join(listErr, closeErr); err != nil {
			return fmt.Errorf("load snapshot checkpoint for stream index recovery: %w", err)
		}
		if len(records) == 0 {
			return fmt.Errorf("cannot recover retained event stream at offset %d without a snapshot checkpoint", first)
		}
		for _, record := range records {
			if err := idx.SeedVersion(record.Key, record.Version); err != nil {
				return fmt.Errorf("seed stream index from snapshot: %w", err)
			}
		}
	}
	const batchSize = 256
	for offset := first; offset < latest; {
		msgs, _, next, err := p.ReadCommittedPage(offset, batchSize, 0, true)
		if err != nil {
			return fmt.Errorf("recover stream index from log offset=%d: %w", offset, err)
		}
		if next <= offset {
			return fmt.Errorf("recover stream index stopped before stable tail at offset=%d tail=%d", offset, latest)
		}
		bounded := msgs[:0]
		for _, msg := range msgs {
			if msg.Offset >= latest {
				break
			}
			bounded = append(bounded, msg)
		}
		if len(bounded) != 0 {
			if err := h.indexMessages(idx, bounded); err != nil {
				return err
			}
		}
		offset = min(next, latest)
	}
	return nil
}

type SnapshotPage struct {
	Snapshots []SnapshotResult `json:"snapshots"`
	Revision  uint64           `json:"revision"`
	Done      bool             `json:"done"`
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
	if t.Policy.AggregateReplay {
		if args["event_id"] == "" {
			return nil, "ERROR: missing_event_id"
		}
		if args["producerId"] == "" {
			return nil, "ERROR: missing_producer_id"
		}
		seq, parseErr := strconv.ParseUint(args["seqNum"], 10, 64)
		if parseErr != nil || seq == 0 {
			return nil, "ERROR: invalid_producer_sequence"
		}
		epoch := int64(0)
		if raw := args["epoch"]; raw != "" {
			parsed, epochErr := strconv.ParseInt(raw, 10, 64)
			if epochErr != nil || parsed < 0 {
				return nil, "ERROR: invalid_producer_epoch"
			}
			epoch = parsed
		}
		sum := sha256.Sum256([]byte(payload))
		msg.EventID, msg.PayloadDigest = args["event_id"], hex.EncodeToString(sum[:])
		msg.ProducerID, msg.SeqNum, msg.Epoch = args["producerId"], seq, epoch
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
	if t.Policy.AggregateReplay && expectedVersion <= idx.GetVersion(key) {
		entries, lookupErr := idx.Lookup(key, expectedVersion)
		if lookupErr != nil {
			return nil, fmt.Sprintf("ERROR: aggregate_index_lookup_failed reason=%q", lookupErr.Error())
		}
		for _, entry := range entries {
			if entry.AggregateVersion != expectedVersion {
				continue
			}
			existing, readErr := p.ReadCommitted(entry.Offset, 1)
			if readErr != nil || len(existing) != 1 {
				return nil, "ERROR: aggregate_proof_unavailable"
			}
			if existing[0].EventID == msg.EventID && existing[0].PayloadDigest == msg.PayloadDigest {
				return &AppendResult{Topic: topicName, Key: key, Version: expectedVersion, Offset: entry.Offset, Partition: partitionID, Message: existing[0]}, ""
			}
			return nil, fmt.Sprintf("ERROR: aggregate_identity_conflict sequence=%d", expectedVersion)
		}
	}
	if t.Policy.AggregateReplay {
		seen, scanErr := aggregateEventIDExists(p, msg.EventID)
		if scanErr != nil {
			return nil, fmt.Sprintf("ERROR: aggregate_proof_unavailable reason=%q", scanErr.Error())
		}
		if seen {
			return nil, fmt.Sprintf("ERROR: aggregate_identity_conflict event_id=%s", msg.EventID)
		}
	}

	var appendedOffset uint64
	var appendedMsg types.Message
	ok, current, err := idx.CheckEnqueueAndAppend(key, expectedVersion, func() (uint64, error) {
		if opts.LeaderAppend {
			msg.LeaderEpoch = opts.LeaderEpoch
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

		if opts.LeaderAppend {
			p.FlushDisk()
		}
		if opts.AfterAppend != nil {
			if err := opts.AfterAppend(topicName, partitionID, appendedMsg); err != nil {
				return 0, err
			}
		}

		if opts.LeaderAppend {
			hwm := p.NextOffset()
			if opts.AfterCommit != nil {
				if err := opts.AfterCommit(topicName, partitionID, hwm); err != nil {
					return 0, err
				}
			} else {
				p.AdvanceHWM()
			}
		}
		return appendedOffset, nil
	})
	recoveredCommit := false
	if err != nil {
		if recoveryErr := h.RecoverIndexFromLog(topicName, partitionID, idx); recoveryErr != nil {
			return nil, fmt.Sprintf("ERROR: append_stream_failed reason=%q recovery_reason=%q", err.Error(), recoveryErr.Error())
		}
		if idx.GetVersion(key) != expectedVersion {
			return nil, fmt.Sprintf("ERROR: append_stream_failed reason=%q", err.Error())
		}
		// The log and HWM commit succeeded even though the derived index append
		// failed. Recovery found the committed event, so report its success.
		recoveredCommit = true
	}
	if !ok && !recoveredCommit {
		return nil, fmt.Sprintf("ERROR: version_conflict current=%d expected=%d", current, expectedVersion)
	}

	indexKey := topicName + ":" + strconv.Itoa(partitionID)
	h.mu.Lock()
	if committed := p.LastStableOffset(); h.indexedHWM[indexKey] < committed {
		h.indexedHWM[indexKey] = committed
	}
	h.mu.Unlock()

	return &AppendResult{Topic: topicName, Key: key, Version: expectedVersion, Offset: appendedOffset, Partition: partitionID, Message: appendedMsg}, ""
}

// aggregateEventIDExists consults the committed retained log rather than an
// in-memory index so an event-id uniqueness decision survives broker restart.
func aggregateEventIDExists(p *topic.Partition, eventID string) (bool, error) {
	limit := p.LastStableOffset()
	for offset := p.OffsetRange().Earliest; offset < limit; {
		messages, err := p.ReadCommitted(offset, 256)
		if err != nil {
			return false, err
		}
		if len(messages) == 0 {
			return false, fmt.Errorf("committed log scan made no progress")
		}
		for _, existing := range messages {
			if existing.Offset >= limit {
				return false, nil
			}
			if existing.EventID == eventID {
				return true, nil
			}
		}
		next := messages[len(messages)-1].Offset + 1
		if next <= offset {
			return false, fmt.Errorf("committed log scan did not advance")
		}
		offset = next
	}
	return false, nil
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
		if err != nil || v == 0 {
			writeError(conn, "invalid_from_version")
			return
		}
		fromVersion = v
	}
	paginated := args["max_events"] != "" || args["max_bytes"] != ""
	maxEvents := 256
	if raw := args["max_events"]; raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 4096 {
			writeError(conn, "invalid_max_events")
			return
		}
		maxEvents = value
	}
	maxBytes := 8 << 20
	if raw := args["max_bytes"]; raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1024 || value > 32<<20 {
			writeError(conn, "invalid_max_bytes")
			return
		}
		maxBytes = value
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

	currentVersion := idx.GetVersion(key)
	var entries []StreamIndexEntry
	if paginated {
		entries, err = idx.LookupRange(key, actualFromVersion, currentVersion, maxEvents+1)
	} else {
		entries, err = idx.Lookup(key, actualFromVersion)
		maxBytes = util.MaxMessageSize
	}
	if err != nil {
		writeError(conn, fmt.Sprintf("index_lookup_failed reason=%q", err.Error()))
		return
	}

	p, err := t.GetPartition(partitionID)
	if err != nil {
		writeError(conn, err.Error())
		return
	}

	moreByCount := paginated && len(entries) > maxEvents
	if moreByCount {
		entries = entries[:maxEvents]
	}

	// Collect one bounded page before sending its success envelope.
	msgs := make([]types.Message, 0, len(entries))
	usedBytes := 0
	moreByBytes := false
	for _, entry := range entries {
		batch, err := p.ReadCommitted(entry.Offset, 1)
		if err != nil {
			writeError(conn, fmt.Sprintf("partition_read_failed offset=%d reason=%q", entry.Offset, err.Error()))
			return
		}
		if len(batch) == 0 || batch[0].Key != key || batch[0].AggregateVersion != entry.AggregateVersion {
			writeError(conn, fmt.Sprintf("stream_index_record_mismatch offset=%d", entry.Offset))
			return
		}
		size := estimateEventMessageBytes(batch[0])
		if usedBytes+size > maxBytes {
			if !paginated {
				writeError(conn, fmt.Sprintf("stream_requires_pagination bytes>%d", maxBytes))
				return
			}
			if len(msgs) == 0 {
				writeError(conn, fmt.Sprintf("event_exceeds_page_budget version=%d bytes=%d maximum=%d", entry.AggregateVersion, size, maxBytes))
				return
			}
			moreByBytes = true
			break
		}
		msgs = append(msgs, batch[0])
		usedBytes += size
	}
	batchData, err := util.EncodeBatchMessages(topicName, partitionID, "1", false, msgs)
	if err != nil {
		writeError(conn, fmt.Sprintf("encode_stream_page_failed reason=%q", err.Error()))
		return
	}
	hasMore := moreByCount || moreByBytes
	nextVersion := uint64(0)
	if hasMore && len(msgs) > 0 {
		nextVersion = msgs[len(msgs)-1].AggregateVersion + 1
	}

	// Build JSON envelope.
	envelope := struct {
		Status      string        `json:"status"`
		Topic       string        `json:"topic"`
		Key         string        `json:"key"`
		Partition   int           `json:"partition"`
		Count       int           `json:"count"`
		Snapshot    *SnapshotData `json:"snapshot,omitempty"`
		HasMore     bool          `json:"has_more"`
		NextVersion uint64        `json:"next_version,omitempty"`
	}{
		Status:      "OK",
		Topic:       topicName,
		Key:         key,
		Partition:   partitionID,
		Count:       len(msgs),
		HasMore:     hasMore,
		NextVersion: nextVersion,
	}
	if snap != nil && snap.Version >= fromVersion {
		envelope.Snapshot = snap
	}

	envJSON, err := json.Marshal(envelope)
	if err != nil {
		writeError(conn, fmt.Sprintf("marshal envelope: %v", err))
		return
	}
	if len(envJSON) > util.MaxMessageSize {
		writeError(conn, fmt.Sprintf("stream_envelope_too_large bytes=%d maximum=%d", len(envJSON), util.MaxMessageSize))
		return
	}

	// Frame 1: JSON envelope. The batch has already been encoded and bounded.
	if err := util.WriteWithLength(conn, envJSON); err != nil {
		return
	}

	// Frame 2: binary batch.
	_ = util.WriteWithLength(conn, batchData)
}

func estimateEventMessageBytes(message types.Message) int {
	return 256 + len(message.Topic) + len(message.ProducerID) + len(message.Payload) + len(message.Key) +
		len(message.EventType) + len(message.Metadata) + len(message.EventID) + len(message.PayloadDigest) +
		len(message.TransactionalID) + len(message.TransactionState) + len(message.TransactionMarker) +
		len(message.ControlBatchType) + len(message.ControlBatchKey) + len(message.ControlBatchValue)
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

	ss, err := h.getSnapshot(topicName, partitionID)
	if err != nil {
		return nil, fmt.Sprintf("ERROR: snapshot_store_failed partition=%d reason=%q", partitionID, err.Error())
	}
	if err := ss.ValidateSave(key, version, payload); err != nil {
		return nil, fmt.Sprintf("ERROR: snapshot_save_failed reason=%q", err.Error())
	}
	result := SnapshotResult{Topic: topicName, Key: key, Version: version, Partition: partitionID, Payload: payload}
	if afterSave != nil {
		if err := afterSave(result); err != nil {
			return nil, fmt.Sprintf("ERROR: snapshot_replicate_failed reason=%q", err.Error())
		}
	}
	// Publish locally only after the distributed callback has reached the
	// required replica quorum.
	if err := ss.Save(key, version, payload); err != nil {
		return nil, fmt.Sprintf("ERROR: snapshot_save_failed reason=%q", err.Error())
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

// ListSnapshots returns all latest snapshots for a topic partition.
func (h *Handler) ListSnapshots(topicName string, partitionID int) ([]SnapshotResult, string) {
	return h.ListSnapshotsPage(topicName, partitionID, "", 0)
}

// ListSnapshotsPage returns a key-ordered page for bounded replica catch-up.
func (h *Handler) ListSnapshotsPage(topicName string, partitionID int, afterKey string, limit int) ([]SnapshotResult, string) {
	page, errResp := h.ListSnapshotsPageAtRevision(topicName, partitionID, afterKey, limit, 0)
	return page.Snapshots, errResp
}

func (h *Handler) ListSnapshotsPageAtRevision(topicName string, partitionID int, afterKey string, limit int, expectedRevision uint64) (SnapshotPage, string) {
	t := h.tm.GetTopic(topicName)
	if t == nil {
		return SnapshotPage{}, fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if !t.IsEventSourcing {
		return SnapshotPage{}, fmt.Sprintf("ERROR: event_sourcing_not_enabled topic=%s", topicName)
	}
	if _, err := t.GetPartition(partitionID); err != nil {
		return SnapshotPage{}, fmt.Sprintf("ERROR: partition_lookup_failed partition=%d reason=%q", partitionID, err.Error())
	}
	ss, err := h.getSnapshot(topicName, partitionID)
	if err != nil {
		return SnapshotPage{}, fmt.Sprintf("ERROR: snapshot_store_failed partition=%d reason=%q", partitionID, err.Error())
	}
	records, revision, done, err := ss.ListPageAtRevision(afterKey, limit, expectedRevision)
	if err != nil {
		return SnapshotPage{}, fmt.Sprintf("ERROR: snapshot_list_failed reason=%q", err.Error())
	}
	result := make([]SnapshotResult, 0, len(records))
	for _, rec := range records {
		result = append(result, SnapshotResult{Topic: topicName, Key: rec.Key, Version: rec.Version, Partition: partitionID, Payload: rec.Payload})
	}
	return SnapshotPage{Snapshots: result, Revision: revision, Done: done}, ""
}

// FetchSnapshot returns the latest snapshot for a topic partition and aggregate key.
func (h *Handler) FetchSnapshot(topicName string, partitionID int, key string) (*SnapshotResult, string) {
	if key == "" {
		return nil, "ERROR: missing_key"
	}
	snaps, errResp := h.ListSnapshots(topicName, partitionID)
	if errResp != "" {
		return nil, errResp
	}
	for _, snap := range snaps {
		if snap.Key == key {
			return &snap, ""
		}
	}
	return nil, ""
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
		delete(h.indexedHWM, key)
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

// writeError writes the canonical textual error envelope used by the wire
// transport to produce a typed broker error.
func writeError(conn net.Conn, msg string) {
	msg = strings.TrimSpace(msg)
	if !strings.HasPrefix(msg, "ERROR:") {
		msg = "ERROR: " + msg
	}
	_ = util.WriteWithLength(conn, []byte(msg))
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
