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

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// Handler processes event sourcing commands (APPEND_STREAM, READ_STREAM, etc.).
type Handler struct {
	replicaIndexGenerations map[string]disk.ContentGeneration
	tm                      *topic.TopicManager

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
// getIndex is retained for package tests. Production consumers keep their view
// until all use of the returned cache object has finished.
func (h *Handler) getIndex(topicName string, partitionID int) (idx *StreamIndex, err error) {
	err = h.withReadView(topicName, partitionID, func(v *topic.PartitionReadView) error {
		idx, err = h.getIndexWithView(topicName, partitionID, v)
		return err
	})
	return
}

func (h *Handler) withReadView(name string, partition int, fn func(*topic.PartitionReadView) error) error {
	t := h.tm.GetTopic(name)
	if t == nil {
		return fmt.Errorf("topic_not_found topic=%s", name)
	}
	if !t.IsEventSourcing {
		return fmt.Errorf("event_sourcing_not_enabled topic=%s", name)
	}
	p, err := t.GetPartition(partition)
	if err != nil {
		return err
	}
	return p.WithReadView(fn)
}

func (h *Handler) getIndexWithView(topicName string, partitionID int, view *topic.PartitionReadView) (*StreamIndex, error) {
	if err := view.ValidateIdentity(topicName, partitionID); err != nil {
		return nil, err
	}
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
	stableTail := view.LastStableOffset()
	if err := h.recoverIndexWithView(topicName, partitionID, idx, view, stableTail); err != nil {
		_ = idx.Close()
		return nil, err
	}
	h.indexes[key] = idx
	// Recovery reads only the stable committed prefix. Never claim that an
	// unresolved transactional tail has been indexed merely because it is in
	// the partition HWM.
	h.indexedHWM[key] = stableTail
	return idx, nil
}

// getSnapshot returns the SnapshotStore for the given topic and partition, creating it lazily.
func (h *Handler) getSnapshotWithView(topicName string, partitionID int, view *topic.PartitionReadView) (*SnapshotStore, error) {
	if err := view.ValidateIdentity(topicName, partitionID); err != nil {
		return nil, err
	}
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
	return h.withReadView(topicName, partitionID, func(v *topic.PartitionReadView) error {
		_, err := h.getIndexWithView(topicName, partitionID, v)
		return err
	})
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
	var version uint64
	err := h.withReadView(topicName, partition, func(v *topic.PartitionReadView) error {
		idx, err := h.getIndexWithView(topicName, partition, v)
		if err != nil {
			return err
		}
		version = idx.GetVersion(key)
		return nil
	})
	return version, partition, err
}

// IndexCommittedToHWM advances the derived stream index only through records
// visible below the partition's stable committed boundary.
func (h *Handler) IndexCommittedToHWM(topicName string, partitionID int, targetHWM uint64) error {
	return h.withReadView(topicName, partitionID, func(v *topic.PartitionReadView) error {
		h.indexSyncMu.Lock()
		defer h.indexSyncMu.Unlock()
		idx, err := h.getIndexWithView(topicName, partitionID, v)
		if err != nil {
			return err
		}
		key := topicName + ":" + strconv.Itoa(partitionID)
		h.mu.RLock()
		start := h.indexedHWM[key]
		h.mu.RUnlock()
		end := min(targetHWM, v.LastStableOffset())
		if end <= start {
			return nil
		}
		if err := h.replayIndex(idx, v, start, end); err != nil {
			h.mu.Lock()
			delete(h.replicaIndexGenerations, key)
			h.mu.Unlock()
			return err
		}
		h.mu.Lock()
		h.indexedHWM[key] = end
		h.mu.Unlock()
		return nil
	})
}

func (h *Handler) RecoverIndexFromLog(topicName string, partitionID int, idx *StreamIndex) error {
	return h.withReadView(topicName, partitionID, func(v *topic.PartitionReadView) error {
		h.indexSyncMu.Lock()
		defer h.indexSyncMu.Unlock()
		h.mu.Lock()
		delete(h.replicaIndexGenerations, topicName+":"+strconv.Itoa(partitionID))
		h.mu.Unlock()
		return h.recoverIndexWithView(topicName, partitionID, idx, v, v.LastStableOffset())
	})
}

func (h *Handler) recoverIndexWithView(topicName string, partitionID int, idx *StreamIndex, view *topic.PartitionReadView, latest uint64) error {
	if err := idx.Reset(); err != nil {
		return fmt.Errorf("reset stream index: %w", err)
	}
	first := view.FirstOffset()
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
	return h.replayIndex(idx, view, first, latest)
}

func (h *Handler) replayIndex(idx *StreamIndex, view *topic.PartitionReadView, first, latest uint64) error {
	const batchSize = 256
	for offset := first; offset < latest; {
		msgs, _, next, err := view.ReadCommittedPage(offset, batchSize, 0, true)
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

// AppendStream owns replacement exclusion for standalone callers. Callback-bearing
// callers must use AppendStreamWithControllerFence with the documented locks.
func (h *Handler) AppendStream(cmd string, opts AppendOptions) (*AppendResult, string) {
	if opts.AfterAppend != nil || opts.AfterCommit != nil {
		return nil, "ERROR: append_callbacks_require_controller_fence"
	}
	return h.appendStream(cmd, opts, false)
}

// AppendStreamWithControllerFence requires the caller to hold the topic lifecycle,
// partition write and BeginReplicationMutation locks through all callbacks. This
// avoids recursive reconcile locking while a bootstrap writer is waiting.
func (h *Handler) AppendStreamWithControllerFence(cmd string, opts AppendOptions) (*AppendResult, string) {
	return h.appendStream(cmd, opts, true)
}

func (h *Handler) appendStream(cmd string, opts AppendOptions, controllerFenced bool) (*AppendResult, string) {
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

	if !controllerFenced {
		release := p.BeginReplicationMutation()
		defer release()
	}
	var idx *StreamIndex
	var duplicate *AppendResult
	err = p.WithReadView(func(view *topic.PartitionReadView) error {
		var errResp string
		duplicate, errResp = func() (*AppendResult, string) {
			var err error
			idx, err = h.getIndexWithView(topicName, partitionID, view)
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
					existing, readErr := view.ReadCommittedRange(entry.Offset, entry.Offset+1, 1)
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
				seen, scanErr := aggregateEventIDExists(view, msg.EventID)
				if scanErr != nil {
					return nil, fmt.Sprintf("ERROR: aggregate_proof_unavailable reason=%q", scanErr.Error())
				}
				if seen {
					return nil, fmt.Sprintf("ERROR: aggregate_identity_conflict event_id=%s", msg.EventID)
				}
			}

			return nil, ""
		}()
		if errResp != "" {
			return errors.New(errResp)
		}
		return nil
	})
	if err != nil {
		if strings.HasPrefix(err.Error(), "ERROR:") {
			return nil, err.Error()
		}
		return nil, "ERROR: " + err.Error()
	}
	if duplicate != nil {
		return duplicate, ""
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
	appendErr := err
	recoveredCommit := false
	postErr := p.WithReadView(func(view *topic.PartitionReadView) error {
		if appendErr != nil {
			h.indexSyncMu.Lock()
			defer h.indexSyncMu.Unlock()
			if recoveryErr := h.recoverIndexWithView(topicName, partitionID, idx, view, view.LastStableOffset()); recoveryErr != nil {
				return fmt.Errorf("append_stream_failed reason=%q recovery_reason=%q", appendErr.Error(), recoveryErr.Error())
			}
			if idx.GetVersion(key) != expectedVersion {
				return fmt.Errorf("append_stream_failed reason=%q", appendErr.Error())
			}
			recoveredCommit = true
		}
		if !ok && !recoveredCommit {
			return fmt.Errorf("version_conflict current=%d expected=%d", current, expectedVersion)
		}
		committed := view.LastStableOffset()
		indexKey := topicName + ":" + strconv.Itoa(partitionID)
		h.mu.Lock()
		if h.indexedHWM[indexKey] < committed {
			h.indexedHWM[indexKey] = committed
		}
		h.mu.Unlock()
		return nil
	})
	if postErr != nil {
		return nil, "ERROR: " + postErr.Error()
	}

	return &AppendResult{Topic: topicName, Key: key, Version: expectedVersion, Offset: appendedOffset, Partition: partitionID, Message: appendedMsg}, ""
}

// aggregateEventIDExists consults the committed retained log rather than an
// in-memory index so an event-id uniqueness decision survives broker restart.
func aggregateEventIDExists(view *topic.PartitionReadView, eventID string) (bool, error) {
	limit := view.LastStableOffset()
	for offset := view.FirstOffset(); offset < limit; {
		messages, _, next, err := view.ReadCommittedPage(offset, 256, 0, true)
		if err != nil {
			return false, err
		}
		for _, existing := range messages {
			if existing.Offset >= limit {
				return false, nil
			}
			if existing.EventID == eventID {
				return true, nil
			}
		}
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

	var snap *SnapshotData
	var msgs []types.Message
	var moreByCount, moreByBytes bool
	err := h.withReadView(topicName, partitionID, func(view *topic.PartitionReadView) error {
		idx, err := h.getIndexWithView(topicName, partitionID, view)
		if err != nil {
			return err
		}

		// Check snapshot: if one exists with version >= fromVersion, use it as base.
		ss, err := h.getSnapshotWithView(topicName, partitionID, view)
		if err != nil {
			return err
		}

		snap, err = ss.Read(key)
		if err != nil {
			return fmt.Errorf("snapshot_read_failed reason=%q", err.Error())
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
			maxBytes = util.MaxMessageSize
			maxEvents = maxBytes / 256
			entries, err = idx.LookupRange(key, actualFromVersion, currentVersion, maxEvents+1)
			if len(entries) > maxEvents {
				return fmt.Errorf("stream_requires_pagination bytes>%d", maxBytes)
			}
		}
		if err != nil {
			return fmt.Errorf("index_lookup_failed reason=%q", err.Error())
		}

		moreByCount = paginated && len(entries) > maxEvents
		if moreByCount {
			entries = entries[:maxEvents]
		}

		// Collect one bounded page before sending its success envelope.
		msgs = make([]types.Message, 0, len(entries))
		usedBytes := 0
		moreByBytes = false
		for _, entry := range entries {
			batch, err := view.ReadCommittedRange(entry.Offset, entry.Offset+1, 1)
			if err != nil {
				return fmt.Errorf("partition_read_failed offset=%d reason=%q", entry.Offset, err.Error())
			}
			if len(batch) == 0 || batch[0].Key != key || batch[0].AggregateVersion != entry.AggregateVersion {
				return fmt.Errorf("stream_index_record_mismatch offset=%d", entry.Offset)
			}
			size := estimateEventMessageBytes(batch[0])
			if usedBytes+size > maxBytes {
				if !paginated {
					return fmt.Errorf("stream_requires_pagination bytes>%d", maxBytes)
				}
				if len(msgs) == 0 {
					return fmt.Errorf("event_exceeds_page_budget version=%d bytes=%d maximum=%d", entry.AggregateVersion, size, maxBytes)
				}
				moreByBytes = true
				break
			}
			msgs = append(msgs, detachEventMessage(batch[0]))
			usedBytes += size
		}
		return nil
	})
	if err != nil {
		writeError(conn, err.Error())
		return
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

	result := SnapshotResult{Topic: topicName, Key: key, Version: version, Partition: partitionID, Payload: payload}
	var generation uint64
	validate := func(view *topic.PartitionReadView, save bool) error {
		idx, err := h.getIndexWithView(topicName, partitionID, view)
		if err != nil {
			return err
		}
		currentVersion := idx.GetVersion(key)
		if version > currentVersion {
			return fmt.Errorf("snapshot_version_exceeds_stream version=%d current=%d", version, currentVersion)
		}
		ss, err := h.getSnapshotWithView(topicName, partitionID, view)
		if err != nil {
			return err
		}
		if save {
			return ss.Save(key, version, payload)
		}
		return ss.ValidateSave(key, version, payload)
	}
	err = h.withReadView(topicName, partitionID, func(view *topic.PartitionReadView) error {
		generation = view.Generation()
		return validate(view, false)
	})
	if err != nil {
		return nil, "ERROR: " + err.Error()
	}
	if afterSave != nil {
		if err := afterSave(result); err != nil {
			return nil, fmt.Sprintf("ERROR: snapshot_replicate_failed reason=%q", err.Error())
		}
	}
	err = h.withReadView(topicName, partitionID, func(view *topic.PartitionReadView) error {
		if generation != view.Generation() {
			return fmt.Errorf("snapshot_generation_changed retryable=true")
		}
		return validate(view, true)
	})
	if err != nil {
		return nil, "ERROR: " + err.Error()
	}
	return &result, ""

}

func (h *Handler) SaveSnapshotReplica(result SnapshotResult) string {
	err := h.withReadView(result.Topic, result.Partition, func(view *topic.PartitionReadView) error {
		ss, err := h.getSnapshotWithView(result.Topic, result.Partition, view)
		if err != nil {
			return err
		}
		return ss.Save(result.Key, result.Version, result.Payload)
	})
	if err != nil {
		return "ERROR: snapshot_save_failed reason=" + strconv.Quote(err.Error())
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
	var page SnapshotPage
	err := h.withReadView(topicName, partitionID, func(view *topic.PartitionReadView) error {
		ss, err := h.getSnapshotWithView(topicName, partitionID, view)
		if err != nil {
			return err
		}
		records, revision, done, err := ss.ListPageAtRevision(afterKey, limit, expectedRevision)
		if err != nil {
			return err
		}
		result := make([]SnapshotResult, 0, len(records))
		for _, rec := range records {
			result = append(result, SnapshotResult{Topic: topicName, Key: rec.Key, Version: rec.Version, Partition: partitionID, Payload: rec.Payload})
		}
		page = SnapshotPage{Snapshots: result, Revision: revision, Done: done}
		return nil
	})
	if err != nil {
		return SnapshotPage{}, "ERROR: snapshot_list_failed reason=" + strconv.Quote(err.Error())
	}
	return page, ""
}

// FetchSnapshot returns the latest snapshot for a topic partition and aggregate key.
func (h *Handler) FetchSnapshot(topicName string, partitionID int, key string) (*SnapshotResult, string) {
	if key == "" {
		return nil, "ERROR: missing_key"
	}
	var result *SnapshotResult
	err := h.withReadView(topicName, partitionID, func(view *topic.PartitionReadView) error {
		ss, err := h.getSnapshotWithView(topicName, partitionID, view)
		if err != nil {
			return err
		}
		snap, err := ss.Read(key)
		if err != nil {
			return err
		}
		if snap != nil {
			result = &SnapshotResult{Topic: topicName, Partition: partitionID, Key: key, Version: snap.Version, Payload: snap.Payload}
		}
		return nil
	})
	if err != nil {
		return nil, "ERROR: snapshot_read_failed reason=" + strconv.Quote(err.Error())
	}
	return result, ""
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

	result, errResp := h.FetchSnapshot(topicName, partitionID, key)
	if errResp != "" {
		return errResp
	}
	var snap *SnapshotData
	if result != nil {
		snap = &SnapshotData{Version: result.Version, Payload: result.Payload}
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

	version, _, err := h.CurrentVersion(topicName, key)
	if err != nil {
		return "ERROR: " + err.Error()
	}
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
		delete(h.replicaIndexGenerations, key)
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

// Detach mutable wire fields before releasing the partition generation view.
func detachEventMessage(message types.Message) types.Message {
	message.ControlBatchKey = append([]byte(nil), message.ControlBatchKey...)
	message.ControlBatchValue = append([]byte(nil), message.ControlBatchValue...)
	return message
}
