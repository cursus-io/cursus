package eventsource

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

const (
	defaultHistoryRecords = 100
	maxHistoryRecords     = 1000
	defaultHistoryBytes   = 1 << 20
	maxHistoryBytes       = 4 << 20
)

// HandleReadStreamHistory returns original, committed events only. Unlike
// HandleReadStream it never reads a snapshot and therefore cannot silently
// skip events preceding a snapshot.
func (h *Handler) HandleReadStreamHistory(cmd string, conn net.Conn) {
	args := parseKeyValueArgs(cmd[len("READ_STREAM_HISTORY "):])
	topicName, key := args["topic"], args["key"]
	if topicName == "" || key == "" {
		writeHistoryError(conn, "missing_topic_or_key")
		return
	}
	from, err := historyUint(args, "from_version", 1)
	if err != nil || from == 0 {
		writeHistoryError(conn, "invalid_from_version")
		return
	}
	maxRecords, err := historyBoundedInt(args["max_records"], defaultHistoryRecords, maxHistoryRecords)
	if err != nil {
		writeHistoryError(conn, "invalid_max_records")
		return
	}
	maxBytes, err := historyBoundedInt(args["max_bytes"], defaultHistoryBytes, maxHistoryBytes)
	if err != nil {
		writeHistoryError(conn, "invalid_max_bytes")
		return
	}
	t := h.tm.GetTopic(topicName)
	if t == nil || !t.IsEventSourcing {
		writeHistoryError(conn, "event_sourcing_not_available")
		return
	}
	partition := t.GetPartitionForMessage(types.Message{Key: key})
	idx, err := h.getIndex(topicName, partition)
	if err != nil {
		writeHistoryError(conn, "stream_index_failed")
		return
	}
	head := idx.GetVersion(key)
	to := head
	if supplied := args["to_version"]; supplied != "" {
		to, err = strconv.ParseUint(supplied, 10, 64)
		if err != nil || to < from || to > head {
			writeHistoryError(conn, "invalid_to_version")
			return
		}
	}
	entries, err := idx.LookupRange(key, from, to, maxRecords)
	if err != nil {
		writeHistoryError(conn, "index_lookup_failed")
		return
	}
	p, err := t.GetPartition(partition)
	if err != nil {
		writeHistoryError(conn, "partition_not_found")
		return
	}
	messages := make([]types.Message, 0, len(entries))
	completeness := "complete"
	next := uint64(0)
	used := 0
	expected := from
	for _, entry := range entries {
		if entry.AggregateVersion != expected {
			completeness = "partial"
			next = expected
			break
		}
		batch, readErr := p.ReadCommittedRange(entry.Offset, entry.Offset+1, 1)
		if readErr != nil || len(batch) != 1 || batch[0].Key != key || batch[0].AggregateVersion != entry.AggregateVersion || batch[0].Offset != entry.Offset {
			completeness = "partial"
			next = entry.AggregateVersion + 1
			break
		}
		message := batch[0]
		size := len(message.Payload) + len(message.Key) + len(message.Metadata) + len(message.EventType) + 64
		if len(messages) == 0 && size > maxBytes {
			writeHistoryError(conn, fmt.Sprintf("record_too_large version=%d offset=%d", message.AggregateVersion, message.Offset))
			return
		}
		if used+size > maxBytes {
			next = message.AggregateVersion
			break
		}
		if message.AggregateVersion != expected {
			completeness = "partial"
			next = expected
			break
		}
		messages = append(messages, message)
		used += size
		expected++
		next = expected
	}
	hasMore := next != 0 && next <= to && (len(messages) == maxRecords || next <= to)
	if len(entries) == 0 {
		if from <= to {
			completeness = "unknown"
		}
		next = 0
		hasMore = false
	} else if completeness == "complete" && next != 0 && next <= to && len(entries) < maxRecords {
		// A fixed range ended before its advertised upper bound. The index can no
		// longer prove continuity (for example after retention), so never describe
		// this as a complete history or offer a cursor that repeats the gap.
		completeness = "partial"
		hasMore = false
	} else if completeness != "complete" {
		hasMore = false
	}
	envelope := struct {
		Status       string `json:"status"`
		Topic        string `json:"topic"`
		Key          string `json:"key"`
		Partition    int    `json:"partition"`
		NextVersion  uint64 `json:"next_version,omitempty"`
		HeadVersion  uint64 `json:"head_version"`
		Completeness string `json:"completeness"`
		HasMore      bool   `json:"has_more"`
	}{"OK", topicName, key, partition, next, head, completeness, hasMore}
	data, err := json.Marshal(envelope)
	if err != nil {
		writeHistoryError(conn, "marshal_failed")
		return
	}
	if err := util.WriteWithLength(conn, data); err != nil {
		return
	}
	batch, err := util.EncodeBatchMessages(topicName, partition, "1", false, messages)
	if err != nil {
		return
	}
	_ = util.WriteWithLength(conn, batch)
}

func historyUint(args map[string]string, name string, fallback uint64) (uint64, error) {
	if args[name] == "" {
		return fallback, nil
	}
	return strconv.ParseUint(args[name], 10, 64)
}

func historyBoundedInt(value string, fallback, maximum int) (int, error) {
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 || parsed > maximum {
		return 0, fmt.Errorf("invalid")
	}
	return parsed, nil
}

func writeHistoryError(conn net.Conn, message string) {
	data, _ := json.Marshal(map[string]string{"status": "ERROR", "error": message})
	_ = util.WriteWithLength(conn, data)
}
