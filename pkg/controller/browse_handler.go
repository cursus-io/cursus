package controller

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

const (
	defaultBrowseRecords = 100
	maxBrowseRecords     = 1000
	defaultBrowseBytes   = 1 << 20
	maxBrowseBytes       = 4 << 20
)

// HandleBrowseMessagesCommand reads a bounded, committed-only page directly
// from a partition. It deliberately never touches the coordinator, joins a
// group, or commits an offset.
func (ch *CommandHandler) HandleBrowseMessagesCommand(conn net.Conn, cmd string) {
	args := parseKeyValueArgs(cmd[len("BROWSE_MESSAGES "):])
	topicName := args["topic"]
	if topicName == "" {
		writeBrowseError(conn, "missing_topic")
		return
	}
	partition, err := requiredNonNegativeInt(args, "partition")
	if err != nil {
		writeBrowseError(conn, err.Error())
		return
	}
	from, err := requiredUint(args, "from_offset")
	if err != nil {
		writeBrowseError(conn, err.Error())
		return
	}
	maxRecords, err := boundedInt(args["max_records"], defaultBrowseRecords, maxBrowseRecords, "max_records")
	if err != nil {
		writeBrowseError(conn, err.Error())
		return
	}
	maxBytes, err := boundedInt(args["max_bytes"], defaultBrowseBytes, maxBrowseBytes, "max_bytes")
	if err != nil {
		writeBrowseError(conn, err.Error())
		return
	}
	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		writeBrowseError(conn, fmt.Sprintf("topic_not_found topic=%s", topicName))
		return
	}
	p, err := t.GetPartition(partition)
	if err != nil {
		writeBrowseError(conn, fmt.Sprintf("partition_not_found partition=%d", partition))
		return
	}
	readableEnd := p.LastStableOffset()
	if requested := args["to_offset"]; requested != "" {
		to, parseErr := strconv.ParseUint(requested, 10, 64)
		if parseErr != nil || to < from {
			writeBrowseError(conn, "invalid_to_offset")
			return
		}
		if to < readableEnd {
			readableEnd = to
		}
	}
	messages, err := p.ReadCommittedRange(from, readableEnd, maxRecords)
	if err != nil {
		writeBrowseError(conn, fmt.Sprintf("range_error requested=%d earliest=%d latest=%d", from, p.FirstOffset(), readableEnd))
		return
	}
	selected := make([]types.Message, 0, len(messages))
	used := 0
	for _, message := range messages {
		size := browseMessageSize(message)
		if len(selected) == 0 && size > maxBytes {
			writeBrowseError(conn, fmt.Sprintf("record_too_large offset=%d size=%d max_bytes=%d", message.Offset, size, maxBytes))
			return
		}
		if used+size > maxBytes {
			break
		}
		selected = append(selected, message)
		used += size
	}
	next := readableEnd
	if len(selected) > 0 && (len(selected) < len(messages) || len(messages) == maxRecords) {
		next = selected[len(selected)-1].Offset + 1
	}
	hasMore := next < readableEnd
	envelope := struct {
		Status            string `json:"status"`
		Topic             string `json:"topic"`
		Partition         int    `json:"partition"`
		NextOffset        uint64 `json:"next_offset"`
		EarliestOffset    uint64 `json:"earliest_offset"`
		ReadableEndOffset uint64 `json:"readable_end_offset"`
		HasMore           bool   `json:"has_more"`
	}{"OK", topicName, partition, next, p.FirstOffset(), readableEnd, hasMore}
	data, err := json.Marshal(envelope)
	if err != nil {
		writeBrowseError(conn, "marshal_failed")
		return
	}
	if err := util.WriteWithLength(conn, data); err != nil {
		return
	}
	batch, err := util.EncodeBatchMessages(topicName, partition, "1", false, selected)
	if err != nil {
		return
	}
	_ = util.WriteWithLength(conn, batch)
}

func writeBrowseError(conn net.Conn, message string) {
	data, _ := json.Marshal(map[string]string{"status": "ERROR", "error": message})
	_ = util.WriteWithLength(conn, data)
}

func requiredNonNegativeInt(args map[string]string, name string) (int, error) {
	value, ok := args[name]
	if !ok || value == "" {
		return 0, fmt.Errorf("missing_%s", name)
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("invalid_%s", name)
	}
	return parsed, nil
}

func requiredUint(args map[string]string, name string) (uint64, error) {
	value := args[name]
	if value == "" {
		return 0, fmt.Errorf("missing_%s", name)
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid_%s", name)
	}
	return parsed, nil
}

func boundedInt(value string, fallback, maximum int, name string) (int, error) {
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 || parsed > maximum {
		return 0, fmt.Errorf("invalid_%s", name)
	}
	return parsed, nil
}

func browseMessageSize(message types.Message) int {
	return len(message.Payload) + len(message.Key) + len(message.Metadata) + len(message.EventType) + 64
}
