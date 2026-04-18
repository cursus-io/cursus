package sdk

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Event represents a domain event to be appended to a stream.
type Event struct {
	Type          string // e.g., "OrderCreated"
	SchemaVersion uint32 // default 1
	Payload       string // serialized event data
	Metadata      string // optional JSON metadata
}

// StreamEvent is an event read from a stream, with version and offset.
// NOTE: Type, SchemaVersion, and Metadata are not yet populated by ReadStream
// because the current batch wire format (EncodeBatchMessages) does not carry
// event sourcing fields. These will be filled once the batch protocol is extended.
type StreamEvent struct {
	Version       uint64
	Offset        uint64
	Type          string
	SchemaVersion uint32
	Payload       string
	Metadata      string
}

// Snapshot holds a stored aggregate snapshot.
type Snapshot struct {
	Version uint64 `json:"version"`
	Payload string `json:"payload"`
}

// StreamData is the result of reading a stream.
type StreamData struct {
	Snapshot *Snapshot
	Events   []StreamEvent
}

// AppendResult is returned after successfully appending an event.
type AppendResult struct {
	Version   uint64
	Offset    uint64
	Partition int
}

// EventStore provides event sourcing operations against a Cursus broker.
type EventStore struct {
	topic      string
	producerID string
	addr       string
	mu         sync.Mutex
	conn       net.Conn
}

// NewEventStore creates an EventStore for the given topic.
func NewEventStore(addr, topic, producerID string) *EventStore {
	return &EventStore{
		topic:      topic,
		producerID: producerID,
		addr:       addr,
	}
}

// getConn returns an existing or new TCP connection.
func (es *EventStore) getConn() (net.Conn, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	if es.conn != nil {
		return es.conn, nil
	}

	conn, err := net.DialTimeout("tcp", es.addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", es.addr, err)
	}
	es.conn = conn
	return conn, nil
}

// resetConn closes and clears the connection (for retry).
func (es *EventStore) resetConn() {
	es.mu.Lock()
	defer es.mu.Unlock()
	if es.conn != nil {
		_ = es.conn.Close()
		es.conn = nil
	}
}

// sendCommand sends a text command and returns the response string.
func (es *EventStore) sendCommand(cmd string) (string, error) {
	conn, err := es.getConn()
	if err != nil {
		return "", err
	}

	data := EncodeMessage("", cmd)
	if err := WriteWithLength(conn, data); err != nil {
		es.resetConn()
		return "", fmt.Errorf("write: %w", err)
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		es.resetConn()
		return "", fmt.Errorf("read: %w", err)
	}

	return string(resp), nil
}

// CreateTopic creates an event-sourcing-enabled topic if it doesn't exist.
func (es *EventStore) CreateTopic(partitions int) error {
	resp, err := es.sendCommand(fmt.Sprintf("CREATE topic=%s partitions=%d event_sourcing=true", es.topic, partitions))
	if err != nil {
		return err
	}
	if strings.HasPrefix(resp, "ERROR") {
		return fmt.Errorf("broker: %s", resp)
	}
	return nil
}

// Append appends an event to an aggregate stream with optimistic concurrency.
// expectedVersion is the current version of the aggregate (0 for new aggregates).
func (es *EventStore) Append(key string, expectedVersion uint64, event *Event) (*AppendResult, error) {
	sv := event.SchemaVersion
	if sv == 0 {
		sv = 1
	}

	cmd := fmt.Sprintf("APPEND_STREAM topic=%s key=%s expected_version=%d event_type=%s schema_version=%d producerId=%s",
		es.topic, key, expectedVersion, event.Type, sv, es.producerID)
	if event.Metadata != "" {
		cmd += fmt.Sprintf(" metadata=%s", event.Metadata)
	}
	cmd += fmt.Sprintf(" message=%s", event.Payload)

	resp, err := es.sendCommand(cmd)
	if err != nil {
		return nil, err
	}

	if strings.HasPrefix(resp, "ERROR:") {
		return nil, fmt.Errorf("broker: %s", resp)
	}

	return parseAppendResponse(resp), nil
}

// parseAppendResponse parses "OK version=N offset=N partition=N" into AppendResult.
func parseAppendResponse(resp string) *AppendResult {
	result := &AppendResult{}
	parts := strings.Fields(resp)
	for _, p := range parts {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "version":
			result.Version, _ = strconv.ParseUint(kv[1], 10, 64)
		case "offset":
			result.Offset, _ = strconv.ParseUint(kv[1], 10, 64)
		case "partition":
			result.Partition, _ = strconv.Atoi(kv[1])
		}
	}
	return result
}

// ReadStream reads all events for an aggregate, automatically using snapshots.
func (es *EventStore) ReadStream(key string) (*StreamData, error) {
	return es.ReadStreamFrom(key, 0)
}

// ReadStreamFrom reads events starting from a specific version.
// If fromVersion is 0, the broker auto-resolves using snapshots.
func (es *EventStore) ReadStreamFrom(key string, fromVersion uint64) (*StreamData, error) {
	cmd := fmt.Sprintf("READ_STREAM topic=%s key=%s", es.topic, key)
	if fromVersion > 0 {
		cmd += fmt.Sprintf(" from_version=%d", fromVersion)
	}

	conn, err := es.getConn()
	if err != nil {
		return nil, err
	}

	data := EncodeMessage("", cmd)
	if err := WriteWithLength(conn, data); err != nil {
		es.resetConn()
		return nil, fmt.Errorf("write: %w", err)
	}

	// Frame 1: JSON envelope
	envData, err := ReadWithLength(conn)
	if err != nil {
		es.resetConn()
		return nil, fmt.Errorf("read envelope: %w", err)
	}

	var envelope struct {
		Snapshot *Snapshot `json:"snapshot"`
		Count    int       `json:"count"`
	}
	if err := json.Unmarshal(envData, &envelope); err != nil {
		return nil, fmt.Errorf("unmarshal envelope: %w", err)
	}

	// Frame 2: Binary batch
	batchData, err := ReadWithLength(conn)
	if err != nil {
		es.resetConn()
		return nil, fmt.Errorf("read batch: %w", err)
	}

	result := &StreamData{
		Snapshot: envelope.Snapshot,
	}

	if len(batchData) > 0 {
		msgs, _, _, err := DecodeBatchMessages(batchData)
		if err != nil {
			return nil, fmt.Errorf("decode batch: %w", err)
		}
		for _, m := range msgs {
			result.Events = append(result.Events, StreamEvent{
				Version:       m.AggregateVersion,
				Offset:        m.Offset,
				Type:          m.EventType,
				SchemaVersion: m.SchemaVersion,
				Payload:       m.Payload,
				Metadata:      m.Metadata,
			})
		}
	}

	return result, nil
}

// SaveSnapshot saves a snapshot for an aggregate at the given version.
func (es *EventStore) SaveSnapshot(key string, version uint64, payload string) error {
	cmd := fmt.Sprintf("SAVE_SNAPSHOT topic=%s key=%s version=%d message=%s",
		es.topic, key, version, payload)

	resp, err := es.sendCommand(cmd)
	if err != nil {
		return err
	}
	if strings.HasPrefix(resp, "ERROR") {
		return fmt.Errorf("broker: %s", resp)
	}
	return nil
}

// ReadSnapshot reads the latest snapshot for an aggregate.
func (es *EventStore) ReadSnapshot(key string) (*Snapshot, error) {
	resp, err := es.sendCommand(fmt.Sprintf("READ_SNAPSHOT topic=%s key=%s", es.topic, key))
	if err != nil {
		return nil, err
	}
	if resp == "NULL" || strings.Contains(resp, "NOT_FOUND") {
		return nil, nil
	}

	var snap Snapshot
	if err := json.Unmarshal([]byte(resp), &snap); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	return &snap, nil
}

// StreamVersion returns the current version of an aggregate stream.
func (es *EventStore) StreamVersion(key string) (uint64, error) {
	resp, err := es.sendCommand(fmt.Sprintf("STREAM_VERSION topic=%s key=%s", es.topic, key))
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseUint(strings.TrimSpace(resp), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse version: %w", err)
	}
	return v, nil
}

// Close closes the underlying connection.
func (es *EventStore) Close() error {
	es.mu.Lock()
	defer es.mu.Unlock()
	if es.conn != nil {
		err := es.conn.Close()
		es.conn = nil
		return err
	}
	return nil
}
