package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	wireprotocol "github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/wire"
)

// BrowseRequest bounds one committed-only partition read. It never creates a
// consumer group or changes an offset.
type BrowseRequest struct {
	Topic      string
	Partition  int
	FromOffset uint64
	ToOffset   *uint64
	MaxRecords int
	MaxBytes   int
}

type BrowseResult struct {
	Messages          []AdminMessage
	NextOffset        uint64
	EarliestOffset    uint64
	ReadableEndOffset uint64
	HasMore           bool
}

// HistoryRequest bounds a snapshot-free aggregate event read.
type HistoryRequest struct {
	Topic       string
	Key         string
	FromVersion uint64
	ToVersion   *uint64
	MaxRecords  int
	MaxBytes    int
}

type HistoryCompleteness string

const (
	HistoryComplete HistoryCompleteness = "complete"
	HistoryPartial  HistoryCompleteness = "partial"
	HistoryUnknown  HistoryCompleteness = "unknown"
)

type HistoryResult struct {
	Events       []StreamEvent
	NextVersion  uint64
	HeadVersion  uint64
	Completeness HistoryCompleteness
	HasMore      bool
}

type GroupOffset struct {
	Partition int
	Offset    uint64
	Earliest  uint64
	Latest    uint64
	Lag       uint64
}

// AdminMessage is the read-only representation of a broker record exposed by
// observation APIs.
type AdminMessage struct {
	Offset           uint64
	Key              string
	Payload          string
	Metadata         string
	EventType        string
	SchemaVersion    uint32
	AggregateVersion uint64
}

// ProtocolNegotiation describes the capability set requested for one
// short-lived observation connection.
type ProtocolNegotiation struct {
	Version         int
	Features        []string
	RequireFeatures bool
}

type NegotiatedProtocol struct {
	Version     int
	Enabled     []string
	Unsupported []string
}

// NegotiateProtocol queries the command surface over an already negotiated
// Wire v2 connection. It does not send the removed application NEGOTIATE command.
// Version, when supplied, must be the Wire version (2). Feature selection is
// local and does not change server-side connection state or authorization.
func NegotiateProtocol(conn net.Conn, request ProtocolNegotiation) (*NegotiatedProtocol, error) {
	if conn == nil {
		return nil, fmt.Errorf("protocol negotiation connection is nil")
	}
	version := request.Version
	if version == 0 {
		version = int(wire.ProtocolVersion)
	}
	if version != int(wire.ProtocolVersion) {
		return nil, fmt.Errorf("unsupported Wire protocol version %d", version)
	}
	features := append([]string(nil), request.Features...)
	if len(features) == 0 {
		features = []string{"*"}
	}
	if len(features) > 1 {
		seen := make(map[string]struct{}, len(features))
		for _, feature := range features {
			if feature == "" || feature == "*" {
				return nil, fmt.Errorf("invalid protocol feature request")
			}
			seen[feature] = struct{}{}
		}
		features = features[:0]
		for feature := range seen {
			features = append(features, feature)
		}
		sort.Strings(features)
	}
	if _, _, err := wireprotocol.ParseFeatureRequest(strings.Join(features, ",")); err != nil {
		return nil, err
	}
	if err := WriteWithLength(conn, []byte("HELP")); err != nil {
		return nil, fmt.Errorf("send capability query: %w", err)
	}
	response, err := ReadWithLength(conn)
	if err != nil {
		return nil, fmt.Errorf("read capability query: %w", err)
	}
	text := strings.TrimSpace(string(response))
	if brokerErr, ok := ParseBrokerError(text); ok {
		return nil, brokerErr
	}
	fields, err := parseOKResponse(text)
	if err != nil {
		return nil, fmt.Errorf("invalid capability response: %w", err)
	}
	if fields["commands"] == "" {
		return nil, fmt.Errorf("capability response is missing commands")
	}
	available := wireV2CommandFeatures(splitObservationList(fields["commands"]))
	result := &NegotiatedProtocol{Version: version}
	if len(features) == 1 && features[0] == "*" {
		features = available
	}
	for _, feature := range features {
		index := sort.SearchStrings(available, feature)
		if index < len(available) && available[index] == feature {
			result.Enabled = append(result.Enabled, feature)
		} else {
			result.Unsupported = append(result.Unsupported, feature)
		}
	}
	if request.RequireFeatures && len(result.Unsupported) != 0 {
		return nil, fmt.Errorf("requested protocol features were not fully enabled")
	}
	return result, nil
}

// Wire v2 fixes the semantics of these commands; HELP determines which of
// those command families this endpoint exposes. Permissions are checked by
// each operation, not inferred from this read-only capability listing.
func wireV2CommandFeatures(commands []string) []string {
	present := make(map[string]bool, len(commands))
	for _, command := range commands {
		present[command] = true
	}
	families := map[string][]string{
		"browse_messages_v1":              {"BROWSE_MESSAGES"},
		"stream_history_v1":               {"READ_STREAM_HISTORY"},
		"event_sourcing_v1":               {"APPEND_STREAM", "READ_STREAM", "SAVE_SNAPSHOT", "READ_SNAPSHOT"},
		"idempotent_producer_v1":          {"PUBLISH"},
		"offset_resume_v1":                {"COMMIT_OFFSET", "FETCH_OFFSET"},
		"stream_control_v1":               {"STREAM"},
		"structured_errors_v1":            {},
		"topic_compaction_v1":             {"CREATE", "ALTER_TOPIC_CONFIG"},
		"consumer_group_subscriptions_v1": {"REGISTER_GROUP", "JOIN_GROUP", "SYNC_GROUP"},
		"transactional_processing_v1":     {"INIT_PRODUCER_ID", "BEGIN_TXN", "TXN_PUBLISH", "TXN_APPEND_STREAM", "SEND_OFFSETS_TO_TXN", "END_TXN"},
	}
	var features []string
	for feature, required := range families {
		supported := true
		for _, command := range required {
			supported = supported && present[command]
		}
		if supported {
			features = append(features, feature)
		}
	}
	sort.Strings(features)
	return features
}

// Capabilities reports the Wire v2 command families exposed by HELP without
// mutating broker state or changing connection capabilities.
func (c *AdminClient) Capabilities(ctx context.Context) (*NegotiatedProtocol, error) {
	return withObservationConnection(ctx, c, func(conn net.Conn) (*NegotiatedProtocol, error) {
		return NegotiateProtocol(conn, ProtocolNegotiation{Features: []string{"*"}})
	})
}

func (c *AdminClient) ListTopics(ctx context.Context) ([]string, error) {
	response, err := c.execute(ctx, "LIST", true)
	if err != nil {
		return nil, err
	}
	fields, err := parseOKResponse(response)
	if err != nil {
		return nil, err
	}
	return splitObservationList(fields["topics"]), nil
}

func (c *AdminClient) ListGroups(ctx context.Context) ([]string, error) {
	response, err := c.execute(ctx, "LIST_GROUPS", true)
	if err != nil {
		return nil, err
	}
	fields, err := parseOKResponse(response)
	if err != nil {
		return nil, err
	}
	return splitObservationList(fields["groups"]), nil
}

func (c *AdminClient) ListOffsets(ctx context.Context, topic string) ([]PartitionOffsetRange, error) {
	if err := validateSDKTopicName(topic); err != nil {
		return nil, err
	}
	response, err := c.execute(ctx, "LIST_OFFSETS topic="+topic, true)
	if err != nil {
		return nil, err
	}
	return parseListOffsetsResponse(response)
}

func (c *AdminClient) GroupOffsets(ctx context.Context, group, topic string) ([]GroupOffset, error) {
	if !validObservationIdentifier(group) {
		return nil, fmt.Errorf("invalid group")
	}
	ranges, err := c.ListOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	result := make([]GroupOffset, 0, len(ranges))
	for _, item := range ranges {
		response, err := c.execute(ctx, fmt.Sprintf("FETCH_OFFSET topic=%s partition=%d group=%s", topic, item.Partition, group), true)
		if err != nil {
			return nil, err
		}
		fields, err := parseOKResponse(response)
		if err != nil {
			return nil, err
		}
		offset, err := strconv.ParseUint(fields["offset"], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid group offset")
		}
		lag := uint64(0)
		if item.HWM > offset {
			lag = item.HWM - offset
		}
		result = append(result, GroupOffset{Partition: item.Partition, Offset: offset, Earliest: item.Earliest, Latest: item.HWM, Lag: lag})
	}
	return result, nil
}

func (c *AdminClient) BrowseMessages(ctx context.Context, request BrowseRequest) (*BrowseResult, error) {
	if err := validateSDKTopicName(request.Topic); err != nil {
		return nil, err
	}
	if request.Partition < 0 || request.MaxRecords <= 0 || request.MaxBytes <= 0 || (request.ToOffset != nil && *request.ToOffset < request.FromOffset) {
		return nil, fmt.Errorf("invalid browse request")
	}
	command := fmt.Sprintf("BROWSE_MESSAGES topic=%s partition=%d from_offset=%d max_records=%d max_bytes=%d", request.Topic, request.Partition, request.FromOffset, request.MaxRecords, request.MaxBytes)
	if request.ToOffset != nil {
		command += fmt.Sprintf(" to_offset=%d", *request.ToOffset)
	}
	first, second, err := c.observationFrames(ctx, command)
	if err != nil {
		return nil, err
	}
	var envelope struct {
		Status            string `json:"status"`
		Error             string `json:"error"`
		NextOffset        uint64 `json:"next_offset"`
		EarliestOffset    uint64 `json:"earliest_offset"`
		ReadableEndOffset uint64 `json:"readable_end_offset"`
		HasMore           bool   `json:"has_more"`
	}
	if err := json.Unmarshal(first, &envelope); err != nil {
		return nil, fmt.Errorf("decode browse envelope: %w", err)
	}
	if envelope.Status != "OK" {
		return nil, observationFrameError(envelope.Error)
	}
	messages, _, _, err := DecodeBatchMessages(second)
	if err != nil {
		return nil, fmt.Errorf("decode browse batch: %w", err)
	}
	return &BrowseResult{Messages: adminMessages(messages), NextOffset: envelope.NextOffset, EarliestOffset: envelope.EarliestOffset, ReadableEndOffset: envelope.ReadableEndOffset, HasMore: envelope.HasMore}, nil
}

func (c *AdminClient) ReadStreamHistory(ctx context.Context, request HistoryRequest) (*HistoryResult, error) {
	if err := validateSDKTopicName(request.Topic); err != nil {
		return nil, err
	}
	if !validObservationIdentifier(request.Key) || request.FromVersion == 0 || request.MaxRecords <= 0 || request.MaxBytes <= 0 || (request.ToVersion != nil && *request.ToVersion < request.FromVersion) {
		return nil, fmt.Errorf("invalid history request")
	}
	command := fmt.Sprintf("READ_STREAM_HISTORY topic=%s key=%s from_version=%d max_records=%d max_bytes=%d", request.Topic, request.Key, request.FromVersion, request.MaxRecords, request.MaxBytes)
	if request.ToVersion != nil {
		command += fmt.Sprintf(" to_version=%d", *request.ToVersion)
	}
	first, second, err := c.observationFrames(ctx, command)
	if err != nil {
		return nil, err
	}
	var envelope struct {
		Status       string              `json:"status"`
		Error        string              `json:"error"`
		NextVersion  uint64              `json:"next_version"`
		HeadVersion  uint64              `json:"head_version"`
		Completeness HistoryCompleteness `json:"completeness"`
		HasMore      bool                `json:"has_more"`
	}
	if err := json.Unmarshal(first, &envelope); err != nil {
		return nil, fmt.Errorf("decode history envelope: %w", err)
	}
	if envelope.Status != "OK" {
		return nil, observationFrameError(envelope.Error)
	}
	messages, _, _, err := DecodeBatchMessages(second)
	if err != nil {
		return nil, fmt.Errorf("decode history batch: %w", err)
	}
	events := make([]StreamEvent, 0, len(messages))
	for _, message := range messages {
		events = append(events, StreamEvent{Version: message.AggregateVersion, Offset: message.Offset, Type: message.EventType, SchemaVersion: message.SchemaVersion, Payload: message.Payload, Metadata: message.Metadata})
	}
	return &HistoryResult{Events: events, NextVersion: envelope.NextVersion, HeadVersion: envelope.HeadVersion, Completeness: envelope.Completeness, HasMore: envelope.HasMore}, nil
}

func (c *AdminClient) ReadSnapshot(ctx context.Context, topic, key string) (*Snapshot, error) {
	if err := validateSDKTopicName(topic); err != nil {
		return nil, err
	}
	if !validObservationIdentifier(key) {
		return nil, fmt.Errorf("invalid snapshot key")
	}
	response, err := c.execute(ctx, fmt.Sprintf("READ_SNAPSHOT topic=%s key=%s", topic, key), true)
	if err != nil {
		return nil, err
	}
	payload, found, err := parseSnapshotResponse(response)
	if err != nil || !found {
		return nil, err
	}
	var snapshot Snapshot
	if err := json.Unmarshal([]byte(payload), &snapshot); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	return &snapshot, nil
}

func (c *AdminClient) observationFrames(ctx context.Context, command string) ([]byte, []byte, error) {
	type frames struct{ first, second []byte }
	result, err := withObservationConnection(ctx, c, func(conn net.Conn) (frames, error) {
		if err := WriteWithLength(conn, []byte(command)); err != nil {
			return frames{}, fmt.Errorf("send observation command: %w", err)
		}
		first, err := ReadWithLength(conn)
		if err != nil {
			return frames{}, fmt.Errorf("read observation envelope: %w", err)
		}
		var status struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal(first, &status); err != nil {
			return frames{}, fmt.Errorf("decode observation envelope: %w", err)
		}
		if status.Status == "ERROR" {
			return frames{}, observationFrameError(status.Error)
		}
		second, err := ReadWithLength(conn)
		if err != nil {
			return frames{}, fmt.Errorf("read observation batch: %w", err)
		}
		return frames{first: first, second: second}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	return result.first, result.second, nil
}

func withObservationConnection[T any](ctx context.Context, client *AdminClient, operation func(net.Conn) (T, error)) (T, error) {
	var zero T
	if ctx == nil {
		return zero, fmt.Errorf("observation context is nil")
	}
	if client == nil || len(client.config.BrokerAddrs) == 0 {
		return zero, fmt.Errorf("observation client is not configured")
	}
	var lastErr error
	var redirect string
	for attempt := 0; attempt <= client.config.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		addr := client.config.BrokerAddrs[attempt%len(client.config.BrokerAddrs)]
		if redirect != "" {
			addr, redirect = redirect, ""
		}
		conn, err := dialAuthenticatedWireConnection(ctx, addr, time.Duration(client.config.RequestTimeoutMS)*time.Millisecond, client.config.HandshakeTimeoutMS, client.config.CompressionType, client.tlsConfig, client.config.Principal, client.config.AuthToken)
		if err == nil {
			stopCancellation := context.AfterFunc(ctx, func() { _ = conn.Close() })
			deadline := time.Now().Add(time.Duration(client.config.RequestTimeoutMS) * time.Millisecond)
			if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
				deadline = contextDeadline
			}
			var value T
			operationErr := conn.SetDeadline(deadline)
			if operationErr == nil {
				value, operationErr = operation(conn)
			}
			stopCancellation()
			_ = conn.Close()
			if ctxErr := ctx.Err(); ctxErr != nil {
				return zero, ctxErr
			}
			if operationErr == nil {
				return value, nil
			}
			err = operationErr
		}
		lastErr = err
		var brokerErr *BrokerError
		if errors.As(err, &brokerErr) {
			if !brokerErr.Retryable {
				return zero, err
			}
			if strings.EqualFold(brokerErr.Code, "NOT_LEADER") {
				redirect = brokerErr.Fields["leader"]
			}
		}
		if attempt == client.config.MaxRetries {
			break
		}
		backoff := time.Duration(client.config.RetryBackoffMS) * time.Millisecond
		if backoff > 0 {
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return zero, fmt.Errorf("observation request failed after %d attempt(s): %w", client.config.MaxRetries+1, lastErr)
}

func adminMessages(messages []Message) []AdminMessage {
	result := make([]AdminMessage, 0, len(messages))
	for _, message := range messages {
		result = append(result, AdminMessage{Offset: message.Offset, Key: message.Key, Payload: message.Payload, Metadata: message.Metadata, EventType: message.EventType, SchemaVersion: message.SchemaVersion, AggregateVersion: message.AggregateVersion})
	}
	return result
}

func observationFrameError(message string) error {
	if brokerErr, ok := ParseBrokerError("ERROR: " + message); ok {
		return brokerErr
	}
	return fmt.Errorf("broker: %s", message)
}

func splitObservationList(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}

func validObservationIdentifier(value string) bool {
	return value != "" && len(value) <= 249 && !strings.ContainsAny(value, " ,=\t\r\n")
}
