//go:build legacy_sql_saga

package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// AdminClient is a short-lived, context-aware client for broker inspection.
// It uses normal client authentication and feature negotiation, never an
// internal broker token or direct storage access.
type AdminClient struct{ config AdminConfig }

func NewAdminClient(config AdminConfig) (*AdminClient, error) {
	if len(config.BrokerAddrs) == 0 {
		return nil, fmt.Errorf("admin client requires a broker address")
	}
	if (config.Principal == "") != (config.AuthToken == "") {
		return nil, fmt.Errorf("principal and auth token must be configured together")
	}
	return &AdminClient{config: config}, nil
}

func (c *AdminClient) Close() error { return nil }

func (c *AdminClient) Capabilities(ctx context.Context) (*NegotiatedProtocol, error) {
	return withAdminConnection(ctx, c, func(conn net.Conn) (*NegotiatedProtocol, error) {
		return NegotiateProtocol(conn, ProtocolNegotiation{Version: c.config.ProtocolVersion, Features: c.config.ProtocolFeatures})
	})
}

func (c *AdminClient) BrowseMessages(ctx context.Context, request BrowseRequest) (*BrowseResult, error) {
	if err := validateSDKTopicName(request.Topic); err != nil {
		return nil, err
	}
	if request.Partition < 0 || request.MaxRecords <= 0 || request.MaxBytes <= 0 {
		return nil, fmt.Errorf("invalid browse request")
	}
	if request.ToOffset != nil && *request.ToOffset < request.FromOffset {
		return nil, fmt.Errorf("to offset precedes from offset")
	}
	command := fmt.Sprintf("BROWSE_MESSAGES topic=%s partition=%d from_offset=%d max_records=%d max_bytes=%d", request.Topic, request.Partition, request.FromOffset, request.MaxRecords, request.MaxBytes)
	if request.ToOffset != nil {
		command += fmt.Sprintf(" to_offset=%d", *request.ToOffset)
	}
	type envelope struct {
		Status, Error     string
		NextOffset        uint64 `json:"next_offset"`
		EarliestOffset    uint64 `json:"earliest_offset"`
		ReadableEndOffset uint64 `json:"readable_end_offset"`
		HasMore           bool   `json:"has_more"`
	}
	return withAdminFrames(ctx, c, "browse_messages_v1", command, func(first, second []byte) (*BrowseResult, error) {
		var response envelope
		if err := json.Unmarshal(first, &response); err != nil {
			return nil, fmt.Errorf("decode browse envelope: %w", err)
		}
		if response.Status != "OK" {
			return nil, brokerFrameError(response.Error)
		}
		messages, _, _, err := DecodeBatchMessages(second)
		if err != nil {
			return nil, fmt.Errorf("decode browse batch: %w", err)
		}
		return &BrowseResult{Messages: messagesFromBatch(messages), NextOffset: response.NextOffset, EarliestOffset: response.EarliestOffset, ReadableEndOffset: response.ReadableEndOffset, HasMore: response.HasMore}, nil
	})
}

func (c *AdminClient) ReadStreamHistory(ctx context.Context, request HistoryRequest) (*HistoryResult, error) {
	if err := validateSDKTopicName(request.Topic); err != nil {
		return nil, err
	}
	if request.Key == "" || strings.ContainsAny(request.Key, " \t\r\n") || request.FromVersion == 0 || request.MaxRecords <= 0 || request.MaxBytes <= 0 {
		return nil, fmt.Errorf("invalid history request")
	}
	if request.ToVersion != nil && (*request.ToVersion < request.FromVersion) {
		return nil, fmt.Errorf("to version precedes from version")
	}
	command := fmt.Sprintf("READ_STREAM_HISTORY topic=%s key=%s from_version=%d max_records=%d max_bytes=%d", request.Topic, request.Key, request.FromVersion, request.MaxRecords, request.MaxBytes)
	if request.ToVersion != nil {
		command += fmt.Sprintf(" to_version=%d", *request.ToVersion)
	}
	type envelope struct {
		Status, Error string
		NextVersion   uint64              `json:"next_version"`
		HeadVersion   uint64              `json:"head_version"`
		Completeness  HistoryCompleteness `json:"completeness"`
		HasMore       bool                `json:"has_more"`
	}
	return withAdminFrames(ctx, c, "stream_history_v1", command, func(first, second []byte) (*HistoryResult, error) {
		var response envelope
		if err := json.Unmarshal(first, &response); err != nil {
			return nil, fmt.Errorf("decode history envelope: %w", err)
		}
		if response.Status != "OK" {
			return nil, brokerFrameError(response.Error)
		}
		messages, _, _, err := DecodeBatchMessages(second)
		if err != nil {
			return nil, fmt.Errorf("decode history batch: %w", err)
		}
		events := make([]StreamEvent, 0, len(messages))
		for _, message := range messages {
			events = append(events, streamEventFromMessage(message))
		}
		return &HistoryResult{Events: events, NextVersion: response.NextVersion, HeadVersion: response.HeadVersion, Completeness: response.Completeness, HasMore: response.HasMore}, nil
	})
}

func (c *AdminClient) ReadSnapshot(ctx context.Context, topic, key string) (*Snapshot, error) {
	if err := validateSDKTopicName(topic); err != nil {
		return nil, err
	}
	if key == "" || strings.ContainsAny(key, " \t\r\n") {
		return nil, fmt.Errorf("invalid snapshot key")
	}
	response, err := c.command(ctx, fmt.Sprintf("READ_SNAPSHOT topic=%s key=%s", topic, key))
	if err != nil {
		return nil, err
	}
	jsonValue, ok, err := parseSnapshotResponse(response)
	if err != nil || !ok {
		return nil, err
	}
	var snapshot Snapshot
	if err := json.Unmarshal([]byte(jsonValue), &snapshot); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	return &snapshot, nil
}

func (c *AdminClient) ListTopics(ctx context.Context) ([]string, error) {
	response, err := c.command(ctx, "LIST")
	if err != nil { return nil, err }
	fields, err := parseOKResponse(response); if err != nil { return nil, err }
	return splitListField(fields["topics"]), nil
}

func (c *AdminClient) ListGroups(ctx context.Context) ([]string, error) {
	response, err := c.command(ctx, "LIST_GROUPS")
	if err != nil { return nil, err }
	fields, err := parseOKResponse(response); if err != nil { return nil, err }
	return splitListField(fields["groups"]), nil
}

func (c *AdminClient) ListOffsets(ctx context.Context, topic string) ([]PartitionOffsetRange, error) {
	if err := validateSDKTopicName(topic); err != nil { return nil, err }
	response, err := c.command(ctx, "LIST_OFFSETS topic="+topic); if err != nil { return nil, err }
	return parseListOffsetsResponse(response)
}

func (c *AdminClient) GroupOffsets(ctx context.Context, group, topic string) ([]GroupOffset, error) {
	if group == "" || strings.ContainsAny(group, " \t\r\n") { return nil, fmt.Errorf("invalid group") }
	ranges, err := c.ListOffsets(ctx, topic); if err != nil { return nil, err }
	result := make([]GroupOffset, 0, len(ranges))
	for _, item := range ranges {
		response, requestErr := c.command(ctx, fmt.Sprintf("FETCH_OFFSET topic=%s partition=%d group=%s", topic, item.Partition, group))
		if requestErr != nil { return nil, requestErr }
		fields, parseErr := parseOKResponse(response); if parseErr != nil { return nil, parseErr }
		offset, parseErr := strconv.ParseUint(fields["offset"], 10, 64); if parseErr != nil { return nil, fmt.Errorf("invalid group offset") }
		lag := uint64(0); if item.HWM > offset { lag = item.HWM-offset }
		result = append(result, GroupOffset{Partition: item.Partition, Offset: offset, Earliest: item.Earliest, Latest: item.HWM, Lag: lag})
	}
	return result, nil
}

func (c *AdminClient) command(ctx context.Context, command string) (string, error) {
	return withAdminConnection(ctx, c, func(conn net.Conn) (string, error) {
		if err := WriteWithLength(conn, EncodeMessage("", command)); err != nil {
			return "", fmt.Errorf("send command: %w", err)
		}
		response, err := ReadWithLength(conn)
		if err != nil {
			return "", fmt.Errorf("read command: %w", err)
		}
		value := strings.TrimSpace(string(response))
		if brokerErr, ok := ParseBrokerError(value); ok {
			return "", brokerErr
		}
		return value, nil
	})
}

func withAdminFrames[T any](ctx context.Context, client *AdminClient, feature, command string, read func([]byte, []byte) (*T, error)) (*T, error) {
	return withAdminConnection(ctx, client, func(conn net.Conn) (*T, error) {
		negotiated, err := NegotiateProtocol(conn, ProtocolNegotiation{Version: client.config.ProtocolVersion, Features: []string{feature}, RequireFeatures: true})
		if err != nil {
			return nil, fmt.Errorf("negotiate %s: %w", feature, err)
		}
		if len(negotiated.Enabled) != 1 || negotiated.Enabled[0] != feature {
			return nil, fmt.Errorf("broker did not enable %s", feature)
		}
		if err := WriteWithLength(conn, EncodeMessage("", command)); err != nil {
			return nil, fmt.Errorf("send command: %w", err)
		}
		first, err := ReadWithLength(conn)
		if err != nil {
			return nil, fmt.Errorf("read envelope: %w", err)
		}
		var status struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(first, &status); err != nil {
			return nil, fmt.Errorf("decode envelope: %w", err)
		}
		if status.Status == "ERROR" {
			var failure struct {
				Error string `json:"error"`
			}
			_ = json.Unmarshal(first, &failure)
			return nil, brokerFrameError(failure.Error)
		}
		second, err := ReadWithLength(conn)
		if err != nil {
			return nil, fmt.Errorf("read data frame: %w", err)
		}
		return read(first, second)
	})
}

func withAdminConnection[T any](ctx context.Context, client *AdminClient, operation func(net.Conn) (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	config := NewDefaultConsumerConfig()
	config.BrokerAddrs = append([]string(nil), client.config.BrokerAddrs...)
	config.UseTLS, config.TLSCertPath, config.TLSKeyPath = client.config.UseTLS, client.config.TLSCertPath, client.config.TLSKeyPath
	config.Principal, config.AuthToken = client.config.Principal, client.config.AuthToken
	// Negotiation is done per request so each operation can require exactly its
	// capability rather than assuming bootstrap-node support applies to leaders.
	config.ProtocolVersion, config.ProtocolFeatures, config.RequireProtocolFeatures = 0, nil, false
	consumer, err := NewConsumerClient(config)
	if err != nil {
		return zero, err
	}
	conn, _, err := consumer.ConnectWithFailover()
	if err != nil {
		return zero, err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return zero, err
		}
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	return operation(conn)
}

func messagesFromBatch(messages []Message) []AdminMessage {
	result := make([]AdminMessage, 0, len(messages))
	for _, message := range messages {
		result = append(result, AdminMessage{Offset: message.Offset, Key: message.Key, Payload: message.Payload, Metadata: message.Metadata, EventType: message.EventType, SchemaVersion: message.SchemaVersion, AggregateVersion: message.AggregateVersion})
	}
	return result
}

func streamEventFromMessage(message Message) StreamEvent {
	return StreamEvent{Version: message.AggregateVersion, Offset: message.Offset, Type: message.EventType, SchemaVersion: message.SchemaVersion, Payload: message.Payload, Metadata: message.Metadata}
}

func brokerFrameError(message string) error {
	if errorValue, ok := ParseBrokerError("ERROR: " + message); ok {
		return errorValue
	}
	return fmt.Errorf("broker: %s", message)
}
