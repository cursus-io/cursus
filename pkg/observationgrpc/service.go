// Package observationgrpc adapts Cursus's read-only SDK surface to the
// versioned ObservationService protobuf contract. It deliberately has no
// dependency on producer, consumer, transaction, or management operations.
package observationgrpc

import (
	"context"
	"fmt"
	"strconv"

	observationv1 "github.com/cursus-io/cursus/api/gen/go/cursus/observation/v1"
	"github.com/cursus-io/cursus/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxRecords = 500
	maxBytes   = 4 << 20
)

// Backend is the complete read-only dependency required by ObservationService.
// It intentionally cannot mutate a broker or join a consumer group.
type Backend interface {
	Capabilities(context.Context) (*sdk.NegotiatedProtocol, error)
	ListTopics(context.Context) ([]string, error)
	ListOffsets(context.Context, string) ([]sdk.PartitionOffsetRange, error)
	ListGroups(context.Context) ([]string, error)
	GroupOffsets(context.Context, string, string) ([]sdk.GroupOffset, error)
	BrowseMessages(context.Context, sdk.BrowseRequest) (*sdk.BrowseResult, error)
	ReadStreamHistory(context.Context, sdk.HistoryRequest) (*sdk.HistoryResult, error)
	ReadSnapshot(context.Context, string, string) (*sdk.Snapshot, error)
	ClusterStatus(context.Context) (*sdk.ClusterStatus, error)
}

// Service exposes a bounded, typed adapter over a configured Backend.
type Service struct {
	observationv1.UnimplementedObservationServiceServer
	backend Backend
}

func NewService(backend Backend) (*Service, error) {
	if backend == nil {
		return nil, fmt.Errorf("observation backend is nil")
	}
	return &Service{backend: backend}, nil
}

func (s *Service) GetCapabilities(ctx context.Context, _ *observationv1.GetCapabilitiesRequest) (*observationv1.GetCapabilitiesResponse, error) {
	capabilities, err := s.backend.Capabilities(ctx)
	if err != nil {
		return nil, backendError(err)
	}
	return &observationv1.GetCapabilitiesResponse{ProtocolVersion: int32(capabilities.Version), Features: append([]string(nil), capabilities.Enabled...)}, nil
}

func (s *Service) ListTopics(ctx context.Context, _ *observationv1.ListTopicsRequest) (*observationv1.ListTopicsResponse, error) {
	topics, err := s.backend.ListTopics(ctx)
	if err != nil {
		return nil, backendError(err)
	}
	return &observationv1.ListTopicsResponse{Topics: append([]string(nil), topics...)}, nil
}

func (s *Service) ListOffsets(ctx context.Context, request *observationv1.ListOffsetsRequest) (*observationv1.ListOffsetsResponse, error) {
	if request.GetTopic() == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	offsets, err := s.backend.ListOffsets(ctx, request.GetTopic())
	if err != nil {
		return nil, backendError(err)
	}
	response := &observationv1.ListOffsetsResponse{Offsets: make([]*observationv1.PartitionOffset, 0, len(offsets))}
	for _, offset := range offsets {
		response.Offsets = append(response.Offsets, &observationv1.PartitionOffset{Partition: int32(offset.Partition), EarliestOffset: decimal(offset.Earliest), LatestOffset: decimal(offset.Latest), LogEndOffset: decimal(offset.LEO), HighWatermark: decimal(offset.HWM)})
	}
	return response, nil
}

func (s *Service) ListGroups(ctx context.Context, _ *observationv1.ListGroupsRequest) (*observationv1.ListGroupsResponse, error) {
	groups, err := s.backend.ListGroups(ctx)
	if err != nil {
		return nil, backendError(err)
	}
	return &observationv1.ListGroupsResponse{Groups: append([]string(nil), groups...)}, nil
}

func (s *Service) ListGroupOffsets(ctx context.Context, request *observationv1.ListGroupOffsetsRequest) (*observationv1.ListGroupOffsetsResponse, error) {
	if request.GetTopic() == "" || request.GetGroup() == "" {
		return nil, status.Error(codes.InvalidArgument, "topic and group are required")
	}
	offsets, err := s.backend.GroupOffsets(ctx, request.GetGroup(), request.GetTopic())
	if err != nil {
		return nil, backendError(err)
	}
	response := &observationv1.ListGroupOffsetsResponse{Offsets: make([]*observationv1.GroupOffset, 0, len(offsets))}
	for _, offset := range offsets {
		response.Offsets = append(response.Offsets, &observationv1.GroupOffset{Partition: int32(offset.Partition), CommittedOffset: decimal(offset.Offset), EarliestOffset: decimal(offset.Earliest), LatestOffset: decimal(offset.Latest), Lag: decimal(offset.Lag)})
	}
	return response, nil
}

func (s *Service) BrowseMessages(ctx context.Context, request *observationv1.BrowseMessagesRequest) (*observationv1.BrowseMessagesResponse, error) {
	if request.GetTopic() == "" || request.GetPartition() < 0 {
		return nil, status.Error(codes.InvalidArgument, "topic and non-negative partition are required")
	}
	from, err := parseDecimal(request.GetFromOffset(), "from_offset", false)
	if err != nil {
		return nil, err
	}
	to, err := optionalDecimal(request.ToOffset, "to_offset")
	if err != nil {
		return nil, err
	}
	if to != nil && *to < from {
		return nil, status.Error(codes.InvalidArgument, "to_offset precedes from_offset")
	}
	maxRecords, maxBytes, err := bounds(request.GetMaxRecords(), request.GetMaxBytes())
	if err != nil {
		return nil, err
	}
	result, err := s.backend.BrowseMessages(ctx, sdk.BrowseRequest{Topic: request.GetTopic(), Partition: int(request.GetPartition()), FromOffset: from, ToOffset: to, MaxRecords: maxRecords, MaxBytes: maxBytes})
	if err != nil {
		return nil, backendError(err)
	}
	response := &observationv1.BrowseMessagesResponse{NextOffset: decimal(result.NextOffset), EarliestOffset: decimal(result.EarliestOffset), ReadableEndOffset: decimal(result.ReadableEndOffset), HasMore: result.HasMore, Messages: make([]*observationv1.Message, 0, len(result.Messages))}
	for _, message := range result.Messages {
		response.Messages = append(response.Messages, &observationv1.Message{Offset: decimal(message.Offset), Key: message.Key, Payload: []byte(message.Payload), Metadata: []byte(message.Metadata), EventType: message.EventType, SchemaVersion: message.SchemaVersion, AggregateVersion: decimal(message.AggregateVersion)})
	}
	return response, nil
}

func (s *Service) ReadStreamHistory(ctx context.Context, request *observationv1.ReadStreamHistoryRequest) (*observationv1.ReadStreamHistoryResponse, error) {
	if request.GetTopic() == "" || request.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "topic and key are required")
	}
	from, err := parseDecimal(request.GetFromVersion(), "from_version", true)
	if err != nil {
		return nil, err
	}
	to, err := optionalDecimal(request.ToVersion, "to_version")
	if err != nil {
		return nil, err
	}
	if to != nil && *to < from {
		return nil, status.Error(codes.InvalidArgument, "to_version precedes from_version")
	}
	maxRecords, maxBytes, err := bounds(request.GetMaxRecords(), request.GetMaxBytes())
	if err != nil {
		return nil, err
	}
	result, err := s.backend.ReadStreamHistory(ctx, sdk.HistoryRequest{Topic: request.GetTopic(), Key: request.GetKey(), FromVersion: from, ToVersion: to, MaxRecords: maxRecords, MaxBytes: maxBytes})
	if err != nil {
		return nil, backendError(err)
	}
	response := &observationv1.ReadStreamHistoryResponse{NextVersion: decimal(result.NextVersion), HeadVersion: decimal(result.HeadVersion), Completeness: string(result.Completeness), HasMore: result.HasMore, Events: make([]*observationv1.HistoryEvent, 0, len(result.Events))}
	for _, event := range result.Events {
		response.Events = append(response.Events, &observationv1.HistoryEvent{Version: decimal(event.Version), EventType: event.Type, Payload: []byte(event.Payload), Metadata: []byte(event.Metadata), Offset: decimal(event.Offset), SchemaVersion: event.SchemaVersion})
	}
	return response, nil
}

func (s *Service) ReadSnapshot(ctx context.Context, request *observationv1.ReadSnapshotRequest) (*observationv1.ReadSnapshotResponse, error) {
	if request.GetTopic() == "" || request.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "topic and key are required")
	}
	snapshot, err := s.backend.ReadSnapshot(ctx, request.GetTopic(), request.GetKey())
	if err != nil {
		return nil, backendError(err)
	}
	if snapshot == nil {
		return &observationv1.ReadSnapshotResponse{}, nil
	}
	return &observationv1.ReadSnapshotResponse{Found: true, Version: decimal(snapshot.Version), State: []byte(snapshot.Payload)}, nil
}

func (s *Service) GetClusterStatus(ctx context.Context, _ *observationv1.GetClusterStatusRequest) (*observationv1.GetClusterStatusResponse, error) {
	cluster, err := s.backend.ClusterStatus(ctx)
	if err != nil {
		return nil, backendError(err)
	}
	response := &observationv1.GetClusterStatusResponse{RaftLeader: cluster.RaftLeader, RaftState: cluster.RaftState, BrokerCount: int32(cluster.BrokerCount), ActiveBrokers: int32(cluster.ActiveBrokers), InactiveBrokers: int32(cluster.InactiveBrokers), PartitionCount: int32(cluster.PartitionCount), LeaderlessPartitions: int32(cluster.Leaderless), UnderReplicatedPartitions: int32(cluster.UnderReplicated)}
	for _, broker := range cluster.Brokers {
		response.Brokers = append(response.Brokers, &observationv1.ClusterBroker{Id: broker.ID, Status: broker.Status, Address: broker.Addr})
	}
	for _, partition := range cluster.Partitions {
		response.Partitions = append(response.Partitions, &observationv1.ClusterPartition{Key: partition.Key, Topic: partition.Topic, Partition: int32(partition.Partition), Leader: partition.Leader, Replicas: append([]string(nil), partition.Replicas...), InSyncReplicas: append([]string(nil), partition.ISR...), LeaderAvailable: partition.LeaderAvailable, UnderReplicated: partition.UnderReplicated})
	}
	return response, nil
}

func bounds(records, bytes int32) (int, int, error) {
	if records <= 0 || records > maxRecords || bytes <= 0 || bytes > maxBytes {
		return 0, 0, status.Errorf(codes.InvalidArgument, "max_records must be 1..%d and max_bytes must be 1..%d", maxRecords, maxBytes)
	}
	return int(records), int(bytes), nil
}

func parseDecimal(value, field string, nonZero bool) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || (nonZero && parsed == 0) {
		return 0, status.Errorf(codes.InvalidArgument, "%s must be a valid decimal offset", field)
	}
	return parsed, nil
}

func optionalDecimal(value *string, field string) (*uint64, error) {
	if value == nil {
		return nil, nil
	}
	parsed, err := parseDecimal(*value, field, false)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func decimal(value uint64) string { return strconv.FormatUint(value, 10) }

func backendError(err error) error {
	if status.Code(err) != codes.Unknown {
		return err
	}
	return status.Error(codes.Unavailable, "observation backend is unavailable")
}
