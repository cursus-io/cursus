package observationgrpc

import (
	"context"
	"errors"
	"math"
	"net"
	"testing"

	observationv1 "github.com/cursus-io/cursus/api/gen/go/cursus/observation/v1"
	"github.com/cursus-io/cursus/sdk"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type testBackend struct {
	offsets       []sdk.PartitionOffsetRange
	groupOffsets  []sdk.GroupOffset
	browse        sdk.BrowseRequest
	browseResult  *sdk.BrowseResult
	history       sdk.HistoryRequest
	historyResult *sdk.HistoryResult
	capabilities  *sdk.NegotiatedProtocol
	cluster       *sdk.ClusterStatus
	snapshot      *sdk.Snapshot
	err           error
}

func (b *testBackend) Capabilities(context.Context) (*sdk.NegotiatedProtocol, error) {
	if b.capabilities != nil {
		return b.capabilities, nil
	}
	return &sdk.NegotiatedProtocol{Version: 2, Enabled: []string{"browse_messages_v1"}}, b.err
}
func (b *testBackend) ListTopics(context.Context) ([]string, error) { return []string{"orders"}, b.err }
func (b *testBackend) ListOffsets(context.Context, string) ([]sdk.PartitionOffsetRange, error) {
	return b.offsets, b.err
}
func (b *testBackend) ListGroups(context.Context) ([]string, error) {
	return []string{"workers"}, b.err
}
func (b *testBackend) GroupOffsets(context.Context, string, string) ([]sdk.GroupOffset, error) {
	if b.groupOffsets != nil {
		return b.groupOffsets, b.err
	}
	return []sdk.GroupOffset{{Partition: 0, Offset: 11, Earliest: 1, Latest: 20, Lag: 9}}, b.err
}
func (b *testBackend) BrowseMessages(_ context.Context, request sdk.BrowseRequest) (*sdk.BrowseResult, error) {
	b.browse = request
	if b.browseResult != nil {
		return b.browseResult, b.err
	}
	return &sdk.BrowseResult{NextOffset: 9007199254740993, HasMore: true}, b.err
}

func (b *testBackend) ReadStreamHistory(_ context.Context, request sdk.HistoryRequest) (*sdk.HistoryResult, error) {
	b.history = request
	if b.historyResult != nil {
		return b.historyResult, b.err
	}
	return &sdk.HistoryResult{Events: []sdk.StreamEvent{{Version: 14, Offset: 9007199254740993, SchemaVersion: 7, Type: "OrderCreated"}}}, b.err
}
func (b *testBackend) ReadSnapshot(context.Context, string, string) (*sdk.Snapshot, error) {
	return b.snapshot, b.err
}
func (b *testBackend) ClusterStatus(context.Context) (*sdk.ClusterStatus, error) {
	if b.cluster != nil {
		return b.cluster, b.err
	}
	return &sdk.ClusterStatus{}, b.err
}

func TestNewServiceRejectsNilBackend(t *testing.T) {
	_, err := NewService(nil)
	require.EqualError(t, err, "observation backend is nil")
}

func TestServiceMapsReadOnlyResponses(t *testing.T) {
	backend := &testBackend{
		cluster: &sdk.ClusterStatus{
			RaftLeader: "broker-1", RaftState: "leader", BrokerCount: 1, ActiveBrokers: 1,
			Brokers:    []sdk.ClusterBroker{{ID: "broker-1", Status: "active", Addr: "10.0.0.1:7000", ClientAddr: "10.0.0.1:9000"}},
			Partitions: []sdk.ClusterPartition{{Key: "orders-0", Topic: "orders", Partition: 0, Leader: "broker-1", Replicas: []string{"broker-1"}, ISR: []string{"broker-1"}, LeaderAvailable: true}},
		},
		snapshot: &sdk.Snapshot{Version: 3, Payload: `{"paid":true}`},
	}
	service, err := NewService(backend)
	require.NoError(t, err)

	topics, err := service.ListTopics(context.Background(), &observationv1.ListTopicsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"orders"}, topics.Topics)
	groups, err := service.ListGroups(context.Background(), &observationv1.ListGroupsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"workers"}, groups.Groups)
	groupOffsets, err := service.ListGroupOffsets(context.Background(), &observationv1.ListGroupOffsetsRequest{Topic: "orders", Group: "workers"})
	require.NoError(t, err)
	require.Equal(t, "11", groupOffsets.Offsets[0].CommittedOffset)
	require.Equal(t, "9", groupOffsets.Offsets[0].Lag)

	snapshot, err := service.ReadSnapshot(context.Background(), &observationv1.ReadSnapshotRequest{Topic: "orders", Key: "order-1"})
	require.NoError(t, err)
	require.True(t, snapshot.Found)
	require.Equal(t, "3", snapshot.Version)
	require.JSONEq(t, `{"paid":true}`, string(snapshot.State))
	backend.snapshot = nil
	snapshot, err = service.ReadSnapshot(context.Background(), &observationv1.ReadSnapshotRequest{Topic: "orders", Key: "missing"})
	require.NoError(t, err)
	require.False(t, snapshot.Found)

	cluster, err := service.GetClusterStatus(context.Background(), &observationv1.GetClusterStatusRequest{})
	require.NoError(t, err)
	require.Equal(t, "broker-1", cluster.RaftLeader)
	require.Equal(t, "10.0.0.1:7000", cluster.Brokers[0].Address)
	require.Equal(t, []string{"broker-1"}, cluster.Partitions[0].InSyncReplicas)
}

func TestServiceMapsBackendErrorsToUnavailable(t *testing.T) {
	service, err := NewService(&testBackend{err: context.DeadlineExceeded})
	require.NoError(t, err)
	tests := []struct {
		name string
		call func() error
	}{
		{"capabilities", func() error {
			_, err := service.GetCapabilities(context.Background(), &observationv1.GetCapabilitiesRequest{})
			return err
		}},
		{"topics", func() error {
			_, err := service.ListTopics(context.Background(), &observationv1.ListTopicsRequest{})
			return err
		}},
		{"offsets", func() error {
			_, err := service.ListOffsets(context.Background(), &observationv1.ListOffsetsRequest{Topic: "orders"})
			return err
		}},
		{"groups", func() error {
			_, err := service.ListGroups(context.Background(), &observationv1.ListGroupsRequest{})
			return err
		}},
		{"group offsets", func() error {
			_, err := service.ListGroupOffsets(context.Background(), &observationv1.ListGroupOffsetsRequest{Topic: "orders", Group: "workers"})
			return err
		}},
		{"browse", func() error {
			_, err := service.BrowseMessages(context.Background(), &observationv1.BrowseMessagesRequest{Topic: "orders", FromOffset: "0", MaxRecords: 1, MaxBytes: 1})
			return err
		}},
		{"history", func() error {
			_, err := service.ReadStreamHistory(context.Background(), &observationv1.ReadStreamHistoryRequest{Topic: "orders", Key: "order-1", FromVersion: "1", MaxRecords: 1, MaxBytes: 1})
			return err
		}},
		{"snapshot", func() error {
			_, err := service.ReadSnapshot(context.Background(), &observationv1.ReadSnapshotRequest{Topic: "orders", Key: "order-1"})
			return err
		}},
		{"cluster", func() error {
			_, err := service.GetClusterStatus(context.Background(), &observationv1.GetClusterStatusRequest{})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, codes.Unavailable, status.Code(test.call()))
		})
	}

	preserved := status.Error(codes.PermissionDenied, "denied")
	require.Equal(t, preserved, backendError(preserved))
	require.Equal(t, codes.Unavailable, status.Code(backendError(errors.New("broker offline"))))
}

func TestServiceMapsCapabilitiesBrowseAndHistoryResponses(t *testing.T) {
	toOffset := "19"
	toVersion := "4"
	backend := &testBackend{
		capabilities: &sdk.NegotiatedProtocol{Version: 7, Enabled: []string{"browse_messages_v1"}, Unsupported: []string{"future_v1"}},
		browseResult: &sdk.BrowseResult{
			Messages:   []sdk.AdminMessage{{Offset: 11, Key: "order-1", Payload: `{"id":1}`, Metadata: `{"source":"test"}`, EventType: "Created", SchemaVersion: 2, AggregateVersion: 3}},
			NextOffset: 12, EarliestOffset: 1, ReadableEndOffset: 20, HasMore: true,
		},
		historyResult: &sdk.HistoryResult{
			Events:      []sdk.StreamEvent{{Version: 3, Offset: 11, Type: "Created", Payload: `{"id":1}`, Metadata: `{"source":"test"}`, SchemaVersion: 2}},
			NextVersion: 4, HeadVersion: 5, Completeness: sdk.HistoryPartial, HasMore: true,
		},
	}
	service, err := NewService(backend)
	require.NoError(t, err)

	capabilities, err := service.GetCapabilities(context.Background(), &observationv1.GetCapabilitiesRequest{})
	require.NoError(t, err)
	require.Equal(t, int32(7), capabilities.ProtocolVersion)
	require.Equal(t, []string{"browse_messages_v1"}, capabilities.Features)

	browse, err := service.BrowseMessages(context.Background(), &observationv1.BrowseMessagesRequest{Topic: "orders", Partition: 2, FromOffset: "10", ToOffset: &toOffset, MaxRecords: 10, MaxBytes: 1024})
	require.NoError(t, err)
	require.Equal(t, sdk.BrowseRequest{Topic: "orders", Partition: 2, FromOffset: 10, ToOffset: uint64Pointer(19), MaxRecords: 10, MaxBytes: 1024}, backend.browse)
	require.Equal(t, "12", browse.NextOffset)
	require.Equal(t, "20", browse.ReadableEndOffset)
	require.Equal(t, []byte(`{"id":1}`), browse.Messages[0].Payload)
	require.Equal(t, "3", browse.Messages[0].AggregateVersion)

	history, err := service.ReadStreamHistory(context.Background(), &observationv1.ReadStreamHistoryRequest{Topic: "orders", Key: "order-1", FromVersion: "2", ToVersion: &toVersion, MaxRecords: 10, MaxBytes: 1024})
	require.NoError(t, err)
	require.Equal(t, sdk.HistoryRequest{Topic: "orders", Key: "order-1", FromVersion: 2, ToVersion: uint64Pointer(4), MaxRecords: 10, MaxBytes: 1024}, backend.history)
	require.Equal(t, "4", history.NextVersion)
	require.Equal(t, "partial", history.Completeness)
	require.Equal(t, []byte(`{"source":"test"}`), history.Events[0].Metadata)
}

func TestServiceRejectsInvalidRequests(t *testing.T) {
	service, err := NewService(&testBackend{})
	require.NoError(t, err)
	backwardsOffset := "1"
	backwardsVersion := "1"
	tests := []struct {
		name string
		call func() error
	}{
		{"offsets topic", func() error {
			_, err := service.ListOffsets(context.Background(), &observationv1.ListOffsetsRequest{})
			return err
		}},
		{"group offsets identifiers", func() error {
			_, err := service.ListGroupOffsets(context.Background(), &observationv1.ListGroupOffsetsRequest{Topic: "orders"})
			return err
		}},
		{"browse negative partition", func() error {
			_, err := service.BrowseMessages(context.Background(), &observationv1.BrowseMessagesRequest{Topic: "orders", Partition: -1})
			return err
		}},
		{"browse invalid offset", func() error {
			_, err := service.BrowseMessages(context.Background(), &observationv1.BrowseMessagesRequest{Topic: "orders", FromOffset: "not-a-number", MaxRecords: 1, MaxBytes: 1})
			return err
		}},
		{"browse backwards range", func() error {
			_, err := service.BrowseMessages(context.Background(), &observationv1.BrowseMessagesRequest{Topic: "orders", FromOffset: "2", ToOffset: &backwardsOffset, MaxRecords: 1, MaxBytes: 1})
			return err
		}},
		{"history identifiers", func() error {
			_, err := service.ReadStreamHistory(context.Background(), &observationv1.ReadStreamHistoryRequest{Topic: "orders"})
			return err
		}},
		{"history invalid version", func() error {
			_, err := service.ReadStreamHistory(context.Background(), &observationv1.ReadStreamHistoryRequest{Topic: "orders", Key: "order-1", FromVersion: "0", MaxRecords: 1, MaxBytes: 1})
			return err
		}},
		{"history backwards range", func() error {
			_, err := service.ReadStreamHistory(context.Background(), &observationv1.ReadStreamHistoryRequest{Topic: "orders", Key: "order-1", FromVersion: "2", ToVersion: &backwardsVersion, MaxRecords: 1, MaxBytes: 1})
			return err
		}},
		{"snapshot identifiers", func() error {
			_, err := service.ReadSnapshot(context.Background(), &observationv1.ReadSnapshotRequest{Topic: "orders"})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, codes.InvalidArgument, status.Code(test.call()))
		})
	}
}

func uint64Pointer(value uint64) *uint64 { return &value }

func TestListOffsetsPreservesDecimalPrecision(t *testing.T) {
	backend := &testBackend{offsets: []sdk.PartitionOffsetRange{{Partition: 1, Earliest: 1, Latest: 9007199254740993, LEO: 9007199254740993, HWM: 9007199254740992}}}
	client, cleanup := testClient(t, backend)
	defer cleanup()

	response, err := client.ListOffsets(context.Background(), &observationv1.ListOffsetsRequest{Topic: "orders"})
	require.NoError(t, err)
	require.Len(t, response.Offsets, 1)
	require.Equal(t, "9007199254740993", response.Offsets[0].LatestOffset)
	require.Equal(t, "9007199254740992", response.Offsets[0].HighWatermark)
}

func TestBrowseRejectsUnboundedRequestBeforeBackend(t *testing.T) {
	backend := &testBackend{}
	client, cleanup := testClient(t, backend)
	defer cleanup()

	_, err := client.BrowseMessages(context.Background(), &observationv1.BrowseMessagesRequest{Topic: "orders", Partition: 0, FromOffset: "0", MaxRecords: maxRecords + 1, MaxBytes: 1})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, sdk.BrowseRequest{}, backend.browse)
}

func TestBrowsePassesBoundedRequestAndReturnsExactOffset(t *testing.T) {
	backend := &testBackend{}
	client, cleanup := testClient(t, backend)
	defer cleanup()

	response, err := client.BrowseMessages(context.Background(), &observationv1.BrowseMessagesRequest{Topic: "orders", Partition: 1, FromOffset: "7", MaxRecords: 10, MaxBytes: 1024})
	require.NoError(t, err)
	require.Equal(t, sdk.BrowseRequest{Topic: "orders", Partition: 1, FromOffset: 7, MaxRecords: 10, MaxBytes: 1024}, backend.browse)
	require.Equal(t, "9007199254740993", response.NextOffset)
}

func TestReadStreamHistoryPreservesEventOffsetAndSchemaVersion(t *testing.T) {
	client, cleanup := testClient(t, &testBackend{})
	defer cleanup()

	response, err := client.ReadStreamHistory(context.Background(), &observationv1.ReadStreamHistoryRequest{Topic: "orders", Key: "o-1", FromVersion: "1", MaxRecords: 10, MaxBytes: 1024})
	require.NoError(t, err)
	require.Len(t, response.Events, 1)
	require.Equal(t, "9007199254740993", response.Events[0].Offset)
	require.Equal(t, uint32(7), response.Events[0].SchemaVersion)
}

func TestServiceRejectsBackendValuesThatDoNotFitProtoInt32(t *testing.T) {
	tests := []struct {
		name string
		call func(observationv1.ObservationServiceClient) error
	}{
		{
			name: "capability version",
			call: func(client observationv1.ObservationServiceClient) error {
				_, err := client.GetCapabilities(context.Background(), &observationv1.GetCapabilitiesRequest{})
				return err
			},
		},
		{
			name: "topic partition",
			call: func(client observationv1.ObservationServiceClient) error {
				_, err := client.ListOffsets(context.Background(), &observationv1.ListOffsetsRequest{Topic: "orders"})
				return err
			},
		},
		{
			name: "cluster count",
			call: func(client observationv1.ObservationServiceClient) error {
				_, err := client.GetClusterStatus(context.Background(), &observationv1.GetClusterStatusRequest{})
				return err
			},
		},
	}
	backends := []*testBackend{
		{capabilities: &sdk.NegotiatedProtocol{Version: math.MaxInt32 + 1}},
		{offsets: []sdk.PartitionOffsetRange{{Partition: math.MaxInt32 + 1}}},
		{cluster: &sdk.ClusterStatus{BrokerCount: -1}},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, cleanup := testClient(t, backends[index])
			defer cleanup()
			err := test.call(client)
			require.Equal(t, codes.Internal, status.Code(err))
		})
	}
}

func TestServiceRejectsInvalidClusterAndGroupOffsetValues(t *testing.T) {
	tests := []struct {
		name    string
		backend *testBackend
		call    func(*Service) error
	}{
		{"group offset partition", &testBackend{groupOffsets: []sdk.GroupOffset{{Partition: -1}}}, func(service *Service) error {
			_, err := service.ListGroupOffsets(context.Background(), &observationv1.ListGroupOffsetsRequest{Topic: "orders", Group: "workers"})
			return err
		}},
		{"active broker count", &testBackend{cluster: &sdk.ClusterStatus{BrokerCount: 1, ActiveBrokers: -1}}, func(service *Service) error {
			_, err := service.GetClusterStatus(context.Background(), &observationv1.GetClusterStatusRequest{})
			return err
		}},
		{"inactive broker count", &testBackend{cluster: &sdk.ClusterStatus{BrokerCount: 1, ActiveBrokers: 1, InactiveBrokers: -1}}, func(service *Service) error {
			_, err := service.GetClusterStatus(context.Background(), &observationv1.GetClusterStatusRequest{})
			return err
		}},
		{"partition count", &testBackend{cluster: &sdk.ClusterStatus{BrokerCount: 1, ActiveBrokers: 1, PartitionCount: -1}}, func(service *Service) error {
			_, err := service.GetClusterStatus(context.Background(), &observationv1.GetClusterStatusRequest{})
			return err
		}},
		{"leaderless count", &testBackend{cluster: &sdk.ClusterStatus{BrokerCount: 1, ActiveBrokers: 1, Leaderless: -1}}, func(service *Service) error {
			_, err := service.GetClusterStatus(context.Background(), &observationv1.GetClusterStatusRequest{})
			return err
		}},
		{"under replicated count", &testBackend{cluster: &sdk.ClusterStatus{BrokerCount: 1, ActiveBrokers: 1, UnderReplicated: -1}}, func(service *Service) error {
			_, err := service.GetClusterStatus(context.Background(), &observationv1.GetClusterStatusRequest{})
			return err
		}},
		{"partition number", &testBackend{cluster: &sdk.ClusterStatus{BrokerCount: 1, ActiveBrokers: 1, Partitions: []sdk.ClusterPartition{{Partition: -1}}}}, func(service *Service) error {
			_, err := service.GetClusterStatus(context.Background(), &observationv1.GetClusterStatusRequest{})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, err := NewService(test.backend)
			require.NoError(t, err)
			require.Equal(t, codes.Internal, status.Code(test.call(service)))
		})
	}
}

func testClient(t *testing.T, backend Backend) (observationv1.ObservationServiceClient, func()) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	service, err := NewService(backend)
	require.NoError(t, err)
	observationv1.RegisterObservationServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	connection, err := grpc.NewClient("passthrough:///test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return observationv1.NewObservationServiceClient(connection), func() {
		_ = connection.Close()
		server.Stop()
		_ = listener.Close()
	}
}
