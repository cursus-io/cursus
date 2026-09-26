package observationgrpc

import (
	"context"
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
	offsets      []sdk.PartitionOffsetRange
	browse       sdk.BrowseRequest
	capabilities *sdk.NegotiatedProtocol
	cluster      *sdk.ClusterStatus
}

func (b *testBackend) Capabilities(context.Context) (*sdk.NegotiatedProtocol, error) {
	if b.capabilities != nil {
		return b.capabilities, nil
	}
	return &sdk.NegotiatedProtocol{Version: 2, Enabled: []string{"browse_messages_v1"}}, nil
}
func (b *testBackend) ListTopics(context.Context) ([]string, error) { return []string{"orders"}, nil }
func (b *testBackend) ListOffsets(context.Context, string) ([]sdk.PartitionOffsetRange, error) {
	return b.offsets, nil
}
func (b *testBackend) ListGroups(context.Context) ([]string, error) { return []string{"workers"}, nil }
func (b *testBackend) GroupOffsets(context.Context, string, string) ([]sdk.GroupOffset, error) {
	return []sdk.GroupOffset{{Partition: 0, Offset: 11, Earliest: 1, Latest: 20, Lag: 9}}, nil
}
func (b *testBackend) BrowseMessages(_ context.Context, request sdk.BrowseRequest) (*sdk.BrowseResult, error) {
	b.browse = request
	return &sdk.BrowseResult{NextOffset: 9007199254740993, HasMore: true}, nil
}
func (b *testBackend) ReadStreamHistory(context.Context, sdk.HistoryRequest) (*sdk.HistoryResult, error) {
	return &sdk.HistoryResult{Events: []sdk.StreamEvent{{Version: 14, Offset: 9007199254740993, SchemaVersion: 7, Type: "OrderCreated"}}}, nil
}
func (b *testBackend) ReadSnapshot(context.Context, string, string) (*sdk.Snapshot, error) {
	return nil, nil
}
func (b *testBackend) ClusterStatus(context.Context) (*sdk.ClusterStatus, error) {
	if b.cluster != nil {
		return b.cluster, nil
	}
	return &sdk.ClusterStatus{}, nil
}

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
