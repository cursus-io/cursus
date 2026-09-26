package sdk

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestAdminClientObservationSimpleReads(t *testing.T) {
	t.Run("topics", func(t *testing.T) {
		client, commands := observationAdminClient(t, []string{"OK topics=orders,payments"})
		values, err := client.ListTopics(context.Background())
		require.NoError(t, err)
		require.Equal(t, []string{"orders", "payments"}, values)
		require.Equal(t, []string{"LIST"}, receiveObservationCommands(t, commands))
	})
	t.Run("groups", func(t *testing.T) {
		client, commands := observationAdminClient(t, []string{"OK groups=workers"})
		values, err := client.ListGroups(context.Background())
		require.NoError(t, err)
		require.Equal(t, []string{"workers"}, values)
		require.Equal(t, []string{"LIST_GROUPS"}, receiveObservationCommands(t, commands))
	})
	t.Run("offsets", func(t *testing.T) {
		client, commands := observationAdminClient(t, []string{"OK topic=orders partitions=1 offsets=P0:earliest=1:latest=3:leo=4:hwm=3"})
		values, err := client.ListOffsets(context.Background(), "orders")
		require.NoError(t, err)
		require.Equal(t, []PartitionOffsetRange{{Partition: 0, Earliest: 1, Latest: 3, LEO: 4, HWM: 3}}, values)
		require.Equal(t, []string{"LIST_OFFSETS topic=orders"}, receiveObservationCommands(t, commands))
	})
	t.Run("snapshot", func(t *testing.T) {
		client, commands := observationAdminClient(t, []string{`OK snapshot={"version":7,"payload":"state"}`})
		value, err := client.ReadSnapshot(context.Background(), "orders", "order-1")
		require.NoError(t, err)
		require.Equal(t, &Snapshot{Version: 7, Payload: "state"}, value)
		require.Equal(t, []string{"READ_SNAPSHOT topic=orders key=order-1"}, receiveObservationCommands(t, commands))
	})
	t.Run("cluster status", func(t *testing.T) {
		client, commands := observationAdminClient(t, []string{`OK cluster={"raft_leader":"broker-1","raft_state":"leader","broker_count":1,"active_brokers":1,"inactive_brokers":0,"partition_count":0,"leaderless_partitions":0,"under_replicated_partitions":0,"brokers":[{"id":"broker-1","status":"active","addr":"127.0.0.1:9000"}],"partitions":[]}`})
		value, err := client.ClusterStatus(context.Background())
		require.NoError(t, err)
		require.Equal(t, "broker-1", value.RaftLeader)
		require.Equal(t, []string{"CLUSTER_STATUS"}, receiveObservationCommands(t, commands))
	})
}

func TestAdminClientGroupOffsetsUsesReadOnlyCommands(t *testing.T) {
	client, commands := observationAdminClient(t, []string{
		"OK topic=orders partitions=1 offsets=P0:earliest=1:latest=8:leo=9:hwm=8",
		"OK offset=3",
	})
	values, err := client.GroupOffsets(context.Background(), "workers", "orders")
	require.NoError(t, err)
	require.Equal(t, []GroupOffset{{Partition: 0, Offset: 3, Earliest: 1, Latest: 8, Lag: 5}}, values)
	require.Equal(t, []string{"LIST_OFFSETS topic=orders", "FETCH_OFFSET topic=orders partition=0 group=workers"}, receiveObservationCommands(t, commands))
}

func TestAdminClientClusterStatusMapsStandaloneError(t *testing.T) {
	client, commands := observationAdminClient(t, []string{"ERROR: distribution_required command=CLUSTER_STATUS class=validation retryable=false"})
	_, err := client.ClusterStatus(context.Background())
	require.ErrorIs(t, err, ErrStandaloneBroker{})
	require.Equal(t, []string{"CLUSTER_STATUS"}, receiveObservationCommands(t, commands))
}

func TestAdminClientBrowseMessagesReadsBoundedFrames(t *testing.T) {
	batch, err := EncodeBatchMessages("orders", 0, "all", false, []Message{{Offset: 9007199254740993, Key: "order-1", Payload: `{"id":"order-1"}`, Metadata: `{"source":"test"}`, EventType: "OrderCreated", SchemaVersion: 2, AggregateVersion: 4}})
	require.NoError(t, err)
	client, commands := observationFrameClient(t, "browse_messages_v1", `{"status":"OK","next_offset":9007199254740994,"earliest_offset":1,"readable_end_offset":9007199254740994,"has_more":false}`, batch)
	result, err := client.BrowseMessages(context.Background(), BrowseRequest{Topic: "orders", Partition: 0, FromOffset: 1, MaxRecords: 10, MaxBytes: 1024})
	require.NoError(t, err)
	require.Equal(t, uint64(9007199254740994), result.NextOffset)
	require.Equal(t, []AdminMessage{{Offset: 9007199254740993, Key: "order-1", Payload: `{"id":"order-1"}`, Metadata: `{"source":"test"}`, EventType: "OrderCreated", SchemaVersion: 2, AggregateVersion: 4}}, result.Messages)
	require.Equal(t, []string{"NEGOTIATE version=1 features=browse_messages_v1 require_features=true", "BROWSE_MESSAGES topic=orders partition=0 from_offset=1 max_records=10 max_bytes=1024"}, receiveObservationCommands(t, commands))
}

func TestAdminClientReadStreamHistoryReadsBoundedFrames(t *testing.T) {
	batch, err := EncodeBatchMessages("orders", 0, "all", false, []Message{{Offset: 9, Payload: `{"state":"paid"}`, EventType: "OrderPaid", SchemaVersion: 3, AggregateVersion: 2}})
	require.NoError(t, err)
	client, commands := observationFrameClient(t, "stream_history_v1", `{"status":"OK","next_version":3,"head_version":3,"completeness":"complete","has_more":true}`, batch)
	result, err := client.ReadStreamHistory(context.Background(), HistoryRequest{Topic: "orders", Key: "order-1", FromVersion: 2, MaxRecords: 10, MaxBytes: 1024})
	require.NoError(t, err)
	require.Equal(t, uint64(3), result.NextVersion)
	require.Equal(t, []StreamEvent{{Version: 2, Offset: 9, Type: "OrderPaid", SchemaVersion: 3, Payload: `{"state":"paid"}`}}, result.Events)
	require.Equal(t, []string{"NEGOTIATE version=1 features=stream_history_v1 require_features=true", "READ_STREAM_HISTORY topic=orders key=order-1 from_version=2 max_records=10 max_bytes=1024"}, receiveObservationCommands(t, commands))
}

func TestAdminClientObservationRejectsInvalidRequestsBeforeConnecting(t *testing.T) {
	client := &AdminClient{}
	_, err := client.BrowseMessages(context.Background(), BrowseRequest{Topic: "orders", Partition: -1, MaxRecords: 1, MaxBytes: 1})
	require.ErrorContains(t, err, "invalid browse request")
	_, err = client.ReadStreamHistory(context.Background(), HistoryRequest{Topic: "orders", Key: "order-1", MaxRecords: 1, MaxBytes: 1})
	require.ErrorContains(t, err, "invalid history request")
	_, err = client.GroupOffsets(context.Background(), "invalid group", "orders")
	require.ErrorContains(t, err, "invalid group")
}

func observationAdminClient(t *testing.T, responses []string) (*AdminClient, <-chan []string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	commands := make(chan []string, 1)
	go func() {
		defer close(commands)
		defer func() { _ = listener.Close() }()
		seen := make([]string, 0, len(responses))
		for _, response := range responses {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			connection, request, command, requestErr := acceptWireTestRequest(conn)
			if requestErr == nil {
				requestErr = writeWireTestResponse(connection, request, response)
			}
			_ = conn.Close()
			if requestErr != nil {
				return
			}
			seen = append(seen, command)
		}
		commands <- seen
	}()
	client, err := NewAdminClient(&AdminConfig{BrokerAddrs: []string{listener.Addr().String()}, RequestTimeoutMS: 1000})
	require.NoError(t, err)
	return client, commands
}

func observationFrameClient(t *testing.T, feature, envelope string, batch []byte) (*AdminClient, <-chan []string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	commands := make(chan []string, 1)
	go func() {
		defer close(commands)
		defer func() { _ = listener.Close() }()
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		connection, handshakeErr := wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone})
		if handshakeErr != nil {
			return
		}
		negotiation, readErr := connection.ReadFrame()
		if readErr != nil || negotiation.Command != wire.CommandNegotiate {
			return
		}
		negotiated, decodeErr := wire.DecodeCommandPayload(negotiation.Payload)
		if decodeErr != nil || negotiated.Fields["features"] != feature || negotiated.Fields["require_features"] != "true" {
			return
		}
		if writeErr := writeWireTestResponse(connection, negotiation, "OK protocol_version=1 enabled="+feature+" unsupported="); writeErr != nil {
			return
		}
		request, readErr := connection.ReadFrame()
		if readErr != nil {
			return
		}
		payload, decodeErr := wire.DecodeCommandPayload(request.Payload)
		if decodeErr != nil {
			return
		}
		command, renderErr := wire.RenderCommand(request.Command, payload)
		if renderErr != nil {
			return
		}
		if writeErr := connection.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusOK, RequestID: request.RequestID, Payload: []byte(envelope)}); writeErr != nil {
			return
		}
		if writeErr := connection.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusOK, RequestID: request.RequestID, Payload: batch}); writeErr != nil {
			return
		}
		commands <- []string{fmt.Sprintf("NEGOTIATE version=%s features=%s require_features=%s", negotiated.Fields["version"], negotiated.Fields["features"], negotiated.Fields["require_features"]), command}
	}()
	client, err := NewAdminClient(&AdminConfig{BrokerAddrs: []string{listener.Addr().String()}, RequestTimeoutMS: 1000})
	require.NoError(t, err)
	return client, commands
}

func receiveObservationCommands(t *testing.T, commands <-chan []string) []string {
	t.Helper()
	select {
	case values := <-commands:
		return values
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for observation test server")
		return nil
	}
}
