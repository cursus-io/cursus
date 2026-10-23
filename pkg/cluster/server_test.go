package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	clusterClient "github.com/cursus-io/cursus/pkg/cluster/client"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/recovery"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockServiceDiscovery struct {
	mock.Mock
}

type heartbeatValidatingServiceDiscovery struct {
	*MockServiceDiscovery
	validationErr error
}

func (m *heartbeatValidatingServiceDiscovery) ValidateHeartbeat(nodeID, incarnationID string) error {
	m.Called(nodeID, incarnationID)
	return m.validationErr
}

func (m *MockServiceDiscovery) Register() error   { return m.Called().Error(0) }
func (m *MockServiceDiscovery) Deregister() error { return m.Called().Error(0) }
func (m *MockServiceDiscovery) DiscoverBrokers() ([]fsm.BrokerInfo, error) {
	args := m.Called()
	return args.Get(0).([]fsm.BrokerInfo), args.Error(1)
}
func (m *MockServiceDiscovery) AddNode(nodeID string, addr string) (string, error) {
	args := m.Called(nodeID, addr)
	return args.String(0), args.Error(1)
}
func (m *MockServiceDiscovery) AddNodeWithTransactionCoordinatorShards(nodeID string, addr string, shardCount int) (string, error) {
	args := m.Called(nodeID, addr, shardCount)
	return args.String(0), args.Error(1)
}

func (m *MockServiceDiscovery) AddNodeWithRegistration(id, addr string, shards int, info *fsm.BrokerInfo) (string, error) {
	args := m.Called(id, addr, shards, info)
	return args.String(0), args.Error(1)
}

func TestClusterJoinPreservesProcessRegistration(t *testing.T) {
	info := fsm.BrokerInfo{ID: "new", Addr: "new:7000", IncarnationID: "current-process", RecoveryProtocol: 2, LifecycleProtocol: 6, TransactionCoordinatorShards: 7}
	data, err := json.Marshal(info)
	require.NoError(t, err)
	for _, change := range []string{"none", "identity", "shards", "trailing", "unknown"} {
		t.Run(change, func(t *testing.T) {
			msd := new(MockServiceDiscovery)
			server := NewClusterServer(msd)
			fields := map[string]string{"node_id": "new", "address": "new:7000", "transaction_coordinator_shards": "7", "registration": string(data)}
			switch change {
			case "none":
				msd.On("AddNodeWithRegistration", "new", "new:7000", 7, &info).Return("leader", nil).Once()
			case "identity":
				fields["node_id"] = "other"
			case "shards":
				fields["transaction_coordinator_shards"] = "8"
			case "trailing":
				fields["registration"] += " {}"
			case "unknown":
				fields["registration"] = `{"unknown":true}`
			}
			_, responseErr := server.handleJoinCluster(wire.CommandPayload{Fields: fields})
			if change == "none" {
				require.Nil(t, responseErr)
			} else {
				require.NotNil(t, responseErr)
			}
			msd.AssertExpectations(t)
		})
	}
}
func (m *MockServiceDiscovery) RemoveNode(nodeID string) (string, error) {
	args := m.Called(nodeID)
	return args.String(0), args.Error(1)
}
func (m *MockServiceDiscovery) UpdateHeartbeat(nodeID string) { m.Called(nodeID) }
func (m *MockServiceDiscovery) HandleHeartbeat(nodeID string, proofs []fsm.ISRCatchupProof) error {
	return m.Called(nodeID, proofs).Error(0)
}
func (m *MockServiceDiscovery) FetchReplicaCatchup(request fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
	args := m.Called(request)
	return args.Get(0).(fsm.ReplicaCatchupBatch), args.Error(1)
}
func (m *MockServiceDiscovery) StartReconciler(ctx context.Context) { m.Called(ctx) }
func (m *MockServiceDiscovery) Reconcile()                          { m.Called() }

func clusterRoundTrip(t *testing.T, address string, command wire.Command, fields map[string]string) wire.Frame {
	t.Helper()
	conn, err := net.Dial("tcp", address)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	connection, err := wire.ClientHandshake(conn, []wire.Compression{wire.CompressionNone})
	require.NoError(t, err)
	payload, err := wire.EncodeCommandPayload(wire.CommandPayload{Fields: fields})
	require.NoError(t, err)
	require.NoError(t, connection.WriteFrame(wire.Frame{
		Kind: wire.KindRequest, Command: command, RequestID: 1, Payload: payload,
	}))
	response, err := connection.ReadFrame()
	require.NoError(t, err)
	require.Equal(t, command, response.Command)
	require.Equal(t, uint64(1), response.RequestID)
	return response
}

func TestFitReplicaCatchupBatchPreservesFirstUnsentOffset(t *testing.T) {
	batch := fsm.ReplicaCatchupBatch{
		StartOffset: 0, EndOffset: 20, CommittedHWM: 20, Compacted: true, Verified: true,
		Messages: []types.Message{
			{Offset: 2, Payload: "first"},
			{Offset: 4, Payload: "second"},
			{Offset: 8, Payload: "third"},
			{Offset: 12, Payload: "fourth"},
		},
	}
	batch, err := fsm.SealReplicaCatchupBatch(batch)
	require.NoError(t, err)
	expectedPrefix := batch
	expectedPrefix.EndOffset = 8
	expectedPrefix.Verified = false
	expectedPrefix.Messages = expectedPrefix.Messages[:2]
	expectedPrefix, err = fsm.SealReplicaCatchupBatch(expectedPrefix)
	require.NoError(t, err)
	encodedPrefix, err := json.Marshal(expectedPrefix)
	require.NoError(t, err)

	fitted, err := fitReplicaCatchupBatchToLimit(batch, len(encodedPrefix))
	require.NoError(t, err)
	require.True(t, fitted.Compacted)
	require.False(t, fitted.Verified)
	require.NoError(t, fsm.ValidateReplicaCatchupBatchDigest(fitted))
	require.Equal(t, uint64(20), fitted.CommittedHWM)
	require.Equal(t, uint64(8), fitted.EndOffset)
	require.Equal(t, []uint64{2, 4}, []uint64{fitted.Messages[0].Offset, fitted.Messages[1].Offset})
}

func TestClusterServer_Join(t *testing.T) {
	msd := new(MockServiceDiscovery)
	server := NewClusterServer(msd)

	ln, err := server.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	addr := ln.Addr().String()

	t.Run("Join Success", func(t *testing.T) {
		msd.On("AddNode", "node1", "127.0.0.1:9001").Return("leader-addr", nil).Once()

		response := clusterRoundTrip(t, addr, wire.CommandJoinCluster, map[string]string{
			"node_id": "node1", "address": "127.0.0.1:9001",
		})
		require.Equal(t, wire.StatusOK, response.Status)
		var resp joinResponse
		err = json.Unmarshal(response.Payload, &resp)
		assert.NoError(t, err)

		assert.True(t, resp.Success)
		assert.Equal(t, "leader-addr", resp.Leader)
		msd.AssertExpectations(t)
	})

	t.Run("Join Fail", func(t *testing.T) {
		msd.On("AddNode", "node2", "127.0.0.1:9002").Return("", fmt.Errorf("error")).Once()

		response := clusterRoundTrip(t, addr, wire.CommandJoinCluster, map[string]string{
			"node_id": "node2", "address": "127.0.0.1:9002",
		})
		require.Equal(t, wire.StatusError, response.Status)
		brokerError, err := wire.DecodeError(response.Payload)
		require.NoError(t, err)
		assert.Equal(t, "cluster_join_failed", brokerError.Code)
		msd.AssertExpectations(t)
	})

	t.Run("Join Passes Transaction Coordinator Shards", func(t *testing.T) {
		msd.On("AddNodeWithTransactionCoordinatorShards", "node3", "127.0.0.1:9003", 7).Return("leader-addr", nil).Once()
		response := clusterRoundTrip(t, addr, wire.CommandJoinCluster, map[string]string{
			"node_id": "node3", "address": "127.0.0.1:9003", "transaction_coordinator_shards": "7",
		})
		require.Equal(t, wire.StatusOK, response.Status)
		msd.AssertExpectations(t)
	})
}

func TestClusterServer_Heartbeat(t *testing.T) {
	msd := new(MockServiceDiscovery)
	server := NewClusterServer(msd)

	ln, err := server.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	addr := ln.Addr().String()

	msd.On("HandleHeartbeat", "node-hb", []fsm.ISRCatchupProof(nil)).Return(nil).Once()
	msd.On("UpdateHeartbeat", "node-hb").Return().Once()

	response := clusterRoundTrip(t, addr, wire.CommandHeartbeatCluster, map[string]string{"node_id": "node-hb"})
	require.Equal(t, wire.StatusOK, response.Status)
	assert.Contains(t, string(response.Payload), "true")
	msd.AssertExpectations(t)
}

func TestClusterServer_HeartbeatCarriesCatchupProofs(t *testing.T) {
	msd := new(MockServiceDiscovery)
	server := NewClusterServer(msd)
	listener, err := server.Start("127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	proofs := []fsm.ISRCatchupProof{{
		Topic: "orders", Partition: 0, BrokerID: "node-hb",
		CommittedHWM: 3, LocalLEO: 3, LocalHWM: 3, LeaderEpoch: 2, LifecycleEpoch: 1,
	}}
	msd.On("HandleHeartbeat", "node-hb", proofs).Return(nil).Once()
	msd.On("UpdateHeartbeat", "node-hb").Return().Once()
	payload, err := json.Marshal(proofs)
	require.NoError(t, err)
	response := clusterRoundTrip(t, listener.Addr().String(), wire.CommandHeartbeatCluster, map[string]string{
		"node_id": "node-hb", "catchup_proofs": string(payload),
	})
	require.Equal(t, wire.StatusOK, response.Status)
	require.Contains(t, string(response.Payload), `"success":true`)
	msd.AssertExpectations(t)
}

func TestClusterServerRejectsFencedHeartbeatBeforeISRProcessing(t *testing.T) {
	base := new(MockServiceDiscovery)
	service := &heartbeatValidatingServiceDiscovery{
		MockServiceDiscovery: base,
		validationErr:        fmt.Errorf("broker_heartbeat_fenced"),
	}
	service.On("ValidateHeartbeat", "node-hb", "stale-incarnation").Return().Once()
	server := NewClusterServer(service)
	listener, err := server.Start("127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	response := clusterRoundTrip(t, listener.Addr().String(), wire.CommandHeartbeatCluster, map[string]string{
		"node_id": "node-hb", "incarnation_id": "stale-incarnation",
	})
	require.Equal(t, wire.StatusError, response.Status)
	base.AssertNotCalled(t, "HandleHeartbeat", mock.Anything, mock.Anything)
	base.AssertNotCalled(t, "UpdateHeartbeat", mock.Anything)
	service.AssertExpectations(t)
}

func TestClusterServer_FetchesAuthenticatedReplicaCatchupBatch(t *testing.T) {
	msd := new(MockServiceDiscovery)
	server := NewClusterServer(msd)
	listener, err := server.Start("127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	request := fsm.ReplicaCatchupRequest{
		Topic: "orders", Partition: 0, BrokerID: "node-2", NextOffset: 1, CommittedHWM: 2,
		Leader: "node-1", SourceBroker: "node-1", LeaderEpoch: 4, LifecycleEpoch: 1, MaxRecords: 1,
	}
	batch := fsm.ReplicaCatchupBatch{
		Topic: "orders", Partition: 0, BrokerID: "node-2", StartOffset: 1, CommittedHWM: 2,
		Leader: "node-1", SourceBroker: "node-1", LeaderEpoch: 4, LifecycleEpoch: 1,
		Messages: []types.Message{{Offset: 1, Payload: "backfill"}},
	}
	batch, err = fsm.SealReplicaCatchupBatch(batch)
	require.NoError(t, err)
	msd.On("FetchReplicaCatchup", request).Return(batch, nil).Once()
	encoded, err := json.Marshal(request)
	require.NoError(t, err)
	response := clusterRoundTrip(t, listener.Addr().String(), wire.CommandReplicaCatchup, map[string]string{
		"request": string(encoded),
	})
	require.Equal(t, wire.StatusOK, response.Status)
	var decoded fsm.ReplicaCatchupBatch
	require.NoError(t, json.Unmarshal(response.Payload, &decoded))
	require.Equal(t, batch, decoded)
	msd.AssertExpectations(t)
}

func TestClusterCatchupPreservesRetryableAdmissionError(t *testing.T) {
	sd := new(MockServiceDiscovery)
	server := NewClusterServer(sd)
	listener, err := server.Start("127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	request := fsm.ReplicaCatchupRequest{Topic: "orders", BrokerID: "replica", SourceBroker: "source"}
	sd.On("FetchReplicaCatchup", request).Return(fsm.ReplicaCatchupBatch{}, fmt.Errorf("source quota: %w", recovery.ErrBusy)).Once()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = clusterClient.NewTCPClusterClient().FetchReplicaCatchup(ctx, "127.0.0.1", listener.Addr().(*net.TCPAddr).Port, request)
	var remote *wire.BrokerError
	require.ErrorAs(t, err, &remote)
	require.Equal(t, "recovery_busy", remote.Code)
	require.Equal(t, wire.ErrorClassAvailability, remote.Class)
	require.True(t, remote.Retryable)
	require.Equal(t, internalClusterError(recovery.ErrBusy), bootstrapTransferError(recovery.ErrBusy))
	sd.AssertExpectations(t)
}

func TestClusterServer_List(t *testing.T) {
	msd := new(MockServiceDiscovery)
	server := NewClusterServer(msd)

	ln, err := server.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	addr := ln.Addr().String()

	msd.On("DiscoverBrokers").Return([]fsm.BrokerInfo{{ID: "n1"}}, nil).Once()

	response := clusterRoundTrip(t, addr, wire.CommandListCluster, nil)
	require.Equal(t, wire.StatusOK, response.Status)
	assert.Contains(t, string(response.Payload), "n1")
	msd.AssertExpectations(t)
}

type contextCatchupDiscovery struct {
	*MockServiceDiscovery
	observed context.Context
}

func (s *contextCatchupDiscovery) FetchReplicaCatchupContext(ctx context.Context, r fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
	s.observed = ctx
	return fsm.ReplicaCatchupBatch{}, nil
}
func TestCatchupServerUsesBoundedContextServicePath(t *testing.T) {
	sd := &contextCatchupDiscovery{MockServiceDiscovery: new(MockServiceDiscovery)}
	sd.On("FetchReplicaCatchup", mock.Anything).Return(fsm.ReplicaCatchupBatch{}, nil)
	h := NewClusterServer(sd)
	h.requestTimeout = time.Second
	_, err := h.handleReplicaCatchup(wire.CommandPayload{Fields: map[string]string{"request": "{}"}})
	require.Nil(t, err)
	require.NotNil(t, sd.observed, "context-aware service path must be used")
	_, bounded := sd.observed.Deadline()
	require.True(t, bounded)
	require.ErrorIs(t, sd.observed.Err(), context.Canceled, "handler must release request timer")
}

type deadlineRecordingConn struct {
	net.Conn
	deadlines chan time.Time
}

func (c deadlineRecordingConn) SetDeadline(t time.Time) error {
	c.deadlines <- t
	return c.Conn.SetDeadline(t)
}

type dispatchedContextDiscovery struct {
	*MockServiceDiscovery
	contexts chan context.Context
}

func (s *dispatchedContextDiscovery) FetchReplicaCatchupContext(ctx context.Context, _ fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
	s.contexts <- ctx
	return fsm.ReplicaCatchupBatch{}, nil
}
func TestCatchupDispatchUsesSocketRequestDeadline(t *testing.T) {
	sd := &dispatchedContextDiscovery{MockServiceDiscovery: new(MockServiceDiscovery), contexts: make(chan context.Context, 1)}
	h := NewClusterServer(sd)
	server, client := net.Pipe()
	defer func() { _ = client.Close() }()
	deadlines := make(chan time.Time, 4)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		h.handleConnection(deadlineRecordingConn{Conn: server, deadlines: deadlines})
	}()
	connection, err := wire.ClientHandshake(client, []wire.Compression{wire.CompressionNone})
	require.NoError(t, err)
	<-deadlines // handshake
	requestDeadline := <-deadlines
	payload, err := wire.EncodeCommandPayload(wire.CommandPayload{Fields: map[string]string{"request": "{}"}})
	require.NoError(t, err)
	require.NoError(t, connection.WriteFrame(wire.Frame{Kind: wire.KindRequest, Command: wire.CommandReplicaCatchup, RequestID: 1, Payload: payload}))
	observed := <-sd.contexts
	actual, ok := observed.Deadline()
	require.True(t, ok)
	require.Equal(t, requestDeadline, actual)
	_, err = connection.ReadFrame()
	require.NoError(t, err)
	require.NoError(t, client.Close())
	<-stopped
}
