package cluster

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/util"
)

type joinRequest struct {
	NodeID                       string `json:"node_id"`
	Address                      string `json:"address"`
	TransactionCoordinatorShards int    `json:"transaction_coordinator_shards,omitempty"`
}

type joinResponse struct {
	Success bool   `json:"success"`
	Leader  string `json:"leader,omitempty"`
	Error   string `json:"error,omitempty"`
}

type leaveReq struct {
	NodeID string `json:"node_id"`
}

type leaveResp struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

type heartbeatRequest struct {
	NodeID        string                `json:"node_id"`
	CatchupProofs []fsm.ISRCatchupProof `json:"catchup_proofs,omitempty"`
	IncarnationID string                `json:"incarnation_id,omitempty"`
}

type ClusterServer struct {
	sd             controller.ServiceDiscovery
	authTokens     []string
	tlsConfig      *tls.Config
	connectionSlot chan struct{}
	requestTimeout time.Duration
	ctx            context.Context
	cancel         context.CancelFunc
	lifecycleMu    sync.Mutex
	closed         bool
	listeners      map[net.Listener]struct{}
	connections    map[net.Conn]struct{}
	workers        sync.WaitGroup
	closeDone      chan struct{}
}

func NewClusterServer(sd controller.ServiceDiscovery) *ClusterServer {
	return NewSecureClusterServer(sd, "", nil)
}

func (h *ClusterServer) Start(addr string) (net.Listener, error) {
	h.lifecycleMu.Lock()
	closed := h.closed
	h.lifecycleMu.Unlock()
	if closed {
		return nil, net.ErrClosed
	}
	listener, err := listenCluster(addr, h.tlsConfig)
	if err != nil {
		return nil, err
	}

	h.lifecycleMu.Lock()
	if h.closed {
		h.lifecycleMu.Unlock()
		_ = listener.Close()
		return nil, net.ErrClosed
	}
	h.listeners[listener] = struct{}{}
	h.workers.Add(1)
	h.lifecycleMu.Unlock()
	util.Info("TCP cluster server listening at %s (TLS=%v)", addr, h.tlsConfig != nil)

	go func() {
		defer h.workers.Done()
		defer func() { h.lifecycleMu.Lock(); delete(h.listeners, listener); h.lifecycleMu.Unlock() }()
		for {
			conn, err := listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return // graceful shutdown
				}
				util.Error("cluster accept error: %v", err)
				continue
			}
			if h.admitConnection(conn) {
				go h.serveConnection(conn)
			} else {
				_ = conn.Close()
			}
		}
	}()
	return listener, nil
}

func (h *ClusterServer) handleConnection(conn net.Conn) {
	if !h.admitConnection(conn) {
		_ = conn.Close()
		return
	}
	h.serveConnection(conn)
}

func (h *ClusterServer) serveConnection(conn net.Conn) {
	defer h.workers.Done()
	defer func() { h.lifecycleMu.Lock(); delete(h.connections, conn); h.lifecycleMu.Unlock(); <-h.connectionSlot }()
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(h.requestTimeout)); err != nil {
		return
	}
	connection, err := wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone})
	if err != nil {
		util.Debug("cluster Wire v2 handshake failed: %v", err)
		return
	}

	for {
		deadline := time.Now().Add(h.requestTimeout)
		if err := conn.SetDeadline(deadline); err != nil {
			return
		}
		request, err := connection.ReadFrame()
		if err != nil {
			return
		}
		if request.Kind != wire.KindRequest || request.Status != wire.StatusNone || request.RequestID == 0 {
			return
		}
		payload, err := wire.DecodeCommandPayload(request.Payload)
		if err != nil {
			h.writeErrorResponse(connection, request, wire.ErrorPayload{
				Code: "invalid_cluster_request", Class: wire.ErrorClassValidation, Message: err.Error(),
			})
			continue
		}
		if !h.authenticate(payload) {
			h.writeErrorResponse(connection, request, wire.ErrorPayload{
				Code: "cluster_unauthorized", Class: wire.ErrorClassAuthorization, Message: "invalid cluster auth token",
			})
			return
		}
		delete(payload.Fields, "auth_token")
		util.Debug("cluster-server received Wire v2 command: %s", request.Command)

		var response any
		var requestError *wire.ErrorPayload
		switch request.Command {
		case wire.CommandJoinCluster:
			response, requestError = h.handleJoinCluster(payload)
		case wire.CommandLeaveCluster:
			response, requestError = h.handleLeaveCluster(payload)
		case wire.CommandHeartbeatCluster:
			response, requestError = h.handleHeartbeatCluster(payload)
		case wire.CommandReplicaCatchup:
			ctx, cancel := context.WithDeadline(h.ctx, deadline)
			response, requestError = h.handleReplicaCatchupContext(ctx, payload)
			cancel()
		case wire.CommandBootstrapBegin, wire.CommandBootstrapChunk, wire.CommandBootstrapRelease:
			response, requestError = h.handleBootstrapTransfer(conn, request.Command, payload, deadline)
		case wire.CommandListCluster:
			response, requestError = h.handleListCluster()
		default:
			requestError = &wire.ErrorPayload{
				Code: "unsupported_cluster_command", Class: wire.ErrorClassValidation,
				Message: fmt.Sprintf("unsupported cluster command %s", request.Command),
			}
		}
		if requestError != nil {
			h.writeErrorResponse(connection, request, *requestError)
			continue
		}
		h.writeResponse(connection, request, response)
	}
}

func (h *ClusterServer) handleHeartbeatCluster(payload wire.CommandPayload) (any, *wire.ErrorPayload) {
	req := heartbeatRequest{NodeID: payload.Fields["node_id"], IncarnationID: payload.Fields["incarnation_id"]}
	if encoded := payload.Fields["catchup_proofs"]; encoded != "" {
		if err := json.Unmarshal([]byte(encoded), &req.CatchupProofs); err != nil {
			return nil, validationError("invalid heartbeat catchup proofs")
		}
	}
	if req.NodeID == "" {
		return nil, validationError("node_id is required")
	}

	util.Debug("ClusterServer: Received heartbeat from %s", req.NodeID)
	if validator, ok := h.sd.(interface {
		ValidateHeartbeat(string, string) error
	}); ok {
		if err := validator.ValidateHeartbeat(req.NodeID, req.IncarnationID); err != nil {
			return nil, internalClusterError(err)
		}
	}
	if err := h.sd.HandleHeartbeat(req.NodeID, req.CatchupProofs); err != nil {
		return nil, internalClusterError(err)
	}
	if incarnationAware, ok := h.sd.(interface{ UpdateHeartbeatWithIncarnation(string, string) }); ok && req.IncarnationID != "" {
		incarnationAware.UpdateHeartbeatWithIncarnation(req.NodeID, req.IncarnationID)
	} else {
		h.sd.UpdateHeartbeat(req.NodeID)
	}
	return map[string]bool{"success": true}, nil
}

func (h *ClusterServer) handleReplicaCatchup(payload wire.CommandPayload) (any, *wire.ErrorPayload) {
	ctx, cancel := context.WithTimeout(h.ctx, h.requestTimeout)
	defer cancel()
	return h.handleReplicaCatchupContext(ctx, payload)
}

func (h *ClusterServer) handleReplicaCatchupContext(ctx context.Context, payload wire.CommandPayload) (any, *wire.ErrorPayload) {
	encoded := payload.Fields["request"]
	if encoded == "" {
		return nil, validationError("replica catch-up request is required")
	}
	var request fsm.ReplicaCatchupRequest
	if err := json.Unmarshal([]byte(encoded), &request); err != nil {
		return nil, validationError("invalid replica catch-up request")
	}
	var batch fsm.ReplicaCatchupBatch
	var err error
	if contextual, ok := h.sd.(controller.ContextReplicaCatchupSource); ok {
		batch, err = contextual.FetchReplicaCatchupContext(ctx, request)
	} else {
		// Preserve existing third-party ServiceDiscovery implementations.
		batch, err = h.sd.FetchReplicaCatchup(request)
	}
	if err != nil {
		return nil, internalClusterError(err)
	}
	batch, err = fitReplicaCatchupBatch(batch)
	if err != nil {
		return nil, internalClusterError(err)
	}
	return batch, nil
}

func fitReplicaCatchupBatch(batch fsm.ReplicaCatchupBatch) (fsm.ReplicaCatchupBatch, error) {
	return fitReplicaCatchupBatchToLimit(batch, wire.MaxFramePayload)
}

func fitReplicaCatchupBatchToLimit(batch fsm.ReplicaCatchupBatch, limit int) (fsm.ReplicaCatchupBatch, error) {
	for {
		data, err := json.Marshal(batch)
		if err != nil {
			return fsm.ReplicaCatchupBatch{}, err
		}
		if len(data) <= limit {
			return batch, nil
		}
		if len(batch.Messages) <= 1 {
			return fsm.ReplicaCatchupBatch{}, fmt.Errorf("replica catch-up record exceeds Wire v2 frame limit")
		}
		messageCount := len(batch.Messages) / 2
		// The first unsent retained record is the exclusive boundary of this
		// fitted logical range. Keeping the original EndOffset would skip it.
		batch.EndOffset = batch.Messages[messageCount].Offset
		batch.Verified = false
		batch.Messages = batch.Messages[:messageCount]
		batch, err = fsm.SealReplicaCatchupBatch(batch)
		if err != nil {
			return fsm.ReplicaCatchupBatch{}, err
		}
	}
}

func (h *ClusterServer) handleJoinCluster(payload wire.CommandPayload) (any, *wire.ErrorPayload) {
	req := joinRequest{NodeID: payload.Fields["node_id"], Address: payload.Fields["address"]}
	if shardText := payload.Fields["transaction_coordinator_shards"]; shardText != "" {
		shardCount, err := strconv.Atoi(shardText)
		if err != nil || shardCount <= 0 {
			return nil, validationError("transaction_coordinator_shards must be a positive integer")
		}
		req.TransactionCoordinatorShards = shardCount
	}
	if req.NodeID == "" || req.Address == "" {
		return nil, validationError("missing node_id or address")
	}

	var leader string
	var err error
	if registration := payload.Fields["registration"]; registration != "" {
		if len(registration) > 16*1024 {
			return nil, validationError("registration is too large")
		}
		var info fsm.BrokerInfo
		decoder := json.NewDecoder(strings.NewReader(registration))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&info); err != nil || decoder.Decode(new(any)) != io.EOF {
			return nil, validationError("invalid registration")
		}
		if info.ID != req.NodeID || info.Addr != req.Address || info.TransactionCoordinatorShards != req.TransactionCoordinatorShards {
			return nil, validationError("registration does not match JOIN identity or shard count")
		}
		extended, ok := h.sd.(interface {
			AddNodeWithRegistration(string, string, int, *fsm.BrokerInfo) (string, error)
		})
		if !ok {
			return nil, internalClusterError(fmt.Errorf("service discovery does not support process registration"))
		}
		leader, err = extended.AddNodeWithRegistration(req.NodeID, req.Address, req.TransactionCoordinatorShards, &info)
	} else if req.TransactionCoordinatorShards > 0 {
		extended, ok := h.sd.(interface {
			AddNodeWithTransactionCoordinatorShards(string, string, int) (string, error)
		})
		if !ok {
			return nil, internalClusterError(fmt.Errorf("service discovery does not support explicit transaction coordinator shards"))
		}
		leader, err = extended.AddNodeWithTransactionCoordinatorShards(req.NodeID, req.Address, req.TransactionCoordinatorShards)
	} else {
		leader, err = h.sd.AddNode(req.NodeID, req.Address)
	}
	if err != nil {
		return nil, &wire.ErrorPayload{
			Code: "cluster_join_failed", Class: wire.ErrorClassConflict, Message: err.Error(),
			Fields: map[string]string{"leader": leader},
		}
	}
	return joinResponse{Success: true, Leader: leader}, nil
}

func (h *ClusterServer) handleLeaveCluster(payload wire.CommandPayload) (any, *wire.ErrorPayload) {
	req := leaveReq{NodeID: payload.Fields["node_id"]}
	if req.NodeID == "" {
		return nil, validationError("node_id is required")
	}

	_, err := h.sd.RemoveNode(req.NodeID)
	if err != nil {
		return nil, internalClusterError(err)
	}
	return leaveResp{Success: true}, nil
}

func (h *ClusterServer) handleListCluster() (any, *wire.ErrorPayload) {
	nodes, err := h.sd.DiscoverBrokers()
	if err != nil {
		return nil, internalClusterError(fmt.Errorf("discovery failed: %w", err))
	}
	return nodes, nil
}

func (h *ClusterServer) writeResponse(connection *wire.Connection, request wire.Frame, resp any) {
	data, err := json.Marshal(resp)
	if err != nil {
		util.Error("cluster response marshal error: %v", err)
		return
	}
	if err := connection.WriteFrame(wire.Frame{
		Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusOK,
		RequestID: request.RequestID, Payload: data,
	}); err != nil {
		util.Error("cluster response write error: %v", err)
	}
}

func (h *ClusterServer) writeErrorResponse(connection *wire.Connection, request wire.Frame, response wire.ErrorPayload) {
	payload, err := wire.EncodeError(response)
	if err != nil {
		util.Error("cluster error response encode error: %v", err)
		return
	}
	if err := connection.WriteFrame(wire.Frame{
		Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusError,
		RequestID: request.RequestID, Payload: payload,
	}); err != nil {
		util.Error("cluster error response write error: %v", err)
	}
}

func validationError(message string) *wire.ErrorPayload {
	return &wire.ErrorPayload{Code: "invalid_cluster_request", Class: wire.ErrorClassValidation, Message: message}
}

func internalClusterError(err error) *wire.ErrorPayload {
	return &wire.ErrorPayload{Code: "cluster_request_failed", Class: wire.ErrorClassInternal, Message: err.Error()}
}
