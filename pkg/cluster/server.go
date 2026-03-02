package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"

	"github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/util"
)

type joinRequest struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
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

type ClusterServer struct {
	sd controller.ServiceDiscovery
}

func NewClusterServer(sd controller.ServiceDiscovery) *ClusterServer {
	return &ClusterServer{sd: sd}
}

func (h *ClusterServer) Start(addr string) (net.Listener, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	util.Info("TCP cluster server listening at %s", addr)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
					return // graceful shutdown
				}
				util.Error("cluster accept error: %v", err)
				continue
			}
			go h.handleConnection(conn)
		}
	}()
	return listener, nil
}

func (h *ClusterServer) handleConnection(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	for {
		data, err := util.ReadWithLength(conn)
		if err != nil {
			return
		}

		topic, payload, err := util.DecodeMessage(data)
		if err != nil {
			util.Error("⚠️ Decode error: %v", err)
			return
		}

		util.Debug("cluster-server received connection: topic %s, payload %s", topic, payload)

		if strings.HasPrefix(payload, "JOIN_CLUSTER ") {
			h.handleJoinCluster(conn, payload)
		} else if strings.HasPrefix(payload, "LEAVE_CLUSTER ") {
			h.handleLeaveCluster(conn, payload)
		} else if payload == "LIST_CLUSTER" {
			h.handleListCluster(conn)
		}
	}
}

func (h *ClusterServer) handleJoinCluster(conn net.Conn, payload string) {
	jsonData := strings.TrimPrefix(payload, "JOIN_CLUSTER ")
	if jsonData == "" {
		h.writeErrorResponse(conn, "missing join data")
		return
	}

	var req joinRequest
	if err := json.Unmarshal([]byte(jsonData), &req); err != nil {
		h.writeErrorResponse(conn, "invalid json")
		return
	}

	if req.NodeID == "" || req.Address == "" {
		h.writeErrorResponse(conn, "missing node_id or address")
		return
	}

	leader, err := h.sd.AddNode(req.NodeID, req.Address)
	if err != nil {
		resp := joinResponse{
			Success: false,
			Leader:  leader,
			Error:   err.Error(),
		}
		h.writeResponse(conn, resp)
		return
	}

	resp := joinResponse{Success: true, Leader: leader}
	h.writeResponse(conn, resp)
}

func (h *ClusterServer) handleLeaveCluster(conn net.Conn, payload string) {
	jsonData := strings.TrimPrefix(payload, "LEAVE_CLUSTER ")
	if jsonData == "" {
		h.writeErrorResponse(conn, "missing leave data")
		return
	}

	var req leaveReq
	if err := json.Unmarshal([]byte(jsonData), &req); err != nil || req.NodeID == "" {
		h.writeErrorResponse(conn, "invalid request")
		return
	}

	_, err := h.sd.RemoveNode(req.NodeID)
	if err != nil {
		h.writeErrorResponse(conn, err.Error())
		return
	}

	h.writeResponse(conn, leaveResp{Success: true})
}

func (h *ClusterServer) handleListCluster(conn net.Conn) {
	nodes, err := h.sd.DiscoverBrokers()
	if err != nil {
		h.writeErrorResponse(conn, fmt.Sprintf("discovery failed: %v", err))
		return
	}
	h.writeResponse(conn, nodes)
}

func (h *ClusterServer) writeResponse(conn net.Conn, resp interface{}) {
	data, err := json.Marshal(resp)
	if err != nil {
		util.Error("cluster response marshal error: %v", err)
		return
	}

	if err := util.WriteWithLength(conn, data); err != nil {
		util.Error("cluster response write error: %v", err)
	}
}

func (h *ClusterServer) writeErrorResponse(conn net.Conn, errMsg string) {
	resp := joinResponse{Success: false, Error: errMsg}
	h.writeResponse(conn, resp)
}
