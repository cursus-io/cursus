package controller

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/cursus-io/cursus/util"
)

type LocalProcessor interface {
	ProcessCommand(cmd string) string
}

type ClusterRouter struct {
	LocalAddr      string
	brokerID       string
	rm             RaftManager
	clientPort     int
	timeout        time.Duration
	localProcessor LocalProcessor
}

func NewClusterRouter(brokerID, localAddr string, processor LocalProcessor, rm RaftManager, clientPort int) *ClusterRouter {
	return &ClusterRouter{
		brokerID:       brokerID,
		LocalAddr:      localAddr,
		rm:             rm,
		clientPort:     clientPort,
		timeout:        5 * time.Second,
		localProcessor: processor,
	}
}

func (r *ClusterRouter) getLeader() (string, error) {
	leader := r.rm.GetLeaderAddress()
	if leader == "" {
		return "", fmt.Errorf("no leader available from Raft")
	}
	return leader, nil
}

func (r *ClusterRouter) ForwardToLeader(req string) (string, error) {
	leader, err := r.getLeader()
	if r.rm.IsLeader() || (leader != "" && leader == r.LocalAddr) {
		return r.processLocally(req), nil
	}

	if err != nil || leader == "" {
		util.Debug("Leader unknown, attempting local processing as fallback")
		return r.processLocally(req), nil
	}

	host, _, splitErr := net.SplitHostPort(leader)
	if splitErr != nil {
		return "", fmt.Errorf("invalid leader address format: %w", splitErr)
	}

	clientLeaderAddr := fmt.Sprintf("%s:%d", host, r.clientPort)
	resp, err := r.sendRequest(clientLeaderAddr, req)
	if err != nil {
		return "", fmt.Errorf("failed to forward request to leader at %s: %w", clientLeaderAddr, err)
	}
	return resp, nil
}

func (r *ClusterRouter) ForwardDataToLeader(data []byte) (string, error) {
	leader, err := r.getLeader()
	if err != nil {
		return "", err
	}

	if r.rm.IsLeader() || leader == r.LocalAddr {
		return "", fmt.Errorf("internal routing error: cannot forward batch data to self")
	}

	host, _, splitErr := net.SplitHostPort(leader)
	if splitErr != nil {
		return "", fmt.Errorf("invalid leader address format: %w", splitErr)
	}

	clientLeaderAddr := fmt.Sprintf("%s:%d", host, r.clientPort)
	return r.sendDataRequest(clientLeaderAddr, data)
}

func (r *ClusterRouter) processLocally(req string) string {
	if r.localProcessor != nil {
		return r.localProcessor.ProcessCommand(req)
	}
	return "ERROR: no local processor configured"
}

func (r *ClusterRouter) sendRequest(addr, command string) (string, error) {
	return r.sendDataRequest(addr, []byte(command))
}

// sendDataRequest sends raw data (byte slice) to the specified address and expects a response string.
func (r *ClusterRouter) sendDataRequest(addr string, data []byte) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, r.timeout)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()

	if err := conn.SetDeadline(time.Now().Add(r.timeout)); err != nil {
		return "", err
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	if _, err := conn.Write(lenBuf); err != nil {
		return "", fmt.Errorf("failed to write length: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return "", fmt.Errorf("failed to write data: %w", err)
	}

	respLenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, respLenBuf); err != nil {
		return "", fmt.Errorf("failed to read response length: %w", err)
	}

	respLen := binary.BigEndian.Uint32(respLenBuf)

	if respLen == 0 {
		return "", nil
	}

	respBuf := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		return "", fmt.Errorf("failed to read full response body: %w", err)
	}

	return string(respBuf), nil
}
