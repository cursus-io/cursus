package controller

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
)

type MockRaftManagerForForward struct {
	isLeader      bool
	leaderAddress atomic.Value
}

func (m *MockRaftManagerForForward) IsLeader() bool { return m.isLeader }
func (m *MockRaftManagerForForward) GetLeaderAddress() string {
	addr := m.leaderAddress.Load()
	if addr == nil {
		return ""
	}
	return addr.(string)
}
func (m *MockRaftManagerForForward) ApplyCommand(prefix string, data []byte) error { return nil }
func (m *MockRaftManagerForForward) LeaderCh() <-chan bool                         { return nil }
func (m *MockRaftManagerForForward) GetFSM() *fsm.BrokerFSM                        { return nil }
func (m *MockRaftManagerForForward) GetConfiguration() raft.ConfigurationFuture    { return nil }
func (m *MockRaftManagerForForward) ReplicateWithQuorum(t string, p int, msg types.Message, i int, idemp bool, scope string) (types.AckResponse, error) {
	return types.AckResponse{}, nil
}
func (m *MockRaftManagerForForward) ReplicateBatchWithQuorum(t string, p int, msgs []types.Message, i int, a string, idemp bool, scope string) (types.AckResponse, error) {
	return types.AckResponse{}, nil
}
func (m *MockRaftManagerForForward) ApplyResponse(p string, d []byte, t time.Duration) (types.AckResponse, error) {
	return types.AckResponse{}, nil
}
func (m *MockRaftManagerForForward) AddVoter(id string, addr string) error              { return nil }
func (m *MockRaftManagerForForward) RemoveServer(id string) error                       { return nil }
func (m *MockRaftManagerForForward) GetISRManager() replication.ISRManagerInterface     { return nil }

func TestCommandHandler_isLeaderAndForward_WaitRetry(t *testing.T) {
	tm := topic.NewTopicManager(&config.Config{}, nil, nil)
	cfg := &config.Config{EnabledDistribution: true}
	rm := &MockRaftManagerForForward{isLeader: false}
	cc := &controller.ClusterController{
		RaftManager: rm,
	}

	ch := NewCommandHandler(tm, cfg, nil, nil, cc)

	ready := make(chan struct{})
	go func() {
		rm.leaderAddress.Store("localhost:7001")
		close(ready)
	}()

	<-ready
	resp, forwarded, err := ch.isLeaderAndForward("LIST")

	if !forwarded {
		t.Error("Expected forwarded to be true")
	}
	if !strings.Contains(resp, "router is nil") {
		t.Errorf("Expected 'router is nil' error, got %s", resp)
	}
	if err != nil {
		t.Errorf("Expected nil error (handled via response string), got %v", err)
	}
}
