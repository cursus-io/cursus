package server

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestServerWorkerStopJoinsBeforeDependenciesClose(t *testing.T) {
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	stop := startServerWorker(context.Background(), func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
	})
	<-started
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	<-canceled
	select {
	case <-stopped:
		t.Fatal("stop returned while the worker still owned dependencies")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("worker did not join")
	}
	stop()
}

type runtimeRegistrationDiscovery struct {
	clusterController.ServiceDiscovery
	mu               sync.Mutex
	wanted           fsm.BrokerInfo
	state            *fsm.BrokerFSM
	first            bool
	entered, release chan struct{}
}

func (s *runtimeRegistrationDiscovery) LocalRegistrationPayload() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.wanted)
}

func (s *runtimeRegistrationDiscovery) Register() error {
	payload, err := s.LocalRegistrationPayload()
	if err != nil {
		return err
	}
	if !s.first {
		s.first = true
		close(s.entered)
		<-s.release
	}
	if result := s.state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), payload...)}); result != nil {
		return result.(error)
	}
	return nil
}

func TestRegistrationWorkerRepublishesChangesAfterInflightSuccess(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	sd := &runtimeRegistrationDiscovery{state: state, entered: make(chan struct{}), release: make(chan struct{}), wanted: fsm.BrokerInfo{ID: "local", Addr: "raft:1", ClientAddr: "client:2", Status: "active", IncarnationID: "current", LifecycleProtocol: fsm.BrokerProtocolVersionCurrent}}
	cc := &clusterController.ClusterController{RaftManager: &registrationCaptureRaftManager{state: state}}
	sd.wanted.TransactionCoordinatorShards = transaction.DefaultCoordinatorShardCount
	stop := startServerWorker(context.Background(), func(ctx context.Context) { runLocalRegistration(ctx, sd, cc, config.DefaultConfig()) })
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(sd.release) }); stop() })
	select {
	case <-sd.entered:
	case <-time.After(time.Second):
		t.Fatal("first registration did not start")
	}
	sd.mu.Lock()
	sd.wanted.RecoveryProtocol = 2
	sd.wanted.ClientAddr = "client:3"
	sd.mu.Unlock()
	releaseOnce.Do(func() { close(sd.release) })
	require.Eventually(t, func() bool { return localRegistrationCurrent(sd, state) }, 3*time.Second, 10*time.Millisecond)
	stop()
	actual := state.GetBroker("local")
	require.Equal(t, 2, actual.RecoveryProtocol)
	require.Equal(t, "client:3", actual.ClientAddr)
	require.Equal(t, "current", actual.IncarnationID)
	require.Zero(t, actual.TransactionCoordinatorShards, "registration-only shard count is stored separately in the FSM")
}
