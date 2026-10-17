package controller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/hashicorp/raft"
)

type blockedReconcileConfiguration struct {
	raft.ConfigurationFuture
	entered, release chan struct{}
}

func (f *blockedReconcileConfiguration) Error() error {
	close(f.entered)
	<-f.release
	return errors.New("configuration unavailable")
}

type lifecycleDiscoveryRaft struct {
	RaftManager
	state  *fsm.BrokerFSM
	future raft.ConfigurationFuture
}

func (r *lifecycleDiscoveryRaft) IsLeader() bool                             { return true }
func (r *lifecycleDiscoveryRaft) GetFSM() *fsm.BrokerFSM                     { return r.state }
func (r *lifecycleDiscoveryRaft) GetConfiguration() raft.ConfigurationFuture { return r.future }

func TestDiscoveryStopJoinsReconciliation(t *testing.T) {
	future := &blockedReconcileConfiguration{entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(future.release) })
	sd := NewServiceDiscoveryImpl(&lifecycleDiscoveryRaft{state: fsm.NewBrokerFSM(nil, nil), future: future}, "local", "raft:1", "client:2")
	sd.StartReconciler(context.Background())
	sd.StartReconciler(context.Background())
	select {
	case <-future.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("reconciliation did not start")
	}
	stopped := make(chan struct{})
	go func() { sd.StopReconciler(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("stop returned before in-flight reconciliation")
	case <-time.After(20 * time.Millisecond):
	}
	release.Do(func() { close(future.release) })
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not stop")
	}
	sd.StopReconciler()
	sd.StartReconciler(context.Background())
}
