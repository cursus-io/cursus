package controller

import (
	"context"
	"testing"
	"time"
)

type closedLeaderChannelRaftManager struct {
	RaftManager
	leaderCh <-chan bool
}

func (m closedLeaderChannelRaftManager) LeaderCh() <-chan bool {
	return m.leaderCh
}

func TestControllerElectionMonitorStopsWhenLeaderChannelCloses(t *testing.T) {
	leaderCh := make(chan bool)
	close(leaderCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	election := &ControllerElection{
		rm:     closedLeaderChannelRaftManager{leaderCh: leaderCh},
		ctx:    ctx,
		cancel: cancel,
	}
	done := make(chan struct{})
	go func() {
		election.monitorLeadership()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("leadership monitor did not stop after notification channel closed")
	}
}
