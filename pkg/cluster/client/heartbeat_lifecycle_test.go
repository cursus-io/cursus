package client

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
)

func TestHeartbeatStopJoinsProofProvider(t *testing.T) {
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := NewTCPClusterClient().StartHeartbeat(ctx, nil, "local", "process", "127.0.0.1:1", 1, func() []fsm.ISRCatchupProof {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return nil
	})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("heartbeat provider was not called")
	}
	cancel()
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	<-canceled
	select {
	case <-stopped:
		t.Fatal("heartbeat returned before provider released its state")
	default:
	}
	once.Do(func() { close(release) })
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not stop")
	}
	stop()
}
