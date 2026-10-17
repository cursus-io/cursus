package replication

import (
	"context"
	"sync"
	"testing"
	"time"
)

type blockedISRLeader struct{ entered, release chan struct{} }

func (b *blockedISRLeader) IsLeader() bool                  { close(b.entered); <-b.release; return false }
func (*blockedISRLeader) ApplyCommand(string, []byte) error { return nil }

func TestISRStopJoinsActiveRefreshAndPreventsRestart(t *testing.T) {
	applier := &blockedISRLeader{entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(applier.release) })
	manager := NewISRManager(context.Background(), nil, "local", time.Second, applier)
	manager.Start()
	select {
	case <-applier.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("ISR refresh did not start")
	}
	stopped := make(chan struct{})
	go func() { manager.Stop(); close(stopped) }()
	<-manager.ctx.Done()
	select {
	case <-stopped:
		t.Fatal("ISR stop did not join active refresh")
	default:
	}
	release.Do(func() { close(applier.release) })
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("ISR owner did not stop")
	}
	manager.Stop()
	manager.Start()
}

func TestISRStopBeforeStart(t *testing.T) {
	manager := NewISRManager(context.Background(), nil, "local", time.Second, nil)
	manager.Stop()
	manager.Start()
	manager.Stop()
}
