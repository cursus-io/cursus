package controller

import (
	"context"
	"fmt"
	"sync"
)

type handlerMonitor struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// A monitor owns its synchronous initialization too: Close cannot race that
// initialization and destroy dependencies before the goroutine is registered.
func (ch *CommandHandler) beginMonitor(parent context.Context, name string) (context.Context, func(), error) {
	ch.monitorMu.Lock()
	defer ch.monitorMu.Unlock()
	if ch.monitorsClosed || ch.monitors[name] != nil {
		return nil, nil, fmt.Errorf("handler monitor %s unavailable", name)
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	owner := &handlerMonitor{cancel: cancel, done: make(chan struct{})}
	if ch.monitors == nil {
		ch.monitors = make(map[string]*handlerMonitor)
	}
	ch.monitors[name] = owner
	var finishOnce sync.Once
	return ctx, func() {
		finishOnce.Do(func() {
			cancel()
			ch.monitorMu.Lock()
			delete(ch.monitors, name)
			close(owner.done)
			ch.monitorMu.Unlock()
		})
	}, nil
}

func (ch *CommandHandler) stopMonitors() {
	ch.monitorMu.Lock()
	ch.monitorsClosed = true
	owners := make([]*handlerMonitor, 0, len(ch.monitors))
	for _, owner := range ch.monitors {
		owner.cancel()
		owners = append(owners, owner)
	}
	ch.monitorMu.Unlock()
	for _, owner := range owners {
		<-owner.done
	}
}
