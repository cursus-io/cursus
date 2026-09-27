package controller

import (
	"context"
	"sync"
)

type topicLifecycleGates struct {
	mu     sync.Mutex
	byName map[string]*topicLifecycleGateEntry
}

type topicLifecycleGateEntry struct {
	gate *topicLifecycleGate
	refs int
}

type topicLifecycleGate struct {
	mu             sync.Mutex
	changed        chan struct{}
	readers        int
	writer         bool
	waitingWriters int
}

func (gates *topicLifecycleGates) acquire(ctx context.Context, topicName string, exclusive bool) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	gates.mu.Lock()
	if gates.byName == nil {
		gates.byName = make(map[string]*topicLifecycleGateEntry)
	}
	entry := gates.byName[topicName]
	if entry == nil {
		entry = &topicLifecycleGateEntry{gate: &topicLifecycleGate{changed: make(chan struct{})}}
		gates.byName[topicName] = entry
	}
	entry.refs++
	gates.mu.Unlock()

	if err := entry.gate.acquire(ctx, exclusive); err != nil {
		gates.releaseRef(topicName, entry)
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			entry.gate.release(exclusive)
			gates.releaseRef(topicName, entry)
		})
	}, nil
}

func (gates *topicLifecycleGates) releaseRef(topicName string, entry *topicLifecycleGateEntry) {
	gates.mu.Lock()
	defer gates.mu.Unlock()
	entry.refs--
	if entry.refs == 0 && gates.byName[topicName] == entry {
		delete(gates.byName, topicName)
	}
}

func (gate *topicLifecycleGate) acquire(ctx context.Context, exclusive bool) error {
	waitingWriter := false
	for {
		gate.mu.Lock()
		if ctx.Err() != nil {
			if waitingWriter {
				gate.waitingWriters--
				gate.notifyLocked()
			}
			gate.mu.Unlock()
			return ctx.Err()
		}
		if exclusive {
			if !waitingWriter {
				gate.waitingWriters++
				waitingWriter = true
			}
			if !gate.writer && gate.readers == 0 {
				gate.waitingWriters--
				gate.writer = true
				gate.mu.Unlock()
				return nil
			}
		} else if !gate.writer && gate.waitingWriters == 0 {
			gate.readers++
			gate.mu.Unlock()
			return nil
		}
		changed := gate.changed
		gate.mu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
			gate.mu.Lock()
			if waitingWriter {
				gate.waitingWriters--
				gate.notifyLocked()
			}
			gate.mu.Unlock()
			return ctx.Err()
		}
	}
}

func (gate *topicLifecycleGate) release(exclusive bool) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if exclusive {
		gate.writer = false
	} else {
		gate.readers--
	}
	gate.notifyLocked()
}

func (gate *topicLifecycleGate) notifyLocked() {
	close(gate.changed)
	gate.changed = make(chan struct{})
}
