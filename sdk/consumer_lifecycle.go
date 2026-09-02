package sdk

import (
	"context"
	"fmt"
)

// ConsumerState is the complete lifecycle of a Consumer.
type ConsumerState uint32

const (
	ConsumerStateNew ConsumerState = iota
	ConsumerStateRunning
	ConsumerStateRebalancing
	ConsumerStateClosing
	ConsumerStateClosed
)

func (s ConsumerState) String() string {
	switch s {
	case ConsumerStateNew:
		return "new"
	case ConsumerStateRunning:
		return "running"
	case ConsumerStateRebalancing:
		return "rebalancing"
	case ConsumerStateClosing:
		return "closing"
	case ConsumerStateClosed:
		return "closed"
	default:
		return fmt.Sprintf("consumer_state(%d)", s)
	}
}

func (c *Consumer) State() ConsumerState {
	return ConsumerState(c.state.Load())
}

func (c *Consumer) beginStart() error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if state := c.State(); state != ConsumerStateNew {
		return fmt.Errorf("consumer cannot start from %s", state)
	}
	c.state.Store(uint32(ConsumerStateRunning))
	return nil
}

func (c *Consumer) beginRebalance() (uint64, bool) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.State() != ConsumerStateRunning {
		return 0, false
	}
	c.state.Store(uint32(ConsumerStateRebalancing))
	return c.assignmentGeneration.Add(1), true
}

func (c *Consumer) finishRebalance() {
	c.lifecycleMu.Lock()
	if c.State() == ConsumerStateRebalancing {
		c.state.Store(uint32(ConsumerStateRunning))
	}
	c.lifecycleMu.Unlock()
}

func (c *Consumer) assignmentActive(generation uint64) bool {
	return generation != 0 && c.State() == ConsumerStateRunning && c.assignmentGeneration.Load() == generation
}

func (c *Consumer) assignmentContext() context.Context {
	c.lifecycleMu.Lock()
	ctx := c.mainCtx
	c.lifecycleMu.Unlock()
	return ctx
}

func (c *Consumer) cancelAssignment() {
	c.lifecycleMu.Lock()
	c.mainCancel()
	c.lifecycleMu.Unlock()
}

func (c *Consumer) replaceAssignmentContext() context.Context {
	c.lifecycleMu.Lock()
	c.mainCtx, c.mainCancel = context.WithCancel(c.rootCtx)
	ctx := c.mainCtx
	c.lifecycleMu.Unlock()
	return ctx
}

func (c *Consumer) startLifecycleWorker(worker func()) bool {
	c.lifecycleMu.Lock()
	state := c.State()
	if state == ConsumerStateClosing || state == ConsumerStateClosed {
		c.lifecycleMu.Unlock()
		return false
	}
	c.lifecycleWg.Add(1)
	c.lifecycleMu.Unlock()
	go func() {
		defer c.lifecycleWg.Done()
		worker()
	}()
	return true
}
