package coordinator

import (
	"context"
	"errors"
	"sync"
)

// ErrConsumerMetadataRecovering is stable and wire-ready so callers can retry
// ordinary consumer operations after the exclusive recovery lease is released.
var ErrConsumerMetadataRecovering = errors.New("ERROR: consumer_metadata_recovering class=availability retryable=true")

var ErrConsumerMetadataRecoveryStandalone = errors.New("exclusive consumer metadata recovery requires distributed mode")

// consumerAdmission has its own lock, independent of controller, FSM, lifecycle,
// and group locks. Recovery closes admission before draining operations that may
// themselves be waiting for Raft apply. Waiters keep admission closed across
// ownership transfers; cancellation removes only the cancelling waiter's claim.
type consumerAdmission struct {
	mu                           sync.Mutex
	active                       int
	recoveries                   int
	owned                        bool
	lease                        uint64 // Monotonic identity; zero is never an acquired lease.
	changed                      chan struct{}
	requiredGeneration           uint64
	completedGeneration          uint64
	installedGeneration          uint64
	installedLease               uint64
	installedRevision            uint64
	startupPending               bool
	restoreInstalling            bool
	restoreError                 error
	tickets                      map[int]*ConsumerBootstrapTicket
	bootstrapObservationRevision uint64
}

func (g *consumerAdmission) notifyLocked() {
	if g.changed != nil {
		close(g.changed)
	}
	g.changed = make(chan struct{})
}

func (c *Coordinator) admitConsumerOperation() (func(), error) {
	if c == nil || c.standalone {
		return func() {}, nil
	}
	g := &c.consumerAdmission
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.unavailableLocked() {
		return nil, ErrConsumerMetadataRecovering
	}
	g.active++
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			g.active--
			if g.active == 0 {
				g.notifyLocked()
			}
		})
	}, nil
}

// ConsumerOperationsReadinessError is a point-in-time availability check. The
// ordinary APIs also acquire counted admission, so this is not an access token.
func (c *Coordinator) ConsumerOperationsReadinessError() error {
	if c == nil || c.standalone {
		return nil
	}
	g := &c.consumerAdmission
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.unavailableLocked() {
		return ErrConsumerMetadataRecovering
	}
	return nil
}

func (g *consumerAdmission) unavailableLocked() bool {
	return g.recoveries != 0 || len(g.tickets) != 0 || g.startupPending || g.restoreInstalling || g.restoreError != nil || g.completedGeneration != g.requiredGeneration
}

// BeginConsumerMetadataRecovery closes ordinary admission and drains admitted
// operations before granting an exclusive lease. Call BEFORE acquiring any
// controller, FSM, partition, or coordinator lock. The returned release is
// idempotent and must be called on every recovery exit path. Historical FSM
// apply and explicit startup/snapshot APIs bypass this gate to permit draining.
func (c *Coordinator) BeginConsumerMetadataRecovery(ctx context.Context) (func(), error) {
	if c == nil || c.standalone {
		return nil, ErrConsumerMetadataRecoveryStandalone
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g := &c.consumerAdmission
	g.mu.Lock()
	g.recoveries++
	g.notifyLocked()
	for {
		if err := ctx.Err(); err != nil {
			g.recoveries--
			g.notifyLocked()
			g.mu.Unlock()
			return nil, err
		}
		if !g.owned && g.active == 0 {
			g.lease++
			g.owned = true
			g.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					g.mu.Lock()
					defer g.mu.Unlock()
					g.owned = false
					g.recoveries--
					g.notifyLocked()
				})
			}, nil
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		g.mu.Lock()
	}
}
