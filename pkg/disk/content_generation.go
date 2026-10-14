package disk

import (
	"context"
	"errors"
	"fmt"
	"math"
)

// ContentGeneration is an opaque, comparable local witness for a handler's
// existing prefix. Appends preserve it; any possible prefix rewrite invalidates
// it. It is not a remote proof and must never be serialized. The owner reference
// also prevents a replacement handler from reusing an old token's identity.
type ContentGeneration struct {
	owner    *DiskHandler
	revision uint64
}

var ErrStaleContentGeneration = errors.New("stale disk content generation")

// CaptureContentGeneration checks availability and captures a local witness.
func (d *DiskHandler) CaptureContentGeneration() (ContentGeneration, error) {
	return d.CaptureContentGenerationContext(context.Background())
}

func (d *DiskHandler) CaptureContentGenerationContext(ctx context.Context) (ContentGeneration, error) {
	if ctx == nil {
		return ContentGeneration{}, fmt.Errorf("content generation requires context")
	}
	unlock, err := lockProofContext(ctx, &d.mu, proofReadMutex{&d.writeFailureMu})
	if err != nil {
		return ContentGeneration{}, err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return ContentGeneration{}, err
	}
	if err := d.proofAvailabilityLocked(); err != nil {
		return ContentGeneration{}, err
	}
	return d.contentGenerationLocked(), nil
}

func (d *DiskHandler) CheckContentGeneration(token ContentGeneration) error {
	return d.CheckContentGenerationContext(context.Background(), token)
}

func (d *DiskHandler) CheckContentGenerationContext(ctx context.Context, token ContentGeneration) error {
	actual, err := d.CaptureContentGenerationContext(ctx)
	if err != nil {
		return err
	}
	if actual != token {
		return ErrStaleContentGeneration
	}
	return nil
}

// PublishIfContentGeneration holds mu and the availability read lock across the
// check and callback. The caller may hold its partition lock BEFORE entering.
// publish may ONLY update caller-owned, ALREADY-LOCKED scalar state. It must not
// reenter disk, wait, acquire any foreign lock, or perform network/Raft work.
// In particular this never acquires a partition lock or maintenanceMu.
func (d *DiskHandler) PublishIfContentGeneration(token ContentGeneration, publish func()) error {
	if publish == nil {
		return fmt.Errorf("content publication requires callback")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.writeFailureMu.RLock()
	defer d.writeFailureMu.RUnlock()
	if err := d.proofAvailabilityLocked(); err != nil {
		return err
	}
	if token != d.contentGenerationLocked() {
		return ErrStaleContentGeneration
	}
	publish()
	return nil
}

// Caller holds mu and writeFailureMu (read or write). Shutdown and bootstrap
// transitions also hold mu; terminal failures need only writeFailureMu.
func (d *DiskHandler) proofAvailabilityLocked() error {
	select {
	case <-d.done:
		return fmt.Errorf("disk handler is closed")
	default:
	}
	if d.bootstrapPending.Load() {
		return ErrBootstrapPending
	}
	if d.writeFailure != nil {
		return fmt.Errorf("disk handler write unavailable until restart: %w", d.writeFailure)
	}
	return nil
}

func (d *DiskHandler) contentGenerationLocked() ContentGeneration {
	return ContentGeneration{owner: d, revision: d.contentRevision}
}

// Caller holds mu, immediately before a potentially destructive operation.
func (d *DiskHandler) advanceContentGenerationLocked() {
	if d.contentRevision == math.MaxUint64 {
		_ = d.markWriteUnavailable(fmt.Errorf("disk content generation exhausted"))
		return
	}
	d.contentRevision++
}

// GetFirstOffsetContext reads the retained start without an uncancellable wait
// behind a proof scan or mutation. A separate generation witness must span its
// use with later reads, because retention can advance immediately after return.
func (d *DiskHandler) GetFirstOffsetContext(ctx context.Context) (uint64, error) {
	if ctx == nil {
		return 0, fmt.Errorf("retained start requires context")
	}
	unlock, err := lockProofContext(ctx, &d.mu, proofReadMutex{&d.writeFailureMu})
	if err != nil {
		return 0, err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := d.proofAvailabilityLocked(); err != nil {
		return 0, err
	}
	if len(d.segments) == 0 {
		return d.CurrentSegment, nil
	}
	return d.segments[0], nil
}
