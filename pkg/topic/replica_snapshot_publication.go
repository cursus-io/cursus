package topic

import (
	"context"
	"fmt"
	"sync/atomic"
)

// ReplicaSnapshotPublication is a one-use detached proof. It retains the
// original cut's provenance but exposes no reads or replacement candidates.
type ReplicaSnapshotPublication struct {
	consumed      atomic.Bool
	cut           SnapshotReadCut
	transactional bool
}

func (c *SnapshotReadCut) PrepareReplicaSnapshotPublication(ctx context.Context, transactional bool) (*ReplicaSnapshotPublication, error) {
	if err := c.check(ctx); err != nil {
		return nil, err
	}
	if c.purpose != snapshotCutReplica || c.witness == nil {
		return nil, fmt.Errorf("replica snapshot publication requires actual replica cut")
	}
	var err error
	if transactional {
		err = c.ValidateTransactional(ctx)
	} else {
		err = c.ValidateRaw(ctx)
	}
	if err != nil {
		return nil, err
	}
	copied := *c
	copied.view = nil
	return &ReplicaSnapshotPublication{cut: copied, transactional: transactional}, nil
}

// WithReplicaSnapshotPublication follows the caller's controller and FSM
// fences. Only bounded local publication belongs in publish; full replay and
// store opening must finish before entering this scope. The callback must call
// validate after acquiring its local locks, immediately before persistence.
// The guard retains original tokens and expires with the callback. It detects
// changes; it does not lock the transaction manager against future mutations.
func (p *Partition) WithReplicaSnapshotPublication(ctx context.Context, proof *ReplicaSnapshotPublication, actual *ReplicaCatchupWitness, leaderID string, epoch int, lifecycle, hwm uint64, publish func(validate func() error) error) error {
	if proof == nil || !proof.consumed.CompareAndSwap(false, true) {
		return fmt.Errorf("replica snapshot proof missing or consumed")
	}
	if ctx == nil {
		return fmt.Errorf("replica snapshot publication requires context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c := &proof.cut
	if c.p != p || c.purpose != snapshotCutReplica || actual == nil || c.witness != actual || actual.leaderID != leaderID || actual.leader != epoch || actual.lifecycle != lifecycle || actual.hwm != hwm || publish == nil {
		return fmt.Errorf("replica snapshot publication identity mismatch")
	}
	p.reconcileMu.Lock()
	defer p.reconcileMu.Unlock()
	p.visibilityMu.Lock()
	defer p.visibilityMu.Unlock()
	validate := func() error {
		if proof.transactional {
			return c.ValidateTransactional(ctx)
		}
		return c.ValidateRaw(ctx)
	}
	if err := validate(); err != nil {
		return err
	}
	var active atomic.Bool
	active.Store(true)
	defer active.Store(false)
	guard := func() error {
		if !active.Load() {
			return fmt.Errorf("replica snapshot publication guard expired")
		}
		return validate()
	}
	err := publish(guard)
	active.Store(false)
	if err != nil {
		return err
	}
	// A failure here can follow a durable record; report it and prevent ISR
	// acceptance. Rolling back a separately persisted snapshot is not safe.
	return validate()
}
