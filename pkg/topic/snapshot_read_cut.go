package topic

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
)

// Optional resolver provenance. Capture and Check may acquire every resolver
// shard lock, so neither may run under txnMarkerMu or partition state locks.
type transactionDecisionGenerationResolver interface {
	CaptureTransactionDecisionGeneration() (uint64, bool)
	CheckTransactionDecisionGeneration(uint64) bool
}

type snapshotCutPurpose uint8

const (
	snapshotCutOrdinary snapshotCutPurpose = iota
	snapshotCutBootstrapSource
	snapshotCutReplica
)

// SnapshotReadCut is local, opaque, and valid only inside its owning callback.
// It fixes both physical and visibility bounds without copying the log. Callers
// must validate immediately before accepting derived output. Validation is not
// an atomic publication fence against subsequent resolver or disk mutations.
type SnapshotReadCut struct {
	p                   *Partition
	ctx                 context.Context // context inherited by the legacy-signature view bridge
	view                *PartitionReadView
	purpose             snapshotCutPurpose
	active              bool
	generation          disk.ContentGeneration
	anchor, end, stable uint64
	visibility          uint64
	lifecycle           uint64
	lifecycleKnown      bool
	resolver            TransactionDecisionResolver
	binding, decision   uint64
	evidence            uint64
	decisionAvailable   bool
	decisionResolver    transactionDecisionGenerationResolver
	witness             *ReplicaCatchupWitness
}

// WithSnapshotCertification holds reconciliation then visibility exclusion.
// Callbacks must do local work only, detach results, and never reenter public
// read wrappers, perform network/Raft work, or acquire higher-level gates.
// Context is checked around waits and between pages; a gate wait is not preemptible.
func (p *Partition) WithSnapshotCertification(ctx context.Context, fn func(*SnapshotReadCut) error) error {
	if ctx == nil {
		return fmt.Errorf("snapshot certification requires context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.reconcileMu.Lock()
	defer p.reconcileMu.Unlock()
	p.visibilityMu.Lock()
	defer p.visibilityMu.Unlock()
	return p.withSnapshotCutLocked(ctx, snapshotCutOrdinary, nil, fn)
}

// WithReplicaSnapshotCertification accepts only the actual post-apply witness.
// Its reconciliation marker may still be pending; all other recovery failures
// remain fatal. The caller must separately fence cluster leader/ISR authority.
func (p *Partition) WithReplicaSnapshotCertification(ctx context.Context, w *ReplicaCatchupWitness, leaderID string, leaderEpoch int, lifecycle, hwm uint64, fn func(*SnapshotReadCut) error) error {
	if ctx == nil {
		return fmt.Errorf("snapshot certification requires context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.reconcileMu.Lock()
	defer p.reconcileMu.Unlock()
	p.visibilityMu.Lock()
	defer p.visibilityMu.Unlock()
	if w == nil || w.leaderID != leaderID || w.leader != leaderEpoch || w.lifecycle != lifecycle || w.hwm != hwm {
		return fmt.Errorf("replica snapshot witness identity mismatch")
	}
	return p.withSnapshotCutLocked(ctx, snapshotCutReplica, w, fn)
}

func (p *Partition) withSnapshotCutLocked(ctx context.Context, purpose snapshotCutPurpose, w *ReplicaCatchupWitness, fn func(*SnapshotReadCut) error) error {
	if fn == nil {
		return fmt.Errorf("snapshot certification requires callback")
	}
	c, err := p.captureSnapshotCut(ctx, purpose, w)
	if err != nil {
		return err
	}
	defer func() { c.active = false; c.view.active = false }()
	if err := fn(c); err != nil {
		return err
	}
	return c.ValidateRaw(ctx)
}

func (p *Partition) captureSnapshotCut(ctx context.Context, purpose snapshotCutPurpose, w *ReplicaCatchupWitness) (*SnapshotReadCut, error) {
	if _, ok := p.dh.(replicaPageStorage); !ok {
		return nil, ErrReplicaSourceUnsupported
	}
	c := &SnapshotReadCut{p: p, ctx: ctx, purpose: purpose, witness: w, active: true, visibility: p.visibilityGeneration}
	p.recoveryMu.RLock()
	c.lifecycleKnown, c.lifecycle = p.bootstrapDefinitionKnown, p.bootstrapLifecycle
	p.recoveryMu.RUnlock()
	if err := c.validateState(ctx); err != nil {
		return nil, err
	}
	// Capture BEFORE anchor/HWM/flushed bounds. Never refresh this token after
	// reading bounds: doing so could combine different retained generations.
	var err error
	c.generation, err = p.CaptureReplicaContentGeneration(ctx)
	if err != nil {
		return nil, err
	}
	if w != nil && c.generation != w.request.generation {
		return nil, disk.ErrStaleContentGeneration
	}
	// Live append paths can replace markers without the cut's gates. Capture
	// evidence BEFORE bounds, never after a fixed end has already been read.
	p.txnMarkerMu.RLock()
	c.evidence = p.txnEvidenceRevision
	p.txnMarkerMu.RUnlock()
	c.anchor, err = p.ReplicaFirstOffsetContext(ctx)
	if err != nil {
		return nil, err
	}
	hwm, err := p.MetadataBootstrapSourceHWMContext(ctx)
	if err != nil {
		return nil, err
	}
	c.end = min(hwm, p.dh.GetFlushedOffset())
	if c.anchor > c.end || w != nil && c.end != w.hwm {
		return nil, fmt.Errorf("snapshot cut bounds are unavailable")
	}
	p.txnMarkerMu.RLock()
	c.resolver, c.binding = p.txnResolver, p.txnResolverRevision
	p.txnMarkerMu.RUnlock()
	if c.binding != math.MaxUint64 {
		if c.resolver == nil {
			c.decisionAvailable = true
		} else if r, ok := c.resolver.(transactionDecisionGenerationResolver); ok {
			c.decisionResolver = r
			c.decision, c.decisionAvailable = r.CaptureTransactionDecisionGeneration()
		}
	}
	// Ordinary reads may have removed resolved opens. Restore their retained
	// marker evidence into scratch state before evaluating this captured resolver.
	p.txnMarkerMu.RLock()
	open := make(map[transactionMarkerKey]uint64, len(p.txnOpenOffsets))
	for key, offset := range p.txnOpenOffsets {
		open[key] = offset
	}
	for key, marker := range p.txnMarkers {
		if marker.hasOpenOffset && marker.offset > marker.openOffset {
			if offset, ok := open[key]; !ok || marker.openOffset < offset {
				open[key] = marker.openOffset
			}
		}
	}
	c.stable = firstUnresolvedOpenOffset(c.end, max(c.anchor, p.txnRetentionFloor), open, p.txnMarkers, c.resolver)
	p.txnMarkerMu.RUnlock()
	c.view = &PartitionReadView{p: p, active: true, generation: c.visibility, end: c.end, snapshotCut: c}
	if err := c.ValidateRaw(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *SnapshotReadCut) check(ctx context.Context) error {
	if c == nil || !c.active {
		return fmt.Errorf("snapshot read cut has expired")
	}
	if ctx == nil {
		return fmt.Errorf("snapshot read cut requires context")
	}
	return ctx.Err()
}

func (c *SnapshotReadCut) validateState(ctx context.Context) error {
	if err := c.check(ctx); err != nil {
		return err
	}
	p := c.p
	p.mu.RLock()
	p.recoveryMu.RLock()
	var err error
	if c.purpose == snapshotCutReplica {
		err = p.validateWitnessLocked(c.witness, c.witness.leader, c.witness.lifecycle, c.witness.hwm, 0)
	} else {
		err = errors.Join(p.replicaRecoveryErrorLocked(), p.reconciliationErr)
	}
	if p.closed || p.snapshotRecovery || p.HWM < c.end || p.visibilityGeneration != c.visibility || p.bootstrapDefinitionKnown != c.lifecycleKnown || p.bootstrapLifecycle != c.lifecycle {
		err = errors.Join(err, fmt.Errorf("snapshot cut partition state changed"))
	}
	if c.witness != nil && p.bootstrapDefinitionKnown && p.bootstrapLifecycle != c.witness.lifecycle {
		err = errors.Join(err, fmt.Errorf("replica snapshot lifecycle mismatch"))
	}
	err = errors.Join(err, p.hwmReadinessErrorLocked())
	p.recoveryMu.RUnlock()
	p.mu.RUnlock()
	return err
}

// ValidateRaw checks only physical provenance. It can certify a reusable raw
// snapshot ONLY after the caller has classified the entire covered raw prefix
// as nontransactional, including hidden data and control records. It deliberately
// ignores transaction evidence, resolver binding and transaction manager changes.
func (c *SnapshotReadCut) ValidateRaw(ctx context.Context) error {
	if err := c.validateState(ctx); err != nil {
		return err
	}
	return c.p.CheckReplicaContentGeneration(ctx, c.generation)
}

// ValidateTransactional must follow the last visibility evaluation and precede
// acceptance. Unsupported/exhausted provenance fails closed, including for an
// otherwise empty visible result. No manager generation call holds txnMarkerMu.
func (c *SnapshotReadCut) ValidateTransactional(ctx context.Context) error {
	if err := c.ValidateRaw(ctx); err != nil {
		return err
	}
	if !c.decisionAvailable || !c.transactionEvidenceMatches() {
		return fmt.Errorf("snapshot transaction provenance unavailable or changed")
	}
	if c.decisionResolver != nil && !c.decisionResolver.CheckTransactionDecisionGeneration(c.decision) {
		return fmt.Errorf("snapshot transaction decisions changed")
	}
	if !c.transactionEvidenceMatches() {
		return fmt.Errorf("snapshot transaction resolver binding or local evidence changed")
	}
	return c.ValidateRaw(ctx)
}

func (c *SnapshotReadCut) transactionEvidenceMatches() bool {
	c.p.txnMarkerMu.RLock()
	defer c.p.txnMarkerMu.RUnlock()
	return c.binding != math.MaxUint64 && c.p.txnResolverRevision == c.binding &&
		c.evidence != math.MaxUint64 && c.p.txnEvidenceRevision == c.evidence
}

// View bridges identity/cache access without granting bootstrap recovery or
// replica-index authority. Its read methods retain this cut's fixed boundaries.
func (c *SnapshotReadCut) View() *PartitionReadView                  { return c.view }
func (c *SnapshotReadCut) FirstOffset() uint64                       { return c.anchor }
func (c *SnapshotReadCut) CommittedEnd() uint64                      { return c.end }
func (c *SnapshotReadCut) LastStableOffset() uint64                  { return c.stable }
func (c *SnapshotReadCut) ContentGeneration() disk.ContentGeneration { return c.generation }

// ReadRawPage returns every physical record, including hidden transaction and
// control records. Zero byte limits use a bounded 1 MiB default; an allowed
// oversized first record is retried alone up to the disk's maximum record size.
func (c *SnapshotReadCut) ReadRawPage(ctx context.Context, offset uint64, maxRecords, maxBytes int, oversized bool) (disk.RetainedPage, error) {
	return c.readRawPageThrough(ctx, offset, c.end, maxRecords, maxBytes, oversized)
}

func (c *SnapshotReadCut) readRawPageThrough(ctx context.Context, offset, end uint64, maxRecords, maxBytes int, oversized bool) (disk.RetainedPage, error) {
	if err := c.ValidateRaw(ctx); err != nil {
		return disk.RetainedPage{}, err
	}
	if offset < c.anchor || offset > end || end > c.end {
		return disk.RetainedPage{}, &types.OffsetOutOfRangeError{Requested: offset, Earliest: c.anchor, Latest: end}
	}
	if maxRecords <= 0 || maxBytes < 0 {
		return disk.RetainedPage{}, fmt.Errorf("snapshot page requires positive record and nonnegative byte limits")
	}
	if maxBytes == 0 {
		maxBytes = 1 << 20
	}
	page, err := c.p.ReadReplicaRetainedPage(ctx, offset, end, maxRecords, maxBytes)
	if errors.Is(err, disk.ErrRetainedPageLimit) && oversized {
		page, err = c.p.ReadReplicaRetainedPage(ctx, offset, end, 1, disk.MaxMessageSize)
	}
	if err != nil {
		return disk.RetainedPage{}, err
	}
	if page.ContentGeneration != c.generation {
		return disk.RetainedPage{}, disk.ErrStaleContentGeneration
	}
	if page.EndOffset < offset || page.EndOffset > end || page.EndOffset == offset && offset != end || page.Exhausted && page.EndOffset != end {
		return disk.RetainedPage{}, fmt.Errorf("snapshot raw page has invalid progress")
	}
	if err := c.ValidateRaw(ctx); err != nil {
		return disk.RetainedPage{}, err
	}
	return page, nil
}

// ReadVisiblePage consumes at most one bounded raw page, so an entirely filtered
// result can still advance next. Bytes count all decoded records, not just output.
// Final ValidateTransactional is required even if every page returned no error.
func (c *SnapshotReadCut) ReadVisiblePage(ctx context.Context, offset uint64, maxRecords, maxBytes int, oversized bool) ([]types.Message, int, uint64, error) {
	return c.readVisiblePageThrough(ctx, offset, c.stable, maxRecords, maxBytes, oversized)
}

func (c *SnapshotReadCut) readVisiblePageThrough(ctx context.Context, offset, end uint64, maxRecords, maxBytes int, oversized bool) ([]types.Message, int, uint64, error) {
	if err := c.check(ctx); err != nil {
		return nil, 0, offset, err
	}
	if offset >= end && offset >= c.anchor {
		return nil, 0, offset, c.ValidateRaw(ctx)
	}
	page, err := c.readRawPageThrough(ctx, offset, end, maxRecords, maxBytes, oversized)
	if err != nil {
		return nil, 0, offset, err
	}
	visible := make([]types.Message, 0, len(page.Messages))
	c.p.txnMarkerMu.RLock()
	for _, msg := range page.Messages {
		if err = ctx.Err(); err != nil {
			break
		}
		if isReadCommittedVisible(msg, c.end, c.p.txnMarkers, c.resolver) {
			visible = append(visible, msg)
		}
	}
	c.p.txnMarkerMu.RUnlock()
	if err != nil {
		return nil, 0, offset, err
	}
	if err := c.ValidateRaw(ctx); err != nil {
		return nil, 0, offset, err
	}
	return visible, page.DecodedBytes, page.EndOffset, nil
}
