package topic

import (
	"context"
	"errors"
	"fmt"

	"github.com/cursus-io/cursus/pkg/disk"
)

var ErrBootstrapPending = errors.New("partition bootstrap pending")

func (p *Partition) BootstrapPending() bool { return p.bootstrapPending.Load() }
func (p *Partition) bootstrapErrorLocked() error {
	if !p.BootstrapPending() {
		return nil
	}
	return errors.Join(ErrBootstrapPending, p.bootstrapAttemptErr)
}
func (p *Partition) permanentRecoveryError() error {
	p.recoveryMu.RLock()
	defer p.recoveryMu.RUnlock()
	return errors.Join(p.recoveryErr, p.txnRecoveryErr, p.producerRecoveryErr, p.reconciliationErr)
}
func (p *Partition) BootstrapStatus() (*disk.BootstrapMetadata, error) {
	p.recoveryMu.RLock()
	defer p.recoveryMu.RUnlock()
	if p.bootstrapMetadata == nil {
		return nil, p.bootstrapAttemptErr
	}
	m := *p.bootstrapMetadata
	m.Files = append([]disk.BootstrapFile(nil), m.Files...)
	return &m, p.bootstrapAttemptErr
}
func (p *Partition) validateBootstrapDefinition(def Definition) {
	p.recoveryMu.Lock()
	defer p.recoveryMu.Unlock()
	p.bootstrapLifecycle = def.LifecycleEpoch
	p.bootstrapDefinitionKnown = true
	if m := p.bootstrapMetadata; m != nil && (m.Fence.Topic != def.Name || m.Fence.Partition != p.id || m.Fence.LifecycleEpoch != def.LifecycleEpoch) {
		p.bootstrapAttemptErr = fmt.Errorf("pending bootstrap is incompatible with topic definition")
	}
}

// BootstrapSession owns reconcile, visibility, HWM checkpoint, then producer
// checkpoint exclusion. The caller owns lifecycle -> partition write -> FSM
// transition -> materialization, with FSM.mu released. Only local callbacks
// are allowed; never wait for network/Raft or acquire controller gates here.
// No partition mu is held while entering disk maintenance.
type BootstrapSession struct {
	p         *Partition
	disk      *disk.DiskHandler
	metadata  *disk.BootstrapMetadata
	durable   bool
	closed    bool
	completed bool
	prepared  bool
}

func (p *Partition) BeginBootstrapSession() (*BootstrapSession, error) {
	d, ok := p.dh.(*disk.DiskHandler)
	if !ok {
		return nil, fmt.Errorf("bootstrap requires disk storage")
	}
	p.reconcileMu.Lock()
	p.visibilityMu.Lock()
	p.hwmCheckpointMu.Lock()
	p.producerCheckpointMu.Lock()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.producerCheckpointMu.Unlock()
		p.hwmCheckpointMu.Unlock()
		p.visibilityMu.Unlock()
		p.reconcileMu.Unlock()
		return nil, fmt.Errorf("partition is closed")
	}
	p.replicaVerified = false
	// Serialize entering pending with normal HWM/authority setters. A setter
	// that acquired mu first finishes before this transition; a later setter
	// observes pending in its post-lock check.
	wasPending := p.BootstrapPending()
	p.bootstrapPending.Store(true)
	p.mu.Unlock()
	s := &BootstrapSession{p: p, disk: d, durable: wasPending}
	metadata, err := d.PendingBootstrap()
	if err != nil {
		s.durable = true
		_ = s.fail(err)
		s.Close()
		return nil, err
	}
	p.recoveryMu.RLock()
	s.prepared = p.bootstrapPrepared
	if metadata == nil && s.prepared && s.durable {
		metadata = p.bootstrapMetadata
	}
	p.recoveryMu.RUnlock()
	s.metadata = metadata
	s.durable = wasPending || metadata != nil
	if metadata != nil {
		s.remember(metadata)
	}
	return s, nil
}
func (s *BootstrapSession) check() error {
	if s == nil || s.closed {
		return fmt.Errorf("bootstrap session is closed")
	}
	return nil
}

// Prepared reports whether this open session can expose its retained, rebuilt
// generation. It does not consult the on-disk manifest, which completion may
// already have removed before a directory-sync failure.
func (s *BootstrapSession) Prepared() bool { return s.checkPrepared() == nil }

func (s *BootstrapSession) checkPrepared() error {
	if err := s.check(); err != nil {
		return err
	}
	if s.completed || !s.prepared || !s.durable || s.metadata == nil || !s.p.BootstrapPending() {
		return fmt.Errorf("bootstrap session has no pending prepared generation")
	}
	s.p.recoveryMu.RLock()
	retained := s.p.bootstrapPrepared && s.p.bootstrapMetadata != nil && s.p.bootstrapMetadata.TransferID == s.metadata.TransferID
	s.p.recoveryMu.RUnlock()
	if !retained {
		return fmt.Errorf("bootstrap prepared metadata is no longer retained")
	}
	return s.validate(*s.metadata)
}

// WithPreparedReadView reuses the session's owned exclusion and rebuilt indexes.
// The callback is local, cannot re-enter public partition read wrappers, and its
// view expires on return. No raw rescan or catalog fetch occurs on this path.
func (s *BootstrapSession) WithPreparedReadView(fn func(*PartitionReadView) error) error {
	if err := s.checkPrepared(); err != nil {
		return err
	}
	return s.p.withReadViewLocked(true, s.metadata.EndOffset, fn)
}
func (s *BootstrapSession) fail(err error) error {
	if err != nil {
		s.p.recoveryMu.Lock()
		s.p.bootstrapAttemptErr = err
		s.p.recoveryMu.Unlock()
	}
	return err
}
func (s *BootstrapSession) remember(m *disk.BootstrapMetadata) {
	copy := *m
	copy.Files = append([]disk.BootstrapFile(nil), m.Files...)
	s.metadata = &copy
	s.p.recoveryMu.Lock()
	s.p.bootstrapMetadata = &copy
	s.p.recoveryMu.Unlock()
}

// Install detaches derived caches before the irreversible disk replacement.
// An install failure after intent requires reopening the disk handler; Close
// preserves pending state and the immutable catalog for restart recovery.
func (s *BootstrapSession) Install(receiver *disk.BootstrapReceiver, detach func() error) error {
	if err := s.check(); err != nil {
		return err
	}
	if s.completed || s.durable {
		return s.fail(fmt.Errorf("bootstrap already installed; resume with Complete"))
	}
	if receiver == nil || !s.disk.OwnsBootstrapReceiver(receiver) {
		return s.fail(fmt.Errorf("bootstrap receiver belongs to another handler or is nil"))
	}
	m := receiver.Manifest()
	if err := s.validate(m); err != nil {
		return s.fail(err)
	}
	if s.metadata != nil && s.metadata.TransferID != m.TransferID {
		return s.fail(fmt.Errorf("different bootstrap already pending"))
	}
	// The session owns checkpoint exclusion. Read committed authority only on
	// fresh install, before detaching caches or persisting a replacement intent.
	// Pending replay may already have deleted this checkpoint on restart.
	floor, err := loadBootstrapCommittedHWM(s.p.hwmCheckpointPath)
	if err != nil {
		return s.fail(err)
	}
	if m.EndOffset < floor {
		return s.fail(fmt.Errorf("bootstrap end %d is below committed checkpoint %d", m.EndOffset, floor))
	}
	if detach != nil {
		if err := detach(); err != nil {
			return s.fail(err)
		}
	}
	err = receiver.Install()
	pending, pendingErr := s.disk.PendingBootstrap()
	if pending != nil {
		s.durable = true
		s.remember(pending)
	}
	if pendingErr != nil {
		s.durable = true
		return s.fail(errors.Join(err, pendingErr))
	}
	if err != nil {
		return s.fail(err)
	}
	s.durable = true
	s.remember(&m)
	s.p.visibilityGeneration++
	return nil
}
func (s *BootstrapSession) validate(m disk.BootstrapMetadata) error {
	p := s.p
	if m.Fence.Topic != p.topic || m.Fence.Partition != p.id {
		return fmt.Errorf("bootstrap partition identity mismatch")
	}
	p.recoveryMu.RLock()
	known, epoch := p.bootstrapDefinitionKnown, p.bootstrapLifecycle
	p.recoveryMu.RUnlock()
	if known && epoch != m.Fence.LifecycleEpoch {
		return fmt.Errorf("bootstrap lifecycle mismatch")
	}
	// A retained same-process floor also constrains pending/prepared resume.
	// After restart, the validated pending manifest supplies the boundary.
	if floor := p.RecoveryCommittedHWMFloor(); m.EndOffset < floor {
		return fmt.Errorf("bootstrap end %d is below retained committed floor %d", m.EndOffset, floor)
	}
	return nil
}

// Complete also resumes an already installed pending generation. The callback
// receives an offline fixed-end view after raw transaction/producer rebuilding;
// it must durably rebuild all additional derived state before returning.
func (s *BootstrapSession) Complete(rebuild func(*PartitionReadView, []byte) error) error {
	if err := s.check(); err != nil {
		return err
	}
	if s.completed {
		return nil
	}
	if s.metadata == nil {
		return s.fail(fmt.Errorf("bootstrap has no installed manifest"))
	}
	m := *s.metadata
	if err := s.validate(m); err != nil {
		return s.fail(err)
	}
	if s.prepared {
		return s.completePrepared()
	}
	state, err := s.p.scanRawRecoveryState()
	if err != nil {
		return s.fail(err)
	}
	if state.first != m.StartOffset || state.end != m.EndOffset {
		return s.fail(fmt.Errorf("installed bootstrap bounds mismatch"))
	}
	s.p.installTransactionState(state)
	s.p.installProducerState(state)
	catalog, err := s.disk.PendingBootstrapCatalog()
	if err != nil {
		return s.fail(err)
	}
	if rebuild != nil {
		if err = s.p.withReadViewLocked(true, m.EndOffset, func(v *PartitionReadView) error { return rebuild(v, catalog) }); err != nil {
			return s.fail(err)
		}
	}
	if err = s.p.persistProducerStateCheckpointLocked(true); err != nil {
		return s.fail(err)
	}
	if err = s.p.persistHWMCheckpointLocked(m.EndOffset, true); err != nil {
		return s.fail(err)
	}
	s.prepared = true
	s.p.recoveryMu.Lock()
	s.p.bootstrapPrepared = true
	s.p.recoveryMu.Unlock()
	return s.completePrepared()
}
func (s *BootstrapSession) completePrepared() error {
	m := *s.metadata
	if err := s.disk.CompleteBootstrap(m.TransferID); err != nil {
		return s.fail(err)
	}
	p := s.p
	p.mu.Lock()
	p.LEO.Store(m.EndOffset)
	p.HWM = m.EndOffset
	p.hwmAuthoritative = true
	p.snapshotRecovery = false
	p.recoveryCheckpointHWM = 0
	p.recoverySnapshotHWM = 0
	p.mu.Unlock()
	p.recoveryMu.Lock()
	p.bootstrapMetadata = nil
	p.bootstrapPrepared = false
	p.bootstrapAttemptErr = nil
	p.txnRecoveryErr = nil
	p.producerRecoveryErr = nil
	p.recoveryMu.Unlock()
	p.visibilityGeneration++
	p.bootstrapPending.Store(false)
	s.completed = true
	s.durable = false
	p.NotifyNewMessage()
	return nil
}
func (s *BootstrapSession) Close() {
	if s == nil || s.closed {
		return
	}
	s.closed = true
	if !s.durable {
		s.p.bootstrapPending.Store(false)
	}
	s.p.producerCheckpointMu.Unlock()
	s.p.hwmCheckpointMu.Unlock()
	s.p.visibilityMu.Unlock()
	s.p.reconcileMu.Unlock()
}

type BootstrapOutcome int

const (
	BootstrapOutcomeUncertain BootstrapOutcome = iota
	BootstrapOutcomePreIntentAbort
	BootstrapOutcomeCompleted
)

// ClosedOutcome is explicit session evidence. Missing manifests never erase an
// existing pending intent, and an open session cannot prove cleanup is safe.
func (s *BootstrapSession) ClosedOutcome() BootstrapOutcome {
	if s == nil || !s.closed {
		return BootstrapOutcomeUncertain
	}
	if s.completed {
		return BootstrapOutcomeCompleted
	}
	if !s.durable {
		return BootstrapOutcomePreIntentAbort
	}
	return BootstrapOutcomeUncertain
}

// StageBootstrap prepares immutable bytes without acquiring a visibility view.
// Network transfer belongs outside the exclusive install session.
func (p *Partition) StageBootstrap(m disk.BootstrapMetadata) (*disk.BootstrapReceiver, error) {
	d, ok := p.dh.(*disk.DiskHandler)
	if !ok {
		return nil, fmt.Errorf("bootstrap requires disk storage")
	}
	if m.Fence.Topic != p.topic || m.Fence.Partition != p.id {
		return nil, fmt.Errorf("bootstrap partition identity mismatch")
	}
	return d.StageBootstrap(m)
}

// BootstrapSource captures catalogs and log files under one exclusive local
// generation. It does not mark the source pending. Exported files are detached
// and may be transferred after the callback returns.
type BootstrapSource struct {
	view *PartitionReadView
	disk *disk.DiskHandler
	cut  *SnapshotReadCut
}

func (s *BootstrapSource) View() *PartitionReadView { return s.view }

// SnapshotReadCut exposes the fixed certification bounds without changing the
// legacy source view's metadata scanning semantics.
func (s *BootstrapSource) SnapshotReadCut() (*SnapshotReadCut, error) {
	if s == nil || s.cut == nil {
		return nil, fmt.Errorf("bootstrap source has no snapshot cut")
	}
	if err := s.cut.check(context.Background()); err != nil {
		return nil, err
	}
	return s.cut, nil
}

func (s *BootstrapSource) Export(ctx context.Context, fence disk.BootstrapFence, catalog []byte) (*disk.BootstrapExport, error) {
	return s.export(ctx, fence, catalog, false)
}

// ExportOwnedContext leaves cleanup and expiration entirely with the caller.
func (s *BootstrapSource) ExportOwnedContext(ctx context.Context, fence disk.BootstrapFence, catalog []byte) (*disk.BootstrapExport, error) {
	return s.export(ctx, fence, catalog, true)
}

func (s *BootstrapSource) export(ctx context.Context, fence disk.BootstrapFence, catalog []byte, owned bool) (*disk.BootstrapExport, error) {
	if err := s.view.check(); err != nil {
		return nil, err
	}
	if fence.CommittedHWM != s.view.end {
		return nil, fmt.Errorf("bootstrap source HWM mismatch")
	}
	if err := s.cut.ValidateRaw(ctx); err != nil {
		return nil, err
	}
	exportFn := s.disk.ExportBootstrapContext
	if owned {
		exportFn = s.disk.ExportBootstrapOwnedContext
	}
	export, err := exportFn(ctx, s.cut.anchor, s.cut.end, fence, catalog)
	if err != nil {
		return nil, err
	}
	if err := s.cut.ValidateRaw(ctx); err != nil {
		return nil, errors.Join(err, export.Close())
	}
	return export, nil
}
func (p *Partition) WithBootstrapSource(fn func(*BootstrapSource) error) error {
	return p.WithBootstrapSourceContext(context.Background(), fn)
}

// WithBootstrapSourceContext owns the partition gates throughout the full copy.
// Cancellation is checked around waits and by paged readers; mutex waits and
// individual filesystem calls remain synchronous and cannot be preempted.
func (p *Partition) WithBootstrapSourceContext(ctx context.Context, fn func(*BootstrapSource) error) error {
	if ctx == nil || fn == nil {
		return fmt.Errorf("bootstrap source requires context and callback")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d, ok := p.dh.(*disk.DiskHandler)
	if !ok {
		return ErrReplicaSourceUnsupported
	}
	p.reconcileMu.Lock()
	defer p.reconcileMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	p.visibilityMu.Lock()
	defer p.visibilityMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.withSnapshotCutLocked(ctx, snapshotCutBootstrapSource, nil, func(cut *SnapshotReadCut) error {
		return p.withReadViewLocked(true, cut.end, func(v *PartitionReadView) error {
			return fn(&BootstrapSource{view: v, disk: d, cut: cut})
		})
	})
}

func (p *Partition) setProducerRecoveryError(err error) {
	p.recoveryMu.Lock()
	p.producerRecoveryErr = err
	p.recoveryMu.Unlock()
}
