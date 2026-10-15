package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/disk"
)

// own is the only capture/cleanup owner. Cancellation never removes a creating
// entry or permit until non-preemptible filesystem work and late-export Close
// have returned. Ready lifetime is independent of the Begin caller's context.
func (s *bootstrapTransferSource) own(e *bootstrapSourceAttempt) {
	defer s.owners.Done()
	export, err := s.capture(e)
	if err == nil {
		e.cancelCreate()
		close(e.done)
		timer := time.NewTimer(time.Until(e.expires))
		select {
		case <-timer.C:
		case <-e.retire:
		case <-s.ctx.Done():
		}
		timer.Stop()
	}
	s.mu.Lock()
	s.retireLocked(e)
	s.mu.Unlock()
	e.cancelCreate()
	<-e.pinsDrained
	var cleanupErr error
	if export != nil {
		cleanupErr = export.Close()
	}
	// Close failure hands physical deletion debt to disk's existing cleanup owner.
	// No source queue retries Close, and no disk byte/active charge is cleared here.
	s.mu.Lock()
	if cleanupErr != nil && s.cleanupErr == nil {
		s.cleanupErr = fmt.Errorf("bootstrap export cleanup: %w", cleanupErr)
	}
	e.err = errors.Join(err, cleanupErr)
	delete(s.attempts, e.key)
	delete(s.partitions, e.partition)
	if e.transferID != "" {
		delete(s.transfers, e.transferID)
	}
	<-bootstrapSourcePermits
	s.mu.Unlock()
	if err != nil {
		close(e.done)
	}
}

func (s *bootstrapTransferSource) capture(e *bootstrapSourceAttempt) (bootstrapSourceExport, error) {
	ctx := e.createCtx
	ticket, err := s.state.PrepareMetadataBootstrapSource(ctx, e.local, e.request, e.prior)
	mode := fsm.BootstrapCaptureFull
	if e.prior != nil {
		mode = fsm.BootstrapCaptureSuffix
	}
	if errors.Is(err, fsm.ErrBootstrapContinuityMiss) {
		mode = fsm.BootstrapCaptureFull
		ticket, err = s.state.PrepareMetadataBootstrapSource(ctx, e.local, e.request, nil)
	}
	if err != nil {
		return nil, err
	}
	export, err := s.export(ctx, ticket)
	if err != nil {
		return export, err
	}
	if err = ctx.Err(); err != nil {
		return export, err
	}
	id, err := fsm.NewBootstrapAttemptID()
	if err != nil {
		return export, err
	}
	manifest := export.Manifest()
	response := &fsm.BootstrapBeginResponse{Version: fsm.BootstrapTransferVersion, Request: e.request, CaptureMode: mode, StartOffset: manifest.StartOffset, Manifest: manifest, CertificateID: id}
	if err = response.ValidateFor(e.request); err != nil {
		return export, err
	}
	certificate := &bootstrapSourceCertificate{id: id}
	s.mu.Lock()
	if s.closed || e.phase != bootstrapCreating {
		s.mu.Unlock()
		return export, fsm.ErrBootstrapTransferNotFound
	}
	if s.transfers[manifest.TransferID] != nil {
		s.mu.Unlock()
		return export, fsm.ErrBootstrapTransferAttemptConflict
	}
	e.transferID = manifest.TransferID
	s.transfers[e.transferID] = e // Not yet readable; allocate before disk publication.
	s.mu.Unlock()
	for {
		err = s.fence(ctx, ticket, func(scope *fsm.MetadataBootstrapPublication) error {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.closed || s.ctx.Err() != nil || e.phase != bootstrapCreating || s.attempts[e.key] != e {
				return fsm.ErrBootstrapTransferNotFound
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			now := time.Now()
			if deadline, ok := ctx.Deadline(); ok && !now.Before(deadline) {
				return context.DeadlineExceeded
			}
			expires := now.Add(s.ttl)
			slot := s.nextCertificate
			continuity, publishErr := scope.TryPublishCurrent(func() {
				e.response = response
				e.export = export
				e.expires = expires
				e.phase = bootstrapReady
			})
			if publishErr != nil {
				return publishErr
			}
			certificate.continuity = continuity
			certificate.expires = expires
			s.certificates[slot] = certificate
			s.nextCertificate = (slot + 1) % len(s.certificates)
			return nil
		})
		if !errors.Is(err, disk.ErrContentPublicationBusy) {
			return export, err
		}
		// Retry only the final fence. The private immutable export is not recopied.
		if err = waitForContext(ctx, time.Millisecond); err != nil {
			return export, err
		}
	}
}
