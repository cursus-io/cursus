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
		s.waitReady(e)
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

func (s *bootstrapTransferSource) waitReady(e *bootstrapSourceAttempt) {
	for {
		s.mu.Lock()
		expires := e.expires
		s.mu.Unlock()
		timer := time.NewTimer(time.Until(expires))
		select {
		case <-timer.C:
			s.mu.Lock()
			expired := !time.Now().Before(e.expires)
			s.mu.Unlock()
			if expired {
				return
			}
		case <-e.activity:
			timer.Stop()
		case <-e.retire:
			timer.Stop()
			return
		case <-s.ctx.Done():
			timer.Stop()
			return
		}
	}
}

func (s *bootstrapTransferSource) capture(e *bootstrapSourceAttempt) (bootstrapSourceExport, error) {
	ctx := e.createCtx
	releaseWork, err := s.budget.AcquireWork(ctx, e.request.Requester.BrokerID)
	if err != nil {
		return nil, err
	}
	defer releaseWork()
	prepared, err := s.preparePublication(e)
	export := prepared.export
	if err != nil {
		return export, err
	}
	if err = ctx.Err(); err != nil {
		return export, err
	}
	response, certificate := prepared.response, prepared.certificate
	manifest := response.Manifest
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
		err = prepared.fence(ctx, func(publish bootstrapSourcePublish) error {
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
			continuity, publishErr := publish(func() {
				e.response = response
				e.export = export
				e.expires = expires
				if s.maxLifetime > 0 {
					e.hardExpires = now.Add(s.maxLifetime)
				}
				e.phase = bootstrapReady
			})
			if publishErr != nil {
				return publishErr
			}
			if certificate != nil {
				certificate.continuity = continuity
				certificate.expires = expires
				s.certificates[s.nextCertificate] = certificate
				s.nextCertificate = (s.nextCertificate + 1) % len(s.certificates)
			}
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

type bootstrapSourcePublish func(func()) (*fsm.MetadataBootstrapContinuity, error)
type preparedBootstrapPublication struct {
	export      bootstrapSourceExport
	response    *fsm.BootstrapBeginResponse
	fence       func(context.Context, func(bootstrapSourcePublish) error) error
	certificate *bootstrapSourceCertificate
}

func (s *bootstrapTransferSource) preparePublication(e *bootstrapSourceAttempt) (preparedBootstrapPublication, error) {
	if e.request.Purpose == fsm.BootstrapPurposeReplica {
		return s.prepareReplicaPublication(e)
	}
	return s.prepareMetadataPublication(e)
}

func (s *bootstrapTransferSource) prepareReplicaPublication(e *bootstrapSourceAttempt) (prepared preparedBootstrapPublication, err error) {
	if s.replicaCapture == nil {
		return prepared, fsm.ErrBootstrapTransferUnavailable
	}
	ticket, err := s.state.PrepareReplicaBootstrapSource(e.createCtx, e.local, e.request)
	if err != nil {
		return prepared, err
	}
	captured, err := s.replicaCapture(e.createCtx, ticket)
	if captured != nil {
		prepared.export = captured
	}
	if err != nil {
		return prepared, err
	}
	if captured == nil {
		return prepared, fsm.ErrBootstrapTransferUnavailable
	}
	manifest := captured.Manifest()
	prepared.response = &fsm.BootstrapBeginResponse{Version: fsm.BootstrapTransferVersion, Request: e.request, CaptureMode: fsm.BootstrapCaptureFull, StartOffset: manifest.StartOffset, Manifest: manifest}
	if err = prepared.response.ValidateFor(e.request); err != nil {
		return prepared, err
	}
	prepared.fence = func(ctx context.Context, fn func(bootstrapSourcePublish) error) error {
		return s.replicaFence(ctx, captured, func(scope *fsm.ReplicaBootstrapPublication) error {
			return fn(func(publish func()) (*fsm.MetadataBootstrapContinuity, error) {
				return nil, scope.TryPublishOriginal(publish)
			})
		})
	}
	return prepared, nil
}

func (s *bootstrapTransferSource) prepareMetadataPublication(e *bootstrapSourceAttempt) (prepared preparedBootstrapPublication, err error) {
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
		return prepared, err
	}
	export, err := s.export(ctx, ticket)
	prepared.export = export
	if err != nil {
		return prepared, err
	}
	if err = ctx.Err(); err != nil {
		return prepared, err
	}
	id, err := fsm.NewBootstrapAttemptID()
	if err != nil {
		return prepared, err
	}
	manifest := export.Manifest()
	response := &fsm.BootstrapBeginResponse{Version: fsm.BootstrapTransferVersion, Request: e.request, CaptureMode: mode, StartOffset: manifest.StartOffset, Manifest: manifest, CertificateID: id}
	if err = response.ValidateFor(e.request); err != nil {
		return prepared, err
	}
	prepared.response = response
	prepared.certificate = &bootstrapSourceCertificate{id: id}
	prepared.fence = func(ctx context.Context, fn func(bootstrapSourcePublish) error) error {
		return s.fence(ctx, ticket, func(scope *fsm.MetadataBootstrapPublication) error { return fn(scope.TryPublishCurrent) })
	}
	return prepared, nil
}
