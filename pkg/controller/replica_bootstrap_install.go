package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/util"
)

// Delivery retries retain the exact fingerprint and original minimum end until
// the owning attempt expires or metadata invalidates it.
func retryReplicaBootstrapRPC(ctx context.Context, call func() error) error {
	delay := 25 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := call()
		metrics.RecordRecoveryAttempt(metrics.RecoveryReplicaRPC, err)
		var network net.Error
		transient := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &network)
		if err == nil || !transient {
			return err
		}
		if err = waitForContext(ctx, delay); err != nil {
			return err
		}
		metrics.RecordRecoveryWork(metrics.RecoveryReplicaRPC, metrics.RecoveryRetries, 1)
		delay = min(delay*2, time.Second)
	}
}

func (r *replicaBootstrapRecovery) sourceAddress(a *replicaBootstrapAttempt) (string, error) {
	b := r.state.GetBroker(a.request.Source.BrokerID)
	if !consumerRecoveryProcess(b) || b.IncarnationID != a.request.Source.IncarnationID || b.IncarnationEpoch != a.request.Source.IncarnationEpoch {
		return "", fsm.ErrBootstrapTransferStaleFence
	}
	return b.Addr, nil
}

func (r *replicaBootstrapRecovery) receive(ctx context.Context, a *replicaBootstrapAttempt) (result error) {
	ticket, err := r.state.PrepareReplicaBootstrapReceiver(ctx, a.request.Requester, a.request)
	if err != nil {
		return err
	}
	r.mu.Lock()
	a.ticket = ticket
	r.mu.Unlock()
	transferID := ""
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		address := a.address
		if current, err := r.sourceAddress(a); err == nil {
			address = current
		}
		_, _ = r.client.ReleaseBootstrapTransfer(releaseCtx, address, r.port, fsm.BootstrapReleaseRequest{Version: a.request.Version, Requester: a.request.Requester, AttemptID: a.request.AttemptID, TransferID: transferID})
	}
	defer release() // An uncertain Begin delivery may already own source storage.
	var response fsm.BootstrapBeginResponse
	err = retryReplicaBootstrapRPC(ctx, func() error {
		address, err := r.sourceAddress(a)
		if err != nil {
			return err
		}
		var e error
		response, e = r.client.BeginBootstrapTransfer(ctx, address, r.port, a.request)
		return e
	})
	if err != nil {
		return err
	}
	if err = response.ValidateFor(a.request); err != nil {
		return err
	}
	transferID = response.Manifest.TransferID
	stage, err := ticket.Partition().StageBootstrap(response.Manifest)
	if err != nil {
		return err
	}
	defer func() {
		cleanup := stage.Abort()
		// Durable intent owns the stage after Install. Abort cannot remove it;
		// restart/resume, rather than this transient owner, completes recovery.
		if errors.Is(cleanup, disk.ErrBootstrapPending) {
			return
		}
		r.rememberCleanup(cleanup)
		result = errors.Join(result, cleanup)
	}()
	for _, file := range response.Manifest.Files {
		for offset := int64(0); offset < file.Size; {
			chunk := fsm.BootstrapChunkRequest{Version: a.request.Version, Requester: a.request.Requester, AttemptID: a.request.AttemptID, TransferID: transferID, FileID: file.ID, Offset: offset, Size: disk.BootstrapChunkBytes}
			var data fsm.BootstrapChunkResponse
			err = retryReplicaBootstrapRPC(ctx, func() error {
				address, err := r.sourceAddress(a)
				if err != nil {
					return err
				}
				var e error
				data, e = r.client.ReadBootstrapTransferChunk(ctx, address, r.port, chunk, file.Size)
				metrics.RecordRecoveryBytes(metrics.RecoveryReplicaRPC, metrics.RecoveryTransferBytes, int64(len(data.Data)))
				return e
			})
			if err != nil {
				return err
			}
			if err = data.ValidateFor(chunk, file.Size); err != nil {
				return err
			}
			if err = ctx.Err(); err != nil {
				return err
			}
			if err = stage.WriteChunk(file.ID, offset, data.Data); err != nil {
				return err
			}
			offset += int64(len(data.Data))
		}
	}
	if err = stage.PrepareContext(ctx); err != nil {
		return err
	}
	release()
	return r.install(ctx, ticket.Partition(), response.Manifest, stage, ticket, func(protected bool) {
		r.mu.Lock()
		a.installing = protected
		r.mu.Unlock()
	})
}

func (r *replicaBootstrapRecovery) install(ctx context.Context, p *topic.Partition, manifest disk.BootstrapMetadata, receiver *disk.BootstrapReceiver, ticket *fsm.ReplicaBootstrapReceiverTicket, fenced func(bool)) (result error) {
	started := time.Now()
	util.Info("replica_recovery phase=bootstrap_install topic=%q partition=%d start=%d end=%d resume=%t", manifest.Fence.Topic, manifest.Fence.Partition, manifest.StartOffset, manifest.EndOffset, receiver == nil)
	defer func() {
		metrics.RecordRecoveryAttempt(metrics.RecoveryBootstrapInstall, result)
		util.Info("replica_recovery phase=bootstrap_install_finished topic=%q partition=%d start=%d end=%d elapsed_ms=%d error=%q", manifest.Fence.Topic, manifest.Fence.Partition, manifest.StartOffset, manifest.EndOffset, time.Since(started).Milliseconds(), fmt.Sprint(result))
	}()
	ch := r.ch
	eventSourcing := false
	if fenced != nil {
		defer fenced(false)
	}
	validate := func(resuming bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if ch.Cluster.Router.BrokerID() != r.brokerID {
			return fsm.ErrBootstrapTransferStaleFence
		}
		if receiver != nil {
			if resuming {
				return fsm.ErrBootstrapTransferStaleFence
			}
			if err := r.state.ValidateReplicaBootstrapReceiverPrelocked(ticket, manifest.EndOffset); err != nil {
				return err
			}
		} else {
			if !resuming {
				return fsm.ErrBootstrapTransferStaleFence
			}
			current, _ := p.BootstrapStatus()
			if current == nil || current.TransferID != manifest.TransferID {
				return fsm.ErrBootstrapTransferStaleFence
			}
			if err := r.state.ValidateReplicaBootstrapResumePrelocked(r.brokerID, p, manifest); err != nil {
				return err
			}
		}
		var err error
		eventSourcing, err = r.state.ReplicaBootstrapEventSourcingPrelocked(p, manifest)
		if err != nil {
			return err
		}
		// Controller gates and the actual FSM fence are now held. Metadata
		// cannot change until this local attempt leaves the validated boundary.
		if fenced != nil {
			fenced(true)
		}
		return nil
	}
	detach := func() error {
		return ch.ESHandler.DetachPartitionForBootstrap(manifest.Fence.Topic, manifest.Fence.Partition)
	}
	rebuild := func(view *topic.PartitionReadView, catalog []byte) error {
		if !eventSourcing {
			if len(catalog) != 0 {
				return fmt.Errorf("ordinary topic bootstrap contains an application snapshot catalog")
			}
			// BootstrapSession already rebuilt raw producer and transaction state.
			return nil
		}
		return ch.ESHandler.RebuildBootstrapState(manifest.Fence.Topic, manifest.Fence.Partition, view, catalog, manifest.Fence.LifecycleEpoch)
	}
	if manifest.Fence.Topic == config.ConsumerOffsetsTopicName {
		identity := coordinator.ConsumerBootstrapIdentity{Partition: manifest.Fence.Partition, Lifecycle: manifest.Fence.LifecycleEpoch, Attempt: manifest.TransferID}
		delay := 10 * time.Millisecond
		for {
			err := ch.RunConsumerMetadataBootstrap(ctx, identity, receiver, detach, rebuild, validate)
			if fenced != nil {
				fenced(false)
			}
			if !errors.Is(err, fsm.ErrConsumerMetadataFencePending) && !errors.Is(err, ErrConsumerRecoveryPending) {
				return err
			}
			if err = waitForContext(ctx, delay); err != nil {
				return err
			}
			metrics.RecordRecoveryWork(metrics.RecoveryReplicaRPC, metrics.RecoveryRetries, 1)
			delay = min(delay*2, time.Second)
		}
	}
	releaseLifecycle, err := ch.topicLifecycleGates.acquire(ctx, manifest.Fence.Topic, false)
	if err != nil {
		return err
	}
	defer releaseLifecycle()
	writeLock := ch.partitionWriteLock(manifest.Fence.Topic, manifest.Fence.Partition)
	if err = writeLock.LockContext(ctx); err != nil {
		return err
	}
	defer writeLock.Unlock()
	return r.state.WithReplicaBootstrapInstallFence(ctx, func() error {
		if err := validate(p.BootstrapPending()); err != nil {
			return err
		}
		session, err := p.BeginBootstrapSession()
		if err != nil {
			return err
		}
		defer session.Close()
		if receiver != nil {
			if err = session.Install(receiver, detach); err != nil {
				return err
			}
		}
		return session.Complete(rebuild)
	})
}
