package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
)

func (c *consumerMetadataRecoveryCache) source(m fsm.PartitionMetadata, cached *consumerMetadataCacheEntry) (*fsm.BrokerInfo, error) {
	valid := func(id string) *fsm.BrokerInfo {
		if id == c.brokerID || !containsReplica(m.Replicas, id) || !containsReplica(m.ISR, id) {
			return nil
		}
		b := c.state.GetBroker(id)
		if !consumerRecoveryProcess(b) {
			return nil
		}
		host, _, err := net.SplitHostPort(b.Addr)
		if err != nil || host == "" || c.port <= 0 || c.port > 65535 {
			return nil
		}
		return b
	}
	if cached != nil {
		if b := valid(cached.request.Source.BrokerID); b != nil {
			return b, nil
		}
	}
	for _, id := range m.ISR {
		if b := valid(id); b != nil {
			return b, nil
		}
	}
	return nil, ErrConsumerRecoveryPending
}

func compatibleConsumerMetadata(entry *consumerMetadataCacheEntry, r fsm.BootstrapBeginRequest) bool {
	if entry == nil {
		return false
	}
	old := entry.request
	return old.Requester == r.Requester && old.Source == r.Source && old.Topic == r.Topic && old.Partition == r.Partition && old.Leader == r.Leader && old.LeaderEpoch == r.LeaderEpoch && old.LifecycleEpoch == r.LifecycleEpoch
}

// Retry only delivery failures, preserving the exact attempt fingerprint.
func retryConsumerMetadataRPC(ctx context.Context, call func() error) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := call()
		var network net.Error
		transient := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &network)
		if err == nil || !transient || attempt == 1 || ctx.Err() != nil {
			return err
		}
	}
}

func (c *consumerMetadataRecoveryCache) stagePartition(ctx context.Context, requester fsm.BootstrapProcessIdentity, p int, m fsm.PartitionMetadata) (resultErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	old := c.entries[p]
	if old != nil && m.CommittedHWM < old.bounds.EndOffset {
		return ErrConsumerRecoveryPending
	}
	b, err := c.source(m, old)
	if err != nil {
		return err
	}
	attempt, err := fsm.NewBootstrapAttemptID()
	if err != nil {
		return err
	}
	r := fsm.BootstrapBeginRequest{Version: fsm.BootstrapTransferVersion, Purpose: fsm.BootstrapPurposeMetadata, Requester: requester, Source: fsm.BootstrapProcessIdentity{BrokerID: b.ID, IncarnationID: b.IncarnationID, IncarnationEpoch: b.IncarnationEpoch}, Topic: config.ConsumerOffsetsTopicName, Partition: p, Leader: m.Leader, LeaderEpoch: m.LeaderEpoch, LifecycleEpoch: m.LifecycleEpoch, AttemptID: attempt, FixedEnd: m.CommittedHWM}
	if compatibleConsumerMetadata(old, r) {
		r.PriorCertificateID, r.SuffixStart = old.certificate, old.bounds.EndOffset
	}
	if err = r.Validate(); err != nil {
		return err
	}
	select {
	case c.permits <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-c.permits }()
	transferID := ""
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_, _ = c.client.ReleaseBootstrapTransfer(releaseCtx, b.Addr, c.port, fsm.BootstrapReleaseRequest{Version: r.Version, Requester: r.Requester, AttemptID: r.AttemptID, TransferID: transferID})
	}
	defer release() // Includes an uncertain Begin outcome.
	var response fsm.BootstrapBeginResponse
	err = retryConsumerMetadataRPC(ctx, func() error {
		var e error
		response, e = c.client.BeginBootstrapTransfer(ctx, b.Addr, c.port, r)
		return e
	})
	if err != nil {
		return err
	}
	if err = response.ValidateFor(r); err != nil {
		return err
	}
	transferID = response.Manifest.TransferID
	if response.CaptureMode == fsm.BootstrapCaptureSuffix {
		if !compatibleConsumerMetadata(old, r) || old.certificate != r.PriorCertificateID || old.bounds.EndOffset != response.StartOffset {
			return fmt.Errorf("consumer metadata suffix cache mismatch")
		}
		if r.FixedEnd == old.bounds.EndOffset {
			empty := sha256.Sum256(nil)
			for _, file := range response.Manifest.Files {
				if file.Size != 0 || file.SHA256 != hex.EncodeToString(empty[:]) {
					return fmt.Errorf("nonempty same-end metadata suffix")
				}
			}
			old.request, old.certificate = r, response.CertificateID
			return nil
		}
	}
	stage, err := c.root.Stage(r.Topic, p, response.Manifest)
	if err != nil {
		release()
		return err
	}
	prepared := false
	defer func() {
		if !prepared {
			release()
			cleanup := stage.Abort()
			c.rememberCleanup(cleanup)
			resultErr = errors.Join(resultErr, cleanup)
		}
	}()
	for _, file := range response.Manifest.Files {
		for offset := int64(0); offset < file.Size; {
			chunk := fsm.BootstrapChunkRequest{Version: r.Version, Requester: r.Requester, AttemptID: r.AttemptID, TransferID: transferID, FileID: file.ID, Offset: offset, Size: disk.BootstrapChunkBytes}
			var data fsm.BootstrapChunkResponse
			err = retryConsumerMetadataRPC(ctx, func() error {
				var e error
				data, e = c.client.ReadBootstrapTransferChunk(ctx, b.Addr, c.port, chunk, file.Size)
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
	raw, err := stage.Prepare(ctx)
	if err != nil {
		return err
	}
	prepared = true
	release()
	defer func() {
		cleanup := raw.Close() // Idempotent after adoption or suffix consumption.
		c.rememberCleanup(cleanup)
		resultErr = errors.Join(resultErr, cleanup)
	}()
	if response.CaptureMode == fsm.BootstrapCaptureSuffix {
		result, err := old.store.AppendPrepared(ctx, old.bounds, raw)
		if err != nil {
			return err
		}
		// Publication committed; certificate and bounds cannot lag the raw store.
		old.bounds, old.request, old.certificate = result.Bounds, r, response.CertificateID
		c.rememberCleanup(result.CleanupError)
		return result.CleanupError
	}
	adopted, err := c.root.Adopt(raw)
	if err != nil {
		return err
	}
	bounds, err := adopted.Bounds()
	if err != nil {
		return errors.Join(err, adopted.Close())
	}
	store, err := c.constructStore(ctx, adopted, c.budget)
	if err != nil {
		cleanup := adopted.Close()
		c.rememberCleanup(cleanup)
		return errors.Join(err, cleanup)
	}
	c.entries[p] = &consumerMetadataCacheEntry{request: r, bounds: bounds, certificate: response.CertificateID, store: store}
	if old != nil {
		err = retireConsumerMetadataStore(old.store)
		c.rememberCleanup(err)
	}
	return err
}
