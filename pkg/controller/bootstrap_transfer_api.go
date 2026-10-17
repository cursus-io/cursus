package controller

import (
	"context"
	"fmt"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
)

// ConfigureBootstrapTransferSource installs the optional process-owned source.
// The incarnation comes from process startup, never from a transfer request.
// Registration may acquire a new assigned epoch, but must still name this process.
func (ch *CommandHandler) ConfigureBootstrapTransferSource(ctx context.Context, incarnation string) error {
	if ch == nil || ctx == nil || incarnation == "" {
		return fsm.ErrBootstrapTransferUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ch.bootstrapMu.Lock()
	defer ch.bootstrapMu.Unlock()
	if ch.closed {
		return fsm.ErrBootstrapTransferUnavailable
	}
	if ch.bootstrapSource != nil {
		return fmt.Errorf("bootstrap transfer source already configured")
	}
	if ch.Cluster == nil || ch.Cluster.Router == nil || ch.Cluster.RaftManager == nil {
		return fsm.ErrBootstrapTransferUnavailable
	}
	state := ch.Cluster.RaftManager.GetFSM()
	if state == nil {
		return fsm.ErrBootstrapTransferUnavailable
	}
	brokerID := ch.Cluster.Router.BrokerID()
	if brokerID == "" {
		return fsm.ErrBootstrapTransferUnavailable
	}
	identity := func() (fsm.BootstrapProcessIdentity, error) {
		b := state.GetBroker(brokerID)
		if b == nil || b.ID != brokerID || b.IncarnationID != incarnation || b.IncarnationEpoch == 0 {
			return fsm.BootstrapProcessIdentity{}, fsm.ErrBootstrapTransferStaleFence
		}
		return fsm.BootstrapProcessIdentity{BrokerID: brokerID, IncarnationID: incarnation, IncarnationEpoch: b.IncarnationEpoch}, nil
	}
	source := newBootstrapTransferSource(ctx, state, identity)
	source.incarnation = incarnation
	// Capture the dependency before admission. Missing catalog authority disables
	// only replica capture; metadata sources remain independently available.
	if handler := ch.ESHandler; handler != nil {
		source.replicaCapture = func(ctx context.Context, ticket *fsm.ReplicaBootstrapSourceTicket) (*fsm.ReplicaBootstrapCapture, error) {
			return ticket.CaptureContext(ctx, func(source *topic.BootstrapSource, fence disk.BootstrapFence) (*disk.BootstrapExport, error) {
				return handler.ExportBootstrapWithCatalogOwnedContext(ctx, fence.Topic, fence.Partition, source, fence.LifecycleEpoch, fence)
			})
		}
	}
	ch.bootstrapSource = source
	return nil
}

func (ch *CommandHandler) transferSource() (*bootstrapTransferSource, error) {
	if ch == nil {
		return nil, fsm.ErrBootstrapTransferUnavailable
	}
	ch.bootstrapMu.Lock()
	defer ch.bootstrapMu.Unlock()
	if ch.closed || ch.bootstrapSource == nil {
		return nil, fsm.ErrBootstrapTransferUnavailable
	}
	return ch.bootstrapSource, nil
}

// BeginBootstrapTransfer creates or joins one exact source capture attempt.
func (ch *CommandHandler) BeginBootstrapTransfer(ctx context.Context, r fsm.BootstrapBeginRequest) (fsm.BootstrapBeginResponse, error) {
	s, err := ch.transferSource()
	if err != nil {
		return fsm.BootstrapBeginResponse{}, err
	}
	return s.begin(ctx, r)
}

// ReadBootstrapTransferChunk reads an immutable published export under a pin.
func (ch *CommandHandler) ReadBootstrapTransferChunk(ctx context.Context, r fsm.BootstrapChunkRequest) (fsm.BootstrapChunkResponse, error) {
	s, err := ch.transferSource()
	if err != nil {
		return fsm.BootstrapChunkResponse{}, err
	}
	return s.read(ctx, r)
}

// ReleaseBootstrapTransfer acknowledges retirement requested, not cleanup joined.
// It deliberately does not require the old leader, lifecycle or source authority.
func (ch *CommandHandler) ReleaseBootstrapTransfer(ctx context.Context, r fsm.BootstrapReleaseRequest) (fsm.BootstrapReleaseResponse, error) {
	s, err := ch.transferSource()
	if err != nil {
		return fsm.BootstrapReleaseResponse{}, err
	}
	return s.release(ctx, r)
}
