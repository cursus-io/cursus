package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster"
	"github.com/cursus-io/cursus/pkg/cluster/client"
	discovery "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

type observedTransferSource struct {
	*CommandHandler
	begins         chan struct{}
	releases       atomic.Int32
	beginCount     atomic.Int32
	creatorTimeout time.Duration
}

func (s *observedTransferSource) BeginBootstrapTransfer(ctx context.Context, r fsm.BootstrapBeginRequest) (fsm.BootstrapBeginResponse, error) {
	if s.beginCount.Add(1) == 1 && s.creatorTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.creatorTimeout)
		defer cancel()
	}
	s.begins <- struct{}{}
	return s.CommandHandler.BeginBootstrapTransfer(ctx, r)
}
func (s *observedTransferSource) ReleaseBootstrapTransfer(ctx context.Context, r fsm.BootstrapReleaseRequest) (fsm.BootstrapReleaseResponse, error) {
	s.releases.Add(1)
	return s.CommandHandler.ReleaseBootstrapTransfer(ctx, r)
}

func serveTransferFixture(t *testing.T, h *CommandHandler, creatorTimeout ...time.Duration) (*client.TCPClusterClient, *observedTransferSource, int) {
	t.Helper()
	source := &observedTransferSource{CommandHandler: h, begins: make(chan struct{}, 8)}
	if len(creatorTimeout) > 0 {
		source.creatorTimeout = creatorTimeout[0]
	}
	sd := discovery.NewServiceDiscoveryImpl(nil, "source", "", "")
	require.NoError(t, sd.BindBootstrapTransferSource(source))
	server := cluster.NewSecureClusterServer(sd, "secret", nil)
	ln, err := server.Start("127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	return client.NewSecureTCPClusterClient("secret", nil), source, ln.Addr().(*net.TCPAddr).Port
}

func TestBootstrapTransportCanceledDuplicateAndConflictPreserveCreator(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	transferAppend(t, h, s, &r)
	consumer, err := coordinator.NewCoordinatorWithRecovery(context.Background(), h.Config, &dummyPublisher{})
	require.NoError(t, err)
	t.Cleanup(consumer.Stop)
	h.Coordinator = consumer
	consumer.BeginConsumerRestore()
	require.True(t, consumer.ConsumerRecoveryRequirement().Pending())
	entered, resume := make(chan struct{}), make(chan struct{})
	s.export = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
		close(entered)
		<-resume
		return ticket.ExportContext(ctx)
	}
	c, observed, port := serveTransferFixture(t, h)
	creator := make(chan fsm.BootstrapBeginResponse, 1)
	creatorError := make(chan error, 1)
	go func() {
		response, err := c.BeginBootstrapTransfer(context.Background(), "127.0.0.1", port, r)
		creator <- response
		creatorError <- err
	}()
	transferWait(t, entered)
	<-observed.begins
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	duplicate := make(chan error, 1)
	go func() { _, err := c.BeginBootstrapTransfer(ctx, "127.0.0.1", port, r); duplicate <- err }()
	transferWait(t, observed.begins)
	cancel()
	require.ErrorIs(t, <-duplicate, context.Canceled)
	conflict := r
	conflict.FixedEnd++
	_, err = c.BeginBootstrapTransfer(context.Background(), "127.0.0.1", port, conflict)
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferAttemptConflict)
	require.Zero(t, observed.releases.Load())
	close(resume)
	response := <-creator
	require.NoError(t, <-creatorError)
	require.NoError(t, response.ValidateFor(r))
	chunk := transferChunk(response)
	data, err := c.ReadBootstrapTransferChunk(context.Background(), "127.0.0.1", port, chunk, response.Manifest.Files[0].Size)
	require.NoError(t, err)
	require.NotEmpty(t, data.Data)
	digest := sha256.Sum256(data.Data)
	require.Equal(t, response.Manifest.Files[0].SHA256, hex.EncodeToString(digest[:]))
	require.True(t, consumer.ConsumerRecoveryRequirement().Pending())
	repeated, err := c.BeginBootstrapTransfer(context.Background(), "127.0.0.1", port, r)
	require.NoError(t, err)
	require.Equal(t, response, repeated)
	require.Zero(t, observed.releases.Load())
	_, err = c.ReleaseBootstrapTransfer(context.Background(), "127.0.0.1", port, transferRelease(r))
	require.NoError(t, err)
	transferWaitEmpty(t, s)
	require.EqualValues(t, 1, observed.releases.Load())
}

func TestBootstrapTransportDuplicateReceivesSourceDeadline(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	// The first source caller has an earlier deadline than the duplicate.
	entered := make(chan struct{})
	s.export = func(ctx context.Context, _ *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	c, observed, port := serveTransferFixture(t, h, 300*time.Millisecond)
	failures := make(chan error, 2)
	go func() {
		_, err := c.BeginBootstrapTransfer(context.Background(), "127.0.0.1", port, r)
		failures <- err
	}()
	transferWait(t, entered)
	<-observed.begins
	live, cancelLive := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelLive()
	go func() { _, err := c.BeginBootstrapTransfer(live, "127.0.0.1", port, r); failures <- err }()
	transferWait(t, observed.begins)
	for range 2 {
		err := <-failures
		require.ErrorIs(t, err, context.DeadlineExceeded)
		var broker *wire.BrokerError
		require.ErrorAs(t, err, &broker)
		require.Equal(t, "bootstrap_deadline_exceeded", broker.Code)
	}
	require.NoError(t, live.Err())
	require.Zero(t, observed.releases.Load())
	transferWaitEmpty(t, s)
}

func TestBootstrapTransportUncertainDeliveryRequiresExplicitOwnerRelease(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	s.export = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
		close(entered)
		<-resume
		return ticket.ExportContext(ctx)
	}
	c, observed, port := serveTransferFixture(t, h)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := c.BeginBootstrapTransfer(ctx, "127.0.0.1", port, r); done <- err }()
	transferWait(t, entered)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Zero(t, observed.releases.Load())
	// The server has an admitted attempt even though its response cannot arrive.
	close(resume)
	repeated, err := c.BeginBootstrapTransfer(context.Background(), "127.0.0.1", port, r)
	require.NoError(t, err)
	require.NoError(t, repeated.ValidateFor(r))
	ownerCleanup, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancelCleanup()
	_, err = c.ReleaseBootstrapTransfer(ownerCleanup, "127.0.0.1", port, transferRelease(r))
	require.NoError(t, err)
	transferWaitEmpty(t, s)
	require.EqualValues(t, 1, observed.releases.Load())
}
