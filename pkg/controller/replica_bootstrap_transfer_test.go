package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/eventsource"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func replicaTransferFixture(t *testing.T) (*CommandHandler, *bootstrapTransferSource, fsm.BootstrapBeginRequest) {
	t.Helper()
	h, s, r := transferSourceFixture(t)
	require.NoError(t, s.close())
	h.bootstrapSource = nil
	require.NoError(t, h.ESHandler.Close())
	h.ESHandler = eventsource.NewHandler(h.TopicManager)
	require.NoError(t, h.ConfigureBootstrapTransferSource(context.Background(), "source-process"))
	s = h.bootstrapSource
	// Share the same partition with metadata to exercise cross-purpose ownership.
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("%s-%d", r.Topic, i)
		m := s.state.GetPartitionMetadata(key)
		m.Replicas = []string{r.Source.BrokerID, r.Requester.BrokerID}
		m.ISR = []string{r.Source.BrokerID}
		data, err := json.Marshal(m)
		require.NoError(t, err)
		require.Nil(t, s.state.Apply(&raft.Log{Data: append([]byte("PARTITION:"+key+":"), data...)}))
	}
	r.Purpose = fsm.BootstrapPurposeReplica
	return h, s, r
}

func TestReplicaBootstrapTransferMixedAdmissionAndNoCertificateSlots(t *testing.T) {
	h, s, r := replicaTransferFixture(t)
	metadata := r
	metadata.Purpose = fsm.BootstrapPurposeMetadata
	first, err := h.BeginBootstrapTransfer(context.Background(), metadata)
	require.NoError(t, err)
	require.NotEmpty(t, first.CertificateID)
	r.AttemptID = transferID(t)
	_, err = h.BeginBootstrapTransfer(context.Background(), r)
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferAttemptConflict)
	for i := 1; i < bootstrapSourceLimit; i++ {
		request := r
		request.Partition, request.AttemptID = i, transferID(t)
		if i%2 == 0 {
			request.Purpose = fsm.BootstrapPurposeMetadata
		}
		response, err := h.BeginBootstrapTransfer(context.Background(), request)
		require.NoError(t, err)
		if request.Purpose == fsm.BootstrapPurposeReplica {
			require.Empty(t, response.CertificateID)
			require.Positive(t, response.Manifest.Files[1].Size)
		}
	}
	r.Partition = 7
	_, err = h.BeginBootstrapTransfer(context.Background(), r)
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferLimit)
	s.mu.Lock()
	require.Equal(t, 3, s.nextCertificate)
	require.Equal(t, first.CertificateID, s.certificates[0].id)
	for i := 3; i < len(s.certificates); i++ {
		require.Nil(t, s.certificates[i])
	}
	s.mu.Unlock()
	require.NoError(t, h.Close())
	transferWaitEmpty(t, s)
}

func TestReplicaBootstrapTransferFixedDuplicateAndPublicationRetry(t *testing.T) {
	h, s, r := replicaTransferFixture(t)
	p, err := h.TopicManager.GetTopic(r.Topic).GetPartition(0)
	require.NoError(t, err)
	var captures, fences atomic.Int32
	works := make(chan disk.FullBootstrapWork, 1)
	actualCapture := s.replicaCapture
	s.replicaCapture = func(ctx context.Context, ticket *fsm.ReplicaBootstrapSourceTicket) (*fsm.ReplicaBootstrapCapture, error) {
		captures.Add(1)
		// Advance after FSM prepare, before entering the exclusive source cut.
		require.NoError(t, p.ReplicaAppendWithMode([]types.Message{{Offset: 0, Key: "group", Payload: "state"}}, true))
		require.NoError(t, p.ApplyReplicaHWM(1))
		transferMetadata(t, s, r, func(m *fsm.PartitionMetadata) { m.CommittedHWM = 1 })
		capture, err := actualCapture(ctx, ticket)
		if err == nil {
			works <- capture.FullWork()
		}
		return capture, err
	}
	s.replicaFence = func(ctx context.Context, capture *fsm.ReplicaBootstrapCapture, fn func(*fsm.ReplicaBootstrapPublication) error) error {
		if fences.Add(1) < 4 {
			return disk.ErrContentPublicationBusy
		}
		return s.state.WithReplicaBootstrapSourceFence(ctx, capture, fn)
	}
	first, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	require.EqualValues(t, 1, first.Manifest.EndOffset)
	require.EqualValues(t, 1, captures.Load())
	require.EqualValues(t, 4, fences.Load())
	s.mu.Lock()
	entry := s.attempts[bootstrapAttemptKey{r.Requester, r.AttemptID}]
	expires := entry.expires
	s.mu.Unlock()
	require.NoError(t, p.ReplicaAppendWithMode([]types.Message{{Offset: 1, Key: "group", Payload: "next"}}, true))
	require.NoError(t, p.ApplyReplicaHWM(2))
	transferMetadata(t, s, r, func(m *fsm.PartitionMetadata) { m.CommittedHWM = 2 })
	second, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.EqualValues(t, 1, captures.Load())
	s.mu.Lock()
	require.Equal(t, expires, entry.expires)
	require.Zero(t, s.nextCertificate)
	s.mu.Unlock()
	chunk, err := h.ReadBootstrapTransferChunk(context.Background(), transferChunk(first))
	require.NoError(t, err)
	require.EqualValues(t, first.Manifest.Files[0].Size, len(chunk.Data))
	work := <-works
	require.EqualValues(t, 1, work.DecodedFrames)
	require.EqualValues(t, first.Manifest.Files[0].Size, work.CanonicalWriteBytes)
	require.EqualValues(t, first.Manifest.Files[1].Size, work.CatalogWriteBytes)
	require.Equal(t, work.CanonicalWriteBytes+work.CatalogWriteBytes, work.HashReadBytes)
	require.Empty(t, works, "publication retries or duplicates performed another export")
	t.Logf("one full capture; four final fences; decoded source frame bytes=%d; canonical writes=%d; catalog writes=%d; checksum reads=%d; chunk read=%d (user-space I/O; excludes catalog proof/replay and physical device I/O)", work.DecodedFrameBytes, work.CanonicalWriteBytes, work.CatalogWriteBytes, work.HashReadBytes, len(chunk.Data))
}

func TestReplicaBootstrapTransferCanceledLateCaptureRetainsPermit(t *testing.T) {
	for _, how := range []string{"cancel", "release", "close"} {
		t.Run(how, func(t *testing.T) {
			h, s, r := replicaTransferFixture(t)
			entered, resume := make(chan struct{}), make(chan struct{})
			var late *fsm.ReplicaBootstrapCapture
			actual := s.replicaCapture
			s.replicaCapture = func(_ context.Context, ticket *fsm.ReplicaBootstrapSourceTicket) (*fsm.ReplicaBootstrapCapture, error) {
				var err error
				late, err = actual(context.Background(), ticket)
				close(entered)
				<-resume
				return late, err
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := h.BeginBootstrapTransfer(ctx, r); done <- err }()
			transferWait(t, entered)
			joined := make(chan struct{})
			switch how {
			case "cancel":
				cancel()
			case "release":
				_, err := h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
				require.NoError(t, err)
			case "close":
				go func() { _ = h.Close(); close(joined) }()
				require.Eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.closed }, time.Second, time.Millisecond)
			}
			s.mu.Lock()
			require.Len(t, s.attempts, 1)
			require.Len(t, bootstrapSourcePermits, 1)
			s.mu.Unlock()
			if how != "close" {
				r.AttemptID = transferID(t)
				_, err := h.BeginBootstrapTransfer(context.Background(), r)
				require.ErrorIs(t, err, fsm.ErrBootstrapTransferAttemptConflict)
			}
			close(resume)
			require.Error(t, <-done)
			transferWaitEmpty(t, s)
			_, err := late.ReadChunk("catalog", 0, 1)
			require.Error(t, err)
			if how == "close" {
				transferWait(t, joined)
			}
		})
	}
}

func TestReplicaBootstrapTransferReadyLifetimeStartsAtPublication(t *testing.T) {
	h, s, r := replicaTransferFixture(t)
	s.ttl = 80 * time.Millisecond
	entered, resume := make(chan struct{}), make(chan struct{})
	actual := s.replicaCapture
	s.replicaCapture = func(ctx context.Context, ticket *fsm.ReplicaBootstrapSourceTicket) (*fsm.ReplicaBootstrapCapture, error) {
		capture, err := actual(ctx, ticket)
		close(entered)
		<-resume
		return capture, err
	}
	results, errs := make(chan fsm.BootstrapBeginResponse, 1), make(chan error, 1)
	go func() { r, e := h.BeginBootstrapTransfer(context.Background(), r); results <- r; errs <- e }()
	transferWait(t, entered)
	time.Sleep(2 * s.ttl)
	close(resume)
	require.NoError(t, <-errs)
	response := <-results
	chunk := transferChunk(response)
	chunk.FileID = "catalog"
	_, err := h.ReadBootstrapTransferChunk(context.Background(), chunk)
	require.NoError(t, err)
	transferWaitEmpty(t, s)
	_, err = h.ReadBootstrapTransferChunk(context.Background(), chunk)
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferNotFound)
}

func TestReplicaBootstrapMissingCatalogAuthorityPreservesMetadata(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	require.NoError(t, s.close())
	h.bootstrapSource = nil
	require.NoError(t, h.ESHandler.Close())
	h.ESHandler = nil
	require.NoError(t, h.ConfigureBootstrapTransferSource(context.Background(), "source-process"))
	r.Purpose = fsm.BootstrapPurposeReplica
	_, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferUnavailable)
	r.Purpose = fsm.BootstrapPurposeMetadata
	_, err = h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
}

func TestReplicaBootstrapReadyReadPinRetainsOwnedExport(t *testing.T) {
	for _, how := range []string{"release", "expiry"} {
		t.Run(how, func(t *testing.T) {
			h, s, r := replicaTransferFixture(t)
			if how == "expiry" {
				s.ttl = 80 * time.Millisecond
			}
			response, err := h.BeginBootstrapTransfer(context.Background(), r)
			require.NoError(t, err)
			entered, resume := make(chan struct{}), make(chan struct{})
			s.mu.Lock()
			entry := s.attempts[bootstrapAttemptKey{r.Requester, r.AttemptID}]
			owned := entry.export
			entry.export = &transferTestExport{bootstrapSourceExport: owned, read: func() { close(entered); <-resume }}
			s.mu.Unlock()
			chunk := transferChunk(response)
			chunk.FileID = "catalog"
			done := make(chan error, 1)
			go func() { _, err := h.ReadBootstrapTransferChunk(context.Background(), chunk); done <- err }()
			transferWait(t, entered)
			if how == "release" {
				_, err = h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
				require.NoError(t, err)
			}
			require.Eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return entry.phase == bootstrapClosing }, time.Second, time.Millisecond)
			_, err = owned.ReadChunk("catalog", 0, 1)
			require.NoError(t, err, "owned export expired while ready read was pinned")
			close(resume)
			require.NoError(t, <-done)
			transferWaitEmpty(t, s)
			_, err = owned.ReadChunk("catalog", 0, 1)
			require.Error(t, err)
		})
	}
}

func TestReplicaBootstrapTransferRejectedPublicationClosesCapture(t *testing.T) {
	h, s, r := replicaTransferFixture(t)
	actual := s.replicaCapture
	var captured *fsm.ReplicaBootstrapCapture
	s.replicaCapture = func(ctx context.Context, ticket *fsm.ReplicaBootstrapSourceTicket) (*fsm.ReplicaBootstrapCapture, error) {
		var err error
		captured, err = actual(ctx, ticket)
		if err == nil {
			transferRegister(t, s.state, "source", "replacement", "active", 2)
		}
		return captured, err
	}
	_, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferStaleFence)
	transferWaitEmpty(t, s)
	require.NotNil(t, captured)
	_, err = captured.ReadChunk("catalog", 0, 1)
	require.Error(t, err)
}
