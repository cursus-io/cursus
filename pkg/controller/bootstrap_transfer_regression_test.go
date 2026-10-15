package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func transferMetadata(t *testing.T, s *bootstrapTransferSource, r fsm.BootstrapBeginRequest, change func(*fsm.PartitionMetadata)) {
	t.Helper()
	key := r.Topic + "-0"
	m := s.state.GetPartitionMetadata(key)
	change(m)
	data, err := json.Marshal(m)
	require.NoError(t, err)
	require.Nil(t, s.state.Apply(&raft.Log{Data: append([]byte("PARTITION:"+key+":"), data...)}))
}
func transferAppend(t *testing.T, h *CommandHandler, s *bootstrapTransferSource, r *fsm.BootstrapBeginRequest) {
	t.Helper()
	p, err := h.TopicManager.GetTopic(r.Topic).GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, p.ReplicaAppendWithMode([]types.Message{{Offset: r.FixedEnd, Key: "group", Payload: "state"}}, true))
	r.FixedEnd++
	require.NoError(t, p.ApplyReplicaHWM(r.FixedEnd))
	transferMetadata(t, s, *r, func(m *fsm.PartitionMetadata) { m.CommittedHWM = r.FixedEnd })
}

func TestBootstrapTransferSourceReadyOutlivesCreator(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	response, err := h.BeginBootstrapTransfer(ctx, r)
	require.NoError(t, err)
	cancel()
	chunk, err := h.ReadBootstrapTransferChunk(context.Background(), transferChunk(response))
	require.NoError(t, err)
	require.Empty(t, chunk.Data)
	again, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, response, again)
	_, err = h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
	require.NoError(t, err)
	transferWaitEmpty(t, s)
	_, err = h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
	require.NoError(t, err)
	s.mu.Lock()
	require.Empty(t, s.attempts)
	s.mu.Unlock()
}

func TestBootstrapTransferSourceCreationDeadline(t *testing.T) {
	for _, duration := range []time.Duration{time.Minute, 10 * time.Minute} {
		t.Run(duration.String(), func(t *testing.T) {
			h, s, r := transferSourceFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()
			started := time.Now()
			observed := make(chan time.Time, 1)
			s.export = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
				deadline, ok := ctx.Deadline()
				if !ok {
					return nil, errors.New("missing deadline")
				}
				observed <- deadline
				e, err := ticket.ExportContext(ctx)
				if err != nil {
					return nil, err
				}
				return e, nil
			}
			_, err := h.BeginBootstrapTransfer(ctx, r)
			require.NoError(t, err)
			deadline := <-observed
			require.LessOrEqual(t, deadline.Sub(started), min(duration, bootstrapSourceCreateTimeout)+time.Second)
			require.Greater(t, deadline.Sub(started), min(duration, bootstrapSourceCreateTimeout)-time.Second)
		})
	}
}

func TestBootstrapTransferSourceSuffixWorkAndOldCertificate(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	transferAppend(t, h, s, &r)
	works := make(chan disk.RetainedBootstrapWork, 5)
	s.export = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
		e, err := ticket.ExportContext(ctx)
		if err != nil {
			return nil, err
		}
		works <- e.RetainedWork()
		return e, nil
	}
	first, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	fullWork := <-works
	require.Greater(t, fullWork.CanonicalWriteBytes, uint64(0))
	require.Equal(t, fullWork.CanonicalWriteBytes, fullWork.HashReadBytes)
	_, err = h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
	require.NoError(t, err)
	transferWaitEmpty(t, s)
	oldEnd := r.FixedEnd
	transferAppend(t, h, s, &r)
	r.AttemptID = transferID(t)
	r.PriorCertificateID = first.CertificateID
	r.SuffixStart = oldEnd
	suffix, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, fsm.BootstrapCaptureSuffix, suffix.CaptureMode)
	require.Equal(t, oldEnd, suffix.StartOffset)
	suffixWork := <-works
	require.Greater(t, suffixWork.CanonicalWriteBytes, uint64(0))
	require.Equal(t, suffixWork.CanonicalWriteBytes, suffixWork.HashReadBytes)
	_, err = h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
	require.NoError(t, err)
	transferWaitEmpty(t, s)
	// An older reader can still use the original certificate after newer exports.
	r.AttemptID = transferID(t)
	again, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, fsm.BootstrapCaptureSuffix, again.CaptureMode)
	require.Equal(t, suffixWork, <-works)
	_, err = h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
	require.NoError(t, err)
	transferWaitEmpty(t, s)
	r.AttemptID = transferID(t)
	r.PriorCertificateID = suffix.CertificateID
	r.SuffixStart = r.FixedEnd
	sameEnd, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, fsm.BootstrapCaptureSuffix, sameEnd.CaptureMode)
	require.Zero(t, sameEnd.Manifest.Files[0].Size)
	emptyWork := <-works
	require.Zero(t, emptyWork.CanonicalWriteBytes)
	require.Zero(t, emptyWork.HashReadBytes)
	t.Logf("full bytes=%d pages=%d; suffix bytes=%d pages=%d; same-end bytes=%d pages=%d", fullWork.CanonicalWriteBytes, fullWork.Pages, suffixWork.CanonicalWriteBytes, suffixWork.Pages, emptyWork.CanonicalWriteBytes, emptyWork.Pages)
}

func TestBootstrapTransferSourceCertificateEviction(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	first, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	for i := 0; i < 128; i++ {
		_, err = h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
		require.NoError(t, err)
		transferWaitEmpty(t, s)
		r.AttemptID = transferID(t)
		_, err = h.BeginBootstrapTransfer(context.Background(), r)
		require.NoError(t, err)
	}
	_, err = h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
	require.NoError(t, err)
	transferWaitEmpty(t, s)
	r.AttemptID = transferID(t)
	r.PriorCertificateID = first.CertificateID
	response, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, fsm.BootstrapCaptureFull, response.CaptureMode)
	require.Equal(t, r, response.Request)
	s.mu.Lock()
	require.Len(t, s.certificates, 128)
	s.mu.Unlock()
}

func TestBootstrapTransferSourceStaleContinuityFullFallback(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	first, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	_, err = h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
	require.NoError(t, err)
	transferWaitEmpty(t, s)
	transferMetadata(t, s, r, func(m *fsm.PartitionMetadata) { m.LeaderEpoch++ })
	r.LeaderEpoch++
	r.AttemptID = transferID(t)
	r.PriorCertificateID = first.CertificateID
	response, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, fsm.BootstrapCaptureFull, response.CaptureMode)
	require.Equal(t, r, response.Request)
}

func TestBootstrapTransferSourcePublicationBusyDoesNotRecopy(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	transferAppend(t, h, s, &r)
	var copies, publishes atomic.Int32
	works := make(chan disk.RetainedBootstrapWork, 1)
	s.export = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
		copies.Add(1)
		e, err := ticket.ExportContext(ctx)
		if err != nil {
			return nil, err
		}
		works <- e.RetainedWork()
		return e, nil
	}
	s.fence = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket, fn func(*fsm.MetadataBootstrapPublication) error) error {
		if publishes.Add(1) < 4 {
			return disk.ErrContentPublicationBusy
		}
		return s.state.WithMetadataBootstrapSourceFence(ctx, ticket, fn)
	}
	response, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	require.EqualValues(t, 1, copies.Load())
	require.EqualValues(t, 4, publishes.Load())
	work := <-works
	require.EqualValues(t, response.Manifest.Files[0].Size, work.CanonicalWriteBytes)
	require.Equal(t, work.CanonicalWriteBytes, work.HashReadBytes)
}

func TestBootstrapTransferSourceGenerationChangeBeforePublication(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	p, err := h.TopicManager.GetTopic(r.Topic).GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, p.ReplicaAppendWithMode([]types.Message{{Offset: 0, Key: "uncommitted", Payload: "tail"}}, true))
	s.export = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
		e, err := ticket.ExportContext(ctx)
		if err != nil {
			return nil, err
		}
		require.NoError(t, p.TruncateReplicaTail(0))
		return e, nil
	}
	_, err = h.BeginBootstrapTransfer(context.Background(), r)
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferStaleFence)
	transferWaitEmpty(t, s)
}

func TestBootstrapTransferSourceCurrentRegistration(t *testing.T) {
	for _, who := range []string{"source", "requester"} {
		for _, kind := range []string{"inactive", "capability", "incarnation"} {
			t.Run(who+"/"+kind, func(t *testing.T) {
				h, s, r := transferSourceFixture(t)
				response, err := h.BeginBootstrapTransfer(context.Background(), r)
				require.NoError(t, err)
				incarnation, status, capability := who+"-process", "active", 2
				switch kind {
				case "inactive":
					status = "inactive"
				case "capability":
					capability = 1
				case "incarnation":
					incarnation = "replacement"
				}
				transferRegister(t, s.state, who, incarnation, status, capability)
				_, err = h.BeginBootstrapTransfer(context.Background(), r)
				require.ErrorIs(t, err, fsm.ErrBootstrapTransferStaleFence)
				_, chunkErr := h.ReadBootstrapTransferChunk(context.Background(), transferChunk(response))
				_, releaseErr := h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
				if who == "requester" {
					require.ErrorIs(t, chunkErr, fsm.ErrBootstrapTransferStaleFence)
					require.ErrorIs(t, releaseErr, fsm.ErrBootstrapTransferStaleFence)
				} else {
					require.NoError(t, chunkErr)
					require.NoError(t, releaseErr)
				}
			})
		}
	}
}

func TestBootstrapTransferSourcePendingConsumerAndChangedLeaderCleanup(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	c, err := coordinator.NewCoordinatorWithRecovery(context.Background(), h.Config, &dummyPublisher{})
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	h.Coordinator = c
	c.BeginConsumerRestore()
	require.True(t, c.ConsumerRecoveryRequirement().Pending())
	response, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	transferMetadata(t, s, r, func(m *fsm.PartitionMetadata) { m.LeaderEpoch++ })
	_, err = h.ReadBootstrapTransferChunk(context.Background(), transferChunk(response))
	require.NoError(t, err)
	_, err = h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
	require.NoError(t, err)
	transferWaitEmpty(t, s)
}

func TestBootstrapTransferSourceCleanupHandoffAndCloseError(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	debtErr := errors.New("physical cleanup handed to disk owner")
	s.export = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
		e, err := ticket.ExportContext(ctx)
		if err != nil {
			return nil, err
		}
		return &transferTestExport{bootstrapSourceExport: e, closeFn: func() error { return debtErr }}, nil
	}
	_, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	_, err = h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
	require.NoError(t, err)
	transferWaitEmpty(t, s)
	// Logical admission is free after the disk contract has returned ownership.
	require.Empty(t, bootstrapSourcePermits)
	require.ErrorIs(t, h.Close(), debtErr)
	require.ErrorIs(t, h.Close(), debtErr)
}

func TestBootstrapTransferSourceChunkBounds(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	transferAppend(t, h, s, &r)
	response, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	for _, kind := range []string{"valid", "end", "past-end", "zero", "too-large", "wrong-transfer", "wrong-owner", "file"} {
		t.Run(kind, func(t *testing.T) {
			q := transferChunk(response)
			switch kind {
			case "end":
				q.Offset = response.Manifest.Files[0].Size
			case "past-end":
				q.Offset = response.Manifest.Files[0].Size + 1
			case "zero":
				q.Size = 0
			case "too-large":
				q.Size++
			case "wrong-transfer":
				q.TransferID = transferID(t)
			case "wrong-owner":
				q.Requester = r.Source
			case "file":
				q.FileID = "../records"
			}
			result, err := h.ReadBootstrapTransferChunk(context.Background(), q)
			if kind == "valid" || kind == "end" {
				require.NoError(t, err)
				require.NoError(t, result.ValidateFor(q, response.Manifest.Files[0].Size))
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestBootstrapTransferSourceOwnerContextShutdown(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	require.NoError(t, h.Close())
	ctx, cancel := context.WithCancel(context.Background())
	local := r.Source
	s = newBootstrapTransferSource(ctx, s.state, func() (fsm.BootstrapProcessIdentity, error) { return local, nil })
	_, err := s.begin(context.Background(), r)
	require.NoError(t, err)
	cancel()
	transferWaitEmpty(t, s)
	require.NoError(t, s.close())
	_, err = s.begin(context.Background(), r)
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferUnavailable)
}

func TestBootstrapTransferSourceConfigureBeforeRegistration(t *testing.T) {
	for _, prior := range []bool{false, true} {
		t.Run(fmt.Sprint(prior), func(t *testing.T) {
			state := fsm.NewBrokerFSM(nil, nil)
			if prior {
				transferRegister(t, state, "source", "old-process", "active", 2)
			}
			h := newCoordinatorRoutingHandler("source", state, nil)
			t.Cleanup(func() { require.NoError(t, h.Close()) })
			require.NoError(t, h.ConfigureBootstrapTransferSource(context.Background(), "new-process"))
			_, err := h.bootstrapSource.identity()
			require.ErrorIs(t, err, fsm.ErrBootstrapTransferStaleFence)
			transferRegister(t, state, "source", "new-process", "active", 2)
			actual, err := h.bootstrapSource.identity()
			require.NoError(t, err)
			require.Equal(t, "new-process", actual.IncarnationID)
			expected := uint64(1)
			if prior {
				expected = 2
			}
			require.Equal(t, expected, actual.IncarnationEpoch)
		})
	}
}

func TestBootstrapTransferSourceShortChunkRejected(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	transferAppend(t, h, s, &r)
	s.export = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
		e, err := ticket.ExportContext(ctx)
		if err != nil {
			return nil, err
		}
		return &transferTestExport{bootstrapSourceExport: e, data: func([]byte) []byte { return nil }}, nil
	}
	response, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	_, err = h.ReadBootstrapTransferChunk(context.Background(), transferChunk(response))
	require.ErrorContains(t, err, "length mismatch")
}

func TestBootstrapTransferSourceClosingReadDoesNotBlockOtherAttempt(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	var copies atomic.Int32
	s.export = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
		e, err := ticket.ExportContext(ctx)
		if err != nil {
			return nil, err
		}
		if copies.Add(1) == 1 {
			return &transferTestExport{bootstrapSourceExport: e, read: func() { close(entered); <-resume }}, nil
		}
		return e, nil
	}
	first, err := h.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	readDone := make(chan error, 1)
	go func() {
		_, e := h.ReadBootstrapTransferChunk(context.Background(), transferChunk(first))
		readDone <- e
	}()
	transferWait(t, entered)
	_, err = h.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
	require.NoError(t, err)
	other := r
	other.Partition = 1
	other.AttemptID = transferID(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = h.BeginBootstrapTransfer(ctx, other)
	require.NoError(t, err)
	s.mu.Lock()
	require.Len(t, s.attempts, 2)
	s.mu.Unlock()
	close(resume)
	require.NoError(t, <-readDone)
}

func TestBootstrapTransferSourceIndexedCaptureNotReadable(t *testing.T) {
	h, s, r := transferSourceFixture(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	s.fence = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket, fn func(*fsm.MetadataBootstrapPublication) error) error {
		close(entered)
		<-resume
		return s.state.WithMetadataBootstrapSourceFence(ctx, ticket, fn)
	}
	result := make(chan error, 1)
	go func() { _, err := h.BeginBootstrapTransfer(context.Background(), r); result <- err }()
	transferWait(t, entered)
	s.mu.Lock()
	e := s.attempts[bootstrapAttemptKey{r.Requester, r.AttemptID}]
	id := e.transferID
	require.Equal(t, bootstrapCreating, e.phase)
	s.mu.Unlock()
	require.NotEmpty(t, id)
	q := fsm.BootstrapChunkRequest{Version: 1, Requester: r.Requester, AttemptID: r.AttemptID, TransferID: id, FileID: "records", Size: 1}
	_, err := h.ReadBootstrapTransferChunk(context.Background(), q)
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferNotFound)
	release := transferRelease(r)
	release.TransferID = id
	_, err = h.ReleaseBootstrapTransfer(context.Background(), release)
	require.NoError(t, err)
	close(resume)
	require.Error(t, <-result)
	transferWaitEmpty(t, s)
}
