package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type metadataTransferProbe struct {
	ConsumerMetadataRecoveryClient
	begin       func(context.Context, fsm.BootstrapBeginRequest, fsm.BootstrapBeginResponse, error) (fsm.BootstrapBeginResponse, error)
	chunk       func(fsm.BootstrapChunkResponse) fsm.BootstrapChunkResponse
	beforeBegin func(context.Context)
	requests    []fsm.BootstrapBeginRequest
	responses   []fsm.BootstrapBeginResponse
	releases    []fsm.BootstrapReleaseRequest
	bytes       int64
}

func TestConsumerMetadataCacheUsesConfiguredBootstrapLimits(t *testing.T) {
	receiver, source, probe, r := metadataCacheFixture(t, func(cfg *config.Config) { cfg.BootstrapMaxTransferBytes = 1; cfg.BootstrapMaxTotalBytes = 2 })
	transferAppend(t, source, source.bootstrapSource, r)
	_, err := receiver.consumerRecoveryCache.stage(context.Background(), metadataCacheRequest(source, 0))
	var limit *disk.BootstrapLimitError
	require.ErrorAs(t, err, &limit)
	require.EqualValues(t, 1, limit.Limit)
	require.Len(t, probe.releases, 1)
}

func (p *metadataTransferProbe) BeginBootstrapTransfer(ctx context.Context, address string, port int, r fsm.BootstrapBeginRequest) (fsm.BootstrapBeginResponse, error) {
	p.requests = append(p.requests, r)
	if p.beforeBegin != nil {
		p.beforeBegin(ctx)
	}
	response, err := p.ConsumerMetadataRecoveryClient.BeginBootstrapTransfer(ctx, address, port, r)
	p.responses = append(p.responses, response)
	if p.begin != nil {
		return p.begin(ctx, r, response, err)
	}
	return response, err
}
func (p *metadataTransferProbe) ReadBootstrapTransferChunk(ctx context.Context, address string, port int, r fsm.BootstrapChunkRequest, size int64) (fsm.BootstrapChunkResponse, error) {
	response, err := p.ConsumerMetadataRecoveryClient.ReadBootstrapTransferChunk(ctx, address, port, r, size)
	p.bytes += int64(len(response.Data))
	if p.chunk != nil {
		response = p.chunk(response)
	}
	return response, err
}
func (p *metadataTransferProbe) ReleaseBootstrapTransfer(ctx context.Context, address string, port int, r fsm.BootstrapReleaseRequest) (fsm.BootstrapReleaseResponse, error) {
	p.releases = append(p.releases, r)
	return p.ConsumerMetadataRecoveryClient.ReleaseBootstrapTransfer(ctx, address, port, r)
}

func metadataCacheFixture(t *testing.T, configure ...func(*config.Config)) (*CommandHandler, *CommandHandler, *metadataTransferProbe, *fsm.BootstrapBeginRequest) {
	t.Helper()
	source, _, r := transferSourceFixture(t)
	client, observed, port := serveTransferFixture(t, source)
	observed.begins = make(chan struct{}, 64)
	probe := &metadataTransferProbe{ConsumerMetadataRecoveryClient: client}
	receiver := newCoordinatorRoutingHandler("requester", source.Cluster.RaftManager.GetFSM(), nil)
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiscoveryPort = port
	cfg.DiskMinFreeBytes, cfg.DiskMinFreePercent = 0, 0
	for _, apply := range configure {
		apply(cfg)
	}
	receiver.Config = cfg
	require.NoError(t, receiver.ConfigureConsumerMetadataRecovery(context.Background(), "requester-process", probe))
	t.Cleanup(func() { require.NoError(t, receiver.Close()) })
	return receiver, source, probe, &r
}
func metadataCacheRequest(source *CommandHandler, partitions ...int) ConsumerMetadataRecoveryStageRequest {
	state := source.Cluster.RaftManager.GetFSM()
	count := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0").PartitionCount
	r := ConsumerMetadataRecoveryStageRequest{Partitions: partitions, Metadata: make([]fsm.PartitionMetadata, count)}
	for i := range r.Metadata {
		r.Metadata[i] = *state.GetPartitionMetadata(fmt.Sprintf("%s-%d", config.ConsumerOffsetsTopicName, i))
	}
	return r
}
func stageMetadataCache(t *testing.T, receiver, source *CommandHandler) StagedConsumerMetadataRecovery {
	t.Helper()
	staged, err := receiver.consumerRecoveryCache.stage(context.Background(), metadataCacheRequest(source, 0))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, staged.Close()) })
	transferWaitEmpty(t, source.bootstrapSource)
	return staged
}

func TestConsumerMetadataCacheFullSuffixAndFixedLeases(t *testing.T) {
	receiver, source, probe, r := metadataCacheFixture(t)
	var exports []disk.RetainedBootstrapWork
	source.bootstrapSource.export = func(ctx context.Context, ticket *fsm.MetadataBootstrapSourceTicket) (bootstrapSourceExport, error) {
		export, err := ticket.ExportContext(ctx)
		if err == nil {
			exports = append(exports, export.RetainedWork())
		}
		return export, err
	}
	for i := 0; i < 20; i++ {
		transferAppend(t, source, source.bootstrapSource, r)
	}
	first := stageMetadataCache(t, receiver, source)
	cache := receiver.consumerRecoveryCache
	entry := cache.entries[0]
	firstBounds, firstWork, firstBytes := entry.bounds, entry.store.Work(), probe.bytes
	require.Equal(t, int64(firstWork.Initial.ReadBytes), firstBounds.RawBytes)
	require.Equal(t, firstBounds.RawBytes, firstBytes)
	require.Equal(t, fsm.BootstrapCaptureFull, probe.responses[0].CaptureMode)
	firstCertificate := entry.certificate
	// A replay-generation change retains raw identity but still obtains fresh proof.
	request := metadataCacheRequest(source, 0)
	request.RequiredGeneration = 99
	same, err := cache.stage(context.Background(), request)
	require.NoError(t, err)
	require.NoError(t, same.Close())
	transferWaitEmpty(t, source.bootstrapSource)
	require.Equal(t, firstBytes, probe.bytes)
	require.Equal(t, firstWork, entry.store.Work())
	require.Zero(t, exports[1].CanonicalWriteBytes)
	require.Zero(t, exports[1].HashReadBytes)
	require.NotEqual(t, firstCertificate, entry.certificate)
	require.Equal(t, firstCertificate, probe.requests[1].PriorCertificateID)
	for i := 0; i < 2; i++ {
		transferAppend(t, source, source.bootstrapSource, r)
	}
	fresh := stageMetadataCache(t, receiver, source)
	require.Same(t, entry, cache.entries[0])
	require.Equal(t, fsm.BootstrapCaptureSuffix, probe.responses[2].CaptureMode)
	delta := entry.bounds.RawBytes - firstBounds.RawBytes
	require.Equal(t, uint64(delta), exports[2].CanonicalWriteBytes)
	require.Equal(t, uint64(delta), exports[2].HashReadBytes)
	require.Equal(t, delta, probe.bytes-firstBytes)
	require.Equal(t, uint64(delta), entry.store.Work().Suffix.ReadBytes)
	require.Equal(t, uint64(delta), entry.store.Work().Append.RawReadBytes)
	require.Equal(t, uint64(delta), entry.store.Work().Append.RawBytes)
	require.Equal(t, firstWork.Initial, entry.store.Work().Initial)
	oldRecords, err := first.ReadCommittedConsumerMetadata(config.ConsumerOffsetsTopicName, 0, 0, 100)
	require.NoError(t, err)
	require.Len(t, oldRecords, 20)
	records, err := fresh.ReadCommittedConsumerMetadata(config.ConsumerOffsetsTopicName, 0, 0, 100)
	require.NoError(t, err)
	require.Len(t, records, 22)
	count, err := fresh.ExistingPartitionCount(config.ConsumerOffsetsTopicName)
	require.NoError(t, err)
	require.Equal(t, 8, count)
	_, err = fresh.EndTopicOffset(config.ConsumerOffsetsTopicName, 1)
	require.Error(t, err)
	sources := fresh.Sources()
	delete(sources, 0)
	require.Len(t, fresh.Sources(), 1)
	// A certificate miss at the same end must build a full replacement.
	source.bootstrapSource.mu.Lock()
	clear(source.bootstrapSource.certificates[:])
	source.bootstrapSource.mu.Unlock()
	replacement := stageMetadataCache(t, receiver, source)
	require.NotSame(t, entry, cache.entries[0])
	require.Equal(t, fsm.BootstrapCaptureFull, probe.responses[3].CaptureMode)
	records, err = fresh.ReadCommittedConsumerMetadata(config.ConsumerOffsetsTopicName, 0, 0, 100)
	require.NoError(t, err)
	require.Len(t, records, 22)
	require.NoError(t, replacement.Close())
	request = metadataCacheRequest(source, 0)
	request.Metadata[0].CommittedHWM--
	_, err = cache.stage(context.Background(), request)
	require.ErrorIs(t, err, ErrConsumerRecoveryPending)
	require.Len(t, probe.requests, 4)
}

func TestConsumerMetadataCacheFailedSuffixPreservesCertificateAndPrefix(t *testing.T) {
	receiver, source, probe, r := metadataCacheFixture(t)
	transferAppend(t, source, source.bootstrapSource, r)
	oldLease := stageMetadataCache(t, receiver, source)
	entry := receiver.consumerRecoveryCache.entries[0]
	bounds, certificate := entry.bounds, entry.certificate
	transferAppend(t, source, source.bootstrapSource, r)
	probe.chunk = func(r fsm.BootstrapChunkResponse) fsm.BootstrapChunkResponse { r.Data[0] ^= 1; return r }
	_, err := receiver.consumerRecoveryCache.stage(context.Background(), metadataCacheRequest(source, 0))
	require.Error(t, err)
	transferWaitEmpty(t, source.bootstrapSource)
	require.Equal(t, bounds, entry.bounds)
	require.Equal(t, certificate, entry.certificate)
	records, err := oldLease.ReadCommittedConsumerMetadata(r.Topic, 0, 0, 10)
	require.NoError(t, err)
	require.Len(t, records, 1)
	probe.chunk = nil
	stageMetadataCache(t, receiver, source)
	require.Equal(t, certificate, probe.requests[2].PriorCertificateID)
	require.Equal(t, fsm.BootstrapCaptureSuffix, probe.responses[2].CaptureMode)
	require.Equal(t, uint64(2), entry.bounds.EndOffset)
}

func TestConsumerMetadataCachePartialStageKeepsVerifiedEntries(t *testing.T) {
	receiver, source, probe, r := metadataCacheFixture(t)
	transferAppend(t, source, source.bootstrapSource, r)
	probe.begin = func(_ context.Context, request fsm.BootstrapBeginRequest, response fsm.BootstrapBeginResponse, err error) (fsm.BootstrapBeginResponse, error) {
		if request.Partition == 1 {
			response.Manifest.EndOffset++
		}
		return response, err
	}
	_, err := receiver.consumerRecoveryCache.stage(context.Background(), metadataCacheRequest(source, 0, 1))
	require.Error(t, err)
	transferWaitEmpty(t, source.bootstrapSource)
	require.Len(t, receiver.consumerRecoveryCache.entries, 1)
	entry := receiver.consumerRecoveryCache.entries[0]
	bytes := probe.bytes
	probe.begin = nil
	stageMetadataCache(t, receiver, source)
	require.Same(t, entry, receiver.consumerRecoveryCache.entries[0])
	require.Equal(t, bytes, probe.bytes)
	require.Equal(t, fsm.BootstrapCaptureSuffix, probe.responses[2].CaptureMode)
}

func TestConsumerMetadataCacheRejectsInvalidVectorAndAuthority(t *testing.T) {
	receiver, source, probe, _ := metadataCacheFixture(t)
	for _, mutation := range []string{"empty", "oversized", "count", "unknown", "lifecycle", "leader", "index", "duplicate", "local replica", "requester capability", "requester incarnation", "source capability"} {
		t.Run(mutation, func(t *testing.T) {
			r := metadataCacheRequest(source, 0)
			state := source.bootstrapSource.state
			switch mutation {
			case "empty":
				r.Metadata = nil
			case "oversized":
				r.Metadata = make([]fsm.PartitionMetadata, receiver.consumerRecoveryCache.maxPartitions+1)
			case "count":
				r.Metadata[0].PartitionCount++
			case "unknown":
				r.Metadata[0].CommittedHWMKnown = false
			case "lifecycle":
				r.Metadata[0].LifecycleEpoch++
			case "leader":
				r.Metadata[0].Leader = ""
			case "index":
				r.Partitions = []int{-1}
			case "duplicate":
				r.Partitions = []int{0, 0}
			case "local replica":
				r.Metadata[0].Replicas = append(r.Metadata[0].Replicas, "requester")
			case "requester capability":
				transferRegister(t, state, "requester", "requester-process", "active", 0)
			case "requester incarnation":
				receiver.consumerRecoveryCache.incarnation = "other-process"
			case "source capability":
				transferRegister(t, state, "source", "source-process", "active", 0)
			}
			_, err := receiver.consumerRecoveryCache.stage(context.Background(), r)
			require.Error(t, err)
			require.Empty(t, probe.requests)
			receiver.consumerRecoveryCache.incarnation = "requester-process"
			transferRegister(t, state, "requester", "requester-process", "active", 2)
			transferRegister(t, state, "source", "source-process", "active", 2)
		})
	}
}

func TestConsumerMetadataCacheTransferFailureRelease(t *testing.T) {
	for _, fault := range []string{"checksum", "chunk identity", "manifest", "uncertain begin", "lost begin once", "local stage", "canceled begin"} {
		t.Run(fault, func(t *testing.T) {
			receiver, source, probe, r := metadataCacheFixture(t)
			transferAppend(t, source, source.bootstrapSource, r)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch fault {
			case "checksum":
				probe.chunk = func(r fsm.BootstrapChunkResponse) fsm.BootstrapChunkResponse { r.Data[0] ^= 1; return r }
			case "chunk identity":
				probe.chunk = func(r fsm.BootstrapChunkResponse) fsm.BootstrapChunkResponse { r.AttemptID = transferID(t); return r }
			case "manifest":
				probe.begin = func(_ context.Context, _ fsm.BootstrapBeginRequest, r fsm.BootstrapBeginResponse, err error) (fsm.BootstrapBeginResponse, error) {
					r.Manifest.EndOffset++
					return r, err
				}
			case "uncertain begin":
				probe.begin = func(context.Context, fsm.BootstrapBeginRequest, fsm.BootstrapBeginResponse, error) (fsm.BootstrapBeginResponse, error) {
					return fsm.BootstrapBeginResponse{}, io.EOF
				}
			case "lost begin once":
				probe.begin = func(_ context.Context, _ fsm.BootstrapBeginRequest, r fsm.BootstrapBeginResponse, err error) (fsm.BootstrapBeginResponse, error) {
					if len(probe.requests) == 1 {
						return fsm.BootstrapBeginResponse{}, io.EOF
					}
					return r, err
				}
			case "local stage":
				require.NoError(t, receiver.consumerRecoveryCache.root.Close())
			case "canceled begin":
				probe.begin = func(_ context.Context, _ fsm.BootstrapBeginRequest, r fsm.BootstrapBeginResponse, err error) (fsm.BootstrapBeginResponse, error) {
					cancel()
					return fsm.BootstrapBeginResponse{}, context.Canceled
				}
			}
			staged, err := receiver.consumerRecoveryCache.stage(ctx, metadataCacheRequest(source, 0))
			if fault == "lost begin once" {
				require.NoError(t, err)
				require.NoError(t, staged.Close())
			} else {
				require.Error(t, err)
				require.Empty(t, receiver.consumerRecoveryCache.entries)
			}
			require.Len(t, probe.releases, 1)
			require.Equal(t, probe.requests[0].AttemptID, probe.releases[0].AttemptID)
			if len(probe.requests) == 2 {
				require.Equal(t, probe.requests[0], probe.requests[1])
			}
			transferWaitEmpty(t, source.bootstrapSource)
			require.Empty(t, receiver.consumerRecoveryCache.permits)
		})
	}
}

func TestConsumerMetadataCacheCloseJoinsWorkAndLeases(t *testing.T) {
	receiver, source, probe, r := metadataCacheFixture(t)
	transferAppend(t, source, source.bootstrapSource, r)
	lease := stageMetadataCache(t, receiver, source)
	entered, resume := make(chan struct{}), make(chan struct{})
	probe.beforeBegin = func(context.Context) { close(entered); <-resume }
	stageDone := make(chan error, 1)
	go func() {
		_, err := receiver.consumerRecoveryCache.stage(context.Background(), metadataCacheRequest(source, 0))
		stageDone <- err
	}()
	transferWait(t, entered)
	closeDone := make(chan error, 1)
	go func() { closeDone <- receiver.Close() }()
	select {
	case <-closeDone:
		t.Fatal("Close passed non-preemptible work")
	case <-time.After(20 * time.Millisecond):
	}
	close(resume)
	require.Error(t, <-stageDone)
	select {
	case <-closeDone:
		t.Fatal("Close passed retained lease")
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, lease.Close())
	require.NoError(t, <-closeDone)
	_, err := receiver.consumerRecoveryCache.stage(context.Background(), metadataCacheRequest(source, 0))
	require.Error(t, err)
}

func TestConsumerMetadataCacheCloseJoinsLocalConstructionAndAdmission(t *testing.T) {
	receiver, source, probe, r := metadataCacheFixture(t)
	transferAppend(t, source, source.bootstrapSource, r)
	cache := receiver.consumerRecoveryCache
	entered, resume := make(chan struct{}), make(chan struct{})
	cache.constructStore = func(ctx context.Context, raw *disk.AdoptedRawStore, budget *topic.DetachedMetadataBudget) (*topic.DetachedMetadataStore, error) {
		close(entered)
		<-resume
		return topic.NewDetachedMetadataStore(ctx, raw, budget)
	}
	stageDone := make(chan error, 1)
	go func() { _, err := cache.stage(context.Background(), metadataCacheRequest(source, 0)); stageDone <- err }()
	transferWait(t, entered)
	require.Len(t, probe.releases, 1, "remote export is released before history construction")
	require.Len(t, cache.permits, 1)
	closeDone := make(chan error, 1)
	go func() { closeDone <- receiver.Close() }()
	select {
	case <-closeDone:
		t.Fatal("Close passed local construction")
	case <-time.After(20 * time.Millisecond):
	}
	require.Len(t, cache.permits, 1, "permit must survive cancellation until local work joins")
	close(resume)
	require.ErrorIs(t, <-stageDone, context.Canceled)
	require.NoError(t, <-closeDone)
	require.Empty(t, cache.permits)
}

func TestConsumerMetadataCacheConfigurationAndRootIsolation(t *testing.T) {
	receiver, source, probe, _ := metadataCacheFixture(t)
	require.Error(t, topic.ValidateName("@recovery"))
	info, err := os.Stat(filepath.Join(receiver.Config.LogDir, "@recovery", "consumer-metadata"))
	require.NoError(t, err)
	require.True(t, info.IsDir())
	_, err = os.Stat(filepath.Join(receiver.Config.LogDir, config.ConsumerOffsetsTopicName))
	require.True(t, os.IsNotExist(err))
	require.Error(t, receiver.ConfigureConsumerMetadataRecovery(context.Background(), "requester-process", probe))
	bare := newCoordinatorRoutingHandler("requester", source.bootstrapSource.state, nil)
	bare.Config = receiver.Config
	var typedNil *metadataTransferProbe
	require.Error(t, bare.ConfigureConsumerMetadataRecovery(context.Background(), "requester-process", typedNil))
	require.NoError(t, bare.Close())
	require.Error(t, bare.ConfigureConsumerMetadataRecovery(context.Background(), "requester-process", probe))
	cfg := config.DefaultConfig()
	cfg.MaxPartitionsPerTopic, cfg.MaxPartitions = -1, 0
	limit, err := consumerMetadataPartitionLimit(cfg)
	require.NoError(t, err)
	require.Equal(t, 1024, limit)
	cfg.MaxPartitions = 3
	limit, err = consumerMetadataPartitionLimit(cfg)
	require.NoError(t, err)
	require.Equal(t, 3, limit)
	cfg.MaxPartitionsPerTopic, cfg.MaxPartitions = math.MaxInt, math.MaxInt
	_, err = consumerMetadataPartitionLimit(cfg)
	require.Error(t, err)
	require.Equal(t, 2, cap(bootstrapInboundPermits))
	require.Equal(t, 6, cap(bootstrapSourcePermits))
}

func TestConsumerMetadataCacheCurrentReceiverTransactionDecision(t *testing.T) {
	receiver, source, _, r := metadataCacheFixture(t)
	p, err := source.TopicManager.GetTopic(r.Topic).GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, p.ReplicaAppendWithMode([]types.Message{
		{Offset: 0, Payload: "metadata", TransactionalID: "txn", Epoch: 1},
		{Offset: 1, TransactionalID: "txn", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit, ControlBatchCoordinatorEpoch: 7},
	}, true))
	require.NoError(t, p.ApplyReplicaHWM(2))
	transferMetadata(t, source.bootstrapSource, *r, func(m *fsm.PartitionMetadata) { m.CommittedHWM = 2 })
	receiver.TxnManager = transaction.NewManager()
	staged := stageMetadataCache(t, receiver, source)
	receiver.TxnManager.ApplySnapshot(&transaction.Snapshot{ID: "txn", Epoch: 1, State: transaction.StateOpen, CoordinatorEpoch: 7})
	records, err := staged.ReadCommittedConsumerMetadata(r.Topic, 0, 0, 10)
	require.NoError(t, err)
	require.Empty(t, records)
	raw, err := staged.(stagedConsumerHistory).ReadRawConsumerHistory(0, 0, 10)
	require.NoError(t, err)
	require.Len(t, raw, 2, "history proof includes the invisible intent and its control")
	receiver.TxnManager.ApplySnapshot(&transaction.Snapshot{ID: "txn", Epoch: 1, State: transaction.StateCommitted, CoordinatorEpoch: 7})
	records, err = staged.ReadCommittedConsumerMetadata(r.Topic, 0, 0, 10)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, uint64(0), records[0].Offset)
}

func TestConsumerMetadataCachePinnedGenerationsConsumeAdoptedBudget(t *testing.T) {
	receiver, source, probe, _ := metadataCacheFixture(t, func(cfg *config.Config) { cfg.MaxPartitionsPerTopic = 8 })
	var leases []StagedConsumerMetadataRecovery
	for i := 0; i < 10; i++ {
		leases = append(leases, stageMetadataCache(t, receiver, source))
		source.bootstrapSource.mu.Lock()
		clear(source.bootstrapSource.certificates[:])
		source.bootstrapSource.mu.Unlock()
	}
	old := receiver.consumerRecoveryCache.entries[0]
	_, err := receiver.consumerRecoveryCache.stage(context.Background(), metadataCacheRequest(source, 0))
	var limit *disk.BootstrapLimitError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, "adopted stores", limit.Resource)
	require.EqualValues(t, 10, limit.Limit)
	require.Same(t, old, receiver.consumerRecoveryCache.entries[0])
	require.Len(t, probe.releases, 11)
	transferWaitEmpty(t, source.bootstrapSource)
	require.NoError(t, leases[0].Close())
	stageMetadataCache(t, receiver, source)
}

type pausedMetadataReadContext struct {
	context.Context
	checks          int
	entered, resume chan struct{}
}

func (ctx *pausedMetadataReadContext) Err() error {
	ctx.checks++
	if ctx.checks == 4 {
		close(ctx.entered)
		<-ctx.resume
	}
	return ctx.Context.Err()
}

func TestConsumerMetadataCacheCallerCancellationReachesInProgressRead(t *testing.T) {
	receiver, source, _, r := metadataCacheFixture(t)
	for i := 0; i < 4; i++ {
		transferAppend(t, source, source.bootstrapSource, r)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	staged, err := receiver.consumerRecoveryCache.stage(ctx, metadataCacheRequest(source, 0))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, staged.Close()) })
	reader := staged.(*stagedConsumerMetadata)
	paused := &pausedMetadataReadContext{Context: reader.ctx, entered: make(chan struct{}), resume: make(chan struct{})}
	reader.ctx = paused
	finished := make(chan error, 1)
	go func() { _, err := staged.ReadCommittedConsumerMetadata(r.Topic, 0, 0, 100); finished <- err }()
	transferWait(t, paused.entered)
	cancel()
	close(paused.resume)
	require.ErrorIs(t, <-finished, context.Canceled)
	require.NoError(t, receiver.consumerRecoveryCache.ctx.Err())
}

func TestConsumerMetadataCacheSharedInboundAdmission(t *testing.T) {
	permits := make(chan struct{}, 2)
	var receivers, sources []*CommandHandler
	var probes []*metadataTransferProbe
	for i := 0; i < 3; i++ {
		receiver, source, probe, _ := metadataCacheFixture(t)
		receiver.consumerRecoveryCache.permits = permits
		receivers, sources, probes = append(receivers, receiver), append(sources, source), append(probes, probe)
	}
	entered, resume := make(chan struct{}, 3), make(chan struct{})
	for _, probe := range probes {
		probe.beforeBegin = func(context.Context) { entered <- struct{}{}; <-resume }
	}
	results := make(chan error, 3)
	for i := 0; i < 2; i++ {
		go func() {
			staged, err := receivers[i].consumerRecoveryCache.stage(context.Background(), metadataCacheRequest(sources[i], 0))
			if staged != nil {
				err = staged.Close()
			}
			results <- err
		}()
	}
	transferWait(t, entered)
	transferWait(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, err := receivers[2].consumerRecoveryCache.stage(ctx, metadataCacheRequest(sources[2], 0))
		results <- err
	}()
	select {
	case <-entered:
		t.Fatal("third inbound bypassed shared admission")
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	require.ErrorIs(t, <-results, context.Canceled)
	require.Empty(t, probes[2].requests)
	close(resume)
	require.NoError(t, <-results)
	require.NoError(t, <-results)
	require.Empty(t, permits)
}

func TestConsumerMetadataCacheCountAndLeaderChangesRequireFullCapture(t *testing.T) {
	receiver, source, probe, r := metadataCacheFixture(t)
	transferAppend(t, source, source.bootstrapSource, r)
	stageMetadataCache(t, receiver, source)
	prior := receiver.consumerRecoveryCache.entries[0]
	def := source.TopicManager.GetTopic(r.Topic).Definition()
	count := def.Partitions + 1
	data, err := json.Marshal(fsm.TopicCommand{Definition: &def, Patch: &topic.DefinitionPatch{Partitions: &count}, LeaderID: "source"})
	require.NoError(t, err)
	require.Nil(t, source.bootstrapSource.state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), data...)}))
	staged := stageMetadataCache(t, receiver, source)
	actualCount, err := staged.ExistingPartitionCount(r.Topic)
	require.NoError(t, err)
	require.Equal(t, count, actualCount)
	require.Equal(t, prior.certificate, probe.requests[1].PriorCertificateID)
	require.Equal(t, fsm.BootstrapCaptureFull, probe.responses[1].CaptureMode)
	require.NotSame(t, prior, receiver.consumerRecoveryCache.entries[0])
	transferMetadata(t, source.bootstrapSource, *r, func(m *fsm.PartitionMetadata) { m.LeaderEpoch++ })
	stageMetadataCache(t, receiver, source)
	require.Empty(t, probe.requests[2].PriorCertificateID)
	require.Equal(t, fsm.BootstrapCaptureFull, probe.responses[2].CaptureMode)
}

func TestConsumerMetadataCacheLifecycleRetiresOldGenerationBeforeTransfer(t *testing.T) {
	receiver, source, probe, _ := metadataCacheFixture(t)
	staged := stageMetadataCache(t, receiver, source)
	old := receiver.consumerRecoveryCache.entries[0]
	r := metadataCacheRequest(source, 0)
	for i := range r.Metadata {
		r.Metadata[i].LifecycleEpoch++
	}
	// A stale vector cannot be served, but its unreachable old cache is retired.
	_, err := receiver.consumerRecoveryCache.stage(context.Background(), r)
	require.ErrorIs(t, err, fsm.ErrBootstrapTransferStaleFence)
	require.Empty(t, receiver.consumerRecoveryCache.entries)
	_, err = old.store.Acquire()
	require.ErrorIs(t, err, disk.ErrDetachedBootstrapClosed)
	end, err := staged.EndTopicOffset(config.ConsumerOffsetsTopicName, 0)
	require.NoError(t, err)
	require.Zero(t, end)
	require.Empty(t, probe.requests[1].PriorCertificateID)
}

func TestConsumerMetadataCacheSourceIncarnationRequiresFullCapture(t *testing.T) {
	receiver, source, probe, _ := metadataCacheFixture(t)
	stageMetadataCache(t, receiver, source)
	state := source.bootstrapSource.state
	require.NoError(t, source.bootstrapSource.close())
	transferRegister(t, state, "source", "source-replacement", "active", 2)
	source.bootstrapMu.Lock()
	source.bootstrapSource = nil
	source.bootstrapMu.Unlock()
	require.NoError(t, source.ConfigureBootstrapTransferSource(context.Background(), "source-replacement"))
	stageMetadataCache(t, receiver, source)
	require.Empty(t, probe.requests[1].PriorCertificateID)
	require.Equal(t, "source-replacement", probe.requests[1].Source.IncarnationID)
	require.Equal(t, fsm.BootstrapCaptureFull, probe.responses[1].CaptureMode)
}

func TestConsumerMetadataCacheConfigureBeforeRegistrationUsesAssignedEpoch(t *testing.T) {
	source, _, _ := transferSourceFixture(t)
	client, _, port := serveTransferFixture(t, source)
	receiver := newCoordinatorRoutingHandler("late-requester", source.bootstrapSource.state, nil)
	cfg := config.DefaultConfig()
	cfg.LogDir, cfg.DiscoveryPort = t.TempDir(), port
	cfg.DiskMinFreeBytes, cfg.DiskMinFreePercent = 0, 0
	receiver.Config = cfg
	require.NoError(t, receiver.ConfigureConsumerMetadataRecovery(context.Background(), "late-process", client))
	t.Cleanup(func() { require.NoError(t, receiver.Close()) })
	_, err := receiver.consumerRecoveryCache.stage(context.Background(), metadataCacheRequest(source, 0))
	require.ErrorIs(t, err, ErrConsumerRecoveryPending)
	transferRegister(t, source.bootstrapSource.state, "late-requester", "late-process", "active", 2)
	stageMetadataCache(t, receiver, source)
	require.Equal(t, source.bootstrapSource.state.GetBroker("late-requester").IncarnationEpoch, receiver.consumerRecoveryCache.entries[0].request.Requester.IncarnationEpoch)
}

func TestConsumerMetadataCacheCloseReturnsPhysicalCleanupDebtWithoutSpinning(t *testing.T) {
	source, _, _ := transferSourceFixture(t)
	client, _, port := serveTransferFixture(t, source)
	receiver := newCoordinatorRoutingHandler("requester", source.bootstrapSource.state, nil)
	cfg := config.DefaultConfig()
	cfg.LogDir, cfg.DiscoveryPort = t.TempDir(), port
	cfg.DiskMinFreeBytes, cfg.DiskMinFreePercent = 0, 0
	receiver.Config = cfg
	require.NoError(t, receiver.ConfigureConsumerMetadataRecovery(context.Background(), "requester-process", client))
	cache := receiver.consumerRecoveryCache
	staged, err := cache.stage(context.Background(), metadataCacheRequest(source, 0))
	require.NoError(t, err)
	require.NoError(t, staged.Close())
	rootPath := filepath.Join(cfg.LogDir, "@recovery", "consumer-metadata")
	movedPath := rootPath + "-held"
	if err := os.Rename(rootPath, movedPath); err != nil {
		require.NoError(t, receiver.Close())
		t.Skipf("filesystem cannot move a locked root: %v", err)
	}
	require.NoError(t, os.WriteFile(rootPath, []byte("blocked path"), 0600))
	defer func() {
		require.NoError(t, os.Remove(rootPath))
		require.NoError(t, os.Rename(movedPath, rootPath))
		require.NoError(t, cache.root.Close())
	}()
	finished := make(chan error, 1)
	go func() { finished <- receiver.Close() }()
	select {
	case err = <-finished:
		require.ErrorIs(t, err, disk.ErrDetachedBootstrapPending)
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup debt caused Close to spin")
	}
	children, err := os.ReadDir(movedPath)
	require.NoError(t, err)
	require.Greater(t, len(children), 1, "physical child must remain charged and owned")
	_, err = disk.NewDetachedBootstrapRoot(movedPath, disk.DefaultBootstrapLimits(), 0, 0, 10)
	require.Error(t, err, "failed cleanup must retain the kernel lock")
}
