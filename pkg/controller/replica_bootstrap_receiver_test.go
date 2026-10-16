package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/eventsource"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type replicaReceiverClient struct {
	source                   *CommandHandler
	begin                    func(context.Context, fsm.BootstrapBeginRequest, int32) error
	beforeChunk              func(context.Context) error
	begins, releases, chunks atomic.Int32
	bytes                    atomic.Int64
	mu                       sync.Mutex
	requests                 []fsm.BootstrapBeginRequest
	addresses                []string
	releaseContextErr        error
}

func (c *replicaReceiverClient) BeginBootstrapTransfer(ctx context.Context, address string, _ int, r fsm.BootstrapBeginRequest) (fsm.BootstrapBeginResponse, error) {
	n := c.begins.Add(1)
	c.mu.Lock()
	c.requests = append(c.requests, r)
	c.addresses = append(c.addresses, address)
	c.mu.Unlock()
	if c.begin != nil {
		if err := c.begin(ctx, r, n); err != nil {
			return fsm.BootstrapBeginResponse{}, err
		}
	}
	return c.source.BeginBootstrapTransfer(ctx, r)
}
func (c *replicaReceiverClient) ReadBootstrapTransferChunk(ctx context.Context, _ string, _ int, r fsm.BootstrapChunkRequest, _ int64) (fsm.BootstrapChunkResponse, error) {
	c.chunks.Add(1)
	if c.beforeChunk != nil {
		if err := c.beforeChunk(ctx); err != nil {
			return fsm.BootstrapChunkResponse{}, err
		}
	}
	response, err := c.source.ReadBootstrapTransferChunk(ctx, r)
	c.bytes.Add(int64(len(response.Data)))
	return response, err
}
func (c *replicaReceiverClient) ReleaseBootstrapTransfer(ctx context.Context, _ string, _ int, r fsm.BootstrapReleaseRequest) (fsm.BootstrapReleaseResponse, error) {
	c.releases.Add(1)
	c.mu.Lock()
	c.releaseContextErr = ctx.Err()
	c.mu.Unlock()
	return c.source.ReleaseBootstrapTransfer(ctx, r)
}

func replicaReceiverFixture(t *testing.T) (*CommandHandler, *CommandHandler, *replicaReceiverClient, fsm.ReplicaCatchupRequest, fsm.ReplicaCatchupBatch) {
	t.Helper()
	node := func(id string) *CommandHandler {
		cfg := config.DefaultConfig()
		cfg.LogDir = t.TempDir()
		cfg.DiskMinFreeBytes = 1
		cfg.DiskMinFreePercent = .1
		cfg.IndexSize = 4096
		dm := disk.NewDiskManager(cfg)
		tm := topic.NewTopicManager(cfg, dm, nil)
		state := fsm.NewBrokerFSM(tm, nil)
		for _, broker := range []string{"source", "requester"} {
			transferRegister(t, state, broker, broker+"-process", "active", 2)
		}
		def := topic.DefaultDefinition("orders", cfg)
		def.ReplicationFactor = 2
		def.Partitions = 2
		data, err := json.Marshal(fsm.TopicCommand{Definition: &def, LeaderID: "source"})
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), data...)}))
		h := newCoordinatorRoutingHandler(id, state, nil)
		h.Config = cfg
		h.TopicManager = tm
		require.NoError(t, h.ESHandler.Close())
		h.ESHandler = eventsource.NewHandler(tm)
		for p := 0; p < 2; p++ {
			partition, err := tm.GetTopic("orders").GetPartition(p)
			require.NoError(t, err)
			if id == "source" {
				require.NoError(t, partition.ReplicaAppendWithMode([]types.Message{{Offset: 0, Key: "key", Payload: "payload"}}, true))
				require.NoError(t, partition.ApplyReplicaHWM(1))
			}
			key := fmt.Sprintf("orders-%d", p)
			m := state.GetPartitionMetadata(key)
			m.ISR = []string{"source"}
			m.CommittedHWMKnown = true
			m.CommittedHWM = 1
			data, err := json.Marshal(m)
			require.NoError(t, err)
			require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:"+key+":"), data...)}))
		}
		t.Cleanup(func() { require.NoError(t, h.Close()); tm.Stop(); dm.CloseAllHandlers() })
		return h
	}
	source, receiver := node("source"), node("requester")
	require.NoError(t, source.ConfigureBootstrapTransferSource(context.Background(), "source-process"))
	client := &replicaReceiverClient{source: source}
	require.NoError(t, receiver.ConfigureReplicaBootstrapRecovery(context.Background(), "requester-process", client))
	m := receiver.Cluster.RaftManager.GetFSM().GetPartitionMetadata("orders-0")
	request := fsm.ReplicaCatchupRequest{Topic: "orders", Partition: 0, BrokerID: "requester", SourceBroker: "source", SourceAddress: "127.0.0.1:7000", Leader: m.Leader, LeaderEpoch: m.LeaderEpoch, LifecycleEpoch: m.LifecycleEpoch, CommittedHWM: 1, RecoveryProtocol: fsm.IndexedRecoveryProtocolVersion, MaxRecords: 100}
	batch, err := fsm.SealReplicaCatchupBatch(fsm.ReplicaCatchupBatch{Topic: request.Topic, Partition: 0, BrokerID: request.BrokerID, SourceBroker: request.SourceBroker, Leader: request.Leader, LeaderEpoch: request.LeaderEpoch, LifecycleEpoch: request.LifecycleEpoch, CommittedHWM: 1, RecoveryProtocol: request.RecoveryProtocol, BootstrapRequired: true})
	require.NoError(t, err)
	return receiver, source, client, request, batch
}

func receiverAttempt(t *testing.T, h *CommandHandler, partition int) *replicaBootstrapAttempt {
	t.Helper()
	r := h.replicaBootstrap
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.attempts[replicaBootstrapKey{"orders", partition}]
	require.NotNil(t, a)
	return a
}
func receiverPartition(t *testing.T, h *CommandHandler) *topic.Partition {
	t.Helper()
	p, err := h.TopicManager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	return p
}

func TestReplicaBootstrapReceiverSurvivesTurnAndRequiresFreshProof(t *testing.T) {
	h, _, client, request, batch := replicaReceiverFixture(t)
	entered, proceed := make(chan struct{}), make(chan struct{})
	client.begin = func(ctx context.Context, _ fsm.BootstrapBeginRequest, _ int32) error {
		close(entered)
		select {
		case <-proceed:
			return ctx.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	turn, cancel := context.WithCancel(context.Background())
	require.NoError(t, h.RecoverReplicaBootstrap(turn, request, batch))
	a := receiverAttempt(t, h, 0)
	transferWait(t, entered)
	cancel()
	require.NoError(t, h.RecoverReplicaBootstrap(context.Background(), request, batch))
	require.EqualValues(t, 1, client.begins.Load())
	excluded, err := h.ResumePendingReplicaBootstraps(context.Background())
	require.NoError(t, err)
	require.True(t, excluded["orders-0"])
	close(proceed)
	transferWait(t, a.done)
	require.NoError(t, a.err)
	p := receiverPartition(t, h)
	require.EqualValues(t, 1, p.NextOffset())
	require.False(t, p.BootstrapPending())
	require.Empty(t, h.Cluster.RaftManager.GetFSM().BuildISRCatchupProofs("requester"))
	require.EqualValues(t, 1, client.releases.Load())
	require.Positive(t, client.bytes.Load())
	require.Nil(t, client.releaseContextErr)
	excluded, err = h.ResumePendingReplicaBootstraps(context.Background())
	require.NoError(t, err)
	require.True(t, excluded["orders-0"])
	require.False(t, h.ReplicaBootstrapExcluded("orders", 0))
}

func TestReplicaBootstrapReceiverTransientRetriesKeepFingerprint(t *testing.T) {
	h, _, client, request, batch := replicaReceiverFixture(t)
	client.begin = func(_ context.Context, _ fsm.BootstrapBeginRequest, n int32) error {
		if n < 4 {
			return io.ErrUnexpectedEOF
		}
		return nil
	}
	require.NoError(t, h.RecoverReplicaBootstrap(context.Background(), request, batch))
	a := receiverAttempt(t, h, 0)
	transferWait(t, a.done)
	require.NoError(t, a.err)
	require.Len(t, client.requests, 4)
	for _, r := range client.requests {
		require.Equal(t, client.requests[0], r)
	}
	require.EqualValues(t, 1, client.releases.Load())
}

func TestReplicaBootstrapReceiverPollCancelsStaleAttempt(t *testing.T) {
	h, _, client, request, batch := replicaReceiverFixture(t)
	entered := make(chan struct{})
	client.begin = func(ctx context.Context, _ fsm.BootstrapBeginRequest, _ int32) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	require.NoError(t, h.RecoverReplicaBootstrap(context.Background(), request, batch))
	a := receiverAttempt(t, h, 0)
	transferWait(t, entered)
	state := h.Cluster.RaftManager.GetFSM()
	m := state.GetPartitionMetadata("orders-0")
	m.CommittedHWM = 4
	data, err := json.Marshal(m)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:orders-0:"), data...)}))
	_, err = h.ResumePendingReplicaBootstraps(context.Background())
	require.NoError(t, err)
	select {
	case <-a.done:
		t.Fatal("HWM growth canceled immutable attempt")
	default:
	}
	m.LeaderEpoch++
	data, err = json.Marshal(m)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:orders-0:"), data...)}))
	_, err = h.ResumePendingReplicaBootstraps(context.Background())
	require.NoError(t, err)
	transferWait(t, a.done)
	require.ErrorIs(t, a.err, context.Canceled)
	require.EqualValues(t, 1, a.request.MinimumEnd)
	require.EqualValues(t, 1, client.releases.Load())
	require.Nil(t, client.releaseContextErr)
	require.False(t, receiverPartition(t, h).BootstrapPending())
}

func TestReplicaBootstrapReceiverCloseAbortsPreparedStage(t *testing.T) {
	h, _, client, request, batch := replicaReceiverFixture(t)
	lock := h.partitionWriteLock("orders", 0)
	lock.Lock()
	defer lock.Unlock()
	require.NoError(t, h.RecoverReplicaBootstrap(context.Background(), request, batch))
	a := receiverAttempt(t, h, 0)
	require.Eventually(t, func() bool { return client.releases.Load() == 1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, h.CloseReplicaBootstrapRecovery())
	transferWait(t, a.done)
	require.ErrorIs(t, a.err, context.Canceled)
	stages, err := filepath.Glob(filepath.Join(h.Config.LogDir, "orders", "partition_0.bootstrap-*"))
	require.NoError(t, err)
	require.Empty(t, stages)
	require.False(t, receiverPartition(t, h).BootstrapPending())
}

func TestReplicaBootstrapReceiverStaleWhileWaitingForWriteGate(t *testing.T) {
	for _, change := range []string{"leader", "requester incarnation"} {
		t.Run(change, func(t *testing.T) {
			h, _, client, request, batch := replicaReceiverFixture(t)
			lock := h.partitionWriteLock("orders", 0)
			lock.Lock()
			defer lock.Unlock()
			require.NoError(t, h.RecoverReplicaBootstrap(context.Background(), request, batch))
			a := receiverAttempt(t, h, 0)
			require.Eventually(t, func() bool { return client.releases.Load() == 1 }, 5*time.Second, time.Millisecond)
			state := h.Cluster.RaftManager.GetFSM()
			switch change {
			case "leader":
				m := state.GetPartitionMetadata("orders-0")
				m.LeaderEpoch++
				data, err := json.Marshal(m)
				require.NoError(t, err)
				require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:orders-0:"), data...)}))
			case "requester incarnation":
				transferRegister(t, state, "requester", "replacement-process", "active", 2)
			}
			h.replicaBootstrap.mu.Lock()
			protected := a.installing
			h.replicaBootstrap.mu.Unlock()
			require.False(t, protected, "waiting for a controller gate is not a validated installation")
			_, err := h.ResumePendingReplicaBootstraps(context.Background())
			require.NoError(t, err)
			transferWait(t, a.done)
			require.ErrorIs(t, a.err, context.Canceled)
			// Gate is still held: cancellation must join cleanup and release both
			// shared slots without waiting for the blocked install to enter it.
			release1, ok := h.replicaBootstrap.reserve(replicaBootstrapKey{"check", 0})
			require.True(t, ok)
			defer release1()
			release2, ok := h.replicaBootstrap.reserve(replicaBootstrapKey{"check", 1})
			require.True(t, ok)
			defer release2()
			require.False(t, receiverPartition(t, h).BootstrapPending())
			stages, err := filepath.Glob(filepath.Join(h.Config.LogDir, "orders", "partition_0.bootstrap-*"))
			require.NoError(t, err)
			require.Empty(t, stages)
		})
	}
}

func TestReplicaBootstrapReceiverSharedAdmissionAndOffsetsSerialization(t *testing.T) {
	h, _, client, request, batch := replicaReceiverFixture(t)
	r := h.replicaBootstrap
	release1, ok := r.reserve(replicaBootstrapKey{config.ConsumerOffsetsTopicName, 0})
	require.True(t, ok)
	defer release1()
	_, ok = r.reserve(replicaBootstrapKey{config.ConsumerOffsetsTopicName, 1})
	require.False(t, ok)
	release2, ok := r.reserve(replicaBootstrapKey{"provider", 0})
	require.True(t, ok)
	defer release2()
	require.NoError(t, h.RecoverReplicaBootstrap(context.Background(), request, batch))
	require.False(t, h.ReplicaBootstrapExcluded("orders", 0))
	require.Zero(t, client.begins.Load())
}

func TestReplicaBootstrapReceiverResumeUsesLocalPendingOnly(t *testing.T) {
	h, source, client, request, batch := replicaReceiverFixture(t)
	state := h.Cluster.RaftManager.GetFSM()
	r := fsm.BootstrapBeginRequest{Version: 1, Purpose: fsm.BootstrapPurposeReplica, Topic: request.Topic, Partition: 0, Leader: request.Leader, LeaderEpoch: request.LeaderEpoch, LifecycleEpoch: request.LifecycleEpoch, Requester: transferIdentity(state.GetBroker("requester")), Source: transferIdentity(state.GetBroker("source")), AttemptID: transferID(t), MinimumEnd: 1}
	response, err := source.BeginBootstrapTransfer(context.Background(), r)
	require.NoError(t, err)
	p := receiverPartition(t, h)
	stage, err := p.StageBootstrap(response.Manifest)
	require.NoError(t, err)
	for _, file := range response.Manifest.Files {
		chunk := fsm.BootstrapChunkRequest{Version: 1, Requester: r.Requester, AttemptID: r.AttemptID, TransferID: response.Manifest.TransferID, FileID: file.ID, Size: disk.BootstrapChunkBytes}
		data, err := source.ReadBootstrapTransferChunk(context.Background(), chunk)
		require.NoError(t, err)
		require.NoError(t, stage.WriteChunk(file.ID, 0, data.Data))
	}
	require.NoError(t, stage.PrepareContext(context.Background()))
	session, err := p.BeginBootstrapSession()
	require.NoError(t, err)
	require.NoError(t, session.Install(stage, nil))
	session.Close()
	_, err = source.ReleaseBootstrapTransfer(context.Background(), transferRelease(r))
	require.NoError(t, err)
	transferRegister(t, state, "source", "source-process", "inactive", 2)
	h.Config.BootstrapMaxTransferBytes = 1
	h.Config.BootstrapMaxTotalBytes = 1
	excluded, err := h.ResumePendingReplicaBootstraps(context.Background())
	require.NoError(t, err)
	require.True(t, excluded["orders-0"])
	a := receiverAttempt(t, h, 0)
	transferWait(t, a.done)
	require.NoError(t, a.err)
	require.False(t, p.BootstrapPending())
	require.Zero(t, client.begins.Load())
	require.Empty(t, state.BuildISRCatchupProofs("requester"))
	// Neither a receipt nor successful completion bypasses response authentication.
	bad := batch
	bad.BrokerID = "elsewhere"
	require.Error(t, h.RecoverReplicaBootstrap(context.Background(), request, bad))
}

func TestReplicaBootstrapReceiverRejectsTypedNilClient(t *testing.T) {
	h := &CommandHandler{}
	var client *replicaReceiverClient
	require.Error(t, h.ConfigureReplicaBootstrapRecovery(context.Background(), "process", client))
}

func TestReplicaBootstrapReceiverCancellationDuringUncertainBegin(t *testing.T) {
	h, _, client, request, batch := replicaReceiverFixture(t)
	entered := make(chan struct{})
	client.begin = func(ctx context.Context, r fsm.BootstrapBeginRequest, _ int32) error {
		_, err := client.source.BeginBootstrapTransfer(ctx, r)
		if err != nil {
			return err
		}
		close(entered)
		<-ctx.Done()
		return errors.Join(io.EOF, ctx.Err())
	}
	require.NoError(t, h.RecoverReplicaBootstrap(context.Background(), request, batch))
	a := receiverAttempt(t, h, 0)
	transferWait(t, entered)
	require.NoError(t, h.CloseReplicaBootstrapRecovery())
	transferWait(t, a.done)
	require.ErrorIs(t, a.err, context.Canceled)
	require.EqualValues(t, 1, client.releases.Load())
	require.Nil(t, client.releaseContextErr)
	transferWaitEmpty(t, client.source.bootstrapSource)
}

func TestReplicaBootstrapReceiverOffsetsUsesConsumerFence(t *testing.T) {
	source, _, begin := replicaTransferFixture(t)
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = .1
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	c, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, &dummyPublisher{})
	require.NoError(t, err)
	state := fsm.NewBrokerFSM(tm, c)
	for _, id := range []string{"source", "requester"} {
		transferRegister(t, state, id, id+"-process", "active", 2)
	}
	def := source.TopicManager.GetTopic(config.ConsumerOffsetsTopicName).Definition()
	data, err := json.Marshal(fsm.TopicCommand{Definition: &def, LeaderID: "source"})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), data...)}))
	for id := 0; id < def.Partitions; id++ {
		key := fmt.Sprintf("%s-%d", def.Name, id)
		m := source.Cluster.RaftManager.GetFSM().GetPartitionMetadata(key)
		data, err = json.Marshal(m)
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:"+key+":"), data...)}))
	}
	h := newCoordinatorRoutingHandler("requester", state, c)
	h.Config = cfg
	h.TopicManager = tm
	require.NoError(t, h.ESHandler.Close())
	h.ESHandler = eventsource.NewHandler(tm)
	t.Cleanup(func() { require.NoError(t, h.Close()); c.Stop(); tm.Stop(); dm.CloseAllHandlers() })
	client := &replicaReceiverClient{source: source}
	require.NoError(t, h.ConfigureReplicaBootstrapRecovery(context.Background(), "requester-process", client))
	request := fsm.ReplicaCatchupRequest{Topic: def.Name, Partition: 0, BrokerID: "requester", SourceBroker: "source", Leader: begin.Leader, LeaderEpoch: begin.LeaderEpoch, LifecycleEpoch: begin.LifecycleEpoch, RecoveryProtocol: fsm.IndexedRecoveryProtocolVersion}
	batch, err := fsm.SealReplicaCatchupBatch(fsm.ReplicaCatchupBatch{Topic: request.Topic, Partition: 0, BrokerID: request.BrokerID, SourceBroker: request.SourceBroker, Leader: request.Leader, LeaderEpoch: request.LeaderEpoch, LifecycleEpoch: request.LifecycleEpoch, RecoveryProtocol: request.RecoveryProtocol, BootstrapRequired: true})
	require.NoError(t, err)
	require.NoError(t, h.RecoverReplicaBootstrap(context.Background(), request, batch))
	h.replicaBootstrap.mu.Lock()
	a := h.replicaBootstrap.attempts[replicaBootstrapKey{def.Name, 0}]
	h.replicaBootstrap.mu.Unlock()
	require.NotNil(t, a)
	transferWait(t, a.done)
	require.NoError(t, a.err)
	p, err := tm.GetTopic(def.Name).GetPartition(0)
	require.NoError(t, err)
	require.False(t, p.BootstrapPending())
	require.NoError(t, c.ConsumerOperationsReadinessError())
	require.Empty(t, state.BuildISRCatchupProofs("requester"))
	other, err := tm.GetTopic(def.Name).GetPartition(1)
	require.NoError(t, err)
	require.False(t, other.BootstrapPending())
	require.EqualValues(t, 0, other.NextOffset())
}

func TestReplicaBootstrapReceiverStreamsBeyondSixtyFourChunks(t *testing.T) {
	h, source, client, request, batch := replicaReceiverFixture(t)
	sp := receiverPartition(t, source)
	messages := make([]types.Message, 72)
	for i := range messages {
		messages[i] = types.Message{Offset: uint64(i + 1), Payload: strings.Repeat("p", 240<<10)}
	}
	require.NoError(t, sp.ReplicaAppendWithMode(messages, true))
	require.NoError(t, sp.ApplyReplicaHWM(73))
	for _, node := range []*CommandHandler{h, source} {
		state := node.Cluster.RaftManager.GetFSM()
		m := state.GetPartitionMetadata("orders-0")
		m.CommittedHWM = 73
		data, err := json.Marshal(m)
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:orders-0:"), data...)}))
	}
	request.CommittedHWM = 73
	batch.CommittedHWM = 73
	var err error
	batch, err = fsm.SealReplicaCatchupBatch(batch)
	require.NoError(t, err)
	work := make(chan disk.FullBootstrapWork, 1)
	capture := source.bootstrapSource.replicaCapture
	source.bootstrapSource.replicaCapture = func(ctx context.Context, ticket *fsm.ReplicaBootstrapSourceTicket) (*fsm.ReplicaBootstrapCapture, error) {
		result, err := capture(ctx, ticket)
		if err == nil {
			work <- result.FullWork()
		}
		return result, err
	}
	require.NoError(t, h.RecoverReplicaBootstrap(context.Background(), request, batch))
	a := receiverAttempt(t, h, 0)
	select {
	case <-a.done:
	case <-time.After(15 * time.Second):
		t.Fatal("continuous transfer did not finish")
	}
	require.NoError(t, a.err)
	require.Greater(t, client.chunks.Load(), int32(64))
	require.EqualValues(t, 73, receiverPartition(t, h).NextOffset())
	exportWork := <-work
	require.EqualValues(t, exportWork.CanonicalWriteBytes+exportWork.CatalogWriteBytes, client.bytes.Load())
	require.Equal(t, exportWork.CanonicalWriteBytes+exportWork.CatalogWriteBytes, exportWork.HashReadBytes)
	require.EqualValues(t, 73, exportWork.DecodedFrames)
	t.Logf("payload transferred=%d; export decoded frames=%d decoded bytes=%d hash reads=%d", client.bytes.Load(), exportWork.DecodedFrames, exportWork.DecodedFrameBytes, exportWork.HashReadBytes)
}
