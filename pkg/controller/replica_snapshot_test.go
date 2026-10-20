package controller

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"strconv"
	"testing"
	"time"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/eventsource"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func newDiskSnapshotCommandHandler(t *testing.T) (*CommandHandler, *topic.TopicManager) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes, cfg.DiskMinFreePercent = 1, 0.1
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	ch := NewCommandHandler(tm, cfg, nil, nil, nil)
	t.Cleanup(func() { _ = ch.Close(); tm.Stop(); dm.CloseAllHandlers() })
	return ch, tm
}

func snapshotPagePeer(t *testing.T, pages []eventsource.SnapshotPage) (string, <-chan wire.CommandPayload) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	requests := make(chan wire.CommandPayload, len(pages))
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(requests)
		for _, page := range pages {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			func() {
				defer func() { _ = raw.Close() }()
				_ = raw.SetDeadline(time.Now().Add(5 * time.Second))
				connection, err := wire.ServerHandshake(raw, []wire.Compression{wire.CompressionNone})
				if err != nil {
					return
				}
				frame, err := connection.ReadFrame()
				if err != nil {
					return
				}
				request, err := wire.DecodeCommandPayload(frame.Payload)
				if err != nil {
					return
				}
				requests <- request
				encoded, err := json.Marshal(page)
				if err != nil {
					return
				}
				_ = connection.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: frame.Command, RequestID: frame.RequestID, Status: wire.StatusOK, Payload: append([]byte("OK snapshot_page="), encoded...)})
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(6 * time.Second):
			t.Error("snapshot peer did not stop")
		}
	})
	return listener.Addr().String(), requests
}

func TestSnapshotPageResumesOnlySuccessfullyPublishedKeys(t *testing.T) {
	for _, change := range []string{"none", "revision", "invalid-snapshot"} {
		t.Run(change, func(t *testing.T) {
			receiver, state, _, _ := receiverWitnessFixture(t, 0, 2, 1, true)
			_, source, p, _ := receiverWitnessFixture(t, 0, 2, 1, true)
			require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Key: "a", AggregateVersion: 1, Payload: "one"}, {Offset: 1, Key: "b", AggregateVersion: 1, Payload: "two"}}))
			require.NoError(t, p.ApplyReplicaHWM(2))
			request := localRecoveryRequest(t, state, "receiver", "orders", 0)
			batch, err := source.FetchReplicaCatchup(request)
			require.NoError(t, err)
			actual, err := receiver.ApplyReplicaCatchupVerified(context.Background(), request, batch)
			require.NoError(t, err)
			require.NoError(t, fsm.AdvanceReplicaCatchupRequest(&request, batch))
			first := eventsource.SnapshotResult{Topic: "orders", Partition: 0, Key: "a", Version: 1, Payload: "first", LifecycleEpoch: request.LifecycleEpoch}
			second := first
			second.Key, second.Payload = "b", "second"
			pages := []eventsource.SnapshotPage{{Revision: 7, Snapshots: []eventsource.SnapshotResult{first}}, {Revision: 7, Snapshots: []eventsource.SnapshotResult{second}, Done: true}}
			if change == "revision" {
				pages[1].Revision++
			}
			if change == "invalid-snapshot" {
				pages[1].Snapshots[0].Version = 3
			}
			address, received := snapshotPagePeer(t, pages)
			request.SourceAddress = address
			_, portText, err := net.SplitHostPort(address)
			require.NoError(t, err)
			port, err := strconv.Atoi(portText)
			require.NoError(t, err)
			receiver.Cluster.Router = clusterController.NewClusterRouter("receiver", "receiver:9001", nil, receiver.Cluster.RaftManager, port, "", nil)
			progress := &clusterController.ReplicaSnapshotProgress{}
			done, err := receiver.CatchupReplicaSnapshotPage(context.Background(), request, actual, progress)
			require.NoError(t, err)
			require.False(t, done)
			require.Equal(t, clusterController.ReplicaSnapshotProgress{Revision: 7, AfterKey: "a"}, *progress)
			done, err = receiver.CatchupReplicaSnapshotPage(context.Background(), request, actual, progress)
			if change == "none" {
				require.NoError(t, err)
				require.True(t, done)
				require.Equal(t, "b", progress.AfterKey)
				require.NoError(t, state.MarkReplicaCatchupVerified(request, actual))
			} else {
				require.Error(t, err)
				require.False(t, done)
				require.Equal(t, "a", progress.AfterKey)
				snapshot, response := receiver.ESHandler.FetchSnapshot("orders", 0, "b")
				require.Empty(t, response)
				require.Nil(t, snapshot)
			}
			initial := <-received
			require.Empty(t, initial.Fields["after_key"])
			require.Empty(t, initial.Fields["revision"])
			resumed := <-received
			require.Equal(t, base64.RawURLEncoding.EncodeToString([]byte("a")), resumed.Fields["after_key"])
			require.Equal(t, "7", resumed.Fields["revision"])
		})
	}
}

func TestSnapshotCatalogNetworkWaitHoldsNoLocalGatesAndCancels(t *testing.T) {
	receiver, state, p, _ := receiverWitnessFixture(t, 0, 0, 1, true)
	_, source, _, _ := receiverWitnessFixture(t, 0, 0, 1, true)
	request := localRecoveryRequest(t, state, "receiver", "orders", 0)
	batch, err := source.FetchReplicaCatchup(request)
	require.NoError(t, err)
	witness, err := receiver.ApplyReplicaCatchupVerified(context.Background(), request, batch)
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	request.SourceAddress = listener.Addr().String()
	receiver.Cluster.Router = clusterController.NewClusterRouter("receiver", "receiver:9001", nil, receiver.Cluster.RaftManager, listener.Addr().(*net.TCPAddr).Port, "", nil)
	received := make(chan struct{})
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		raw, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = raw.Close() }()
		connection, err := wire.ServerHandshake(raw, []wire.Compression{wire.CompressionNone})
		if err != nil {
			return
		}
		if _, err = connection.ReadFrame(); err != nil {
			return
		}
		close(received)
		_, _ = connection.ReadFrame() // Cancellation must close the owned connection.
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- receiver.CatchupReplicaSnapshots(ctx, request, witness) }()
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("catalog request not received")
	}
	gateCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	release, err := receiver.topicLifecycleGates.acquire(gateCtx, "orders", true)
	require.NoError(t, err)
	lock := receiver.partitionWriteLock("orders", 0)
	require.NoError(t, lock.LockContext(gateCtx))
	require.NoError(t, p.WithReplicaSnapshotCertification(gateCtx, witness, request.Leader, request.LeaderEpoch, request.LifecycleEpoch, request.CommittedHWM, func(*topic.SnapshotReadCut) error { return nil }))
	lock.Unlock()
	release()
	cancel()
	select {
	case err := <-result:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("catalog cancellation did not interrupt network wait")
	}
	select {
	case <-serverDone:
	case <-time.After(2 * time.Second):
		t.Fatal("catalog connection remained open")
	}
}

func TestActualCatchupSnapshotPublishesAfterAdvancingRequest(t *testing.T) {
	receiver, state, _, _ := receiverWitnessFixture(t, 0, 1, 1, true)
	_, source, p, _ := receiverWitnessFixture(t, 0, 1, 1, true)
	require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Key: "a", AggregateVersion: 1, Payload: "event"}}))
	require.NoError(t, p.ApplyReplicaHWM(1))
	request := localRecoveryRequest(t, state, "receiver", "orders", 0)
	batch, err := source.FetchReplicaCatchup(request)
	require.NoError(t, err)
	witness, err := receiver.ApplyReplicaCatchupVerified(context.Background(), request, batch)
	require.NoError(t, err)
	require.NotNil(t, witness)
	require.NoError(t, fsm.AdvanceReplicaCatchupRequest(&request, batch))
	require.Nil(t, request.LocalWitness)
	candidate := eventsource.SnapshotResult{Topic: "orders", Partition: 0, Key: "a", Version: 1, Payload: "snapshot", LifecycleEpoch: request.LifecycleEpoch}
	foreign := request
	foreign.BrokerID = "other-receiver"
	require.ErrorContains(t, receiver.publishCatchupSnapshot(context.Background(), foreign, witness, candidate), "receiver identity")
	require.NoError(t, receiver.publishCatchupSnapshot(context.Background(), request, witness, candidate))
	require.NoError(t, state.MarkReplicaCatchupVerified(request, witness))
	snapshot, response := receiver.ESHandler.FetchSnapshot("orders", 0, "a")
	require.Empty(t, response)
	require.Equal(t, "snapshot", snapshot.Payload)
}

func TestSnapshotCatchupRejectsNilActualBeforeCallback(t *testing.T) {
	receiver, _, _, _ := receiverWitnessFixture(t, 0, 0, 1, true)
	_, source, _, _ := receiverWitnessFixture(t, 0, 0, 1, true)
	called := false
	receiver.Cluster.SetReplicaSnapshotCatchup(func(context.Context, fsm.ReplicaCatchupRequest, *topic.ReplicaCatchupWitness) error {
		called = true
		return nil
	})
	err := receiver.Cluster.RunReplicaCatchupOnceVerified(context.Background(), &witnessSourceFetcher{source: source}, func(context.Context, fsm.ReplicaCatchupRequest, fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
		return nil, nil
	})
	require.ErrorContains(t, err, "actual receiver verification witness")
	require.False(t, called)
}

type snapshotPauseProvider struct {
	manager   *disk.DiskManager
	afterPage func()
}
type snapshotPauseStorage struct {
	*disk.DiskHandler
	provider *snapshotPauseProvider
}

func (p *snapshotPauseProvider) GetHandler(name string, partition int) (types.StorageHandler, error) {
	storage, err := p.manager.GetHandler(name, partition)
	if err != nil {
		return nil, err
	}
	return &snapshotPauseStorage{storage.(*disk.DiskHandler), p}, nil
}
func (s *snapshotPauseStorage) ReadRetainedPage(ctx context.Context, start, end uint64, records, bytes int) (disk.RetainedPage, error) {
	page, err := s.DiskHandler.ReadRetainedPage(ctx, start, end, records, bytes)
	if s.provider.afterPage != nil {
		hook := s.provider.afterPage
		s.provider.afterPage = nil
		hook()
	}
	return page, err
}

func TestSnapshotFullCertificationAllowsFSMProgressThenRejectsChangedFence(t *testing.T) {
	var provider *snapshotPauseProvider
	receiver, state, _, _ := receiverWitnessFixture(t, 0, 1, 1, true, func(dm *disk.DiskManager) topic.HandlerProvider {
		provider = &snapshotPauseProvider{manager: dm}
		return provider
	})
	_, source, p, _ := receiverWitnessFixture(t, 0, 1, 1, true)
	require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Key: "a", AggregateVersion: 1, Payload: "event"}}))
	require.NoError(t, p.ApplyReplicaHWM(1))
	request := localRecoveryRequest(t, state, "receiver", "orders", 0)
	batch, err := source.FetchReplicaCatchup(request)
	require.NoError(t, err)
	witness, err := receiver.ApplyReplicaCatchupVerified(context.Background(), request, batch)
	require.NoError(t, err)
	require.NoError(t, fsm.AdvanceReplicaCatchupRequest(&request, batch))
	progressed := false
	provider.afterPage = func() {
		meta := state.GetPartitionMetadata("orders-0")
		meta.ISR = []string{}
		// This synchronous metadata mutation would deadlock if full replay held
		// the FSM publication fence. It must complete before final authorization.
		applyPartitionMetadata(t, state, "orders", 0, *meta)
		progressed = true
	}
	err = receiver.publishCatchupSnapshot(context.Background(), request, witness, eventsource.SnapshotResult{Topic: "orders", Key: "a", Version: 1, Payload: "bad", LifecycleEpoch: request.LifecycleEpoch})
	require.True(t, progressed)
	require.ErrorContains(t, err, "fence changed")
	snapshot, response := receiver.ESHandler.FetchSnapshot("orders", 0, "a")
	require.Empty(t, response)
	require.Nil(t, snapshot)
}
