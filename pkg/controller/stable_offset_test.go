package controller

import (
	"fmt"
	"net"
	"testing"
	"time"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestReservationFencesFetchConsumeCacheAndStream(t *testing.T) {
	ch, tm, cd, _ := newDiskBackedTransactionHandler(t)
	generation := prepareTransactionGroup(t, tm, cd, "inputs", "workers", "worker")
	require.NoError(t, cd.CommitOffset("workers", "inputs", 0, 3))
	reservation := coordinator.TransactionOffsetReservation{TransactionalID: "tx", ProducerID: "producer", MemberID: "worker", Generation: generation,
		Offsets: []coordinator.ReservedTransactionOffset{{Topic: "inputs", Partition: 0, Offset: 5}}}
	epoch := cd.GetRegistrationEpoch("workers")
	require.NoError(t, cd.PrepareOffsetReservation("workers", epoch, reservation))
	response := ch.handleFetchOffset("FETCH_OFFSET group=workers topic=inputs partition=0")
	require.Contains(t, response, "unstable_offset_commit")
	classification := protocol.ClassifyErrorCode("unstable_offset_commit")
	require.True(t, classification.Retryable)
	require.Equal(t, protocol.ErrorClassAvailability, classification.Class)
	args := CommonArgs{GroupName: "workers", MemberID: "worker", Generation: generation, PartitionID: 0, HasOffset: true, Offset: 9}
	ctx := NewClientContext("workers", 0)
	ctx.OffsetCache[consumerOffsetCacheKey("inputs", args)] = 12
	_, err := ch.readFromTopic("inputs", args, ctx, 1)
	require.ErrorContains(t, err, "unstable_offset_commit", "a cached or explicitly requested position cannot bypass the reservation")
	err = ch.HandleStreamCommand(nil, "STREAM topic=inputs partition=0 group=workers offset=9", ctx)
	require.ErrorContains(t, err, "unstable_offset_commit")
	require.NoError(t, cd.RemoveConsumerForGeneration("workers", "worker", generation))
	require.NoError(t, cd.ResolveOffsetReservation("workers", epoch, "tx", "producer", 0, true))
	require.Equal(t, "OK offset=5", ch.handleFetchOffset("FETCH_OFFSET group=workers topic=inputs partition=0"))
	require.Equal(t, "OK offset=5 found=true", ch.handleFetchOffset("FETCH_OFFSET group=workers topic=inputs partition=0 include_found=true"))
}

func TestConsumeCacheIsScopedToGroupMemberAndGeneration(t *testing.T) {
	ch, tm, _, _ := newDiskBackedTransactionHandler(t)
	require.NoError(t, tm.CreateTopic("inputs", 1, false, false))
	for i := 0; i < 4; i++ {
		require.NoError(t, tm.PublishToPartitionWithAck("inputs", 0, &types.Message{Payload: fmt.Sprint(i)}))
	}
	ctx := NewClientContext("", 0)
	args := CommonArgs{GroupName: "first", MemberID: "member", Generation: 1, PartitionID: 0}
	read := func(want uint64) {
		messages, err := ch.readFromTopic("inputs", args, ctx, 1)
		require.NoError(t, err)
		require.Len(t, messages, 1)
		require.Equal(t, want, messages[0].Offset)
	}
	read(0)
	args.AutoOffsetReset = "latest"
	read(1) // A reset policy is applied only when there is no cached position.
	args.AutoOffsetReset = "earliest"
	args.GroupName = "second"
	read(0)
	args.MemberID = "replacement"
	read(0)
	args.Generation++
	read(0)
}

func TestRunningStreamStopsWhenReservationAppears(t *testing.T) {
	ch, tm, cd, _ := newDiskBackedTransactionHandler(t)
	generation := prepareTransactionGroup(t, tm, cd, "inputs", "workers", "worker")
	ch.StreamManager = stream.NewStreamManager(2, time.Second)
	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	require.NoError(t, ch.HandleStreamCommand(server, "STREAM topic=inputs partition=0 group=workers offset=0", NewClientContext("workers", 0)))
	require.NoError(t, cd.PrepareOffsetReservation("workers", cd.GetRegistrationEpoch("workers"), coordinator.TransactionOffsetReservation{
		TransactionalID: "tx", ProducerID: "producer", MemberID: "worker", Generation: generation,
		Offsets: []coordinator.ReservedTransactionOffset{{Topic: "inputs", Partition: 0, Offset: 1}},
	}))
	require.NoError(t, client.SetReadDeadline(time.Now().Add(3*time.Second)))
	control, err := util.ReadWithLength(client)
	require.NoError(t, err)
	require.Contains(t, string(control), "error", "the existing stream must stop instead of consuming reserved input")
}

func TestStableOffsetResponseRejectsAmbiguousOrUnavailableView(t *testing.T) {
	for _, response := range []string{"OK offset=0", "OK offset=7 found=false", "OK offset=bad found=true", "garbage", "ERROR: unstable_offset_commit group=workers"} {
		_, _, err := parseStableOffsetResponse(response)
		require.Error(t, err, response)
	}
	offset, found, err := parseStableOffsetResponse("OK offset=0 found=true")
	require.NoError(t, err)
	require.True(t, found)
	require.Zero(t, offset)
	_, found, err = parseStableOffsetResponse("OK offset=0 found=false")
	require.NoError(t, err)
	require.False(t, found)
}

func TestConsumeResumeUsesRemoteGroupCoordinator(t *testing.T) {
	ch, tm, cd, _ := newDiskBackedTransactionHandler(t)
	prepareTransactionGroup(t, tm, cd, "inputs", "workers", "worker")
	require.NoError(t, cd.CommitOffset("workers", "inputs", 0, 3)) // stale replica view
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	brokerFSM := fsm.NewBrokerFSM(nil, nil)
	registerRoutingBroker(t, brokerFSM, "coordinator")
	installRoutingOffsetsTopology(t, brokerFSM, "coordinator")
	rm := &coordinatorRoutingRaftManager{brokerFSM: brokerFSM}
	rm.leaderAddress.Store("127.0.0.1:7000")
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.BrokerPort = listener.Addr().(*net.TCPAddr).Port
	cfg.LogDir = ch.Config.LogDir
	require.NoError(t, ch.Close())
	cluster := &clusterController.ClusterController{RaftManager: rm, Router: clusterController.NewClusterRouter("input-leader", "127.0.0.1:7001", nil, rm, cfg.BrokerPort, "", cfg)}
	ch = NewCommandHandler(tm, cfg, cd, nil, cluster)
	t.Cleanup(func() { require.NoError(t, ch.Close()) })
	served := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			served <- err
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		wc, err := wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone})
		if err != nil {
			served <- err
			return
		}
		request, err := wc.ReadFrame()
		if err != nil {
			served <- err
			return
		}
		payload, err := wire.DecodeCommandPayload(request.Payload)
		if err != nil {
			served <- err
			return
		}
		if request.Command != wire.CommandFetchOffset || payload.Fields["include_found"] != "true" || payload.Fields["group"] != "workers" {
			served <- fmt.Errorf("unexpected offset request: %+v", payload)
			return
		}
		served <- wc.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: request.Command, RequestID: request.RequestID, Status: wire.StatusOK, Payload: []byte("OK offset=9 found=true")})
	}()
	p, err := tm.GetTopic("inputs").GetPartition(0)
	require.NoError(t, err)
	offset, err := ch.resolveOffset(p, "inputs", CommonArgs{GroupName: "workers", PartitionID: 0})
	require.NoError(t, err)
	require.Equal(t, uint64(9), offset, "the input leader's stale local offset must not be used")
	require.NoError(t, <-served)
	require.NoError(t, listener.Close())
	_, err = ch.resolveOffset(p, "inputs", CommonArgs{GroupName: "workers", PartitionID: 0})
	require.Error(t, err, "coordinator failure must not fall back to the stale local view")
}
