package controller_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommandHandler_ConsumeFull(t *testing.T) {
	cfg := config.DefaultConfig()
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	_ = tm.CreateTopic("topic1", 1, false, false)

	p, _ := tm.GetTopic("topic1").GetPartition(0)
	p.SetHWM(100)

	coord := coordinator.NewCoordinator(context.Background(), cfg, &DummyPublisher{})
	_ = coord.RegisterGroup("topic1", "g1", 1)
	memberID := "m1"
	_, _ = coord.AddConsumer("g1", memberID)
	coord.Rebalance("g1")

	ch := controller.NewCommandHandler(tm, cfg, coord, nil, nil)
	ctx := controller.NewClientContext("g1", 0)
	ctx.MemberID = memberID
	ctx.Generation = coord.GetGeneration("g1")

	t.Run("HandleConsumeCommand basic", func(t *testing.T) {
		server, client := net.Pipe()
		defer func() { _ = server.Close() }()
		defer func() { _ = client.Close() }()

		errCh := make(chan error, 1)
		go func() {
			cmd := "CONSUME topic=topic1 partition=0 offset=0 group=g1 member=" + memberID
			_, err := ch.HandleConsumeCommand(server, cmd, ctx)
			errCh <- err
		}()

		// Read response
		buf := make([]byte, 1024)
		_ = client.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, err := client.Read(buf)
		assert.NoError(t, err)

		_ = client.Close()
		err = <-errCh
		if err != nil && !strings.Contains(err.Error(), "closed pipe") && !strings.Contains(err.Error(), "EOF") {
			t.Errorf("HandleConsumeCommand failed: %v", err)
		}
	})

	t.Run("HandleConsumeCommand topic pattern", func(t *testing.T) {
		server, client := net.Pipe()
		defer func() { _ = server.Close() }()
		defer func() { _ = client.Close() }()

		errCh := make(chan error, 1)
		go func() {
			cmd := "CONSUME topic=top* partition=0 offset=0 group=g1 member=" + memberID
			_, err := ch.HandleConsumeCommand(server, cmd, ctx)
			errCh <- err
		}()

		buf := make([]byte, 1024)
		_ = client.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, err := client.Read(buf)
		assert.NoError(t, err)

		_ = client.Close()
		err = <-errCh
		if err != nil && !strings.Contains(err.Error(), "closed pipe") && !strings.Contains(err.Error(), "EOF") {
			t.Errorf("HandleConsumeCommand failed: %v", err)
		}
	})

	t.Run("HandleConsumeCommand invalid topic", func(t *testing.T) {
		server, client := net.Pipe()
		defer func() { _ = server.Close() }()
		defer func() { _ = client.Close() }()

		cmd := "CONSUME topic=no-topic partition=0 offset=0 group=g1 member=" + memberID
		_, err := ch.HandleConsumeCommand(server, cmd, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})
}

func TestCommandHandler_StreamSyntax(t *testing.T) {
	ch := controller.NewCommandHandler(nil, nil, nil, nil, nil)

	resp := ch.HandleCommand("STREAM topic=t1 partition=0 group=g1", nil)
	assert.Equal(t, "STREAM_DATA", resp)

	resp = ch.HandleCommand("STREAM topic=t1", nil)
	assert.Contains(t, resp, "ERROR: invalid_stream_syntax")

	resp = ch.HandleCommand("STREAM topic=t1 partition=0 group=g1 isolation=dirty", nil)
	assert.Contains(t, resp, "ERROR: invalid_isolation isolation=dirty")
}

func TestCommandHandler_ConsumeUsesCommittedOffsetBeforeExplicitOffset(t *testing.T) {
	cfg := config.DefaultConfig()
	storage := &sequenceStorage{messages: []types.Message{
		{Offset: 0, Payload: "m0"},
		{Offset: 1, Payload: "m1"},
		{Offset: 2, Payload: "m2"},
		{Offset: 3, Payload: "m3"},
	}}
	tm := topic.NewTopicManager(cfg, &singleStorageProvider{storage: storage}, nil)
	require.NoError(t, tm.CreateTopic("topic1", 1, false, false))

	coord := coordinator.NewCoordinator(context.Background(), cfg, &DummyPublisher{})
	require.NoError(t, coord.RegisterGroup("topic1", "g1", 1))
	assignments, err := coord.AddConsumer("g1", "m1")
	require.NoError(t, err)
	require.Contains(t, assignments, 0)
	generation := coord.GetGeneration("g1")
	require.NoError(t, coord.CommitOffset("g1", "topic1", 0, 2))

	ch := controller.NewCommandHandler(tm, cfg, coord, nil, nil)
	ctx := controller.NewClientContext("g1", 0)
	ctx.MemberID = "m1"
	ctx.Generation = generation

	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	errCh := make(chan error, 1)
	go func() {
		_, err := ch.HandleConsumeCommand(server, fmt.Sprintf("CONSUME topic=topic1 partition=0 offset=0 group=g1 member=m1 generation=%d batch=10", generation), ctx)
		errCh <- err
	}()

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	data, err := util.ReadWithLength(client)
	require.NoError(t, err)
	batch, err := util.DecodeBatchMessages(data)
	require.NoError(t, err)
	require.Len(t, batch.Messages, 2)
	assert.Equal(t, uint64(2), batch.Messages[0].Offset)
	assert.Equal(t, uint64(3), batch.Messages[1].Offset)

	_ = client.Close()
	err = <-errCh
	if err != nil && !strings.Contains(err.Error(), "closed pipe") && !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("HandleConsumeCommand failed: %v", err)
	}
}

type sequenceStorage struct {
	messages    []types.Message
	firstOffset uint64
}

func (s *sequenceStorage) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	var out []types.Message
	for _, msg := range s.messages {
		if msg.Offset >= offset {
			out = append(out, msg)
			if len(out) == max {
				break
			}
		}
	}
	return out, nil
}

func (s *sequenceStorage) GetFirstOffset() uint64 {
	if s.firstOffset > 0 {
		return s.firstOffset
	}
	if len(s.messages) > 0 {
		return s.messages[0].Offset
	}
	return 0
}
func (s *sequenceStorage) tailOffset() uint64 {
	if len(s.messages) == 0 {
		return s.firstOffset
	}
	return s.messages[len(s.messages)-1].Offset + 1
}

func (s *sequenceStorage) GetAbsoluteOffset() uint64 {
	return s.tailOffset()
}

func (s *sequenceStorage) GetFlushedOffset() uint64 {
	return s.tailOffset()
}

func (s *sequenceStorage) GetLatestOffset() uint64 {
	return s.tailOffset()
}

func (s *sequenceStorage) GetSegmentPath(uint64) string {
	return ""
}

func (s *sequenceStorage) AppendMessage(string, int, *types.Message) (uint64, error) {
	return 0, nil
}

func (s *sequenceStorage) AppendMessageSync(string, int, *types.Message) (uint64, error) {
	return 0, nil
}

func (s *sequenceStorage) AppendMessageWithOffset(string, int, *types.Message) error {
	return nil
}

func (s *sequenceStorage) WriteBatch([]types.DiskMessage) error {
	return nil
}

func (s *sequenceStorage) TruncateTo(uint64) error {
	return nil
}

func (s *sequenceStorage) Flush() {}

func (s *sequenceStorage) Close() error {
	return nil
}

type singleStorageProvider struct {
	storage types.StorageHandler
}

func (p *singleStorageProvider) GetHandler(string, int) (types.StorageHandler, error) {
	return p.storage, nil
}

func TestCommandHandler_ConsumeIsStatelessForStaleGeneration(t *testing.T) {
	cfg := config.DefaultConfig()
	storage := &sequenceStorage{messages: []types.Message{
		{Offset: 0, Payload: "m0"},
		{Offset: 1, Payload: "m1"},
	}}
	tm := topic.NewTopicManager(cfg, &singleStorageProvider{storage: storage}, nil)
	require.NoError(t, tm.CreateTopic("ownership-topic", 1, false, false))

	coord := coordinator.NewCoordinator(context.Background(), cfg, &DummyPublisher{})
	require.NoError(t, coord.RegisterGroup("ownership-topic", "ownership-group", 1))
	_, err := coord.AddConsumer("ownership-group", "member-1")
	require.NoError(t, err)
	generation := coord.GetGeneration("ownership-group")

	ch := controller.NewCommandHandler(tm, cfg, coord, nil, nil)
	ctx := controller.NewClientContext("ownership-group", 0)
	ctx.MemberID = "member-1"
	ctx.Generation = generation

	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	errCh := make(chan error, 1)
	go func() {
		_, err := ch.HandleConsumeCommand(server, fmt.Sprintf("CONSUME topic=ownership-topic partition=0 offset=0 group=ownership-group member=member-1 generation=%d batch=10", generation+1), ctx)
		errCh <- err
	}()

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	data, err := util.ReadWithLength(client)
	require.NoError(t, err)
	batch, err := util.DecodeBatchMessages(data)
	require.NoError(t, err)
	require.Len(t, batch.Messages, 2)
	assert.Equal(t, uint64(0), batch.Messages[0].Offset)
	assert.Equal(t, uint64(1), batch.Messages[1].Offset)

	_ = client.Close()
	err = <-errCh
	if err != nil && !strings.Contains(err.Error(), "closed pipe") && !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("HandleConsumeCommand failed: %v", err)
	}
}

func TestCommandHandler_ConsumeWritesOffsetOutOfRangeFrame(t *testing.T) {
	cfg := config.DefaultConfig()
	storage := &sequenceStorage{
		firstOffset: 2,
		messages: []types.Message{
			{Offset: 2, Payload: "m2"},
			{Offset: 3, Payload: "m3"},
		},
	}
	tm := topic.NewTopicManager(cfg, &singleStorageProvider{storage: storage}, nil)
	require.NoError(t, tm.CreateTopic("retained-topic", 1, false, false))
	p, err := tm.GetTopic("retained-topic").GetPartition(0)
	require.NoError(t, err)
	p.SetHWM(4)

	coord := coordinator.NewCoordinator(context.Background(), cfg, &DummyPublisher{})
	require.NoError(t, coord.RegisterGroup("retained-topic", "retained-group", 1))

	ch := controller.NewCommandHandler(tm, cfg, coord, nil, nil)
	ctx := controller.NewClientContext("retained-group", 0)

	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	errCh := make(chan error, 1)
	go func() {
		_, err := ch.HandleConsumeCommand(server, "CONSUME topic=retained-topic partition=0 offset=0 group=retained-group member=m1 batch=10", ctx)
		errCh <- err
	}()

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	data, err := util.ReadWithLength(client)
	require.NoError(t, err)
	assert.Equal(t, "ERROR: OFFSET_OUT_OF_RANGE requested=0 earliest=2 latest=4", string(data))
	require.NoError(t, <-errCh)
}

func TestCommandHandler_ConsumeReadIsolationModes(t *testing.T) {
	cfg := config.DefaultConfig()
	storage := &sequenceStorage{messages: []types.Message{
		{Offset: 0, Payload: "plain"},
		{Offset: 1, Payload: "open", TransactionalID: "tx-open", TransactionState: types.TransactionStateOpen},
		{Offset: 2, Payload: "committed", TransactionalID: "tx-commit", TransactionState: types.TransactionStateOpen},
		{Offset: 3, Payload: "commit-marker", TransactionalID: "tx-commit", TransactionMarker: types.TransactionMarkerCommit},
		{Offset: 4, Payload: "after"},
	}}
	tm := topic.NewTopicManager(cfg, &singleStorageProvider{storage: storage}, nil)
	require.NoError(t, tm.CreateTopic("isolation-topic", 1, false, false))
	p, err := tm.GetTopic("isolation-topic").GetPartition(0)
	require.NoError(t, err)
	p.SetHWM(5)

	coord := coordinator.NewCoordinator(context.Background(), cfg, &DummyPublisher{})
	require.NoError(t, coord.RegisterGroup("isolation-topic", "isolation-group", 1))

	ch := controller.NewCommandHandler(tm, cfg, coord, nil, nil)

	committed := consumeTestBatch(t, ch, "CONSUME topic=isolation-topic partition=0 offset=0 group=isolation-group member=m1 batch=10", controller.NewClientContext("isolation-group", 0))
	require.Len(t, committed.Messages, 1)
	assert.Equal(t, "plain", committed.Messages[0].Payload)

	uncommitted := consumeTestBatch(t, ch, "CONSUME topic=isolation-topic partition=0 offset=0 group=isolation-group member=m1 batch=10 isolation=read_uncommitted", controller.NewClientContext("isolation-group", 0))
	require.Len(t, uncommitted.Messages, 5)
	assert.Equal(t, "plain", uncommitted.Messages[0].Payload)
	assert.Equal(t, "open", uncommitted.Messages[1].Payload)
	assert.Equal(t, "committed", uncommitted.Messages[2].Payload)
	assert.Equal(t, "commit-marker", uncommitted.Messages[3].Payload)
	assert.Equal(t, "after", uncommitted.Messages[4].Payload)
}

func TestCommandHandler_ConsumeRejectsInvalidIsolation(t *testing.T) {
	cfg := config.DefaultConfig()
	ch := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	_, err := ch.HandleConsumeCommand(server, "CONSUME topic=t partition=0 offset=0 group=g member=m isolation=dirty", controller.NewClientContext("g", 0))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid_isolation isolation=dirty")

	resp := ch.HandleCommand("CONSUME topic=t partition=0 offset=0 member=m isolation=dirty", controller.NewClientContext("g", 0))
	assert.Contains(t, resp, "ERROR: invalid_isolation isolation=dirty")
}

func consumeTestBatch(t *testing.T, ch *controller.CommandHandler, cmd string, ctx *controller.ClientContext) *types.Batch {
	t.Helper()
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	errCh := make(chan error, 1)
	go func() {
		_, err := ch.HandleConsumeCommand(server, cmd, ctx)
		errCh <- err
	}()

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	data, err := util.ReadWithLength(client)
	require.NoError(t, err)
	batch, err := util.DecodeBatchMessages(data)
	require.NoError(t, err)

	_ = client.Close()
	err = <-errCh
	if err != nil && !strings.Contains(err.Error(), "closed pipe") && !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("HandleConsumeCommand failed: %v", err)
	}
	return batch
}
