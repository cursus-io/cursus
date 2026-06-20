package controller_test

import (
	"context"
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
	assert.Contains(t, resp, "ERROR: invalid STREAM syntax")
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
	require.NoError(t, coord.CommitOffset("g1", "topic1", 0, 2))

	ch := controller.NewCommandHandler(tm, cfg, coord, nil, nil)
	ctx := controller.NewClientContext("g1", 0)

	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	errCh := make(chan error, 1)
	go func() {
		_, err := ch.HandleConsumeCommand(server, "CONSUME topic=topic1 partition=0 offset=0 group=g1 member=m1 batch=10", ctx)
		errCh <- err
	}()

	_ = client.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
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
	messages []types.Message
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

func (s *sequenceStorage) GetAbsoluteOffset() uint64 {
	return uint64(len(s.messages))
}

func (s *sequenceStorage) GetFlushedOffset() uint64 {
	return uint64(len(s.messages))
}

func (s *sequenceStorage) GetLatestOffset() uint64 {
	return uint64(len(s.messages))
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
