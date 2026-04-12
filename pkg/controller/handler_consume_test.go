package controller_test

import (
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/stretchr/testify/assert"
)

func TestCommandHandler_ConsumeFull(t *testing.T) {
	cfg := config.DefaultConfig()
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	tm.CreateTopic("topic1", 1, false)
	
	p, _ := tm.GetTopic("topic1").GetPartition(0)
	p.SetHWM(100)

	coord := coordinator.NewCoordinator(cfg, &DummyPublisher{})
	coord.RegisterGroup("topic1", "g1", 1)
	memberID := "m1"
	_, _ = coord.AddConsumer("g1", memberID)
	coord.Rebalance("g1")

	ch := controller.NewCommandHandler(tm, cfg, coord, nil, nil)
	ctx := controller.NewClientContext("g1", 0)
	ctx.MemberID = memberID
	ctx.Generation = coord.GetGeneration("g1")

	t.Run("HandleConsumeCommand basic", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()

		go func() {
			cmd := "CONSUME topic=topic1 partition=0 offset=0 group=g1 member=" + memberID
			_, err := ch.HandleConsumeCommand(server, cmd, ctx)
			if err != nil {
				t.Logf("HandleConsumeCommand error: %v", err)
			}
		}()

		// Read response
		buf := make([]byte, 1024)
		client.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		_, err := client.Read(buf)
		assert.NoError(t, err)
	})

	t.Run("HandleConsumeCommand topic pattern", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()

		go func() {
			cmd := "CONSUME topic=top* partition=0 offset=0 group=g1 member=" + memberID
			_, err := ch.HandleConsumeCommand(server, cmd, ctx)
			if err != nil {
				t.Logf("HandleConsumeCommand error: %v", err)
			}
		}()

		buf := make([]byte, 1024)
		client.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		_, err := client.Read(buf)
		assert.NoError(t, err)
	})

	t.Run("HandleConsumeCommand invalid topic", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()

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
