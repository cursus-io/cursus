package controller_test

import (
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func TestCommandHandler_Publish(t *testing.T) {
	cfg := &config.Config{}
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	ch := controller.NewCommandHandler(tm, cfg, nil, nil, nil)
	ctx := controller.NewClientContext("test-group", 0)

	_ = tm.CreateTopic("test-topic", 1, false)

	t.Run("PUBLISH basic", func(t *testing.T) {
		cmd := "PUBLISH topic=test-topic producerId=p1 acks=1 message=hello"
		resp := ch.HandleCommand(cmd, ctx)
		
		var ack types.AckResponse
		if err := json.Unmarshal([]byte(resp), &ack); err != nil {
			t.Fatalf("Failed to unmarshal response: %v, resp: %s", err, resp)
		}
		if ack.Status != "OK" {
			t.Errorf("Expected OK status, got: %s", ack.Status)
		}
	})

	t.Run("PUBLISH acks=0", func(t *testing.T) {
		cmd := "PUBLISH topic=test-topic producerId=p1 acks=0 message=hello"
		resp := ch.HandleCommand(cmd, ctx)
		if resp != "OK" {
			t.Errorf("Expected OK, got: %s", resp)
		}
	})

	t.Run("PUBLISH missing parameters", func(t *testing.T) {
		cmds := []string{
			"PUBLISH producerId=p1 message=hello",
			"PUBLISH topic=test-topic message=hello",
			"PUBLISH topic=test-topic producerId=p1",
		}
		for _, cmd := range cmds {
			resp := ch.HandleCommand(cmd, ctx)
			if !strings.Contains(resp, "ERROR: missing") {
				t.Errorf("Expected error for command '%s', got: %s", cmd, resp)
			}
		}
	})

	t.Run("PUBLISH non-existent topic", func(t *testing.T) {
		start := time.Now()
		cmd := "PUBLISH topic=no-topic producerId=p1 message=hello"
		resp := ch.HandleCommand(cmd, ctx)
		duration := time.Since(start)
		
		if !strings.Contains(resp, "ERROR: topic 'no-topic' does not exist") {
			t.Errorf("Expected topic not found error, got: %s", resp)
		}
		if duration < 500*time.Millisecond {
			t.Errorf("Expected retry delay, but took only %v", duration)
		}
	})
}

func TestCommandHandler_Consume(t *testing.T) {
	cfg := &config.Config{}
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	ch := controller.NewCommandHandler(tm, cfg, nil, nil, nil)
	ctx := controller.NewClientContext("test-group", 0)

	_ = tm.CreateTopic("test-topic", 1, false)

	t.Run("CONSUME syntax error", func(t *testing.T) {
		resp := ch.HandleCommand("CONSUME topic=test-topic", ctx)
		if !strings.Contains(resp, "ERROR: invalid CONSUME syntax") {
			t.Errorf("Expected syntax error, got: %s", resp)
		}
	})

	t.Run("HandleConsumeCommand missing parameters", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()

		go func() {
			buf := make([]byte, 1024)
			_, _ = server.Read(buf)
		}()

		_, err := ch.HandleConsumeCommand(client, "CONSUME topic=test-topic", ctx)
		if err == nil || !strings.Contains(err.Error(), "missing") {
			t.Errorf("Expected missing parameter error, got: %v", err)
		}
	})
}

func TestCommandHandler_BatchPublish(t *testing.T) {
	cfg := &config.Config{}
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	ch := controller.NewCommandHandler(tm, cfg, nil, nil, nil)

	_ = tm.CreateTopic("test-topic", 1, false)

	t.Run("HandleBatchMessage valid", func(t *testing.T) {
		msgs := []types.Message{
			{Payload: "m1", ProducerID: "p1", SeqNum: 1},
			{Payload: "m2", ProducerID: "p1", SeqNum: 2},
		}
		data, err := util.EncodeBatchMessages("test-topic", 0, "1", false, msgs)
		if err != nil {
			t.Fatalf("Failed to encode batch: %v", err)
		}
		resp, err := ch.HandleBatchMessage(data, nil)
		if err != nil {
			t.Fatalf("HandleBatchMessage failed: %v", err)
		}
		
		var ack types.AckResponse
		if err := json.Unmarshal([]byte(resp), &ack); err != nil {
			t.Fatalf("Failed to unmarshal response: %v, resp: %s", err, resp)
		}
		if ack.Status != "OK" {
			t.Errorf("Expected OK status, got: %s", ack.Status)
		}
	})

	t.Run("HandleBatchMessage acks=0", func(t *testing.T) {
		msgs := []types.Message{
			{Payload: "m1", ProducerID: "p1", SeqNum: 3},
		}
		data, err := util.EncodeBatchMessages("test-topic", 0, "0", false, msgs)
		if err != nil {
			t.Fatalf("Failed to encode batch: %v", err)
		}
		resp, err := ch.HandleBatchMessage(data, nil)
		if err != nil {
			t.Fatalf("HandleBatchMessage failed: %v", err)
		}
		if resp != "OK" {
			t.Errorf("Expected OK, got: %s", resp)
		}
	})
}
