package controller_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
)

// Mock Storage Handler
type mockStorage struct{}

func (m *mockStorage) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	return []types.Message{}, nil
}
func (m *mockStorage) GetAbsoluteOffset() uint64               { return 0 }
func (m *mockStorage) GetLatestOffset() uint64                 { return 0 }
func (m *mockStorage) GetSegmentPath(baseOffset uint64) string { return "" }

func (m *mockStorage) AppendMessage(topic string, partition int, msg *types.Message) (uint64, error) {
	return 0, nil
}
func (m *mockStorage) AppendMessageSync(topic string, partition int, msg *types.Message) (uint64, error) {
	return 0, nil
}
func (m *mockStorage) WriteBatch(batch []types.DiskMessage) error { return nil }

func (m *mockStorage) Flush()       {}
func (m *mockStorage) Close() error { return nil }

type mockHandlerProvider struct{}

func (m *mockHandlerProvider) GetHandler(topic string, partitionID int) (types.StorageHandler, error) {
	return &mockStorage{}, nil
}

func TestCommandHandler_Real(t *testing.T) {
	cfg := &config.Config{}
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	ch := controller.NewCommandHandler(tm, cfg, nil, nil, nil)
	ctx := controller.NewClientContext("test-group", 0)

	t.Run("HELP command", func(t *testing.T) {
		resp := ch.HandleCommand("HELP", ctx)
		if !strings.Contains(resp, "Available commands") {
			t.Errorf("Expected HELP response, got: %s", resp)
		}
	})

	t.Run("CREATE and LIST topics", func(t *testing.T) {
		resp := ch.HandleCommand("CREATE topic=test-topic partitions=2", ctx)
		if !strings.Contains(resp, "Topic 'test-topic' now has 2 partitions") {
			t.Errorf("Failed to create topic: %s", resp)
		}

		resp = ch.HandleCommand("LIST", ctx)
		if !strings.Contains(resp, "test-topic") {
			t.Errorf("Topic not found in list: %s", resp)
		}
	})

	t.Run("DESCRIBE topic", func(t *testing.T) {
		resp := ch.HandleCommand("DESCRIBE topic=test-topic", ctx)
		var meta struct {
			Topic      string `json:"topic"`
			Partitions []any  `json:"partitions"`
		}
		err := json.Unmarshal([]byte(resp), &meta)
		if err != nil {
			t.Fatalf("Failed to unmarshal DESCRIBE response: %v\nResponse: %s", err, resp)
		}
		if meta.Topic != "test-topic" || len(meta.Partitions) != 2 {
			t.Errorf("Unexpected DESCRIBE result: %+v", meta)
		}
	})

	t.Run("DELETE topic", func(t *testing.T) {
		resp := ch.HandleCommand("DELETE topic=test-topic", ctx)
		if !strings.Contains(resp, "deleted") {
			t.Errorf("Failed to delete topic: %s", resp)
		}

		resp = ch.HandleCommand("LIST", ctx)
		if resp != "(no topics)" {
			t.Errorf("Expected empty list, got: %s", resp)
		}
	})

	t.Run("Unknown command", func(t *testing.T) {
		resp := ch.HandleCommand("INVALID_CMD", ctx)
		if !strings.Contains(resp, "ERROR: unknown command") {
			t.Errorf("Expected error for unknown command, got: %s", resp)
		}
	})

	t.Run("Empty command", func(t *testing.T) {
		resp := ch.HandleCommand("", ctx)
		if !strings.Contains(resp, "ERROR: empty command") {
			t.Errorf("Expected error for empty command, got: %s", resp)
		}
	})
}

func TestCommandHandler_GroupCommands(t *testing.T) {
	cfg := &config.Config{}
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	// We might need a real Coordinator if we want to test these, but for now we check the "coordinator not available" response
	ch := controller.NewCommandHandler(tm, cfg, nil, nil, nil)
	ctx := controller.NewClientContext("test-group", 0)

	if err := tm.CreateTopic("topic1", 1, false, false); err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	commands := []struct {
		cmd    string
		expect string
	}{
		{"REGISTER_GROUP topic=topic1 group=g1", "coordinator not available"},
		{"JOIN_GROUP topic=topic1 group=g1 member=m1", "coordinator not available"},
		{"HEARTBEAT topic=topic1 group=g1 member=m1", "coordinator not available"},
		{"LEAVE_GROUP topic=topic1 group=g1 member=m1", "✅ Left group 'g1'"},
		{"COMMIT_OFFSET topic=topic1 partition=0 group=g1 offset=10", "offset manager not available"},
		{"FETCH_OFFSET topic=topic1 partition=0 group=g1", "offset manager not available"},
	}

	for _, tc := range commands {
		resp := ch.HandleCommand(tc.cmd, ctx)
		if !strings.Contains(resp, tc.expect) {
			t.Errorf("Expected '%s' for command '%s', got: %s", tc.expect, tc.cmd, resp)
		}
	}
}

func TestCommandHandler_ErrorResponses(t *testing.T) {
	ch := controller.NewCommandHandler(nil, nil, nil, nil, nil)

	t.Run("CREATE missing topic", func(t *testing.T) {
		resp := ch.HandleCommand("CREATE partitions=3", nil)
		if !strings.Contains(resp, "missing topic parameter") {
			t.Errorf("Expected error for missing topic, got: %s", resp)
		}
	})

	t.Run("DELETE missing topic", func(t *testing.T) {
		resp := ch.HandleCommand("DELETE topic=", nil)
		if !strings.Contains(resp, "missing topic parameter") {
			t.Errorf("Expected error for missing topic, got: %s", resp)
		}
	})
}

func TestCommandHandler_EventSourcingCreate(t *testing.T) {
	cfg := &config.Config{}
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	ch := controller.NewCommandHandler(tm, cfg, nil, nil, nil)
	ctx := controller.NewClientContext("test-group", 0)

	resp := ch.HandleCommand("CREATE topic=es-topic partitions=2 event_sourcing=true", ctx)
	if !strings.Contains(resp, "Topic 'es-topic' now has 2 partitions") {
		t.Fatalf("Failed to create event-sourcing topic: %s", resp)
	}

	tObj := tm.GetTopic("es-topic")
	if tObj == nil {
		t.Fatal("Topic 'es-topic' not found after creation")
	}
	if !tObj.IsEventSourcing {
		t.Error("Expected IsEventSourcing=true on the created topic")
	}
}

func TestCommandHandler_PublishAndAck(t *testing.T) {
	cfg := &config.Config{}
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	ch := controller.NewCommandHandler(tm, cfg, nil, nil, nil)
	ctx := controller.NewClientContext("test-group", 0)

	// Create topic first
	resp := ch.HandleCommand("CREATE topic=pub-topic partitions=1", ctx)
	if !strings.Contains(resp, "pub-topic") {
		t.Fatalf("Failed to create topic: %s", resp)
	}

	// Publish with acks=1
	resp = ch.HandleCommand("PUBLISH topic=pub-topic acks=1 producerId=p1 seqNum=1 epoch=10 message=hello", ctx)

	var ack types.AckResponse
	if err := json.Unmarshal([]byte(resp), &ack); err != nil {
		t.Fatalf("Failed to unmarshal publish response: %v\nResponse: %s", err, resp)
	}

	if ack.Status != "OK" {
		t.Errorf("Expected status OK, got %q", ack.Status)
	}
	if ack.ProducerID != "p1" {
		t.Errorf("Expected ProducerID 'p1', got %q", ack.ProducerID)
	}
	if ack.ProducerEpoch != 10 {
		t.Errorf("Expected ProducerEpoch 10, got %d", ack.ProducerEpoch)
	}
	if ack.SeqStart != 1 || ack.SeqEnd != 1 {
		t.Errorf("Expected SeqStart=1 SeqEnd=1, got %d %d", ack.SeqStart, ack.SeqEnd)
	}
}

func TestCommandHandler_ProcessCommand(t *testing.T) {
	cfg := &config.Config{}
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	ch := controller.NewCommandHandler(tm, cfg, nil, nil, nil)

	resp := ch.ProcessCommand("HELP")
	if !strings.Contains(resp, "Available commands") {
		t.Errorf("Expected HELP response from ProcessCommand, got: %s", resp)
	}

	resp = ch.ProcessCommand("LIST")
	if resp != "(no topics)" {
		t.Errorf("Expected empty list, got: %s", resp)
	}
}

func TestCommandHandler_PublishAcksZero(t *testing.T) {
	cfg := &config.Config{}
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	ch := controller.NewCommandHandler(tm, cfg, nil, nil, nil)
	ctx := controller.NewClientContext("test-group", 0)

	ch.HandleCommand("CREATE topic=ack0-topic partitions=1", ctx)

	resp := ch.HandleCommand("PUBLISH topic=ack0-topic acks=0 producerId=p1 message=fire-and-forget", ctx)
	if resp != "OK" {
		t.Errorf("Expected 'OK' for acks=0, got: %s", resp)
	}
}

func TestCommandHandler_PublishErrors(t *testing.T) {
	cfg := &config.Config{}
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	ch := controller.NewCommandHandler(tm, cfg, nil, nil, nil)
	ctx := controller.NewClientContext("test-group", 0)

	// Missing topic
	resp := ch.HandleCommand("PUBLISH acks=1 producerId=p1 message=hello", ctx)
	if !strings.Contains(resp, "ERROR") {
		t.Errorf("Expected error for missing topic, got: %s", resp)
	}

	// Missing message
	resp = ch.HandleCommand("PUBLISH topic=t1 acks=1 producerId=p1", ctx)
	if !strings.Contains(resp, "ERROR") {
		t.Errorf("Expected error for missing message, got: %s", resp)
	}

	// Missing producerId
	resp = ch.HandleCommand("PUBLISH topic=t1 acks=1 message=hello", ctx)
	if !strings.Contains(resp, "ERROR") {
		t.Errorf("Expected error for missing producerId, got: %s", resp)
	}

	// Invalid acks
	ch.HandleCommand("CREATE topic=t1 partitions=1", ctx)
	resp = ch.HandleCommand("PUBLISH topic=t1 acks=2 producerId=p1 message=hello", ctx)
	if !strings.Contains(resp, "ERROR") {
		t.Errorf("Expected error for invalid acks, got: %s", resp)
	}

	// Nonexistent topic
	resp = ch.HandleCommand("PUBLISH topic=nonexistent acks=1 producerId=p1 message=hello", ctx)
	if !strings.Contains(resp, "ERROR") {
		t.Errorf("Expected error for nonexistent topic, got: %s", resp)
	}
}

func TestCommandHandler_EventSourcingCommands(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{LogDir: dir}
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	ch := controller.NewCommandHandler(tm, cfg, nil, nil, nil)
	ctx := controller.NewClientContext("test-group", 0)

	// Create event-sourcing topic
	ch.HandleCommand("CREATE topic=es-cmd-orders partitions=2 event_sourcing=true", ctx)

	// APPEND_STREAM
	resp := ch.HandleCommand("APPEND_STREAM topic=es-cmd-orders key=cmd-order-1 version=1 event_type=OrderCreated message={\"id\":1}", ctx)
	if !strings.HasPrefix(resp, "OK") {
		t.Fatalf("Expected OK for APPEND_STREAM, got: %s", resp)
	}

	// STREAM_VERSION
	resp = ch.HandleCommand("STREAM_VERSION topic=es-cmd-orders key=cmd-order-1", ctx)
	if resp != "1" {
		t.Errorf("Expected version 1, got: %s", resp)
	}

	// SAVE_SNAPSHOT
	resp = ch.HandleCommand("SAVE_SNAPSHOT topic=es-cmd-orders key=cmd-order-1 version=1 message={\"state\":\"created\"}", ctx)
	if !strings.HasPrefix(resp, "OK") {
		t.Errorf("Expected OK for SAVE_SNAPSHOT, got: %s", resp)
	}

	// READ_SNAPSHOT
	resp = ch.HandleCommand("READ_SNAPSHOT topic=es-cmd-orders key=cmd-order-1", ctx)
	if !strings.Contains(resp, "\"version\":1") {
		t.Errorf("Expected snapshot with version 1, got: %s", resp)
	}

	// Version conflict
	resp = ch.HandleCommand("APPEND_STREAM topic=es-cmd-orders key=cmd-order-1 version=1 event_type=Dup message=fail", ctx)
	if !strings.Contains(resp, "version_conflict") {
		t.Errorf("Expected version conflict, got: %s", resp)
	}
}

func TestCommandHandler_CaseInsensitivity(t *testing.T) {
	cfg := &config.Config{}
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	ch := controller.NewCommandHandler(tm, cfg, nil, nil, nil)
	ctx := controller.NewClientContext("test-group", 0)

	cmds := []struct {
		cmd    string
		expect string
	}{
		{"create topic=T1", "Topic 'T1' now has 4 partitions"},
		{"list", "T1"},
		{"describe topic=T1", "partitions"},
		{"delete topic=T1", "deleted"},
	}

	for _, tc := range cmds {
		resp := ch.HandleCommand(tc.cmd, ctx)
		if !strings.Contains(strings.ToLower(resp), strings.ToLower(tc.expect)) {
			t.Errorf("Command '%s' failed. Expected to contain '%s', got '%s'", tc.cmd, tc.expect, resp)
		}
	}
}
