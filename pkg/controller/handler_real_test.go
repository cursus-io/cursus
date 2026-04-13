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
func (m *mockStorage) GetAbsoluteOffset() uint64        { return 0 }
func (m *mockStorage) GetLatestOffset() uint64          { return 0 }
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

	_ = tm.CreateTopic("topic1", 1, false)

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
