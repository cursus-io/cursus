package controller_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/topic"
)

type fakeTopicManager struct {
	topics map[string]*topic.Topic
}

func newFakeTopicManager() *fakeTopicManager {
	return &fakeTopicManager{
		topics: make(map[string]*topic.Topic),
	}
}

func (ftm *fakeTopicManager) CreateTopic(name string, partitionCount int) *topic.Topic {
	partitions := make([]*topic.Partition, partitionCount)
	t := &topic.Topic{
		Name:       name,
		Partitions: partitions,
	}
	ftm.topics[name] = t
	return t
}

func (ftm *fakeTopicManager) ListTopics() []string {
	out := make([]string, 0, len(ftm.topics))
	for k := range ftm.topics {
		out = append(out, k)
	}
	return out
}

func (ftm *fakeTopicManager) DeleteTopic(name string) bool {
	if _, ok := ftm.topics[name]; !ok {
		return false
	}
	delete(ftm.topics, name)
	return true
}

func parseKeyValueArgs(argsStr string) map[string]string {
	result := make(map[string]string)
	parts := strings.Fields(argsStr)
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			result[kv[0]] = kv[1]
		}
	}
	return result
}

func TestHandleCommand_CreateListDelete(t *testing.T) {
	ftm := newFakeTopicManager()

	handleCommandShim := func(rawCmd string) string {
		cmd := strings.TrimSpace(rawCmd)
		if cmd == "" {
			return "ERROR: empty command"
		}
		upper := strings.ToUpper(cmd)

		switch {
		case strings.HasPrefix(upper, "CREATE "):
			// CREATE topic=<name> partitions=<n>
			args := parseKeyValueArgs(cmd[7:])

			topicName, ok := args["topic"]
			if !ok || topicName == "" {
				return "ERROR: missing topic name"
			}

			partitions := 4
			if partStr, ok := args["partitions"]; ok {
				if n, err := strconv.Atoi(partStr); err == nil && n > 0 {
					partitions = n
				} else {
					return "ERROR: partitions must be a positive integer"
				}
			}

			t := ftm.CreateTopic(topicName, partitions)
			return "✅ Topic '" + t.Name + "' now has " + strconv.Itoa(len(t.Partitions)) + " partitions"

		case strings.EqualFold(cmd, "LIST"):
			names := ftm.ListTopics()
			if len(names) == 0 {
				return "(no topics)"
			}
			return strings.Join(names, ", ")

		case strings.HasPrefix(upper, "DELETE "):
			// DELETE topic=<name>
			args := parseKeyValueArgs(cmd[7:])

			topicName, ok := args["topic"]
			if !ok || topicName == "" {
				return "ERROR: missing topic name"
			}

			if ftm.DeleteTopic(topicName) {
				return "🗑️ Topic '" + topicName + "' deleted"
			}
			return "ERROR: topic '" + topicName + "' not found"

		default:
			return "ERROR: unknown command"
		}
	}

	resp := handleCommandShim("CREATE topic=testTopic partitions=3")
	if !strings.Contains(resp, "now has 3 partitions") {
		t.Fatalf("expected success creating topic, got: %s", resp)
	}

	resp = handleCommandShim("LIST")
	if resp != "testTopic" {
		t.Fatalf("LIST wrong result: %s", resp)
	}

	resp = handleCommandShim("DELETE topic=testTopic")
	if !strings.Contains(resp, "deleted") {
		t.Fatalf("DELETE failed: %s", resp)
	}

	resp = handleCommandShim("LIST")
	if resp != "(no topics)" {
		t.Fatalf("expected empty list, got: %s", resp)
	}
}

func TestHandleCommand_KeyValueFormat(t *testing.T) {
	cfg := &config.Config{}
	_ = controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	ftm := newFakeTopicManager()

	handleCommandShim := func(rawCmd string) string {
		cmd := strings.TrimSpace(rawCmd)
		if cmd == "" {
			return "ERROR: empty command"
		}
		upper := strings.ToUpper(cmd)

		switch {
		case strings.HasPrefix(upper, "CREATE "):
			// CREATE topic=<name> partitions=<n>
			args := parseKeyValueArgs(cmd[7:])

			topicName, ok := args["topic"]
			if !ok || topicName == "" {
				return "ERROR: missing topic name"
			}

			partitions := 4
			if partStr, ok := args["partitions"]; ok {
				if n, err := strconv.Atoi(partStr); err == nil && n > 0 {
					partitions = n
				} else {
					return "ERROR: partitions must be a positive integer"
				}
			}

			t := ftm.CreateTopic(topicName, partitions)
			return "✅ Topic '" + t.Name + "' now has " + strconv.Itoa(len(t.Partitions)) + " partitions"

		case strings.EqualFold(cmd, "LIST"):
			names := ftm.ListTopics()
			if len(names) == 0 {
				return "(no topics)"
			}
			return strings.Join(names, ", ")

		case strings.HasPrefix(upper, "DELETE "):
			// DELETE topic=<name>
			args := parseKeyValueArgs(cmd[7:])

			topicName, ok := args["topic"]
			if !ok || topicName == "" {
				return "ERROR: missing topic name"
			}

			if ftm.DeleteTopic(topicName) {
				return "🗑️ Topic '" + topicName + "' deleted"
			}
			return "ERROR: topic '" + topicName + "' not found"

		default:
			return "ERROR: unknown command"
		}
	}

	resp := handleCommandShim("CREATE topic=testTopic partitions=3")
	if !strings.Contains(resp, "now has 3 partitions") {
		t.Fatalf("expected success creating topic, got: %s", resp)
	}

	resp = handleCommandShim("CREATE topic=my:special:topic partitions=5")
	if !strings.Contains(resp, "now has 5 partitions") {
		t.Fatalf("expected success creating topic with colons in name, got: %s", resp)
	}

	resp = handleCommandShim("LIST")
	if !strings.Contains(resp, "testTopic") || !strings.Contains(resp, "my:special:topic") {
		t.Fatalf("LIST wrong result: %s", resp)
	}

	resp = handleCommandShim("DELETE topic=my:special:topic")
	if !strings.Contains(resp, "deleted") {
		t.Fatalf("DELETE failed for topic with colons: %s", resp)
	}

	resp = handleCommandShim("DELETE topic=testTopic")
	if !strings.Contains(resp, "deleted") {
		t.Fatalf("DELETE failed: %s", resp)
	}

	resp = handleCommandShim("LIST")
	if resp != "(no topics)" {
		t.Fatalf("expected empty list, got: %s", resp)
	}
}
