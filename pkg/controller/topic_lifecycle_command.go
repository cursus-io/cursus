package controller

import (
	"encoding/json"
	"strings"

	"github.com/cursus-io/cursus/pkg/eventsource"
	"github.com/cursus-io/cursus/pkg/types"
)

func lifecycleTopicName(input commandInput) string {
	if topicName := input.Args["topic"]; topicName != "" {
		return topicName
	}
	switch input.Name {
	case "REPLICATE_MESSAGE":
		var command types.MessageCommand
		if json.Unmarshal([]byte(commandPayload(input.Raw)), &command) == nil {
			return command.Topic
		}
	case "REPLICATE_SNAPSHOT":
		var snapshot eventsource.SnapshotResult
		if json.Unmarshal([]byte(commandPayload(input.Raw)), &snapshot) == nil {
			return snapshot.Topic
		}
	case "RAFT_APPLY":
		var payload map[string]json.RawMessage
		if json.Unmarshal([]byte(commandPayload(input.Raw)), &payload) == nil {
			var topicName string
			if json.Unmarshal(payload["topic"], &topicName) == nil {
				return topicName
			}
		}
	}
	return ""
}

func commandPayload(command string) string {
	payloadIndex := strings.Index(command, "payload=")
	if payloadIndex < 0 {
		return ""
	}
	return strings.TrimSpace(command[payloadIndex+len("payload="):])
}
