package controller

import (
	"encoding/json"
	"strings"

	"github.com/cursus-io/cursus/pkg/eventsource"
	"github.com/cursus-io/cursus/pkg/types"
)

// lifecycleTopicName extracts the topic affected by a lifecycle-sensitive command.
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
		var payload struct {
			Topic string `json:"topic"`
		}
		if json.Unmarshal([]byte(commandPayload(input.Raw)), &payload) == nil {
			return payload.Topic
		}
	}
	return ""
}

// lifecycleOperationExclusive identifies commands that mutate topic existence or data.
func lifecycleOperationExclusive(input commandInput) bool {
	switch input.Name {
	case "DELETE", "TRUNCATE":
		return true
	case "RAFT_APPLY":
		switch strings.ToUpper(strings.TrimSpace(input.Args["type"])) {
		case "TOPIC_DELETE", "TOPIC_TRUNCATE":
			return true
		}
	}
	return false
}

func commandPayload(command string) string {
	payloadIndex := strings.Index(command, "payload=")
	if payloadIndex < 0 {
		return ""
	}
	return strings.TrimSpace(command[payloadIndex+len("payload="):])
}
