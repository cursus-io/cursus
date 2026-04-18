package controller

import (
	"encoding/json"
	"fmt"
	"strings"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/eventsource"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

const DefaultMaxPollRecords = 8192
const STREAM_DATA_SIGNAL = "STREAM_DATA"

type CommandHandler struct {
	TopicManager  *topic.TopicManager
	Config        *config.Config
	Coordinator   *coordinator.Coordinator
	StreamManager *stream.StreamManager
	Cluster       *clusterController.ClusterController
	ESHandler     *eventsource.Handler
	commands      []commandEntry
}

// commandEntry defines a single command routing rule.
type commandEntry struct {
	prefix  string
	exact   bool
	handler func(cmd string, ctx *ClientContext) string
}

type ConsumeArgs struct {
	Topic     string
	Partition int
	Offset    uint64
}

func NewCommandHandler(
	tm *topic.TopicManager,
	cfg *config.Config,
	cd *coordinator.Coordinator,
	sm *stream.StreamManager,
	cc *clusterController.ClusterController,
) *CommandHandler {
	ch := &CommandHandler{
		TopicManager:  tm,
		Config:        cfg,
		Coordinator:   cd,
		StreamManager: sm,
		Cluster:       cc,
		ESHandler:     eventsource.NewHandler(tm),
	}
	ch.commands = []commandEntry{
		{prefix: "HELP", exact: true, handler: func(cmd string, ctx *ClientContext) string { return ch.handleHelp() }},
		{prefix: "LIST_CLUSTER", exact: true, handler: func(cmd string, ctx *ClientContext) string { return ch.handleListCluster() }},
		{prefix: "LIST", exact: true, handler: func(cmd string, ctx *ClientContext) string { return ch.handleList() }},
		{prefix: "CREATE ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleCreate(cmd) }},
		{prefix: "DELETE ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleDelete(cmd) }},
		{prefix: "PUBLISH ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handlePublish(cmd) }},
		{prefix: "REGISTER_GROUP ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleRegisterGroup(cmd) }},
		{prefix: "JOIN_GROUP ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleJoinGroup(cmd, ctx) }},
		{prefix: "SYNC_GROUP ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleSyncGroup(cmd) }},
		{prefix: "LEAVE_GROUP ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleLeaveGroup(cmd) }},
		{prefix: "FETCH_OFFSET ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleFetchOffset(cmd) }},
		{prefix: "GROUP_STATUS ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleGroupStatus(cmd) }},
		{prefix: "DESCRIBE ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleDescribeTopic(cmd) }},
		{prefix: "HEARTBEAT ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleHeartbeat(cmd) }},
		{prefix: "COMMIT_OFFSET ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleCommitOffset(cmd) }},
		{prefix: "BATCH_COMMIT ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleBatchCommit(cmd) }},
		{prefix: "APPEND_STREAM ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.ESHandler.HandleAppendStream(cmd) }},
		{prefix: "STREAM_VERSION ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.ESHandler.HandleStreamVersion(cmd) }},
		{prefix: "SAVE_SNAPSHOT ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.ESHandler.HandleSaveSnapshot(cmd) }},
		{prefix: "READ_SNAPSHOT ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.ESHandler.HandleReadSnapshot(cmd) }},
		{prefix: "READ_STREAM ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return STREAM_DATA_SIGNAL }},
		{prefix: "REPLICATE_MESSAGE ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleReplicateMessage(cmd) }},
	}
	return ch
}

func (ch *CommandHandler) logCommandResult(cmd, response string) {
	status := "SUCCESS"
	if strings.HasPrefix(response, "ERROR:") {
		status = "FAILURE"
	}
	cleanResponse := strings.ReplaceAll(response, "\n", " ")
	util.Debug("status: '%s', command: '%s' to Response '%s'", status, cmd, cleanResponse)
}

// HandleCommand processes non-streaming commands and returns a signal for streaming commands.
func (ch *CommandHandler) HandleCommand(rawCmd string, ctx *ClientContext) string {
	cmd := strings.TrimSpace(rawCmd)
	if cmd == "" {
		return ch.fail(rawCmd, "ERROR: empty command")
	}

	upper := strings.ToUpper(cmd)

	if strings.HasPrefix(upper, "STREAM ") {
		return ch.validateStreamSyntax(cmd, rawCmd)
	}
	if strings.HasPrefix(upper, "CONSUME ") {
		return ch.validateConsumeSyntax(cmd, rawCmd)
	}

	resp := ch.handleCommandByType(cmd, upper, ctx)
	ch.logCommandResult(rawCmd, resp)
	return resp
}

// handleCommandByType dispatches to the matching command handler from the registry.
func (ch *CommandHandler) handleCommandByType(cmd, upper string, ctx *ClientContext) string {
	for _, entry := range ch.commands {
		if entry.exact {
			if strings.EqualFold(cmd, entry.prefix) {
				return entry.handler(cmd, ctx)
			}
		} else {
			if strings.HasPrefix(upper, entry.prefix) {
				return entry.handler(cmd, ctx)
			}
		}
	}
	return "ERROR: unknown command: " + cmd
}

func (ch *CommandHandler) fail(raw, msg string) string {
	ch.logCommandResult(raw, msg)
	return msg
}

func (ch *CommandHandler) errorResponse(msg string) string {
	errorResp := types.AckResponse{
		Status:   "ERROR",
		ErrorMsg: msg,
	}
	respBytes, err := json.Marshal(errorResp)
	if err != nil {
		return fmt.Sprintf("failed to marshal error resp: %v", err)
	}

	return string(respBytes)
}

func parseKeyValueArgs(argsStr string) map[string]string {
	result := make(map[string]string)

	messageIdx := strings.Index(argsStr, "message=")

	if messageIdx != -1 {
		beforeMessage := argsStr[:messageIdx]
		parts := strings.Fields(beforeMessage)
		for _, part := range parts {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				result[kv[0]] = kv[1]
			}
		}
		result["message"] = strings.TrimSpace(argsStr[messageIdx+8:])
	} else {
		parts := strings.Fields(argsStr)
		for _, part := range parts {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				result[kv[0]] = kv[1]
			}
		}
	}
	return result
}
