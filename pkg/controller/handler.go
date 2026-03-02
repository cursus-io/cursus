package controller

import (
	"encoding/json"
	"fmt"
	"strings"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
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
	return &CommandHandler{
		TopicManager:  tm,
		Config:        cfg,
		Coordinator:   cd,
		StreamManager: sm,
		Cluster:       cc,
	}
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

// handleCommandByType delegates to specific command handlers
func (ch *CommandHandler) handleCommandByType(cmd, upper string, ctx *ClientContext) string {
	switch {
	case strings.EqualFold(cmd, "HELP"):
		return ch.handleHelp()
	case strings.HasPrefix(upper, "CREATE "):
		return ch.handleCreate(cmd)
	case strings.HasPrefix(upper, "DELETE "):
		return ch.handleDelete(cmd)
	case strings.EqualFold(cmd, "LIST"):
		return ch.handleList()
	case strings.HasPrefix(upper, "PUBLISH "):
		return ch.handlePublish(cmd)
	case strings.HasPrefix(upper, "REGISTER_GROUP "):
		return ch.handleRegisterGroup(cmd)
	case strings.HasPrefix(upper, "JOIN_GROUP "):
		return ch.handleJoinGroup(cmd, ctx)
	case strings.HasPrefix(upper, "SYNC_GROUP "):
		return ch.handleSyncGroup(cmd)
	case strings.HasPrefix(upper, "LEAVE_GROUP "):
		return ch.handleLeaveGroup(cmd)
	case strings.HasPrefix(upper, "FETCH_OFFSET "):
		return ch.handleFetchOffset(cmd)
	case strings.HasPrefix(upper, "GROUP_STATUS "):
		return ch.handleGroupStatus(cmd)
	case strings.HasPrefix(upper, "DESCRIBE "):
		return ch.handleDescribeTopic(cmd)
	case strings.HasPrefix(upper, "HEARTBEAT "):
		return ch.handleHeartbeat(cmd)
	case strings.HasPrefix(upper, "COMMIT_OFFSET "):
		return ch.handleCommitOffset(cmd)
	case strings.HasPrefix(upper, "BATCH_COMMIT "):
		return ch.handleBatchCommit(cmd)
	default:
		return "ERROR: unknown command: " + cmd
	}
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
