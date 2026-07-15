package controller

import (
	"fmt"
	"strings"
	"sync"
	"time"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/eventsource"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
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
	TxnManager    *transaction.Manager
	commands      []commandEntry

	coordCache   map[string]coordCacheEntry
	coordCacheMu sync.RWMutex
	txnApplyMu   sync.Mutex
}

func transactionalIDExpiration(cfg *config.Config) time.Duration {
	if cfg == nil || cfg.TransactionalIDExpirationMS <= 0 {
		return 7 * 24 * time.Hour
	}
	return time.Duration(cfg.TransactionalIDExpirationMS) * time.Millisecond
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
		coordCache:    make(map[string]coordCacheEntry),
		Cluster:       cc,
		ESHandler:     eventsource.NewHandler(tm),
		TxnManager:    transaction.NewManagerWithExpiration(transactionalIDExpiration(cfg)),
	}
	if cc != nil && cc.RaftManager != nil {
		if fsm := cc.RaftManager.GetFSM(); fsm != nil {
			fsm.SetTransactionManager(ch.TxnManager)
		}
	}
	ch.commands = []commandEntry{
		{prefix: "AUTH ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleAuth(cmd, ctx) }},
		{prefix: "HELP", exact: true, handler: func(cmd string, ctx *ClientContext) string { return ch.handleHelp() }},
		{prefix: "LIST_CLUSTER", exact: true, handler: func(cmd string, ctx *ClientContext) string { return ch.handleListCluster() }},
		{prefix: "LIST", exact: true, handler: func(cmd string, ctx *ClientContext) string { return ch.handleList() }},
		{prefix: "CREATE ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleCreate(cmd) }},
		{prefix: "DELETE ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleDelete(cmd) }},
		{prefix: "PUBLISH ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handlePublish(cmd, ctx) }},
		{prefix: "REGISTER_GROUP ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleRegisterGroup(cmd) }},
		{prefix: "JOIN_GROUP ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleJoinGroup(cmd, ctx) }},
		{prefix: "SYNC_GROUP ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleSyncGroup(cmd) }},
		{prefix: "LEAVE_GROUP ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleLeaveGroup(cmd) }},
		{prefix: "FETCH_OFFSET ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleFetchOffset(cmd) }},
		{prefix: "LIST_OFFSETS", exact: true, handler: func(cmd string, ctx *ClientContext) string { return ch.handleListOffsets(cmd, ctx) }},
		{prefix: "LIST_OFFSETS ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleListOffsets(cmd, ctx) }},
		{prefix: "GROUP_STATUS ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleGroupStatus(cmd) }},
		{prefix: "DESCRIBE ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleDescribeTopic(cmd) }},
		{prefix: "HEARTBEAT ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleHeartbeat(cmd) }},
		{prefix: "COMMIT_OFFSET ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleCommitOffset(cmd) }},
		{prefix: "BATCH_COMMIT ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleBatchCommit(cmd) }},
		{prefix: "INIT_PRODUCER_ID ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleInitProducerID(cmd) }},
		{prefix: "BEGIN_TXN ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleBeginTxn(cmd) }},
		{prefix: "TXN_PUBLISH ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleTxnPublish(cmd, ctx) }},
		{prefix: "SEND_OFFSETS_TO_TXN ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleSendOffsetsToTxn(cmd) }},
		{prefix: "END_TXN ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleEndTxn(cmd) }},
		{prefix: "TXN_STATUS ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleTxnStatus(cmd) }},
		{prefix: "APPEND_STREAM ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleAppendStream(cmd) }},
		{prefix: "STREAM_VERSION ", exact: false, handler: func(cmd string, ctx *ClientContext) string {
			return ch.handleEventSourceRoutedCommand(cmd, "STREAM_VERSION ", ch.ESHandler.HandleStreamVersion)
		}},
		{prefix: "SAVE_SNAPSHOT ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleSaveSnapshot(cmd) }},
		{prefix: "READ_SNAPSHOT ", exact: false, handler: func(cmd string, ctx *ClientContext) string {
			return ch.handleEventSourceRoutedCommand(cmd, "READ_SNAPSHOT ", ch.ESHandler.HandleReadSnapshot)
		}},
		{prefix: "READ_STREAM ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return STREAM_DATA_SIGNAL }},
		{prefix: "METADATA ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleMetadata(cmd) }},
		{prefix: "FIND_COORDINATOR ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleFindCoordinator(cmd) }},
		{prefix: "REPLICATE_MESSAGE ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleReplicateMessage(cmd) }},
		{prefix: "REPLICATE_SNAPSHOT ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleReplicateSnapshot(cmd) }},
		{prefix: "LIST_SNAPSHOTS ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleListSnapshots(cmd) }},
		{prefix: "FETCH_SNAPSHOT ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleFetchSnapshot(cmd) }},
		{prefix: "CATCHUP_SNAPSHOTS ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleCatchupSnapshots(cmd) }},
		{prefix: "RAFT_APPLY ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleRaftApply(cmd) }},
	}
	return ch
}

var internalCommandPrefixes = []string{
	"REPLICATE_MESSAGE ",
	"REPLICATE_SNAPSHOT ",
	"LIST_SNAPSHOTS ",
	"FETCH_SNAPSHOT ",
	"CATCHUP_SNAPSHOTS ",
	"RAFT_APPLY ",
}

func (ch *CommandHandler) logCommandResult(cmd, response string) {
	status := "SUCCESS"
	if strings.HasPrefix(response, "ERROR:") {
		status = "FAILURE"
	}
	cleanCmd := redactCommandSecrets(cmd)
	cleanResponse := redactCommandSecrets(strings.ReplaceAll(response, "\n", " "))
	util.Debug("status: '%s', command: '%s' to Response '%s'", status, cleanCmd, cleanResponse)
}

func redactCommandSecrets(s string) string {
	keys := []string{"internal_token=", "auth_token=", "token="}
	for _, key := range keys {
		s = redactOneCommandSecret(s, key)
	}
	return s
}

func redactOneCommandSecret(s, key string) string {
	idx := strings.Index(s, key)
	if idx == -1 {
		return s
	}

	var b strings.Builder
	for idx != -1 {
		b.WriteString(s[:idx])
		b.WriteString(key)
		b.WriteString("<redacted>")
		rest := s[idx+len(key):]
		end := 0
		for end < len(rest) && rest[end] > ' ' {
			end++
		}
		s = rest[end:]
		idx = strings.Index(s, key)
	}
	b.WriteString(s)
	return b.String()
}

// HandleCommand processes non-streaming commands and returns a signal for streaming commands.
func (ch *CommandHandler) HandleCommand(rawCmd string, ctx *ClientContext) string {
	cmd := strings.TrimSpace(rawCmd)
	if cmd == "" {
		return ch.fail(rawCmd, "ERROR: empty_command")
	}

	upper := strings.ToUpper(cmd)

	if strings.HasPrefix(upper, "STREAM ") {
		return ch.validateStreamSyntax(cmd, rawCmd)
	}
	if strings.HasPrefix(upper, "CONSUME ") {
		return ch.validateConsumeSyntax(cmd, rawCmd)
	}
	if name, ok := internalCommandName(upper); ok {
		if resp := ch.authorizeInternalCommand(name, cmd); resp != "" {
			return ch.fail(rawCmd, resp)
		}
	}

	if resp := ch.authorizeClientCommand(cmd, upper, ctx); resp != "" {
		return ch.fail(rawCmd, resp)
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
	return fmt.Sprintf("ERROR: unknown_command command=%s", cmd)
}

func internalCommandName(upper string) (string, bool) {
	for _, prefix := range internalCommandPrefixes {
		if strings.HasPrefix(upper, prefix) {
			return strings.TrimSpace(prefix), true
		}
	}
	return "", false
}

func (ch *CommandHandler) authorizeInternalCommand(name, cmd string) string {
	if ch == nil || ch.Config == nil || !ch.Config.EnabledDistribution {
		return ""
	}
	token := ch.Config.InternalAuthToken
	if token == "" {
		return fmt.Sprintf("ERROR: internal_auth_not_configured command=%s", name)
	}
	args := parseKeyValueArgs(strings.TrimPrefix(cmd, name+" "))
	if !constantTimeStringEqual(args["internal_token"], token) {
		return fmt.Sprintf("ERROR: internal_command_unauthorized command=%s", name)
	}
	return ""
}

func (ch *CommandHandler) internalAuthPrefix() string {
	if ch != nil && ch.Config != nil && ch.Config.InternalAuthToken != "" {
		return "internal_token=" + ch.Config.InternalAuthToken + " "
	}
	return ""
}

func (ch *CommandHandler) fail(raw, msg string) string {
	ch.logCommandResult(raw, msg)
	return msg
}

func (ch *CommandHandler) errorResponse(msg string) string {
	return fmt.Sprintf("ERROR: broker_error reason=%q", msg)
}

// Close releases resources held by the command handler (e.g., event-sourcing indexes and snapshots).
func (ch *CommandHandler) Close() error {
	if ch.ESHandler != nil {
		return ch.ESHandler.Close()
	}
	return nil
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
