package wire

import (
	"fmt"
	"strings"
)

type Command uint16

const (
	CommandUnknown Command = iota
	CommandAuth
	CommandCreate
	CommandAlterTopicConfig
	CommandDelete
	CommandTruncate
	CommandList
	CommandListCluster
	CommandClusterStatus
	CommandElectLeader
	CommandPublish
	CommandConsume
	CommandStream
	CommandHelp
	CommandHeartbeat
	CommandJoinGroup
	CommandSyncGroup
	CommandLeaveGroup
	CommandCommitOffset
	CommandBatchCommit
	CommandRegisterGroup
	CommandGroupStatus
	CommandFetchOffset
	CommandListGroups
	CommandListOffsets
	CommandDescribe
	CommandInitProducerID
	CommandBeginTxn
	CommandTxnPublish
	CommandSendOffsetsToTxn
	CommandEndTxn
	CommandTxnStatus
	CommandAppendStream
	CommandReadStream
	CommandSaveSnapshot
	CommandReadSnapshot
	CommandStreamVersion
	CommandReplicateMessage
	CommandReplicateSnapshot
	CommandListSnapshots
	CommandFetchSnapshot
	CommandCatchupSnapshots
	CommandFindCoordinator
	CommandRaftApply
	CommandMetadata
	CommandInternalBatch
	CommandProtocolInfo
	CommandNegotiate
	CommandExit
)

var commandNames = [...]string{
	CommandUnknown:           "UNKNOWN",
	CommandAuth:              "AUTH",
	CommandCreate:            "CREATE",
	CommandAlterTopicConfig:  "ALTER_TOPIC_CONFIG",
	CommandDelete:            "DELETE",
	CommandTruncate:          "TRUNCATE",
	CommandList:              "LIST",
	CommandListCluster:       "LIST_CLUSTER",
	CommandClusterStatus:     "CLUSTER_STATUS",
	CommandElectLeader:       "ELECT_LEADER",
	CommandPublish:           "PUBLISH",
	CommandConsume:           "CONSUME",
	CommandStream:            "STREAM",
	CommandHelp:              "HELP",
	CommandHeartbeat:         "HEARTBEAT",
	CommandJoinGroup:         "JOIN_GROUP",
	CommandSyncGroup:         "SYNC_GROUP",
	CommandLeaveGroup:        "LEAVE_GROUP",
	CommandCommitOffset:      "COMMIT_OFFSET",
	CommandBatchCommit:       "BATCH_COMMIT",
	CommandRegisterGroup:     "REGISTER_GROUP",
	CommandGroupStatus:       "GROUP_STATUS",
	CommandFetchOffset:       "FETCH_OFFSET",
	CommandListGroups:        "LIST_GROUPS",
	CommandListOffsets:       "LIST_OFFSETS",
	CommandDescribe:          "DESCRIBE",
	CommandInitProducerID:    "INIT_PRODUCER_ID",
	CommandBeginTxn:          "BEGIN_TXN",
	CommandTxnPublish:        "TXN_PUBLISH",
	CommandSendOffsetsToTxn:  "SEND_OFFSETS_TO_TXN",
	CommandEndTxn:            "END_TXN",
	CommandTxnStatus:         "TXN_STATUS",
	CommandAppendStream:      "APPEND_STREAM",
	CommandReadStream:        "READ_STREAM",
	CommandSaveSnapshot:      "SAVE_SNAPSHOT",
	CommandReadSnapshot:      "READ_SNAPSHOT",
	CommandStreamVersion:     "STREAM_VERSION",
	CommandReplicateMessage:  "REPLICATE_MESSAGE",
	CommandReplicateSnapshot: "REPLICATE_SNAPSHOT",
	CommandListSnapshots:     "LIST_SNAPSHOTS",
	CommandFetchSnapshot:     "FETCH_SNAPSHOT",
	CommandCatchupSnapshots:  "CATCHUP_SNAPSHOTS",
	CommandFindCoordinator:   "FIND_COORDINATOR",
	CommandRaftApply:         "RAFT_APPLY",
	CommandMetadata:          "METADATA",
	CommandInternalBatch:     "INTERNAL_BATCH",
	CommandProtocolInfo:      "PROTOCOL_INFO",
	CommandNegotiate:         "NEGOTIATE",
	CommandExit:              "EXIT",
}

var commandsByName = func() map[string]Command {
	result := make(map[string]Command, len(commandNames)-1)
	for command := CommandAuth; int(command) < len(commandNames); command++ {
		result[commandNames[command]] = command
	}
	return result
}()

func ParseCommand(name string) (Command, error) {
	command, ok := commandsByName[strings.ToUpper(strings.TrimSpace(name))]
	if !ok {
		return CommandUnknown, fmt.Errorf("unknown Wire v2 command %q", name)
	}
	return command, nil
}

func Commands() []Command {
	result := make([]Command, 0, len(commandNames)-1)
	for command := CommandAuth; int(command) < len(commandNames); command++ {
		result = append(result, command)
	}
	return result
}

func (c Command) String() string {
	if int(c) < len(commandNames) && c > CommandUnknown {
		return commandNames[c]
	}
	return fmt.Sprintf("UNKNOWN(%d)", c)
}

func (c Command) valid() bool {
	return c > CommandUnknown && int(c) < len(commandNames)
}
