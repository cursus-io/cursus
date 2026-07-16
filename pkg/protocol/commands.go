package protocol

import "strings"

const maxTextCommandLength = len("SEND_OFFSETS_TO_TXN")

var textCommands = map[string]struct{}{
	"AUTH":                {},
	"CREATE":              {},
	"DELETE":              {},
	"LIST":                {},
	"LIST_CLUSTER":        {},
	"CLUSTER_STATUS":      {},
	"ELECT_LEADER":        {},
	"PUBLISH":             {},
	"CONSUME":             {},
	"STREAM":              {},
	"HELP":                {},
	"HEARTBEAT":           {},
	"JOIN_GROUP":          {},
	"LEAVE_GROUP":         {},
	"COMMIT_OFFSET":       {},
	"BATCH_COMMIT":        {},
	"REGISTER_GROUP":      {},
	"GROUP_STATUS":        {},
	"FETCH_OFFSET":        {},
	"LIST_GROUPS":         {},
	"LIST_OFFSETS":        {},
	"SYNC_GROUP":          {},
	"DESCRIBE":            {},
	"INIT_PRODUCER_ID":    {},
	"BEGIN_TXN":           {},
	"TXN_PUBLISH":         {},
	"SEND_OFFSETS_TO_TXN": {},
	"END_TXN":             {},
	"TXN_STATUS":          {},
	"APPEND_STREAM":       {},
	"READ_STREAM":         {},
	"SAVE_SNAPSHOT":       {},
	"READ_SNAPSHOT":       {},
	"STREAM_VERSION":      {},
	"REPLICATE_MESSAGE":   {},
	"REPLICATE_SNAPSHOT":  {},
	"LIST_SNAPSHOTS":      {},
	"FETCH_SNAPSHOT":      {},
	"CATCHUP_SNAPSHOTS":   {},
	"FIND_COORDINATOR":    {},
	"RAFT_APPLY":          {},
	"METADATA":            {},
	"INTERNAL_BATCH":      {},
	"PROTOCOL_INFO":       {},
	"NEGOTIATE":           {},
}

// IsTextCommand distinguishes a raw command from the legacy topic envelope.
// DecodeMessage alone is not sufficient because the first two ASCII bytes of
// a long command can also form a valid uint16 topic length.
func IsTextCommand(value string) bool {
	trimmed := strings.TrimLeft(value, " \t\r\n")
	if trimmed == "" {
		return false
	}
	first := trimmed[0]
	if (first < 'A' || first > 'Z') && (first < 'a' || first > 'z') {
		return false
	}
	searchEnd := len(trimmed)
	if searchEnd > maxTextCommandLength+1 {
		searchEnd = maxTextCommandLength + 1
	}
	end := strings.IndexAny(trimmed[:searchEnd], " \t\r\n")
	if end == -1 {
		if len(trimmed) > maxTextCommandLength {
			return false
		}
		end = len(trimmed)
	}
	_, ok := textCommands[strings.ToUpper(trimmed[:end])]
	return ok
}
