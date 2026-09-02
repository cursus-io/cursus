package protocol

import (
	"strings"

	"github.com/cursus-io/cursus/pkg/wire"
)

// IsTextCommand reports whether input starts with a registered command token.
func IsTextCommand(value string) bool {
	trimmed := strings.TrimLeft(value, " \t\r\n")
	if trimmed == "" {
		return false
	}
	first := trimmed[0]
	if (first < 'A' || first > 'Z') && (first < 'a' || first > 'z') {
		return false
	}
	end := strings.IndexAny(trimmed, " \t\r\n")
	if end == -1 {
		end = len(trimmed)
	}
	command, err := wire.ParseCommand(trimmed[:end])
	if err != nil {
		return false
	}
	switch command {
	case wire.CommandJoinCluster, wire.CommandLeaveCluster, wire.CommandHeartbeatCluster:
		return false
	default:
		return true
	}
}
