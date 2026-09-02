package metrics

import (
	"time"

	"github.com/cursus-io/cursus/pkg/protocol"
)

// RecordCommand records a bounded command name and its wire response.
func RecordCommand(command, response string, elapsed time.Duration) {
	result := "ok"
	if protocol.IsErrorResponse(response) {
		result = "error"
		CommandErrors.WithLabelValues(command, errorCode(response)).Inc()
	}
	CommandsTotal.WithLabelValues(command, result).Inc()
	CommandDuration.WithLabelValues(command).Observe(elapsed.Seconds())
}

func errorCode(response string) string {
	code, ok := protocol.ErrorCode(response)
	if !ok {
		return "unknown"
	}
	return code
}
