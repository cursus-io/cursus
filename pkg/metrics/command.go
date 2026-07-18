package metrics

import (
	"strings"
	"time"
)

// RecordCommand records a bounded command name and its wire response.
func RecordCommand(command, response string, elapsed time.Duration) {
	result := "ok"
	if strings.HasPrefix(strings.TrimSpace(response), "ERROR:") {
		result = "error"
		CommandErrors.WithLabelValues(command, errorCode(response)).Inc()
	}
	CommandsTotal.WithLabelValues(command, result).Inc()
	CommandDuration.WithLabelValues(command).Observe(elapsed.Seconds())
}

func errorCode(response string) string {
	trimmed := strings.TrimSpace(response)
	trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "ERROR:"))
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return "unknown"
	}
	return fields[0]
}
