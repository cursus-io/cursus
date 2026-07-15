package sdk

import (
	"strings"
)

func parseOKFields(resp string) map[string]string {
	out := make(map[string]string)
	for _, field := range strings.Fields(resp) {
		if field == "OK" || !strings.Contains(field, "=") {
			continue
		}
		parts := strings.SplitN(field, "=", 2)
		out[parts[0]] = parts[1]
	}
	return out
}

func hasOKStatus(resp string) bool {
	fields := strings.Fields(strings.TrimSpace(resp))
	return len(fields) > 0 && fields[0] == "OK"
}
