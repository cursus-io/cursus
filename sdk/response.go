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
