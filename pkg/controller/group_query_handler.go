package controller

import (
	"fmt"
	"sort"
	"strings"
)

func (ch *CommandHandler) handleListGroups() string {
	if ch.Coordinator == nil {
		return "ERROR: coordinator_not_available"
	}
	groups := ch.Coordinator.ListGroups()
	sort.Strings(groups)
	return fmt.Sprintf("OK count=%d groups=%s", len(groups), strings.Join(groups, ","))
}
