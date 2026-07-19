package controller

import (
	"fmt"
	"sort"
	"strings"
)

type commandHelpEntry struct {
	name  string
	order int
}

var standaloneHelpEntries = []commandHelpEntry{
	{name: "CONSUME", order: 5},
	{name: "STREAM", order: 6},
	{name: "EXIT", order: 38},
}

func (ch *CommandHandler) handleHelp() string {
	entries := append([]commandHelpEntry(nil), standaloneHelpEntries...)
	for _, command := range ch.commands {
		if command.helpOrder > 0 {
			entries = append(entries, commandHelpEntry{name: command.name(), order: command.helpOrder})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].order < entries[j].order })

	names := make([]string, len(entries))
	for i := range entries {
		names[i] = entries[i].name
	}
	return fmt.Sprintf("OK commands=%s", strings.Join(names, ","))
}
