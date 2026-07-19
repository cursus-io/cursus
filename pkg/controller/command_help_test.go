package controller

import "testing"

func TestHelpDescriptorOrdersAndNamesAreUnique(t *testing.T) {
	ch, _ := newTestHandler(t)
	orders := make(map[int]string)
	names := make(map[string]int)

	check := func(entry commandHelpEntry) {
		if previous, exists := orders[entry.order]; exists {
			t.Errorf("HELP order %d is shared by %s and %s", entry.order, previous, entry.name)
		}
		if previous, exists := names[entry.name]; exists {
			t.Errorf("HELP command %s has orders %d and %d", entry.name, previous, entry.order)
		}
		orders[entry.order] = entry.name
		names[entry.name] = entry.order
	}

	for _, entry := range standaloneHelpEntries {
		check(entry)
	}
	for _, command := range ch.commands {
		if command.helpOrder > 0 {
			check(commandHelpEntry{name: command.name(), order: command.helpOrder})
		}
	}
}
