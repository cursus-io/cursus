package controller

import (
	"strings"
	"testing"
)

func TestListGroupsReturnsSortedGroups(t *testing.T) {
	ch, _, coordinator := newTestHandlerWithCoordinator(t)
	for _, group := range []string{"zeta", "alpha"} {
		if err := coordinator.RegisterGroup("orders", group, 1); err != nil {
			t.Fatal(err)
		}
	}
	if got := ch.HandleCommand("LIST_GROUPS", NewClientContext("", 0)); got != "OK count=2 groups=alpha,zeta" {
		t.Fatalf("unexpected LIST_GROUPS response: %q", got)
	}
}

func TestListGroupsCommandContract(t *testing.T) {
	ch, _ := newTestHandler(t)
	if got := ch.HandleCommand("list_groups", NewClientContext("", 0)); got != "ERROR: coordinator_not_available" {
		t.Fatalf("unexpected coordinator response: %q", got)
	}
	if got := ch.HandleCommand("LIST_GROUPS extra=x", NewClientContext("", 0)); !strings.Contains(got, "unknown_command") {
		t.Fatalf("LIST_GROUPS accepted extra arguments: %q", got)
	}
}
