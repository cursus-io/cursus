package controller

import (
	"reflect"
	"testing"
)

func TestCommandDescriptorPermissions(t *testing.T) {
	ch, _ := newTestHandler(t)
	tests := []struct {
		command string
		want    []string
	}{
		{command: "CREATE topic=orders", want: []string{PermissionAdmin}},
		{command: "LIST", want: []string{PermissionTopicRead}},
		{command: "PUBLISH topic=orders message=value", want: []string{PermissionTopicWrite}},
		{command: "CONSUME topic=orders", want: []string{PermissionTopicRead, PermissionGroup}},
		{command: "STREAM topic=orders", want: []string{PermissionTopicRead, PermissionGroup}},
		{command: "READ_STREAM topic=orders", want: []string{PermissionTopicRead}},
		{command: "LIST_GROUPS", want: []string{PermissionGroup}},
		{command: "TXN_PUBLISH transactional_id=tx topic=orders", want: []string{PermissionTransaction, PermissionTopicWrite}},
		{command: "SEND_OFFSETS_TO_TXN transactional_id=tx", want: []string{PermissionTransaction, PermissionGroup}},
		{command: "FIND_COORDINATOR group=workers", want: []string{PermissionGroup}},
		{command: "FIND_COORDINATOR transactional_id=tx", want: []string{PermissionTransaction}},
		{command: "HELP"},
		{command: "UNKNOWN"},
	}
	for _, test := range tests {
		input := decodeCommandInput(test.command)
		if got := ch.commandPermissions(input); !reflect.DeepEqual(got, test.want) {
			t.Errorf("permissions for %q = %v, want %v", test.command, got, test.want)
		}
	}
}

func TestInternalCommandDescriptors(t *testing.T) {
	ch, _ := newTestHandler(t)
	want := map[string]bool{
		"REPLICATE_MESSAGE":  true,
		"REPLICATE_SNAPSHOT": true,
		"LIST_SNAPSHOTS":     true,
		"FETCH_SNAPSHOT":     true,
		"CATCHUP_SNAPSHOTS":  true,
		"RAFT_APPLY":         true,
	}
	got := make(map[string]bool)
	for _, entry := range ch.commands {
		if entry.internal {
			got[entry.name()] = true
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("internal command descriptors = %v, want %v", got, want)
	}
}

func TestCommandDescriptorVariantsShareMetadata(t *testing.T) {
	ch, _ := newTestHandler(t)
	type metadata struct {
		internal    bool
		permissions []string
	}
	seen := make(map[string]metadata)
	for _, entry := range ch.commands {
		current := metadata{internal: entry.internal, permissions: entry.permissions}
		previous, ok := seen[entry.name()]
		if !ok {
			seen[entry.name()] = current
			continue
		}
		if previous.internal != current.internal || !reflect.DeepEqual(previous.permissions, current.permissions) {
			t.Errorf("descriptor variants for %s have inconsistent metadata: first=%+v current=%+v", entry.name(), previous, current)
		}
	}
}
