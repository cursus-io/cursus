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
