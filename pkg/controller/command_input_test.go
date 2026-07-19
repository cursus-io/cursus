package controller

import "testing"

func TestDecodeCommandInput(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		wantName string
		wantArgs map[string]string
	}{
		{name: "empty", raw: "  ", wantArgs: map[string]string{}},
		{name: "exact", raw: "list", wantName: "LIST", wantArgs: map[string]string{}},
		{name: "arguments", raw: "  metadata   topic=orders partition=1  ", wantName: "METADATA", wantArgs: map[string]string{"topic": "orders", "partition": "1"}},
		{name: "tab separator", raw: "METADATA\ttopic=orders", wantName: "METADATA", wantArgs: map[string]string{"topic": "orders"}},
		{name: "message", raw: "PUBLISH topic=orders message=hello world", wantName: "PUBLISH", wantArgs: map[string]string{"topic": "orders", "message": "hello world"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := decodeCommandInput(test.raw)
			if got.Name != test.wantName {
				t.Fatalf("name = %q, want %q", got.Name, test.wantName)
			}
			if len(got.Args) != len(test.wantArgs) {
				t.Fatalf("args = %#v, want %#v", got.Args, test.wantArgs)
			}
			for key, want := range test.wantArgs {
				if got.Args[key] != want {
					t.Errorf("arg %s = %q, want %q", key, got.Args[key], want)
				}
			}
		})
	}
}
