package sdk

import "testing"

func TestOpenWireConnectionRequiresConnection(t *testing.T) {
	if _, err := openWireConnection(nil, 0, "none"); err == nil {
		t.Fatal("nil Wire v2 connection was accepted")
	}
}
