package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestCommittedFixtureMatchesCanonicalGoCodec(t *testing.T) {
	want, err := buildFixture()
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join("..", "..", "testdata", "client-conformance", "wire-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("client conformance fixture is stale; regenerate it with go run ./cmd/client-conformance-fixture")
	}
}
