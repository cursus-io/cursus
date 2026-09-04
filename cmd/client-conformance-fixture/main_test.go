package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type writerFunc func([]byte) (int, error)

func (write writerFunc) Write(data []byte) (int, error) {
	return write(data)
}

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

func TestWriteFixturePropagatesWriteFailure(t *testing.T) {
	want := errors.New("write failed")
	err := writeFixture(writerFunc(func([]byte) (int, error) {
		return 0, want
	}), []byte("fixture"))
	if !errors.Is(err, want) {
		t.Fatalf("writeFixture error = %v, want %v", err, want)
	}
}

func TestWriteFixtureRejectsShortWrite(t *testing.T) {
	err := writeFixture(writerFunc(func(data []byte) (int, error) {
		return len(data) - 1, nil
	}), []byte("fixture"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeFixture error = %v, want %v", err, io.ErrShortWrite)
	}
}
