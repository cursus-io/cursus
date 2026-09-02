package main

import (
	"bytes"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
)

func TestRunExecutesWireV2Command(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		server, err := wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone})
		if err != nil {
			serverDone <- err
			return
		}
		frame, err := server.ReadFrame()
		if err != nil {
			serverDone <- err
			return
		}
		if frame.Command != wire.CommandList || frame.Kind != wire.KindRequest {
			serverDone <- &unexpectedFrameError{}
			return
		}
		serverDone <- server.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: wire.CommandList, Status: wire.StatusOK, RequestID: frame.RequestID, Payload: []byte("OK topics=0")})
	}()

	var stdout, stderr bytes.Buffer
	code := run([]string{"--broker", listener.Addr().String(), "--timeout", "2s", "LIST"}, func(string) string { return "" }, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run exit=%d stderr=%s", code, stderr.String())
	}
	if got := stdout.String(); got != "OK topics=0\n" {
		t.Fatalf("stdout=%q", got)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not complete")
	}
}

func TestRunRejectsIncompleteAuthenticationFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"--broker", "127.0.0.1:9000", "--principal", "operator", "LIST"}, func(string) string { return "" }, &stdout, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "must be provided together") {
		t.Fatalf("exit=%d stderr=%q", code, stderr.String())
	}
}

type unexpectedFrameError struct{}

func (*unexpectedFrameError) Error() string { return "unexpected Wire v2 frame" }
