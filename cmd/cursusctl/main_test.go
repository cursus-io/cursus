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

func TestRunPrintsBrowseMessagesBatch(t *testing.T) {
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
		if frame.Command != wire.CommandBrowseMessages || frame.Kind != wire.KindRequest {
			serverDone <- &unexpectedFrameError{}
			return
		}
		if err := server.WriteFrame(wire.Frame{
			Kind: wire.KindResponse, Command: wire.CommandBrowseMessages, Status: wire.StatusOK,
			RequestID: frame.RequestID, Payload: []byte(`{"status":"OK","next_offset":1}`),
		}); err != nil {
			serverDone <- err
			return
		}
		batch, err := wire.EncodeBatch(wire.Batch{
			Topic: "orders", Partition: 0, Acks: "1",
			Messages: []wire.Message{{Topic: "orders", Partition: 0, Offset: 0, Payload: "durable-value"}},
		})
		if err != nil {
			serverDone <- err
			return
		}
		serverDone <- server.WriteFrame(wire.Frame{
			Kind: wire.KindResponse, Command: wire.CommandBrowseMessages, Status: wire.StatusOK,
			RequestID: frame.RequestID, Payload: batch,
		})
	}()

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"--broker", listener.Addr().String(), "--timeout", "2s",
		"BROWSE_MESSAGES", "topic=orders", "partition=0", "from_offset=0",
	}, func(string) string { return "" }, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run exit=%d stderr=%s", code, stderr.String())
	}
	if got := stdout.String(); !strings.Contains(got, `"next_offset":1`) || !strings.Contains(got, `"Payload":"durable-value"`) {
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

func TestRunRejectsIncompleteTLSClientIdentity(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"--broker", "127.0.0.1:9000", "--tls-cert", "client.crt", "LIST"}, func(string) string { return "" }, &stdout, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "--tls-cert and --tls-key must be provided together") {
		t.Fatalf("exit=%d stderr=%q", code, stderr.String())
	}
}

type unexpectedFrameError struct{}

func (*unexpectedFrameError) Error() string { return "unexpected Wire v2 frame" }
