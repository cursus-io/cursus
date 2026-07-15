package sdk

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/util"
)

func TestConfiguredNegotiationHasBoundedTimeout(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()
	release := make(chan struct{})
	requestRead := make(chan struct{})
	go func() {
		_, _ = ReadWithLength(server)
		close(requestRead)
		<-release
	}()

	start := time.Now()
	err := negotiateConfiguredProtocol(client, 1, []string{"structured_errors_v1"}, false, 25)
	close(release)
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("expected timeout, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("negotiation timeout was not bounded: %v", time.Since(start))
	}
	<-requestRead
}

func TestConfiguredNegotiationClearsDeadlineAfterSuccess(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()
	serverDone := make(chan error, 1)
	go func() {
		first, err := ReadWithLength(server)
		if err != nil {
			serverDone <- err
			return
		}
		_, command, err := util.DecodeMessage(first)
		if err != nil {
			serverDone <- err
			return
		}
		if !strings.HasPrefix(command, "NEGOTIATE ") {
			serverDone <- fmt.Errorf("unexpected command: %s", command)
			return
		}
		if err := WriteWithLength(server, []byte("OK protocol_version=1 enabled=structured_errors_v1 unsupported=")); err != nil {
			serverDone <- err
			return
		}
		second, err := ReadWithLength(server)
		if err != nil {
			serverDone <- err
			return
		}
		_, command, err = util.DecodeMessage(second)
		if err != nil {
			serverDone <- err
			return
		}
		if command != "PROTOCOL_INFO" {
			serverDone <- fmt.Errorf("unexpected command: %s", command)
			return
		}
		serverDone <- WriteWithLength(server, []byte("OK protocol=cursus min_version=1 max_version=1 default_version=1 features=structured_errors_v1 error_classes=validation"))
	}()

	if err := negotiateConfiguredProtocol(client, 1, []string{"structured_errors_v1"}, false, 25); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := FetchProtocolInfo(client); err != nil {
		t.Fatalf("cleared connection deadline was not reusable: %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestFetchProtocolInfoRejectsInvalidVersionRange(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()
	go func() {
		_, _ = ReadWithLength(server)
		_ = WriteWithLength(server, []byte("OK protocol=cursus min_version=2 max_version=1 default_version=1 features= error_classes="))
	}()
	if _, err := FetchProtocolInfo(client); err == nil {
		t.Fatal("invalid broker protocol range was accepted")
	}
}
