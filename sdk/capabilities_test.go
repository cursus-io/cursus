package sdk

import (
	"errors"
	"net"
	"reflect"
	"testing"

	"github.com/cursus-io/cursus/util"
)

func TestFetchProtocolInfo(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	serverDone := make(chan error, 1)
	go func() {
		data, err := ReadWithLength(server)
		if err != nil {
			serverDone <- err
			return
		}
		_, command, err := util.DecodeMessage(data)
		if err != nil {
			serverDone <- err
			return
		}
		if command != "PROTOCOL_INFO" {
			serverDone <- errors.New("unexpected command: " + command)
			return
		}
		serverDone <- WriteWithLength(server, []byte("OK protocol=cursus min_version=1 max_version=2 default_version=2 features=a,b error_classes=routing,validation"))
	}()

	info, err := FetchProtocolInfo(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	if info.Protocol != "cursus" || info.MinimumVersion != 1 || info.MaximumVersion != 2 || info.DefaultVersion != 2 {
		t.Fatalf("unexpected protocol info: %+v", info)
	}
	if !reflect.DeepEqual(info.Features, []string{"a", "b"}) {
		t.Fatalf("features = %v", info.Features)
	}
}

func TestNegotiateProtocolSortsFeaturesAndParsesResult(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	serverDone := make(chan error, 1)
	go func() {
		data, err := ReadWithLength(server)
		if err != nil {
			serverDone <- err
			return
		}
		_, command, err := util.DecodeMessage(data)
		if err != nil {
			serverDone <- err
			return
		}
		want := "NEGOTIATE version=1 features=offset_resume_v1,structured_errors_v1 require_features=false"
		if command != want {
			serverDone <- errors.New("unexpected command: " + command)
			return
		}
		serverDone <- WriteWithLength(server, []byte("OK protocol_version=1 enabled=offset_resume_v1,structured_errors_v1 unsupported="))
	}()

	result, err := NegotiateProtocol(client, ProtocolNegotiation{
		Features: []string{"structured_errors_v1", "offset_resume_v1", "structured_errors_v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	if result.Version != 1 || !reflect.DeepEqual(result.Enabled, []string{"offset_resume_v1", "structured_errors_v1"}) {
		t.Fatalf("unexpected negotiation: %+v", result)
	}
}

func TestNegotiateProtocolReturnsTypedBrokerError(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = ReadWithLength(server)
		_ = WriteWithLength(server, []byte("ERROR: UNSUPPORTED_PROTOCOL_VERSION class=validation retryable=false requested=2 min=1 max=1"))
	}()

	_, err := NegotiateProtocol(client, ProtocolNegotiation{Version: 2})
	var brokerErr *BrokerError
	if !errors.As(err, &brokerErr) {
		t.Fatalf("expected BrokerError, got %T: %v", err, err)
	}
	if brokerErr.Code != "UNSUPPORTED_PROTOCOL_VERSION" || brokerErr.Class != ErrorClassValidation || brokerErr.Retryable {
		t.Fatalf("unexpected broker error: %+v", brokerErr)
	}
}

func TestNormalizeFeatureNamesRejectsCommandInjection(t *testing.T) {
	if _, err := normalizeFeatureNames([]string{"structured_errors_v1 require_features=false"}); err == nil {
		t.Fatal("invalid feature name was accepted")
	}
}
