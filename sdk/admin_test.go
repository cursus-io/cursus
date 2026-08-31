package sdk

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildAdminCreateTopicCommandPreservesExplicitZeroFalseAndEmptyACL(t *testing.T) {
	partitions := 3
	replicationFactor := 3
	idempotent := false
	eventSourcing := false
	retentionHours := 0
	retentionBytes := int64(0)
	emptyACL := []string{}

	command, err := buildAdminCreateTopicCommand("orders", TopicDefinitionPatch{
		Partitions:        &partitions,
		ReplicationFactor: &replicationFactor,
		Idempotent:        &idempotent,
		EventSourcing:     &eventSourcing,
		RetentionHours:    &retentionHours,
		RetentionBytes:    &retentionBytes,
		ReadACL:           &emptyACL,
	})
	require.NoError(t, err)
	require.Equal(t,
		"CREATE topic=orders partitions=3 replication_factor=3 idempotent=false event_sourcing=false retention_hours=0 retention_bytes=0 read_acl=",
		command,
	)
}

func TestParseTopicDefinitionResponse(t *testing.T) {
	definition, err := parseTopicDefinitionResponse(
		"OK topic=orders partitions=3 revision=7 replication_factor=3 idempotent=true event_sourcing=false cleanup_policy=delete partitioner=round_robin auth_policy=acl read_acl=reader write_acl=writer retention_hours=24 retention_bytes=8192",
	)
	require.NoError(t, err)
	require.Equal(t, "orders", definition.Topic)
	require.Equal(t, uint64(7), definition.Revision)
	require.Equal(t, 3, definition.Partitions)
	require.Equal(t, 3, definition.ReplicationFactor)
	require.True(t, definition.Idempotent)
	require.False(t, definition.EventSourcing)
	require.Equal(t, []string{"reader"}, definition.ReadACL)
	require.Equal(t, []string{"writer"}, definition.WriteACL)
}

func TestBuildAndParseAdminDeleteTopic(t *testing.T) {
	command, err := buildAdminDeleteTopicCommand("orders", DeleteTopicOptions{IfExists: true})
	require.NoError(t, err)
	require.Equal(t, "DELETE topic=orders if_exists=true", command)

	result, err := parseDeleteTopicResponse("OK topic=orders deleted=false cleanup_pending=false")
	require.NoError(t, err)
	require.Equal(t, DeleteTopicResult{Topic: "orders", Deleted: false}, result)
}

func TestAdminClientRetriesRetryableFailureOnNextBroker(t *testing.T) {
	firstAddr, firstResult := startAdminTestServer(t, "ERROR: no_raft_leader class=availability retryable=true")
	secondAddr, secondResult := startAdminTestServer(t,
		"OK topic=orders partitions=3 revision=1 replication_factor=3 idempotent=false event_sourcing=false cleanup_policy=delete partitioner=hash_key auth_policy=open read_acl= write_acl= retention_hours=0 retention_bytes=0",
	)
	client, err := NewAdminClient(&AdminConfig{
		BrokerAddrs:      []string{firstAddr, secondAddr},
		MaxRetries:       1,
		RequestTimeoutMS: 1000,
	})
	require.NoError(t, err)
	partitions := 3
	definition, err := client.CreateTopicContext(context.Background(), "orders", TopicDefinitionPatch{Partitions: &partitions})
	require.NoError(t, err)
	require.Equal(t, "orders", definition.Topic)
	require.Equal(t, uint64(1), definition.Revision)
	require.Equal(t, "CREATE topic=orders partitions=3", receiveAdminTestCommand(t, firstResult))
	require.Equal(t, "CREATE topic=orders partitions=3", receiveAdminTestCommand(t, secondResult))
}

func TestAdminClientRetriesNegotiationTransportFailureOnNextBroker(t *testing.T) {
	firstAddr, firstResult := startAdminNegotiationTestServer(t, true)
	secondAddr, secondResult := startAdminNegotiationTestServer(t, false)
	client, err := NewAdminClient(&AdminConfig{
		BrokerAddrs:      []string{firstAddr, secondAddr},
		MaxRetries:       1,
		RequestTimeoutMS: 1000,
		ProtocolVersion:  1,
	})
	require.NoError(t, err)

	definition, err := client.CreateTopicContext(context.Background(), "orders", TopicDefinitionPatch{})
	require.NoError(t, err)
	require.Equal(t, "orders", definition.Topic)
	require.Equal(t, "NEGOTIATE version=1 features= require_features=false", receiveAdminTestCommand(t, firstResult))
	require.Equal(t, "CREATE topic=orders", receiveAdminTestCommand(t, secondResult))
}

func TestAdminClientRetriesAmbiguousDeleteOnlyWhenExplicitlyIdempotent(t *testing.T) {
	t.Run("legacy delete stops with unknown outcome", func(t *testing.T) {
		firstAddr, firstResult := startAdminCommandDropServer(t)
		secondAddr, secondResult := startAdminTestServer(t, "OK topic=orders deleted=true")
		client, err := NewAdminClient(&AdminConfig{
			BrokerAddrs: []string{firstAddr, secondAddr}, MaxRetries: 1, RequestTimeoutMS: 1000,
		})
		require.NoError(t, err)

		_, err = client.DeleteTopic("orders", DeleteTopicOptions{})
		require.ErrorContains(t, err, "outcome is unknown and was not retried")
		require.Equal(t, "DELETE topic=orders", receiveAdminTestCommand(t, firstResult))
		select {
		case result := <-secondResult:
			t.Fatalf("legacy delete unexpectedly retried on the second broker: %+v", result)
		case <-time.After(100 * time.Millisecond):
		}
	})

	t.Run("if exists delete retries unknown outcome", func(t *testing.T) {
		firstAddr, firstResult := startAdminCommandDropServer(t)
		secondAddr, secondResult := startAdminTestServer(t, "OK topic=orders deleted=false")
		client, err := NewAdminClient(&AdminConfig{
			BrokerAddrs: []string{firstAddr, secondAddr}, MaxRetries: 1, RequestTimeoutMS: 1000,
		})
		require.NoError(t, err)

		result, err := client.DeleteTopic("orders", DeleteTopicOptions{IfExists: true})
		require.NoError(t, err)
		require.Equal(t, DeleteTopicResult{Topic: "orders", Deleted: false}, result)
		require.Equal(t, "DELETE topic=orders if_exists=true", receiveAdminTestCommand(t, firstResult))
		require.Equal(t, "DELETE topic=orders if_exists=true", receiveAdminTestCommand(t, secondResult))
	})
}

type adminTestResult struct {
	command string
	err     error
}

func startAdminTestServer(t *testing.T, response string) (string, <-chan adminTestResult) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	result := make(chan adminTestResult, 1)
	go func() {
		defer close(result)
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			result <- adminTestResult{err: acceptErr}
			return
		}
		defer conn.Close()
		payload, readErr := ReadWithLength(conn)
		if readErr != nil {
			result <- adminTestResult{err: readErr}
			return
		}
		if len(payload) < 2 {
			result <- adminTestResult{err: fmt.Errorf("encoded command is too short")}
			return
		}
		topicLength := int(binary.BigEndian.Uint16(payload[:2]))
		if 2+topicLength > len(payload) {
			result <- adminTestResult{err: fmt.Errorf("encoded command topic length is invalid")}
			return
		}
		command := string(payload[2+topicLength:])
		if writeErr := WriteWithLength(conn, []byte(response)); writeErr != nil {
			result <- adminTestResult{err: writeErr}
			return
		}
		result <- adminTestResult{command: command}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Addr().String(), result
}

func receiveAdminTestCommand(t *testing.T, result <-chan adminTestResult) string {
	t.Helper()
	select {
	case received := <-result:
		require.NoError(t, received.err)
		return received.command
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for admin test server")
		return ""
	}
}

func startAdminNegotiationTestServer(t *testing.T, closeAfterNegotiation bool) (string, <-chan adminTestResult) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	result := make(chan adminTestResult, 1)
	go func() {
		defer close(result)
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			result <- adminTestResult{err: acceptErr}
			return
		}
		defer conn.Close()
		negotiation, readErr := readAdminTestCommand(conn)
		if readErr != nil {
			result <- adminTestResult{err: readErr}
			return
		}
		if closeAfterNegotiation {
			result <- adminTestResult{command: negotiation}
			return
		}
		if writeErr := WriteWithLength(conn, []byte("OK protocol_version=1 enabled= unsupported=")); writeErr != nil {
			result <- adminTestResult{err: writeErr}
			return
		}
		command, readErr := readAdminTestCommand(conn)
		if readErr != nil {
			result <- adminTestResult{err: readErr}
			return
		}
		if writeErr := WriteWithLength(conn, []byte("OK topic=orders partitions=4 revision=1 replication_factor=3 idempotent=false event_sourcing=false cleanup_policy=delete partitioner=hash_key auth_policy=open read_acl= write_acl= retention_hours=0 retention_bytes=0")); writeErr != nil {
			result <- adminTestResult{err: writeErr}
			return
		}
		result <- adminTestResult{command: command}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Addr().String(), result
}

func startAdminCommandDropServer(t *testing.T) (string, <-chan adminTestResult) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	result := make(chan adminTestResult, 1)
	go func() {
		defer close(result)
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			result <- adminTestResult{err: acceptErr}
			return
		}
		defer conn.Close()
		command, readErr := readAdminTestCommand(conn)
		result <- adminTestResult{command: command, err: readErr}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Addr().String(), result
}

func readAdminTestCommand(conn net.Conn) (string, error) {
	payload, err := ReadWithLength(conn)
	if err != nil {
		return "", err
	}
	if len(payload) < 2 {
		return "", fmt.Errorf("encoded command is too short")
	}
	topicLength := int(binary.BigEndian.Uint16(payload[:2]))
	if 2+topicLength > len(payload) {
		return "", fmt.Errorf("encoded command topic length is invalid")
	}
	return string(payload[2+topicLength:]), nil
}
