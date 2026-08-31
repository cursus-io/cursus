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
