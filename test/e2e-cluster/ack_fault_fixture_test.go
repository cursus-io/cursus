package e2e_cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

const ackFaultProject = "cursus-ack-faults"
const ackFaultPortShift = 23000

type ackFaultJournal struct {
	t        *testing.T
	file     *os.File
	attempts []writeAttempt
}

func newAckFaultJournal(t *testing.T) *ackFaultJournal {
	t.Helper()
	dir := os.Getenv("CURSUS_ACK_FAULT_RESULTS")
	if dir == "" {
		dir = t.TempDir()
	}
	require.NoError(t, os.MkdirAll(dir, 0o750))
	file, err := os.OpenFile(filepath.Join(dir, t.Name()+".jsonl"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	return &ackFaultJournal{t: t, file: file}
}

func (j *ackFaultJournal) record(kind string, value any) {
	j.t.Helper()
	event := struct {
		At    time.Time `json:"at"`
		Kind  string    `json:"kind"`
		Value any       `json:"value"`
	}{time.Now().UTC(), kind, value}
	data, err := json.Marshal(event)
	require.NoError(j.t, err)
	_, err = j.file.Write(append(data, '\n'))
	require.NoError(j.t, err)
	require.NoError(j.t, j.file.Sync())
	j.t.Log(string(data))
}

// Exactly one attempt. In particular, no BrokerClient retries can conceal the
// lost-response window or route a direct stale-leader probe to another broker.
func (j *ackFaultJournal) publish(addr, topic string, message types.Message) (writeAttempt, bool) {
	j.t.Helper()
	attempt := writeAttempt{ID: writeIdentity(message), Target: addr, Message: message,
		PayloadHash: writePayloadHash(message.Payload), Outcome: "unknown"}
	j.record("invoke", attempt)
	command := fmt.Sprintf("PUBLISH topic=%s partition=0 acks=all producerId=%s epoch=%d seqNum=%d isIdempotent=true message=%s",
		topic, message.ProducerID, message.Epoch, message.SeqNum, message.Payload)
	response, sent, err := ackFaultRoundTrip(addr, command)
	if err != nil {
		attempt.Error = err.Error()
		var brokerError *wire.BrokerError
		if errors.As(err, &brokerError) {
			attempt.Outcome = "broker_error"
		}
	} else {
		var ack types.AckResponse
		require.NoError(j.t, json.Unmarshal(response, &ack), "%s", response)
		require.Equal(j.t, "OK", ack.Status, "%s", response)
		attempt.Outcome = "acknowledged"
		attempt.AckOffset = ack.LastOffset
	}
	j.attempts = append(j.attempts, attempt)
	j.record("complete", attempt)
	return attempt, sent
}

func ackFaultRoundTrip(addr, command string) ([]byte, bool, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return nil, false, err
	}
	client, err := wire.NewClientConn(conn, "none")
	if err != nil {
		return nil, false, err
	}
	if err := client.Send([]byte(command)); err != nil {
		return nil, false, err
	}
	response, err := client.Receive()
	return response, true, err
}

func newAckFaultCluster(t *testing.T, topic string) *ClusterTestContext {
	t.Helper()
	require.Empty(t, os.Getenv("CURSUS_CLEANUP_PORT_OFFSET"), "cleanup fixture overrides cannot be combined")
	t.Setenv("CURSUS_E2E_PORT_SHIFT", strconv.Itoa(ackFaultPortShift))
	root, err := filepath.Abs("../..")
	require.NoError(t, err)
	config, err := filepath.Abs("../cluster/config.yaml")
	require.NoError(t, err)
	data, err := os.ReadFile(composeFile)
	require.NoError(t, err)
	text := strings.ReplaceAll(string(data), "context: ../..", "context: "+filepath.ToSlash(root))
	// As in test/cluster, only broker 1 initializes the static voter set.
	// Bootstrapping all three races with receiving the first Raft log entry.
	text = strings.ReplaceAll(text, "- BOOTSTRAP_CLUSTER=true", "- BOOTSTRAP_CLUSTER=false")
	text = strings.Replace(text, "- BOOTSTRAP_CLUSTER=false", "- BOOTSTRAP_CLUSTER=true", 1)
	text = strings.ReplaceAll(text, "../cluster/config.yaml", filepath.ToSlash(config))
	text = strings.ReplaceAll(text, "dockerfile: Dockerfile", "dockerfile: test/e2e-cluster/Dockerfile.ack-faults")
	text = strings.ReplaceAll(text, "name: cluster_network", "name: "+ackFaultProject)
	for node := 1; node <= 3; node++ {
		text = strings.ReplaceAll(text, fmt.Sprintf("container_name: broker-%d", node),
			"container_name: "+ackFaultContainer(node)+"\n    cap_add: [NET_ADMIN]")
		text = strings.ReplaceAll(text, fmt.Sprintf("ADVERTISED_BROKER_PORT=%d", 9000+node),
			fmt.Sprintf("ADVERTISED_BROKER_PORT=%d", brokerPort(node)))
		for _, port := range []int{9000 + node, 7999 + node, 9080 + node, 9100 + node} {
			text = strings.ReplaceAll(text, fmt.Sprintf(`"%d:`, port), fmt.Sprintf(`"127.0.0.1:%d:`, port+ackFaultPortShift))
		}
		text = strings.ReplaceAll(text, fmt.Sprintf("127.0.0.1:%d:", 19000+node), fmt.Sprintf("127.0.0.1:%d:", 19000+node+ackFaultPortShift))
	}
	compose := filepath.Join(t.TempDir(), "compose.yml")
	require.NoError(t, os.WriteFile(compose, []byte(text), 0o600))
	ctx := givenClusterRestart(t, ackFaultProject, compose).WithTopic(topic).WithPartitions(1).WithIdempotent(true).WithAcks("all")
	t.Cleanup(ctx.Cleanup)
	ctx.WhenCluster().CreateTopic().WaitForTopicMetadata()
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "ack fault fixture")
	return ctx
}

func ackFaultContainer(node int) string {
	return fmt.Sprintf("%s-broker-%d", ackFaultProject, node)
}

func ackFaultDocker(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	require.NoError(t, err, "docker %v: %s", args, output)
	return strings.TrimSpace(string(output))
}

// IP rules affect only this container's network namespace, in both directions.
// Host clients retain their TCP connection to :9000. Existing connections between
// brokers are covered as well as new Raft, discovery and replication connections.
func isolateAckFaultLeader(t *testing.T, leader int, journal *ackFaultJournal) func() {
	t.Helper()
	container := ackFaultContainer(leader)
	var peers []string
	for node := 1; node <= 3; node++ {
		if node == leader {
			continue
		}
		ip := ackFaultDocker(t, "inspect", "--format", "{{(index .NetworkSettings.Networks \""+ackFaultProject+"\").IPAddress}}", ackFaultContainer(node))
		require.NotNil(t, net.ParseIP(ip), "invalid peer address")
		peers = append(peers, ip)
	}
	// Cleanup is registered before installing rules so partial setup also heals.
	healed := false
	heal := func() {
		if healed {
			return
		}
		for _, ip := range peers {
			for _, rule := range [][]string{{"INPUT", "-s"}, {"OUTPUT", "-d"}} {
				args := []string{"exec", "-u", "0", container, "iptables", "-D", rule[0], rule[1], ip, "-j", "DROP"}
				cmdCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				output, err := exec.CommandContext(cmdCtx, "docker", args...).CombinedOutput()
				cancel()
				if err != nil {
					t.Logf("firewall cleanup: %v: %s", err, output)
				}
			}
		}
		healed = true
		journal.record("network_healed", container)
	}
	t.Cleanup(heal)
	for _, ip := range peers {
		for _, rule := range [][]string{{"INPUT", "-s"}, {"OUTPUT", "-d"}} {
			ackFaultDocker(t, "exec", "-u", "0", container, "iptables", "-I", rule[0], "1", rule[1], ip, "-j", "DROP")
			ackFaultDocker(t, "exec", "-u", "0", container, "iptables", "-C", rule[0], rule[1], ip, "-j", "DROP")
		}
	}
	journal.record("network_isolated", map[string]any{"container": container, "peers": peers})
	return heal
}

func dropAckFaultReply(t *testing.T, upstream string) (string, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan error, 1)
	go func() {
		done <- forwardAndDropPublishReply(listener, upstream)
		_ = listener.Close()
	}()
	return listener.Addr().String(), done
}

func forwardAndDropPublishReply(listener net.Listener, upstream string) error {
	client, err := listener.Accept()
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	if err := client.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		return err
	}
	down, err := wire.ServerHandshake(client, []wire.Compression{wire.CompressionNone})
	if err != nil {
		return err
	}
	broker, err := net.DialTimeout("tcp", upstream, 3*time.Second)
	if err != nil {
		return err
	}
	defer func() { _ = broker.Close() }()
	if err := broker.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return err
	}
	up, err := wire.ClientHandshake(broker, []wire.Compression{wire.CompressionNone})
	if err != nil {
		return err
	}
	request, err := down.ReadFrame()
	if err != nil {
		return err
	}
	if request.Kind != wire.KindRequest || request.Command != wire.CommandPublish {
		return fmt.Errorf("expected publish request, got %v", request)
	}
	if err := up.WriteFrame(request); err != nil {
		return err
	}
	response, err := up.ReadFrame()
	if err != nil {
		return err
	}
	if response.Kind != wire.KindResponse || response.Command != request.Command || response.RequestID != request.RequestID || response.Status != wire.StatusOK {
		return fmt.Errorf("publish did not return correlated success: %v", response)
	}
	var ack types.AckResponse
	if err := json.Unmarshal(response.Payload, &ack); err != nil {
		return err
	}
	if ack.Status != "OK" {
		return fmt.Errorf("publish was not acknowledged: %s", response.Payload)
	}
	// Close the downstream without writing any response. The caller must retain
	// an unknown outcome even though the fault controller knows it committed.
	return nil
}
