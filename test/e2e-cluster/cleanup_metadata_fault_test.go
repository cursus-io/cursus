package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

func setCleanupReadFault(t *testing.T, nodes []int, enabled bool) {
	t.Helper()
	operation := "touch"
	if !enabled {
		operation = "rm"
	}
	for _, node := range nodes {
		output, err := exec.Command("docker", "exec", cleanupContainerName(node), operation, "/run/cursus-e2e-faults/consumer-cleanup-read-error").CombinedOutput()
		require.NoError(t, err, string(output))
	}
}

func requireCleanupReadFaultObserved(t *testing.T, addrs []string, nodes []int) {
	t.Helper()
	require.NoError(t, eventually(t, "metadata read fault holds local materialization", clusterReadyTimeout, func() (bool, string, error) {
		for i, addr := range addrs {
			client := e2e.NewBrokerClient([]string{addr})
			status, err := client.SendCommand("", "CLUSTER_STATUS", 5*time.Second)
			client.Close()
			var parsed struct {
				Pending int `json:"topic_materialization_pending"`
			}
			if err != nil || json.Unmarshal([]byte(strings.TrimPrefix(status, "OK cluster=")), &parsed) != nil || parsed.Pending == 0 {
				return false, fmt.Sprintf("%s: %s err=%v", addr, status, err), nil
			}
			logs, err := exec.Command("docker", "logs", "--tail", "100", cleanupContainerName(nodes[i])).CombinedOutput()
			if err != nil || !strings.Contains(string(logs), "injected consumer cleanup read failure") {
				return false, fmt.Sprintf("node %d has not observed the read fault: %v", nodes[i], err), nil
			}
		}
		return true, "every survivor reports pending cleanup with the injected read reason", nil
	}))
}

// Receive a real registration before truncate, but do not admit it until after
// leader failover and snapshot restore. It must become a new group lifecycle;
// the old truncate receipt must preserve its later offsets and membership.
func holdCleanupRegistration(t *testing.T, upstream string) func() {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	entered, release := make(chan struct{}), make(chan struct{})
	proxyDone := make(chan error, 1)
	go func() {
		proxyDone <- forwardDelayedRegistration(t.Context().Done(), listener, upstream, entered, release)
	}()
	clientDone := make(chan error, 1)
	go func() {
		client := e2e.NewBrokerClient([]string{listener.Addr().String()})
		defer client.Close()
		response, err := client.SendCommand("", "REGISTER_GROUP topic="+cleanupRecoveryTopic+" group="+cleanupRecoveryGroup, 10*time.Minute)
		if err == nil && !strings.HasPrefix(response, "OK") {
			err = fmt.Errorf("delayed registration failed: %s", response)
		}
		clientDone <- err
	}()
	select {
	case <-entered:
	case err := <-proxyDone:
		t.Fatalf("registration proxy failed before interception: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("registration was not intercepted")
	}
	return func() {
		close(release)
		require.NoError(t, <-proxyDone)
		require.NoError(t, <-clientDone)
	}
}

func forwardDelayedRegistration(cancel <-chan struct{}, listener net.Listener, upstream string, entered, release chan struct{}) error {
	client, err := listener.Accept()
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	_ = client.SetDeadline(time.Now().Add(10 * time.Minute))
	down, err := wire.ServerHandshake(client, []wire.Compression{wire.CompressionNone})
	if err != nil {
		return err
	}
	request, err := down.ReadFrame()
	if err != nil {
		return err
	}
	if request.Command != wire.CommandRegisterGroup {
		return fmt.Errorf("unexpected delayed command %s", request.Command)
	}
	close(entered)
	select {
	case <-release:
	case <-cancel:
		return fmt.Errorf("test ended before registration release")
	}
	broker, err := net.DialTimeout("tcp", upstream, 5*time.Second)
	if err != nil {
		return err
	}
	defer func() { _ = broker.Close() }()
	_ = broker.SetDeadline(time.Now().Add(30 * time.Second))
	up, err := wire.ClientHandshake(broker, []wire.Compression{wire.CompressionNone})
	if err != nil {
		return err
	}
	if err := up.WriteFrame(request); err != nil {
		return err
	}
	response, err := up.ReadFrame()
	if err != nil {
		return err
	}
	return down.WriteFrame(response)
}
