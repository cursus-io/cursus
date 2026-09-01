package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

const (
	brokerRestartChildEnv  = "CURSUS_BROKER_RESTART_E2E_CHILD"
	brokerRestartLogEnv    = "CURSUS_BROKER_RESTART_E2E_LOG_DIR"
	brokerRestartPortEnv   = "CURSUS_BROKER_RESTART_E2E_PORT"
	brokerRestartHealthEnv = "CURSUS_BROKER_RESTART_E2E_HEALTH_PORT"
)

var joinResponsePattern = regexp.MustCompile(`generation=([0-9]+) member=([^ ]+)`)

func TestStandaloneBrokerSamePVCRestartE2E(t *testing.T) {
	if os.Getenv(brokerRestartChildEnv) == "1" {
		runBrokerRestartChild(t)
		return
	}

	root := t.TempDir()
	firstBrokerPort, firstHealthPort := reserveBrokerTestPorts(t)
	first, firstOutput := startBrokerRestartChild(t, root, firstBrokerPort, firstHealthPort)
	waitForBrokerReady(t, firstHealthPort, firstOutput)

	require.True(t, strings.HasPrefix(brokerCommand(t, firstBrokerPort, "CREATE topic=events partitions=2"), "OK"))
	require.Equal(t, "OK group=workers topic=events registered=true", brokerCommand(t, firstBrokerPort, "REGISTER_GROUP topic=events group=workers"))
	require.Equal(t, "OK group=empty-workers topic=events registered=true", brokerCommand(t, firstBrokerPort, "REGISTER_GROUP topic=events group=empty-workers"))
	join := brokerCommand(t, firstBrokerPort, "JOIN_GROUP topic=events group=workers member=e2e")
	match := joinResponsePattern.FindStringSubmatch(join)
	require.Len(t, match, 3, join)
	generation, err := strconv.Atoi(match[1])
	require.NoError(t, err)
	member := match[2]
	commit := fmt.Sprintf("COMMIT_OFFSET topic=events partition=0 group=workers offset=37 member=%s generation=%d", member, generation)
	require.Equal(t, "OK", brokerCommand(t, firstBrokerPort, commit))

	require.NoError(t, first.Process.Kill())
	require.Error(t, first.Wait(), "first broker must stop without graceful shutdown")

	secondBrokerPort, secondHealthPort := reserveBrokerTestPorts(t)
	second, secondOutput := startBrokerRestartChild(t, root, secondBrokerPort, secondHealthPort)
	t.Cleanup(func() {
		if second.ProcessState == nil || !second.ProcessState.Exited() {
			_ = second.Process.Kill()
			_ = second.Wait()
		}
	})
	waitForBrokerReady(t, secondHealthPort, secondOutput)

	groups := brokerCommand(t, secondBrokerPort, "LIST_GROUPS")
	require.Contains(t, groups, "empty-workers")
	require.Contains(t, groups, "workers")
	status := brokerCommand(t, secondBrokerPort, "GROUP_STATUS group=workers")
	require.Contains(t, status, `"topic_name":"events"`)
	require.Contains(t, status, `"partition_count":2`)
	require.Equal(t, "OK offset=37", brokerCommand(t, secondBrokerPort, "FETCH_OFFSET topic=events partition=0 group=workers"))
	require.Equal(t, "OK offset=0", brokerCommand(t, secondBrokerPort, "FETCH_OFFSET topic=events partition=0 group=empty-workers"))
}

func runBrokerRestartChild(t *testing.T) {
	brokerPort, err := strconv.Atoi(os.Getenv(brokerRestartPortEnv))
	require.NoError(t, err)
	healthPort, err := strconv.Atoi(os.Getenv(brokerRestartHealthEnv))
	require.NoError(t, err)
	cfg := config.DefaultConfig()
	cfg.LogDir = os.Getenv(brokerRestartLogEnv)
	cfg.EnabledDistribution = false
	cfg.EnableExporter = false
	cfg.BrokerPort = brokerPort
	cfg.HealthCheckPort = healthPort
	require.NoError(t, runBroker(context.Background(), cfg))
}

func startBrokerRestartChild(t *testing.T, root string, brokerPort, healthPort int) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestStandaloneBrokerSamePVCRestartE2E$")
	command.Env = append(os.Environ(),
		brokerRestartChildEnv+"=1",
		brokerRestartLogEnv+"="+root,
		brokerRestartPortEnv+"="+strconv.Itoa(brokerPort),
		brokerRestartHealthEnv+"="+strconv.Itoa(healthPort),
		"WARGAME_BROKER_ALLOW_NEW_CONSUMER_GROUP_BOOTSTRAP=0",
	)
	output := &bytes.Buffer{}
	command.Stdout = output
	command.Stderr = output
	require.NoError(t, command.Start())
	t.Cleanup(func() {
		if command.ProcessState == nil || !command.ProcessState.Exited() {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	return command, output
}

func waitForBrokerReady(t *testing.T, healthPort int, output *bytes.Buffer) {
	t.Helper()
	url := fmt.Sprintf("http://127.0.0.1:%d/ready", healthPort)
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil) // #nosec G107 -- loopback test child.
		require.NoError(t, err)
		response, err := client.Do(request)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("broker did not become ready: %s", output.String())
}

func brokerCommand(t *testing.T, port int, command string) string {
	t.Helper()
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	connection, err := dialer.DialContext(t.Context(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	defer func() { _ = connection.Close() }()
	require.NoError(t, connection.SetDeadline(time.Now().Add(5*time.Second)))
	commandID, commandPayload, err := wire.ParseCommandText(command)
	require.NoError(t, err)
	payload, err := wire.EncodeCommandPayload(commandPayload)
	require.NoError(t, err)
	framed, err := wire.ClientHandshake(connection, []wire.Compression{wire.CompressionNone})
	require.NoError(t, err)
	require.NoError(t, framed.WriteFrame(wire.Frame{
		Kind: wire.KindRequest, Command: commandID, RequestID: 1, Payload: payload,
	}))
	response, err := framed.ReadFrame()
	require.NoError(t, err)
	require.Equal(t, uint64(1), response.RequestID)
	return string(response.Payload)
}

func reserveBrokerTestPorts(t *testing.T) (int, int) {
	t.Helper()
	var listenConfig net.ListenConfig
	brokerListener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { require.NoError(t, brokerListener.Close()) }()

	healthListener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { require.NoError(t, healthListener.Close()) }()

	brokerPort := brokerListener.Addr().(*net.TCPAddr).Port
	healthPort := healthListener.Addr().(*net.TCPAddr).Port
	require.NotEqual(t, brokerPort, healthPort)
	return brokerPort, healthPort
}
