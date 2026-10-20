package client

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func catchupMetric(t *testing.T, name, label, value string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			matched := 0
			for _, actual := range metric.Label {
				if actual.GetName() == "stage" && actual.GetValue() == "catchup_rpc" || actual.GetName() == label && actual.GetValue() == value {
					matched++
				}
			}
			if matched == 2 {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestCatchupMetricsIncludeRejectedResponseBytes(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	response := []byte(`{"malformed`)
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		connection, err := wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone})
		if err != nil {
			done <- err
			return
		}
		request, err := connection.ReadFrame()
		if err == nil {
			err = connection.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusOK, RequestID: request.RequestID, Payload: response})
		}
		done <- err
	}()
	bytes := catchupMetric(t, "cursus_recovery_work_total", "unit", "transfer_bytes")
	failures := catchupMetric(t, "cursus_recovery_attempts_total", "outcome", "failure")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = NewTCPClusterClient().FetchReplicaCatchup(ctx, listener.Addr().String(), listener.Addr().(*net.TCPAddr).Port, fsm.ReplicaCatchupRequest{})
	require.ErrorContains(t, err, "decode replica catch-up response")
	require.NoError(t, <-done)
	require.Equal(t, float64(len(response)), catchupMetric(t, "cursus_recovery_work_total", "unit", "transfer_bytes")-bytes)
	require.Equal(t, float64(1), catchupMetric(t, "cursus_recovery_attempts_total", "outcome", "failure")-failures)
}
