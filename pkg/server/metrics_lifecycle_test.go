package server

import (
	"fmt"
	"net"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestRunServer_ClosesExporterWhenBrokerListenerFails(t *testing.T) {
	brokerListener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer func() { _ = brokerListener.Close() }()

	exporterReservation, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	exporterPort := exporterReservation.Addr().(*net.TCPAddr).Port
	require.NoError(t, exporterReservation.Close())

	cfg := config.DefaultConfig()
	cfg.EnableExporter = true
	cfg.ExporterPort = exporterPort
	cfg.BrokerPort = brokerListener.Addr().(*net.TCPAddr).Port

	err = RunServer(cfg, nil, nil, nil, nil)
	require.Error(t, err)

	exporterListener, err := net.Listen("tcp", fmt.Sprintf(":%d", exporterPort))
	require.NoError(t, err, "RunServer should release its exporter on startup failure")
	defer func() { _ = exporterListener.Close() }()
}
