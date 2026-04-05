package server

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster"
	client "github.com/cursus-io/cursus/pkg/cluster/client"
	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/util"
)

const (
	maxWorkers             = 1000
	DefaultHealthCheckPort = 9080
)

var brokerReady = &atomic.Bool{}

// RunServer starts the broker with optional TLS and gzip
func RunServer(cfg *config.Config, tm *topic.TopicManager, dm *disk.DiskManager, cd *coordinator.Coordinator, sm *stream.StreamManager) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if cfg.EnableExporter {
		metrics.StartMetricsServer(cfg.ExporterPort)
		util.Info("📈 Prometheus exporter started on port %d", cfg.ExporterPort)
	} else {
		util.Info("📉 Exporter disabled")
	}

	addr := fmt.Sprintf(":%d", cfg.BrokerPort)
	var ln net.Listener
	var err error
	if cfg.UseTLS {
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cfg.TLSCert},
			MinVersion:   tls.VersionTLS12,
		}
		ln, err = tls.Listen("tcp", addr, tlsConfig)
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return err
	}

	util.Info("🧩 Broker listening on %s (TLS=%v, Compression=%v)", addr, cfg.UseTLS, cfg.CompressionType)
	brokerReady.Store(true)

	if cd != nil {
		cd.Start()
		util.Info("🔄 Coordinator started with heartbeat monitoring")
	}

	var cc *clusterController.ClusterController
	if cfg.EnabledDistribution {
		brokerID := fmt.Sprintf("%s-%d", cfg.AdvertisedHost, cfg.BrokerPort)
		localAddr := fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.RaftPort)
		raftServerID := brokerID

		var err error
		clusterClient := client.TCPClusterClient{}
		rm, err := replication.NewRaftReplicationManager(cfg, raftServerID, dm, tm, cd, clusterClient)
		if err != nil {
			return fmt.Errorf("failed to create raft replication manager: %w", err)
		}

		sd := clusterController.NewServiceDiscovery(rm, brokerID, localAddr)
		discoveryAddr := fmt.Sprintf(":%d", cfg.DiscoveryPort)
		cs := cluster.NewClusterServer(sd)
		go func() {
			if _, err := cs.Start(discoveryAddr); err != nil {
				util.Error("discovery-server start error: %v", err)
			}
		}()

		cc = clusterController.NewClusterController(ctx, cfg, rm, sd, brokerID, localAddr)

		if cd != nil {
			cd.SetLeaderChecker(cc.IsLeader)
		}

		// Start background heartbeats to all cluster members
		clusterClient.StartHeartbeat(ctx, cfg.StaticClusterMembers, brokerID, localAddr, cfg.DiscoveryPort)

		globalCH := controller.NewCommandHandler(tm, cfg, cd, sm, cc)
		cc.SetLocalProcessor(globalCH)

		go func() {
			util.Info("🔄 Starting cluster leader election monitor...")
			for isLeader := range rm.LeaderCh() {
				if isLeader {
					util.Info("🎉 Became cluster leader! Registering self and starting controller.")
					if regErr := sd.Register(); regErr != nil {
						util.Error("❌ Failed to register as leader: %v", regErr)
						continue
					}
					util.Info("✅ Cluster registration completed")
				} else {
					util.Info("💀 Lost cluster leadership. Stopping controller functions.")
				}
			}
		}()

		util.Info("🌐 Distributed clustering enabled (brokerID=%s, localAddr=%s)", brokerID, localAddr)
	}

	healthPort := cfg.HealthCheckPort
	if healthPort == 0 {
		healthPort = DefaultHealthCheckPort
	}
	startHealthCheckServer(healthPort, brokerReady)
	workerCh := make(chan net.Conn, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		go func() {
			for conn := range workerCh {
				HandleConnection(ctx, conn, tm, cfg, cd, sm, cc)
			}
		}()
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			util.Error("⚠️ Accept error: %v", err)
			continue
		}
		workerCh <- conn
	}
}

// HandleConnection processes a single client connection
func HandleConnection(ctx context.Context, conn net.Conn, tm *topic.TopicManager, cfg *config.Config, cd *coordinator.Coordinator, sm *stream.StreamManager, cc *clusterController.ClusterController) {
	defer func() { _ = conn.Close() }()

	clientCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmdHandler, cmdCtx := initializeConnection(cfg, tm, cd, sm, cc)

	for {
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			util.Error("⚠️ SetReadDeadline error: %v", err)
			return
		}

		data, err := readMessage(conn, cfg.CompressionType)
		if err != nil {
			select {
			case <-clientCtx.Done():
				return
			default:
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				return
			}
		}

		shouldExit, err := processMessage(data, cmdHandler, cmdCtx, conn)
		if err != nil || shouldExit {
			return
		}
	}
}

func initializeConnection(cfg *config.Config, tm *topic.TopicManager, cd *coordinator.Coordinator, sm *stream.StreamManager, cc *clusterController.ClusterController) (*controller.CommandHandler, *controller.ClientContext) {
	cmdHandler := controller.NewCommandHandler(tm, cfg, cd, sm, cc)
	ctx := controller.NewClientContext("default-group", 0)
	return cmdHandler, ctx
}

func readMessage(conn net.Conn, compressionType string) ([]byte, error) {
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		util.Error("⚠️ SetReadDeadline error: %v", err)
		return nil, err
	}

	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		if err != io.EOF {
			util.Error("⚠️ Read length error: %v", err)
		}
		return nil, err
	}

	msgLen := binary.BigEndian.Uint32(lenBuf)
	msgBuf := make([]byte, msgLen)
	if _, err := io.ReadFull(conn, msgBuf); err != nil {
		if err != io.EOF {
			util.Error("⚠️ Read message error: %v (len=%d)", err, len(msgBuf))
		}
		return nil, err
	}

	data, err := util.DecompressMessage(msgBuf, compressionType)
	if err != nil {
		util.Error("⚠️ Decompress error: %v", err)
		return nil, err
	}

	return data, nil
}

func processMessage(data []byte, cmdHandler *controller.CommandHandler, ctx *controller.ClientContext, conn net.Conn) (bool, error) {
	if isBatchMessage(data) {
		resp, err := cmdHandler.HandleBatchMessage(data, conn)
		if err != nil {
			return false, err
		}
		writeResponse(conn, resp)
		return false, nil
	}

	_, payload, err := util.DecodeMessage(data)
	if err != nil {
		rawInput := string(data)
		rawInput = strings.Trim(rawInput, "\x00 \t\n\r")
		if isCommand(rawInput) {
			return handleCommandMessage(rawInput, cmdHandler, ctx, conn)
		}
		util.Error("⚠️ Decode error and not a raw command: %v [%s]", err, string(data))
		return false, err
	}

	payload = strings.Trim(payload, "\x00 \t\n\r")

	if strings.HasPrefix(strings.ToUpper(payload), "HEARTBEAT") {
		writeResponseWithTimeout(conn, "OK", 10*time.Second)
		return false, nil
	}

	if strings.HasPrefix(strings.ToUpper(payload), "JOIN_GROUP") ||
		strings.HasPrefix(strings.ToUpper(payload), "SYNC_GROUP") ||
		strings.HasPrefix(strings.ToUpper(payload), "LEAVE_GROUP") {
		resp := cmdHandler.HandleCommand(payload, ctx)
		writeResponse(conn, resp)
		return false, nil
	}

	if isCommand(payload) {
		return handleCommandMessage(payload, cmdHandler, ctx, conn)
	}

	rawInput := strings.TrimSpace(string(data))
	util.Debug("[%s] Received unrecognized input: %s", conn.RemoteAddr().String(), rawInput)
	writeResponse(conn, "ERROR: malformed input - missing topic or payload")
	return true, nil
}

func handleCommandMessage(payload string, cmdHandler *controller.CommandHandler, ctx *controller.ClientContext, conn net.Conn) (bool, error) {
	resp := cmdHandler.HandleCommand(payload, ctx)
	if resp == controller.STREAM_DATA_SIGNAL {
		if strings.HasPrefix(strings.ToUpper(payload), "STREAM ") {
			if err := cmdHandler.HandleStreamCommand(conn, payload, ctx); err != nil {
				writeResponse(conn, fmt.Sprintf("ERROR: %v", err))
			}
			return true, nil
		} else {
			if _, err := cmdHandler.HandleConsumeCommand(conn, payload, ctx); err != nil {
				writeResponse(conn, fmt.Sprintf("ERROR: %v", err))
			}
			return false, nil
		}
	}
	if resp != "" {
		writeResponse(conn, resp)
	}
	return false, nil
}

// isBatchMessage checks if the data is in binary batch format
func isBatchMessage(data []byte) bool {
	if len(data) < 6 {
		return false
	}
	if data[0] != 0xBA || data[1] != 0x7C {
		return false
	}

	topicLen := binary.BigEndian.Uint16(data[2:4])
	if topicLen == 0 || int(topicLen)+2 > len(data) {
		return false
	}
	return true
}

func isCommand(s string) bool {
	keywords := []string{"CREATE", "DELETE", "LIST", "PUBLISH", "CONSUME", "STREAM", "HELP",
		"HEARTBEAT", "JOIN_GROUP", "LEAVE_GROUP", "COMMIT_OFFSET", "BATCH_COMMIT", "REGISTER_GROUP",
		"GROUP_STATUS", "FETCH_OFFSET", "LIST_GROUPS", "SYNC_GROUP", "DESCRIBE"}
	for _, k := range keywords {
		if strings.HasPrefix(strings.ToUpper(s), k) {
			return true
		}
	}
	return false
}

// writeResponseWithTimeout adds write timeout
func writeResponseWithTimeout(conn net.Conn, msg string, timeout time.Duration) {
	resp := []byte(msg)
	respLen := make([]byte, 4)
	binary.BigEndian.PutUint32(respLen, uint32(len(resp)))

	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		util.Error("⚠️ SetWriteDeadline error: %v", err)
		return
	}
	defer func() {
		if err := conn.SetWriteDeadline(time.Time{}); err != nil {
			util.Error("Failed to reset write deadline: %v", err)
		}
	}()

	if _, err := conn.Write(respLen); err != nil {
		util.Error("⚠️ Write length error: %v", err)
		return
	}
	if _, err := conn.Write(resp); err != nil {
		util.Error("⚠️ Write response error: %v", err)
		return
	}
}

func writeResponse(conn net.Conn, msg string) {
	resp := []byte(msg)
	respLen := make([]byte, 4)
	binary.BigEndian.PutUint32(respLen, uint32(len(resp)))

	if _, err := conn.Write(respLen); err != nil {
		util.Error("⚠️ Write length error: %v", err)
		return
	}
	if _, err := conn.Write(resp); err != nil {
		util.Error("⚠️ Write response error: %v", err)
		return
	}
}

// startHealthCheckServer starts a simple HTTP server for health checks
func startHealthCheckServer(port int, brokerReady *atomic.Bool) {
	mux := http.NewServeMux()
	healthHandler := func(w http.ResponseWriter, r *http.Request) {
		if !brokerReady.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			if _, err := w.Write([]byte("Broker not ready: Main listener not active")); err != nil {
				util.Error("⚠️ Health check response write error: %v", err)
			}
			return
		}

		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK")); err != nil {
			util.Error("⚠️ Health check response write error: %v", err)
		}
	}

	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/", healthHandler)

	addr := fmt.Sprintf(":%d", port)

	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			util.Error("❌ Health check server failed: %v", err)
		}
	}()
	util.Info("🩺 Health check endpoint started on port %d", port)
}
