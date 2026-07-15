package server

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
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
		rm, err := replication.NewRaftReplicationManager(ctx, cfg, raftServerID, tm, cd, clusterClient)
		if err != nil {
			return fmt.Errorf("failed to create raft replication manager: %w", err)
		}

		clientHost := cfg.AdvertisedClientHost
		if clientHost == "" {
			clientHost = cfg.AdvertisedHost
		}
		clientPort := cfg.AdvertisedBrokerPort
		if clientPort == 0 {
			clientPort = cfg.BrokerPort
		}
		clientAddr := fmt.Sprintf("%s:%d", clientHost, clientPort)

		sd := clusterController.NewServiceDiscovery(rm, brokerID, localAddr, clientAddr)
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

		// Every node should attempt to join the cluster via seeds
		go func() {
			util.Info("🚀 Attempting to join cluster via seeds...")
			// Wait a bit for Raft to initialize
			time.Sleep(2 * time.Second)

			if err := clusterClient.JoinCluster(cfg.StaticClusterMembers, brokerID, localAddr, cfg.DiscoveryPort); err != nil {
				util.Warn("⚠️ Join cluster attempt failed: %v. This is normal if already part of the cluster.", err)
			} else {
				util.Info("✅ Successfully joined cluster")
			}

			// Register self with ClientAddr — try local first, then forward to leader
			go func() {
				for i := 0; i < 15; i++ {
					time.Sleep(3 * time.Second)
					// Try direct Raft apply (works if we're the leader)
					if err := sd.Register(); err == nil {
						util.Info("✅ Registered with client address %s", clientAddr)
						return
					}
					// Forward via RAFT_APPLY to leader
					if cc != nil && cc.Router != nil {
						brokerJSON, _ := json.Marshal(map[string]interface{}{
							"id": brokerID, "addr": localAddr, "client_addr": clientAddr,
							"status": "active",
						})
						raftCmd := fmt.Sprintf("RAFT_APPLY %stype=REGISTER payload=%s", internalAuthPrefix(cfg), string(brokerJSON))
						encodedCmd := util.EncodeMessage("", raftCmd)
						if resp, err := cc.Router.ForwardToLeader(string(encodedCmd)); err == nil && !strings.HasPrefix(resp, "ERROR") {
							util.Info("✅ Registered via leader with client address %s", clientAddr)
							return
						}
					}
				}
			}()
		}()

		go func() {
			util.Info("🔄 Starting cluster leader election monitor...")
			for isLeader := range rm.LeaderCh() {
				if isLeader {
					util.Info("🎉 Became cluster leader! Syncing all members with FSM.")
					if regErr := sd.Register(); regErr != nil {
						util.Error("❌ Failed to register as leader: %v", regErr)
					}
					// Immediate reconcile ensures all Raft members are in FSM
					sd.Reconcile()
				} else {
					util.Info("💀 Lost cluster leadership.")
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

	globalCH := controller.NewCommandHandler(tm, cfg, cd, sm, cc)
	if cc != nil {
		cc.SetLocalProcessor(globalCH)
	}
	if cfg.EnabledDistribution && cfg.InternalBrokerPort > 0 {
		if err := startInternalBrokerListener(ctx, cfg, globalCH); err != nil {
			return err
		}
	}
	if err := globalCH.RecoverPreparedTransactions(); err != nil {
		return fmt.Errorf("failed to recover prepared transactions: %w", err)
	}
	brokerReady.Store(true)

	go func() {
		<-ctx.Done()
		if err := globalCH.Close(); err != nil {
			util.Error("Failed to close command handler: %v", err)
		}
	}()

	workerCh := make(chan net.Conn, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		go func() {
			for conn := range workerCh {
				handleConn(ctx, conn, globalCH)
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

func startInternalBrokerListener(ctx context.Context, cfg *config.Config, cmdHandler *controller.CommandHandler) error {
	addr := fmt.Sprintf(":%d", cfg.InternalBrokerPort)
	var ln net.Listener
	var err error
	if cfg.InternalUseTLS {
		ln, err = tls.Listen("tcp", addr, cfg.InternalServerTLSConfig())
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("failed to start internal broker listener on %s: %w", addr, err)
	}

	util.Info("🔒 Internal broker listener started on %s (mTLS=%v)", addr, cfg.InternalUseTLS)
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	workerCh := make(chan net.Conn, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		go func() {
			for conn := range workerCh {
				handleInternalConn(ctx, conn, cmdHandler)
			}
		}()
	}
	go func() {
		defer close(workerCh)
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					util.Error("⚠️ Internal accept error: %v", err)
					continue
				}
			}
			select {
			case workerCh <- conn:
			default:
				util.Warn("⚠️ Internal worker pool saturated; closing connection from %s", conn.RemoteAddr())
				_ = conn.Close()
			}
		}
	}()
	return nil
}

func handleInternalConn(ctx context.Context, conn net.Conn, cmdHandler *controller.CommandHandler) {
	handleConnWithContext(ctx, conn, cmdHandler, controller.NewInternalClientContext("default-group", 0))
}

// handleConn processes a connection using a shared CommandHandler.
func handleConn(ctx context.Context, conn net.Conn, cmdHandler *controller.CommandHandler) {
	handleConnWithContext(ctx, conn, cmdHandler, controller.NewClientContext("default-group", 0))
}

func handleConnWithContext(ctx context.Context, conn net.Conn, cmdHandler *controller.CommandHandler, cmdCtx *controller.ClientContext) {
	isStreamed := false
	defer func() {
		if !isStreamed {
			_ = conn.Close()
		}
	}()

	clientCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			util.Error("⚠️ SetReadDeadline error: %v", err)
			return
		}

		data, err := readMessage(conn, cmdHandler.Config.CompressionType)
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
		if err != nil {
			return
		}
		if shouldExit {
			_, payload, decodeErr := util.DecodeMessage(data)
			cmd := ""
			if decodeErr == nil {
				cmd = strings.TrimSpace(payload)
			} else {
				cmd = strings.TrimSpace(string(data))
			}

			if strings.HasPrefix(strings.ToUpper(cmd), "STREAM ") {
				isStreamed = true
			}
			return
		}
	}
}

// HandleConnection processes a single client connection (creates a new CommandHandler per call).
// Deprecated: prefer handleConn with a shared CommandHandler to avoid file descriptor leaks.
func HandleConnection(ctx context.Context, conn net.Conn, tm *topic.TopicManager, cfg *config.Config, cd *coordinator.Coordinator, sm *stream.StreamManager, cc *clusterController.ClusterController) {
	isStreamed := false
	defer func() {
		if !isStreamed {
			_ = conn.Close()
		}
	}()

	clientCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmdHandler, cmdCtx := initializeConnection(cfg, tm, cd, sm, cc)
	defer func() { _ = cmdHandler.Close() }()

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
		if err != nil {
			return
		}
		if shouldExit {
			// Check if this was a STREAM command to prevent closing the connection
			_, payload, decodeErr := util.DecodeMessage(data)
			cmd := ""
			if decodeErr == nil {
				cmd = strings.TrimSpace(payload)
			} else {
				cmd = strings.TrimSpace(string(data))
			}

			if strings.HasPrefix(strings.ToUpper(cmd), "STREAM ") {
				isStreamed = true
			}
			return
		}
	}
}

func internalAuthPrefix(cfg *config.Config) string {
	if cfg != nil && cfg.InternalAuthToken != "" {
		return "internal_token=" + cfg.InternalAuthToken + " "
	}
	return ""
}

func initializeConnection(cfg *config.Config, tm *topic.TopicManager, cd *coordinator.Coordinator, sm *stream.StreamManager, cc *clusterController.ClusterController) (*controller.CommandHandler, *controller.ClientContext) {
	cmdHandler := controller.NewCommandHandler(tm, cfg, cd, sm, cc)
	ctx := controller.NewClientContext("default-group", 0)
	return cmdHandler, ctx
}

func readMessage(conn net.Conn, compressionType string) ([]byte, error) {
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
		if ctx != nil && ctx.Internal && cmdHandler.Config != nil && cmdHandler.Config.InternalAuthToken != "" && !cmdHandler.Config.InternalUseTLS {
			writeResponse(conn, "ERROR: internal_batch_requires_token_wrapper")
			return false, nil
		}
		resp, err := cmdHandler.HandleBatchMessage(data, conn, ctx)
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
		if strings.HasPrefix(strings.ToUpper(rawInput), "INTERNAL_BATCH ") {
			return handleInternalBatchMessage(rawInput, cmdHandler, ctx, conn)
		}
		if isCommand(rawInput) {
			if resp := authorizeInternalListenerCommand(rawInput, cmdHandler, ctx); resp != "" {
				writeResponse(conn, resp)
				return false, nil
			}
			return handleCommandMessage(rawInput, cmdHandler, ctx, conn)
		}
		util.Error("⚠️ Decode error and not a raw command: %v [%s]", err, string(data))
		writeResponse(conn, fmt.Sprintf("ERROR: decode_failed reason=%q", err.Error()))
		return false, nil
	}

	payload = strings.Trim(payload, "\x00 \t\n\r")

	if strings.HasPrefix(strings.ToUpper(payload), "JOIN_GROUP") ||
		strings.HasPrefix(strings.ToUpper(payload), "SYNC_GROUP") ||
		strings.HasPrefix(strings.ToUpper(payload), "LEAVE_GROUP") {
		resp := cmdHandler.HandleCommand(payload, ctx)
		writeResponse(conn, resp)
		return false, nil
	}

	if strings.HasPrefix(strings.ToUpper(payload), "INTERNAL_BATCH ") {
		return handleInternalBatchMessage(payload, cmdHandler, ctx, conn)
	}
	if isCommand(payload) {
		if resp := authorizeInternalListenerCommand(payload, cmdHandler, ctx); resp != "" {
			writeResponse(conn, resp)
			return false, nil
		}
		return handleCommandMessage(payload, cmdHandler, ctx, conn)
	}

	rawInput := strings.TrimSpace(string(data))
	util.Debug("[%s] Received unrecognized input: %s", conn.RemoteAddr().String(), rawInput)
	writeResponse(conn, "ERROR: malformed_input reason=missing_topic_or_payload")
	return true, nil
}

func authorizeInternalListenerCommand(payload string, cmdHandler *controller.CommandHandler, ctx *controller.ClientContext) string {
	if ctx == nil || !ctx.Internal || cmdHandler == nil || cmdHandler.Config == nil {
		return ""
	}
	if cmdHandler.Config.InternalUseTLS {
		return ""
	}
	token := strings.TrimSpace(cmdHandler.Config.InternalAuthToken)
	if token == "" {
		return "ERROR: internal_auth_not_configured command=INTERNAL_LISTENER"
	}
	if parseInternalCommandArgs(payload)["internal_token"] != token {
		return "ERROR: internal_command_unauthorized command=INTERNAL_LISTENER"
	}
	return ""
}

func handleInternalBatchMessage(payload string, cmdHandler *controller.CommandHandler, ctx *controller.ClientContext, conn net.Conn) (bool, error) {
	if ctx == nil || !ctx.Internal {
		writeResponse(conn, "ERROR: internal_command_unauthorized command=INTERNAL_BATCH")
		return false, nil
	}
	if resp := authorizeInternalListenerCommand(payload, cmdHandler, ctx); resp != "" {
		writeResponse(conn, resp)
		return false, nil
	}
	encoded := parseInternalCommandArgs(payload)["payload"]
	if encoded == "" {
		writeResponse(conn, "ERROR: missing_payload command=INTERNAL_BATCH")
		return false, nil
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		writeResponse(conn, fmt.Sprintf("ERROR: invalid_payload command=INTERNAL_BATCH reason=%q", err.Error()))
		return false, nil
	}
	resp, err := cmdHandler.HandleBatchMessage(data, conn, ctx)
	if err != nil {
		return false, err
	}
	writeResponse(conn, resp)
	return false, nil
}

func parseInternalCommandArgs(payload string) map[string]string {
	args := map[string]string{}
	for _, field := range strings.Fields(payload) {
		key, value, ok := strings.Cut(field, "=")
		if ok {
			args[key] = value
		}
	}
	return args
}
func handleCommandMessage(payload string, cmdHandler *controller.CommandHandler, ctx *controller.ClientContext, conn net.Conn) (bool, error) {
	if strings.HasPrefix(strings.ToUpper(payload), "READ_STREAM ") {
		cmdHandler.HandleReadStreamCommand(conn, payload)
		return false, nil
	}

	resp := cmdHandler.HandleCommand(payload, ctx)
	if resp == controller.STREAM_DATA_SIGNAL {
		if strings.HasPrefix(strings.ToUpper(payload), "STREAM ") {
			if err := cmdHandler.HandleStreamCommand(conn, payload, ctx); err != nil {
				if errors.Is(err, controller.ErrStreamRejected) {
					return false, nil
				}
				writeResponse(conn, fmt.Sprintf("ERROR: command_failed reason=%q", err.Error()))
				return false, nil
			}
			return true, nil
		} else {
			if _, err := cmdHandler.HandleConsumeCommand(conn, payload, ctx); err != nil {
				writeResponse(conn, fmt.Sprintf("ERROR: command_failed reason=%q", err.Error()))
			}
			return false, nil
		}
	}
	if resp == "" {
		resp = "ERROR: empty_command_response"
	}
	writeResponse(conn, resp)
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
	keywords := []string{"CREATE", "DELETE", "LIST", "LIST_CLUSTER", "PUBLISH", "CONSUME", "STREAM", "HELP",
		"HEARTBEAT", "JOIN_GROUP", "LEAVE_GROUP", "COMMIT_OFFSET", "BATCH_COMMIT", "REGISTER_GROUP",
		"GROUP_STATUS", "FETCH_OFFSET", "LIST_GROUPS", "SYNC_GROUP", "DESCRIBE",
		"APPEND_STREAM", "READ_STREAM", "SAVE_SNAPSHOT", "READ_SNAPSHOT", "STREAM_VERSION",
		"REPLICATE_MESSAGE", "REPLICATE_SNAPSHOT", "LIST_SNAPSHOTS", "FETCH_SNAPSHOT", "CATCHUP_SNAPSHOTS",
		"FIND_COORDINATOR", "RAFT_APPLY", "METADATA", "INTERNAL_BATCH"}
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
