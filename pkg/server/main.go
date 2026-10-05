package server

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/ackpolicy"
	"github.com/cursus-io/cursus/pkg/cluster"
	client "github.com/cursus-io/cursus/pkg/cluster/client"
	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/observability"
	"github.com/cursus-io/cursus/pkg/observationgrpc"
	wireprotocol "github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/cursus/util"
)

const (
	defaultMaxWorkers      = 1000
	defaultIdleTimeout     = 60 * time.Second
	readDeadlinePoll       = 5 * time.Second
	DefaultHealthCheckPort = 9080
)

// ErrConsumerMetadataRecovery routes a post-Raft replay failure into the
// diagnostics-only server instead of opening the client listener.
var ErrConsumerMetadataRecovery = errors.New("consumer metadata recovery failed")

// RunServer starts the broker with optional TLS and gzip
func RunServer(cfg *config.Config, tm *topic.TopicManager, dm *disk.DiskManager, cd *coordinator.Coordinator, sm *stream.StreamManager) error {
	return RunServerContext(context.Background(), cfg, tm, dm, cd, sm)
}

// RunServerContext starts the broker and shuts it down when ctx is canceled.
func RunServerContext(ctx context.Context, cfg *config.Config, tm *topic.TopicManager, dm *disk.DiskManager, cd *coordinator.Coordinator, sm *stream.StreamManager) error {
	if ctx == nil {
		return fmt.Errorf("server context must not be nil")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var cc *clusterController.ClusterController
	var rm *replication.RaftReplicationManager
	var clusterClient *client.TCPClusterClient
	var discoveryListener net.Listener
	healthState := NewHealthState()
	var startupComplete atomic.Bool
	healthState.AddCheck("broker_startup", func(context.Context) error {
		if !startupComplete.Load() {
			return fmt.Errorf("broker initialization is in progress")
		}
		return nil
	})
	addStorageReadinessChecks(healthState, tm, dm)
	if cd != nil {
		addConsumerMetadataReadinessCheck(healthState, cd)
	}
	healthState.SetReady(true)
	healthPort := cfg.HealthCheckPort
	if healthPort == 0 {
		healthPort = DefaultHealthCheckPort
	}
	healthServer, healthErr := startHealthCheckServer(healthPort, healthState)
	if healthErr != nil {
		return fmt.Errorf("start health server: %w", healthErr)
	}
	defer func() {
		healthState.SetReady(false)
		shutdownHTTPServer(healthServer)
		if discoveryListener != nil {
			_ = discoveryListener.Close()
		}
		if rm != nil {
			if shutdownErr := rm.Shutdown(); shutdownErr != nil {
				util.Error("raft shutdown failed: %v", shutdownErr)
			}
		}
	}()
	if cfg.EnabledDistribution {
		brokerID := fmt.Sprintf("%s-%d", cfg.AdvertisedHost, cfg.BrokerPort)
		localAddr := fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.RaftPort)
		raftServerID := brokerID

		var err error
		clusterClient = client.NewSecureTCPClusterClient(cfg.InternalAuthToken, cfg.InternalClientTLSConfig())
		rm, err = replication.NewRaftReplicationManager(ctx, cfg, raftServerID, tm, cd, *clusterClient)
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
		cs := cluster.NewSecureClusterServerWithTokens(sd, cfg.InternalAuthToken, cfg.InternalAuthTokenNext, cfg.InternalServerTLSConfig())
		discoveryListener, err = cs.Start(discoveryAddr)
		if err != nil {
			return fmt.Errorf("start discovery server: %w", err)
		}
		go closeListenerOnDone(ctx, discoveryListener)

		cc = clusterController.NewClusterController(ctx, cfg, rm, sd, brokerID, localAddr)
		sd.StartReconciler(ctx)
		incarnationID := ""
		if identity, ok := sd.(interface{ BrokerIncarnationID() string }); ok {
			incarnationID = identity.BrokerIncarnationID()
		}
		clusterClient.StartLeaderHeartbeat(ctx, rm.GetLeaderAddress, brokerID, incarnationID, cfg.DiscoveryPort)
		// ISR proof propagation remains peer based; coordinator membership does not.
		clusterClient.StartHeartbeat(
			ctx,
			cfg.StaticClusterMembers,
			brokerID,
			incarnationID,
			localAddr,
			cfg.DiscoveryPort,
			func() []fsm.ISRCatchupProof {
				if manager := rm.GetISRManager(); manager != nil {
					return manager.BuildCatchupProofs()
				}
				return nil
			},
		)

		// Every node should attempt to join the cluster via seeds
		go func() {
			util.Info("🚀 Attempting to join cluster via seeds...")
			// Wait a bit for Raft to initialize
			time.Sleep(2 * time.Second)

			if err := clusterClient.JoinClusterWithTransactionCoordinatorShards(cfg.StaticClusterMembers, brokerID, localAddr, cfg.DiscoveryPort, cfg.TransactionCoordinatorShards); err != nil {
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
							"status": "active", "lifecycle_protocol": fsm.BrokerProtocolVersionCurrent, "incarnation_id": incarnationID, "transaction_coordinator_shards": cfg.TransactionCoordinatorShards,
						})
						raftCmd := fmt.Sprintf("RAFT_APPLY %stype=REGISTER payload=%s", internalAuthPrefix(cfg), string(brokerJSON))
						if resp, err := cc.Router.ForwardToLeader(raftCmd); err == nil && !wireprotocol.IsErrorResponse(resp) {
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

		// Bootstrap only after every current Raft voter is durably registered.
		// Followers register asynchronously, so a leader event alone is too early
		// to choose the internal topic's replica set. This loop does no local
		// mutation and becomes a no-op after the single durable TOPIC command.
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if !rm.IsLeader() || rm.GetFSM().GetPartitionMetadata(config.ConsumerOffsetsTopicName+"-0") != nil {
						continue
					}
					if err := clusterController.BootstrapConsumerOffsetsTopic(rm, cfg); err != nil {
						util.Debug("consumer offsets topology bootstrap pending: %v", err)
					} else {
						util.Info("consumer offsets topology committed through Raft")
					}
				}
			}
		}()

		util.Info("🌐 Distributed clustering enabled (brokerID=%s, localAddr=%s)", brokerID, localAddr)
	}
	globalCH := controller.NewCommandHandler(tm, cfg, cd, sm, cc)
	requestBudget := newRequestMemoryBudget(cfg.MaxInflightRequests, cfg.MaxInflightRequestBytes)
	defer func() {
		if err := globalCH.Close(); err != nil {
			util.Error("Failed to close command handler: %v", err)
		}
	}()
	if !cfg.EnabledDistribution {
		journalPath := filepath.Join(cfg.LogDir, "__transaction_state.journal")
		if err := globalCH.ConfigureTransactionJournal(journalPath); err != nil {
			return fmt.Errorf("initialize standalone transaction journal: %w", err)
		}
	}
	if cd != nil {
		cd.SetGroupSessionCallbacks(globalCH.IsGroupCoordinator, globalCH.ExpireGroupMembers)
		cd.SetGroupObservationBatchResolver(globalCH.ResolveGroupCoordinators)
		cd.Start()
		util.Info("🔄 Coordinator started with heartbeat monitoring")
	}
	if cc != nil {
		cc.SetLocalProcessor(globalCH)
		cc.SetReplicaSnapshotCatchup(globalCH.CatchupReplicaSnapshots)
		cc.StartTopologyReconciler(ctx)
		cc.StartReplicaCatchup(ctx, clusterClient, globalCH.ApplyReplicaCatchup)
	}
	if cfg.EnabledDistribution && cfg.InternalBrokerPort > 0 {
		// Followers use this authenticated listener to forward their durable
		// broker registration to the Raft leader. Consumer-offset topology
		// bootstrap waits for those registrations, so this listener must be
		// available before consumer metadata recovery begins.
		shutdownInternal, err := startInternalBrokerListener(ctx, cfg, globalCH, requestBudget)
		if err != nil {
			return err
		}
		defer shutdownInternal()
	}
	if cfg.EnabledDistribution {
		if cd == nil {
			return fmt.Errorf("%w: coordinator unavailable", ErrConsumerMetadataRecovery)
		}
		if err := awaitDistributedConsumerMetadataRecovery(ctx, cd, tm); err != nil {
			return err
		}
	}
	if err := registerStaticConsumerGroups(cfg, tm, globalCH); err != nil {
		return fmt.Errorf("register static consumer groups: %w", err)
	}
	if cfg.ObservationGRPCPort > 0 {
		shutdownObservation, err := startObservationGRPC(ctx, cfg)
		if err != nil {
			return err
		}
		defer shutdownObservation()
		util.Info("Read-only observation gRPC listener started on 127.0.0.1:%d", cfg.ObservationGRPCPort)
	}
	if err := globalCH.RecoverPreparedTransactions(); err != nil {
		return fmt.Errorf("failed to recover prepared transactions: %w", err)
	}
	globalCH.StartTransactionTimeoutMonitor(ctx)

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
	defer func() { _ = ln.Close() }()
	go closeListenerOnDone(ctx, ln)
	util.Info("🧩 Broker listening on %s (TLS=%v, Compression=%v)", addr, cfg.UseTLS, cfg.CompressionType)

	if cfg.EnabledDistribution {
		healthState.AddCheck("cluster_leader", func(context.Context) error {
			if cc == nil {
				return fmt.Errorf("cluster controller unavailable")
			}
			_, leaderErr := cc.GetClusterLeader()
			return leaderErr
		})
		healthState.AddCheck("cluster_topology", func(context.Context) error {
			if cc == nil || cc.RaftManager == nil || cc.RaftManager.GetFSM() == nil {
				return clusterTopologyReadinessError(nil, cfg.MinInSyncReplicas)
			}
			return clusterTopologyReadinessError(cc.RaftManager.GetFSM(), cfg.MinInSyncReplicas)
		})
		healthState.AddCheck("topic_materialization", func(context.Context) error {
			if cc == nil || cc.RaftManager == nil || cc.RaftManager.GetFSM() == nil {
				return fmt.Errorf("topic materialization state unavailable")
			}
			return cc.RaftManager.GetFSM().TopicMaterializationReadinessError()
		})
		healthState.AddCheck("cluster_transactions", func(context.Context) error {
			if cc == nil || cc.RaftManager == nil || cc.RaftManager.GetFSM() == nil {
				return fmt.Errorf("cluster transaction state unavailable")
			}
			return cc.RaftManager.GetFSM().TransactionRecoveryReadinessError()
		})
		healthState.AddCheck("replica_materialization", func(context.Context) error {
			if cc == nil || cc.RaftManager == nil || cc.RaftManager.GetFSM() == nil {
				return fmt.Errorf("replica materialization state unavailable")
			}
			return cc.RaftManager.GetFSM().ReplicaMaterializationReadinessError(cc.BrokerID())
		})
	}

	runtimeCollector := observability.NewCollector(tm, cd, dm, sm, cc, healthState, globalCH.TxnManager)
	if cfg.EnableExporter {
		metricsServer, startErr := metrics.StartMetricsServer(cfg.ExporterPort, runtimeCollector)
		if startErr != nil {
			return fmt.Errorf("start metrics exporter: %w", startErr)
		}
		defer shutdownHTTPServer(metricsServer)
		util.Info("📈 Prometheus exporter started on port %d", cfg.ExporterPort)
	} else {
		util.Info("📉 Exporter disabled")
	}

	workerCount := maxClientConnections(cfg)
	workerCh := make(chan net.Conn, workerCount)
	connectionSlots := newConnectionLimiter(workerCount)
	var workerWG sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for conn := range workerCh {
				handleConn(ctx, conn, globalCH, requestBudget)
			}
		}()
	}
	defer func() {
		healthState.SetReady(false)
		cancel()
		close(workerCh)
		workerWG.Wait()
	}()
	startupComplete.Store(true)

	var temporaryDelay time.Duration
	for {
		healthState.SetReady(true)
		if err := connectionSlots.Acquire(ctx); err != nil {
			return err
		}
		conn, err := ln.Accept()
		if err != nil {
			connectionSlots.Release()
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return fmt.Errorf("broker listener closed: %w", err)
			}
			healthState.SetReady(false)
			if temporaryDelay == 0 {
				temporaryDelay = 5 * time.Millisecond
			} else {
				temporaryDelay *= 2
			}
			if maximum := time.Second; temporaryDelay > maximum {
				temporaryDelay = maximum
			}
			util.Warn("accept error; retrying in %s: %v", temporaryDelay, err)
			time.Sleep(temporaryDelay)
			continue
		}
		temporaryDelay = 0
		conn = newLimitedConnection(conn, connectionSlots.Release)
		select {
		case workerCh <- conn:
		case <-ctx.Done():
			_ = conn.Close()
			connectionSlots.Release()
			return ctx.Err()
		}
	}
}

// startObservationGRPC creates the loopback-only observation adapter used by
// the broker process. Keeping the listener setup separate from RunServerContext
// makes its security configuration and lifecycle independently testable.
func startObservationGRPC(ctx context.Context, cfg *config.Config) (func(), error) {
	if cfg == nil {
		return nil, fmt.Errorf("observation gRPC config is nil")
	}
	if (cfg.ObservationGRPCPrincipal == "") != (cfg.ObservationGRPCAuthToken == "") {
		return nil, fmt.Errorf("observation gRPC principal and auth token must be configured together")
	}
	backend, err := sdk.NewAdminClient(&sdk.AdminConfig{
		BrokerAddrs: []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.BrokerPort))},
		UseTLS:      cfg.UseTLS, TLSCertPath: cfg.TLSCertPath, TLSKeyPath: cfg.TLSKeyPath,
		Principal: cfg.ObservationGRPCPrincipal, AuthToken: cfg.ObservationGRPCAuthToken,
	})
	if err != nil {
		return nil, fmt.Errorf("create observation gRPC backend: %w", err)
	}
	shutdown, err := observationgrpc.Start(ctx, net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.ObservationGRPCPort)), backend)
	if err != nil {
		return nil, err
	}
	return shutdown, nil
}

func awaitDistributedConsumerMetadataRecovery(ctx context.Context, cd *coordinator.Coordinator, tm *topic.TopicManager) error {
	const retryInterval = 100 * time.Millisecond
	for {
		if consumerMetadataHWMReady(tm) {
			err := cd.ReloadDistributedConsumerMetadata()
			if err == nil {
				return nil
			}
			if !errors.Is(err, types.ErrCommittedHWMUnavailable) {
				return fmt.Errorf("%w: %w", ErrConsumerMetadataRecovery, err)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryInterval):
		}
	}
}

func consumerMetadataHWMReady(tm *topic.TopicManager) bool {
	if tm == nil {
		return false
	}
	current := tm.GetTopic(config.ConsumerOffsetsTopicName)
	if current == nil || len(current.Partitions) == 0 {
		return false
	}
	for _, partition := range current.Partitions {
		if partition == nil || !partition.HWMKnown() {
			return false
		}
	}
	return true
}

func registerStaticConsumerGroups(cfg *config.Config, tm *topic.TopicManager, handler *controller.CommandHandler) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	if len(cfg.StaticConsumerGroups) == 0 {
		return nil
	}
	if tm == nil {
		return fmt.Errorf("topic manager is unavailable")
	}
	if handler == nil || handler.Coordinator == nil {
		return fmt.Errorf("consumer group coordinator is unavailable")
	}
	for _, group := range cfg.StaticConsumerGroups {
		if strings.TrimSpace(group.Name) == "" {
			return fmt.Errorf("static consumer group name is empty")
		}
		if group.ConsumerCount <= 0 {
			return fmt.Errorf("static consumer group %q has invalid consumer count %d", group.Name, group.ConsumerCount)
		}
		if len(group.Topics) == 0 {
			return fmt.Errorf("static consumer group %q has no topics", group.Name)
		}

		localTopics := make([]*topic.Topic, 0, len(group.Topics))
		partitionCounts := make(map[string]int, len(group.Topics))
		for _, topicName := range group.Topics {
			current := tm.GetTopic(topicName)
			if current == nil {
				return fmt.Errorf("static consumer group %q references missing topic %q", group.Name, topicName)
			}
			actualPartitions := len(current.Partitions)
			if actualPartitions == 0 {
				return fmt.Errorf("static consumer group %q topic %q has no partitions", group.Name, topicName)
			}
			if configured := group.TopicPartitions[topicName]; configured > 0 && configured != actualPartitions {
				return fmt.Errorf(
					"static consumer group %q topic %q partition count mismatch: configured=%d actual=%d",
					group.Name, topicName, configured, actualPartitions,
				)
			}
			localTopics = append(localTopics, current)
			partitionCounts[topicName] = actualPartitions
		}

		owned := true
		if cfg.EnabledDistribution {
			var err error
			owned, err = handler.ResolveGroupCoordinator(group.Name)
			if err != nil {
				return fmt.Errorf("resolve coordinator for static consumer group %q: %w", group.Name, err)
			}
		}
		if owned {
			var err error
			if len(group.Topics) == 1 {
				topicName := group.Topics[0]
				err = handler.Coordinator.RegisterGroup(topicName, group.Name, partitionCounts[topicName])
			} else {
				err = handler.Coordinator.RegisterGroupSubscription(group.Name, group.Topics, "", partitionCounts)
			}
			if err != nil {
				return fmt.Errorf("persist static consumer group %q: %w", group.Name, err)
			}
		}
		for _, current := range localTopics {
			current.RegisterConsumerGroup(group.Name, group.ConsumerCount)
		}
	}
	return nil
}

func closeListenerOnDone(ctx context.Context, ln net.Listener) {
	<-ctx.Done()
	_ = ln.Close()
}

func startInternalBrokerListener(ctx context.Context, cfg *config.Config, cmdHandler *controller.CommandHandler, budgets ...*requestMemoryBudget) (func(), error) {
	addr := fmt.Sprintf(":%d", cfg.InternalBrokerPort)
	var ln net.Listener
	var err error
	if cfg.InternalUseTLS {
		ln, err = tls.Listen("tcp", addr, cfg.InternalServerTLSConfig())
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to start internal broker listener on %s: %w", addr, err)
	}

	util.Info("🔒 Internal broker listener started on %s (mTLS=%v)", addr, cfg.InternalUseTLS)
	internalCtx, cancel := context.WithCancel(ctx)
	workerCount := maxClientConnections(cfg)
	requestBudget := newRequestMemoryBudget(cfg.MaxInflightRequests, cfg.MaxInflightRequestBytes)
	if len(budgets) > 0 && budgets[0] != nil {
		requestBudget = budgets[0]
	}
	workerCh := make(chan net.Conn, workerCount)
	connectionSlots := newConnectionLimiter(workerCount)
	var workerWG sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for conn := range workerCh {
				handleConnWithBudget(internalCtx, conn, cmdHandler, controller.NewInternalClientContext("default-group", 0), requestBudget)
			}
		}()
	}
	var acceptWG sync.WaitGroup
	acceptWG.Add(1)
	go func() {
		defer close(workerCh)
		defer acceptWG.Done()
		for {
			if err := connectionSlots.Acquire(internalCtx); err != nil {
				return
			}
			conn, err := ln.Accept()
			if err != nil {
				connectionSlots.Release()
				select {
				case <-internalCtx.Done():
					return
				default:
					util.Error("⚠️ Internal accept error: %v", err)
					continue
				}
			}
			conn = newLimitedConnection(conn, connectionSlots.Release)
			select {
			case workerCh <- conn:
			default:
				util.Warn("⚠️ Internal worker pool saturated; closing connection from %s", conn.RemoteAddr())
				_ = conn.Close()
			}
		}
	}()
	var shutdownOnce sync.Once
	shutdown := func() {
		shutdownOnce.Do(func() {
			cancel()
			_ = ln.Close()
			acceptWG.Wait()
			workerWG.Wait()
		})
	}
	return shutdown, nil
}

func observeClientConnection() func() {
	metrics.ClientConnectionsTotal.Inc()
	metrics.ClientConnectionsActive.Inc()
	return metrics.ClientConnectionsActive.Dec
}

// handleConn processes a connection using a shared CommandHandler.
func handleConn(ctx context.Context, conn net.Conn, cmdHandler *controller.CommandHandler, budgets ...*requestMemoryBudget) {
	defer observeClientConnection()()
	budget := newRequestMemoryBudget(cmdHandler.Config.MaxInflightRequests, cmdHandler.Config.MaxInflightRequestBytes)
	if len(budgets) > 0 && budgets[0] != nil {
		budget = budgets[0]
	}
	handleConnWithBudget(ctx, conn, cmdHandler, controller.NewClientContext("default-group", 0), budget)
}

func handleConnWithContext(ctx context.Context, conn net.Conn, cmdHandler *controller.CommandHandler, cmdCtx *controller.ClientContext) {
	budget := newRequestMemoryBudget(cmdHandler.Config.MaxInflightRequests, cmdHandler.Config.MaxInflightRequestBytes)
	handleConnWithBudget(ctx, conn, cmdHandler, cmdCtx, budget)
}

func handleConnWithBudget(ctx context.Context, conn net.Conn, cmdHandler *controller.CommandHandler, cmdCtx *controller.ClientContext, budget *requestMemoryBudget) {
	isStreamed := false
	defer func() {
		if !isStreamed {
			_ = conn.Close()
		}
	}()

	clientCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopContextClose := context.AfterFunc(clientCtx, func() { _ = conn.Close() })
	defer stopContextClose()
	cmdCtx.SetRequestContext(clientCtx)
	idleTimeout := clientIdleTimeout(cmdHandler.Config)
	requestTimeout := clientRequestTimeout(cmdHandler.Config)
	lastActivity := time.Now()
	if err := conn.SetDeadline(lastActivity.Add(min(idleTimeout, requestTimeout))); err != nil {
		return
	}
	wireConnection, responseConn, err := negotiateServerConnection(conn, requestTimeout)
	if err != nil {
		return
	}
	_ = conn.SetDeadline(time.Time{})

	activity := &requestActivity{conn: conn, last: time.Now()}
	requests := make(chan admittedRequest)
	readPumpCtx, stopReadPump := context.WithCancel(clientCtx)
	pumpDone := make(chan struct{})
	// Keep partial headers/payloads across polling deadlines instead of restarting
	// frame decoding after a timeout in the middle of a frame.
	reader := &requestReader{Conn: conn, ctx: readPumpCtx, activity: activity, idleTimeout: idleTimeout}
	wireConnection.SetReader(reader)
	go func() {
		defer close(pumpDone)
		pumpWireRequests(readPumpCtx, cancel, wireConnection, activity, budget, requests)
	}()
	defer func() {
		stopReadPump()
		_ = conn.SetReadDeadline(time.Now())
		<-pumpDone
		if isStreamed {
			_ = conn.SetReadDeadline(time.Time{})
		}
	}()
	for {
		var request admittedRequest
		select {
		case <-clientCtx.Done():
			return
		case next, ok := <-requests:
			if !ok {
				return
			}
			request = next
		}
		close(request.accepted)
		// A STREAM frame is a pump barrier. No subsequent read starts until this
		// request fails; successful registration transfers the connection completely.
		if request.frame.Command == wire.CommandStream {
			_ = conn.SetReadDeadline(time.Time{})
		}
		requestParent := clientCtx
		if requestSuppressesResponse(request.frame.Payload, cmdCtx) {
			// Once a complete fire-and-forget frame is accepted, a subsequent
			// client close must not race it out of the broker. Server shutdown and
			// the request timeout still bound the detached work.
			requestParent = ctx
		}
		requestCtx, cancelRequest := context.WithTimeout(requestParent, requestTimeout)
		responseConn.setRequest(request.frame, requestCtx)
		cmdCtx.SetRequestContext(requestCtx)
		shouldExit, err := processMessage(request.frame.Payload, cmdHandler, cmdCtx, responseConn)
		cmdCtx.SetRequestContext(clientCtx)
		cancelRequest()
		if shouldExit || err != nil {
			stopReadPump()
		}
		request.frame.Payload = nil
		request.finish()
		if err != nil {
			return
		}
		if shouldExit {
			isStreamed = request.frame.Command == wire.CommandStream
			return
		}
	}
}

func requestSuppressesResponse(data []byte, ctx *controller.ClientContext) bool {
	if wire.IsBatch(data) {
		return suppressBatchPublishResponse(data, ctx)
	}
	return suppressPublishResponse(string(data), ctx)
}

type connectionLimiter struct {
	slots chan struct{}
}

type limitedConnection struct {
	net.Conn
	release   func()
	closeOnce sync.Once
	closeErr  error
}

func newLimitedConnection(conn net.Conn, release func()) net.Conn {
	return &limitedConnection{Conn: conn, release: release}
}

func (c *limitedConnection) Close() error {
	c.closeOnce.Do(func() {
		if c.Conn != nil {
			c.closeErr = c.Conn.Close()
		}
		if c.release != nil {
			c.release()
		}
	})
	return c.closeErr
}

func newConnectionLimiter(limit int) *connectionLimiter {
	if limit <= 0 {
		limit = 1
	}
	return &connectionLimiter{slots: make(chan struct{}, limit)}
}

func (l *connectionLimiter) Acquire(ctx context.Context) error {
	if l == nil {
		return fmt.Errorf("connection limiter is nil")
	}
	select {
	case l.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *connectionLimiter) Release() {
	if l == nil {
		return
	}
	select {
	case <-l.slots:
	default:
	}
}

func maxClientConnections(cfg *config.Config) int {
	if cfg != nil && cfg.MaxClientConnections > 0 {
		return cfg.MaxClientConnections
	}
	return defaultMaxWorkers
}

func clientIdleTimeout(cfg *config.Config) time.Duration {
	if cfg != nil && cfg.ClientIdleTimeoutMS > 0 {
		return time.Duration(cfg.ClientIdleTimeoutMS) * time.Millisecond
	}
	return defaultIdleTimeout
}

func clientRequestTimeout(cfg *config.Config) time.Duration {
	if cfg != nil && cfg.ClientRequestTimeoutMS > 0 {
		return time.Duration(cfg.ClientRequestTimeoutMS) * time.Millisecond
	}
	return 30 * time.Second
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
		resp = requestDeadlineResponse(resp, ctx)
		if !suppressBatchPublishResponse(data, ctx) {
			writeResponse(conn, decorateServerResponse(resp, ctx))
		}
		return false, nil
	}

	rawInput, isRawCommand := parseRawTextCommand(data)
	if strings.HasPrefix(strings.ToUpper(rawInput), "INTERNAL_BATCH ") {
		return handleInternalBatchMessage(rawInput, cmdHandler, ctx, conn)
	}
	if isRawCommand {
		if resp := authorizeInternalListenerCommand(rawInput, cmdHandler, ctx); resp != "" {
			writeResponse(conn, resp)
			return false, nil
		}
		return handleCommandMessage(rawInput, cmdHandler, ctx, conn)
	}

	util.Debug("[%s] Received unrecognized input (len=%d)", conn.RemoteAddr().String(), len(rawInput))
	writeResponse(conn, decorateServerResponse("ERROR: malformed_input reason=command_payload_required", ctx))
	return true, nil
}

func parseRawTextCommand(data []byte) (string, bool) {
	rawInput := strings.Trim(string(data), " \t\n\r")
	return rawInput, isCommand(rawInput)
}

func authorizeInternalListenerCommand(payload string, cmdHandler *controller.CommandHandler, ctx *controller.ClientContext) string {
	if ctx == nil || !ctx.Internal || cmdHandler == nil || cmdHandler.Config == nil {
		return ""
	}
	if cmdHandler.Config.InternalUseTLS {
		return ""
	}
	activeToken := strings.TrimSpace(cmdHandler.Config.InternalAuthToken)
	if activeToken == "" {
		return "ERROR: internal_auth_not_configured command=INTERNAL_LISTENER"
	}
	supplied := parseInternalCommandArgs(payload)["internal_token"]
	activeMatch := subtle.ConstantTimeCompare([]byte(supplied), []byte(activeToken)) == 1
	nextMatch := cmdHandler.Config.InternalAuthTokenNext != "" && subtle.ConstantTimeCompare([]byte(supplied), []byte(cmdHandler.Config.InternalAuthTokenNext)) == 1
	if !activeMatch && !nextMatch {
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
	writeResponse(conn, requestDeadlineResponse(resp, ctx))
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
	resp := cmdHandler.HandleCommand(payload, ctx)
	if resp == controller.BROWSE_DATA_SIGNAL {
		cmdHandler.HandleBrowseMessagesCommand(conn, payload)
		return false, nil
	}
	if resp == controller.STREAM_HISTORY_DATA_SIGNAL {
		cmdHandler.HandleReadStreamHistoryCommand(conn, payload)
		return false, nil
	}
	if resp == controller.STREAM_DATA_SIGNAL {
		switch {
		case strings.HasPrefix(strings.ToUpper(payload), "STREAM "):
			if err := cmdHandler.HandleStreamCommand(conn, payload, ctx); err != nil {
				if errors.Is(err, controller.ErrStreamRejected) {
					return false, nil
				}
				writeResponse(conn, commandErrorResponse(err, ctx))
				return false, nil
			}
			return true, nil
		case strings.HasPrefix(strings.ToUpper(payload), "CONSUME "):
			if _, err := cmdHandler.HandleConsumeCommand(conn, payload, ctx); err != nil {
				writeResponse(conn, commandErrorResponse(err, ctx))
			}
			return false, nil
		case strings.HasPrefix(strings.ToUpper(payload), "READ_STREAM "):
			cmdHandler.HandleReadStreamCommand(conn, payload)
			return false, nil
		default:
			writeResponse(conn, decorateServerResponse("ERROR: unknown_command", ctx))
			return false, nil
		}
	}
	if resp == "" {
		resp = "ERROR: empty_command_response"
	}
	resp = requestDeadlineResponse(resp, ctx)
	if !suppressPublishResponse(payload, ctx) {
		writeResponse(conn, decorateServerResponse(resp, ctx))
	}
	return false, nil
}

func requestDeadlineResponse(response string, ctx *controller.ClientContext) string {
	if ctx != nil && errors.Is(ctx.RequestContext().Err(), context.DeadlineExceeded) && response == "ERROR: request_cancelled" {
		return "ERROR: request_timeout outcome=not_accepted"
	}
	return response
}

func suppressPublishResponse(payload string, ctx *controller.ClientContext) bool {
	if ctx != nil && ctx.Internal {
		return false
	}
	requestHeader := payload
	if messageIndex := strings.Index(requestHeader, "message="); messageIndex >= 0 {
		requestHeader = requestHeader[:messageIndex]
	}
	fields := strings.Fields(strings.TrimSpace(requestHeader))
	if len(fields) == 0 || !strings.EqualFold(fields[0], "PUBLISH") {
		return false
	}
	for _, field := range fields[1:] {
		key, value, ok := strings.Cut(field, "=")
		if !ok || key != "acks" {
			continue
		}
		selection, err := ackpolicy.Parse(value)
		return err == nil && selection.Mode == ackpolicy.None
	}
	return false
}

func suppressBatchPublishResponse(data []byte, ctx *controller.ClientContext) bool {
	if ctx != nil && ctx.Internal {
		return false
	}
	batch, err := util.DecodeBatchMessages(data)
	if err != nil {
		return false
	}
	selection, err := ackpolicy.Parse(batch.Acks)
	return err == nil && selection.Mode == ackpolicy.None
}

func commandErrorResponse(err error, ctx *controller.ClientContext) string {
	resp := err.Error()
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		resp = "ERROR: request_timeout outcome=not_accepted"
	case errors.Is(err, context.Canceled):
		resp = "ERROR: request_cancelled"
	}
	if !wireprotocol.IsErrorResponse(resp) {
		resp = fmt.Sprintf("ERROR: command_failed reason=%q", resp)
	}
	return decorateServerResponse(resp, ctx)
}

func decorateServerResponse(resp string, ctx *controller.ClientContext) string {
	return wireprotocol.EnrichErrorResponse(resp)
}

// isBatchMessage checks if the data is in binary batch format
func isBatchMessage(data []byte) bool {
	return wire.IsBatch(data)
}

func isCommand(s string) bool {
	return wireprotocol.IsTextCommand(s)
}

// writeResponseWithTimeout adds write timeout
func writeResponseWithTimeout(conn net.Conn, msg string, timeout time.Duration) {
	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		util.Error("⚠️ SetWriteDeadline error: %v", err)
		return
	}
	defer func() {
		if err := conn.SetWriteDeadline(time.Time{}); err != nil {
			util.Error("Failed to reset write deadline: %v", err)
		}
	}()

	if err := util.WriteWithLength(conn, []byte(msg)); err != nil {
		util.Error("⚠️ Write response error: %v", err)
	}
}

func writeResponse(conn net.Conn, msg string) {
	if err := util.WriteWithLength(conn, []byte(msg)); err != nil {
		util.Error("⚠️ Write response error: %v", err)
	}
}
