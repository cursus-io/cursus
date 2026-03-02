package replication

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/client"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/hashicorp/raft"
)

type RaftInterface interface {
	Apply([]byte, time.Duration) raft.ApplyFuture
	AddVoter(raft.ServerID, raft.ServerAddress, uint64, time.Duration) raft.IndexFuture
	RemoveServer(raft.ServerID, uint64, time.Duration) raft.IndexFuture
	Leader() raft.ServerAddress
	State() raft.RaftState
	GetConfiguration() raft.ConfigurationFuture
	BootstrapCluster(raft.Configuration) raft.Future
	Shutdown() raft.Future
}

type ISRManagerInterface interface {
	HasQuorum(topic string, partition int, minISR int) bool
	UpdateHeartbeat(brokerID string)
	GetISR() []string
}

type RaftReplicationManager struct {
	raft       RaftInterface
	fsm        *fsm.BrokerFSM
	isrManager ISRManagerInterface

	brokerID  string
	localAddr string
	peers     map[string]string // brokerID -> addr
	mu        sync.RWMutex

	isLeader atomic.Bool
	leaderCh chan bool
}

func NewRaftReplicationManager(cfg *config.Config, brokerID string, diskManager *disk.DiskManager, topicManager *topic.TopicManager, coordinator *coordinator.Coordinator, client client.TCPClusterClient) (*RaftReplicationManager, error) {
	brokerFSM := fsm.NewBrokerFSM(diskManager, topicManager, coordinator)

	localAddr := fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.RaftPort)
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(brokerID)

	raftCfg.ProtocolVersion = raft.ProtocolVersionMax
	raftCfg.HeartbeatTimeout = 500 * time.Millisecond
	raftCfg.ElectionTimeout = 1500 * time.Millisecond
	raftCfg.CommitTimeout = 100 * time.Millisecond
	raftCfg.LogLevel = "Debug"

	notifyCh := make(chan bool, 10)
	raftCfg.NotifyCh = notifyCh

	if len(cfg.StaticClusterMembers) >= 3 {
		raftCfg.PreVoteDisabled = true
	}

	dataDir := filepath.Join(cfg.LogDir, "raft")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		util.Error("Failed to create raft data directory %s: %v", dataDir, err)
		return nil, fmt.Errorf("failed to create raft data directory: %w", err)
	}

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()

	snapshots, err := raft.NewFileSnapshotStore(dataDir, 3, os.Stderr)
	if err != nil {
		util.Error("Failed to create snapshot store: %v", err)
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	advertiseTCPAddr, err := net.ResolveTCPAddr("tcp", localAddr)
	if err != nil {
		util.Error("Failed to resolve advertised address %s: %v", localAddr, err)
		return nil, fmt.Errorf("failed to resolve advertised address: %w", err)
	}

	bindAddr := fmt.Sprintf("0.0.0.0:%d", cfg.RaftPort)
	transport, err := raft.NewTCPTransport(bindAddr, advertiseTCPAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		util.Error("Failed to create raft transport: %v", err)
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	r, err := raft.NewRaft(raftCfg, brokerFSM, logStore, stableStore, snapshots, transport)
	if err != nil {
		util.Error("Failed to create raft instance: %v", err)
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}

	if cfg.BootstrapCluster {
		if confFuture := r.GetConfiguration(); confFuture.Error() == nil {
			conf := confFuture.Configuration()
			if len(conf.Servers) == 0 {
				util.Info("🚀 Starting static cluster bootstrap")

				var servers []raft.Server
				if len(cfg.StaticClusterMembers) == 0 {
					if staticMembers := os.Getenv("STATIC_CLUSTER_MEMBERS"); staticMembers != "" {
						cfg.StaticClusterMembers = strings.Split(staticMembers, ",")
					}
				}

				if len(cfg.StaticClusterMembers) == 0 {
					return nil, fmt.Errorf("STATIC_CLUSTER_MEMBERS is required for static cluster bootstrap")
				}

				for _, member := range cfg.StaticClusterMembers {
					member = strings.TrimSpace(member)
					if member == "" {
						continue
					}

					var memberID, memberAddr string
					if strings.Contains(member, "@") {
						parts := strings.SplitN(member, "@", 2)
						if len(parts) == 2 {
							memberID = parts[0]
							memberAddr = parts[1]
						} else {
							continue
						}
					} else {
						memberAddr = member
						memberID = strings.Split(memberAddr, ":")[0]
					}

					servers = append(servers, raft.Server{
						ID:       raft.ServerID(memberID),
						Address:  raft.ServerAddress(memberAddr),
						Suffrage: raft.Voter,
					})
					util.Debug("Added static cluster member: id=%s addr=%s", memberID, memberAddr)
				}

				if len(servers) == 0 {
					return nil, fmt.Errorf("no valid servers found in StaticClusterMembers")
				}

				bootstrapConfig := raft.Configuration{Servers: servers}
				util.Debug("Static bootstrap configuration: %+v", bootstrapConfig)

				future := r.BootstrapCluster(bootstrapConfig)
				if err := future.Error(); err != nil {
					util.Error("Failed to bootstrap static cluster: %v", err)
					return nil, fmt.Errorf("failed to bootstrap static cluster: %w", err)
				}
				util.Info("✅ Static cluster bootstrap completed")
			}
		}
	} else if len(cfg.RaftPeers) > 0 {
		go func() {
			if err := client.JoinCluster(cfg.RaftPeers, brokerID, localAddr, cfg.DiscoveryPort); err != nil {
				util.Error("Failed to join cluster: %v", err)
			}
		}()
	}

	manager := &RaftReplicationManager{
		raft:      r,
		fsm:       brokerFSM,
		brokerID:  brokerID,
		localAddr: localAddr,
		peers:     make(map[string]string),
		leaderCh:  make(chan bool, 10),
	}

	go manager.observeLeadership(notifyCh)

	if cfg.LogLevel == util.LogLevelDebug {
		go func() {
			ticker := time.NewTicker(1 * time.Minute)
			defer ticker.Stop()
			for range ticker.C {
				state := manager.raft.State()
				leaderAddr := manager.raft.Leader()

				if configFuture := manager.raft.GetConfiguration(); configFuture.Error() == nil {
					raftConf := configFuture.Configuration()
					util.Debug("raft: State=%s, Leader=%s, IsLeader=%v, KnownServers=%d", state.String(), leaderAddr, state == raft.Leader, len(raftConf.Servers))
				} else {
					util.Debug("raft: State=%s, Leader=%s, IsLeader=%v", state.String(), leaderAddr, state == raft.Leader)
				}
			}
		}()
	}

	return manager, nil
}

func (rm *RaftReplicationManager) observeLeadership(notifyCh <-chan bool) {
	for isLeader := range notifyCh {
		rm.isLeader.Store(isLeader)

		select {
		case rm.leaderCh <- isLeader:
			util.Debug("Leadership notification sent to leaderCh")
		default:
			util.Warn("Leadership notification dropped: leaderCh is full. State is still updated to %v", isLeader)
		}
	}
}

func (rm *RaftReplicationManager) IsLeader() bool {
	return rm.isLeader.Load()
}

func (rm *RaftReplicationManager) LeaderCh() <-chan bool {
	return rm.leaderCh
}

func (rm *RaftReplicationManager) GetLeaderAddress() string {
	return string(rm.raft.Leader())
}

func (rm *RaftReplicationManager) GetFSM() *fsm.BrokerFSM {
	return rm.fsm
}

func (rm *RaftReplicationManager) GetConfiguration() raft.ConfigurationFuture {
	return rm.raft.GetConfiguration()
}

func (rm *RaftReplicationManager) ApplyCommand(prefix string, data []byte) error {
	fullCmd := []byte(fmt.Sprintf("%s:%s", prefix, string(data)))
	future := rm.raft.Apply(fullCmd, 5*time.Second)
	return future.Error()
}

func (rm *RaftReplicationManager) AddVoter(id string, addr string) error {
	util.Info("Adding voter %s at %s", id, addr)
	future := rm.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 10*time.Second)
	if err := future.Error(); err != nil {
		return err
	}

	rm.mu.Lock()
	rm.peers[id] = addr
	rm.mu.Unlock()
	return nil
}

func (rm *RaftReplicationManager) RemoveServer(id string) error {
	future := rm.raft.RemoveServer(raft.ServerID(id), 0, 10*time.Second)
	if err := future.Error(); err == nil {
		rm.mu.Lock()
		delete(rm.peers, id)
		rm.mu.Unlock()
	}
	return future.Error()
}

// ReplicateWithQuorum processes a single message, ensuring required ISR count is met.
func (rm *RaftReplicationManager) ReplicateWithQuorum(topic string, partition int, msg types.Message, minISR int) (types.AckResponse, error) {
	util.Debug("Replicating with quorum for topic %s partition %d (min ISR: %d)", topic, partition, minISR)

	if rm.isrManager != nil {
		if !rm.isrManager.HasQuorum(topic, partition, minISR) {
			metrics.QuorumOperations.WithLabelValues("write", "failure").Inc()
			util.Error("Insufficient in-sync replicas for topic %s partition %d (min ISR: %d)", topic, partition, minISR)
			return types.AckResponse{}, fmt.Errorf("not enough in-sync replicas for topic %s partition %d but min ISR %d", topic, partition, minISR)
		}
	}

	data, err := json.Marshal(msg)
	if err != nil {
		util.Error("Failed to marshal message for quorum replication: %v", err)
		return types.AckResponse{}, fmt.Errorf("failed to marshal message: %w", err)
	}

	future := rm.raft.Apply([]byte(fmt.Sprintf("MESSAGE:%s", string(data))), 5*time.Second)
	if err := future.Error(); err != nil {
		metrics.QuorumOperations.WithLabelValues("write", "failure").Inc()
		util.Error("Failed to replicate with quorum for topic %s partition %d: %v", topic, partition, err)
		return types.AckResponse{}, fmt.Errorf("failed to replicate with quorum: %w", err)
	}

	ackResponse, ok := future.Response().(types.AckResponse)
	if !ok {
		metrics.QuorumOperations.WithLabelValues("write", "failure").Inc()
		return types.AckResponse{}, fmt.Errorf("raft FSM returned invalid response type")
	}

	metrics.QuorumOperations.WithLabelValues("write", "success").Inc()
	util.Debug("Successfully replicated with quorum for topic %s partition %d", topic, partition)

	return ackResponse, nil
}

// ReplicateBatchWithQuorum processes a batch of messages, ensuring they are replicated
func (rm *RaftReplicationManager) ReplicateBatchWithQuorum(topic string, partition int, messages []types.Message, minISR int, acks string) (types.AckResponse, error) {
	if len(messages) == 0 {
		return types.AckResponse{}, nil
	}

	util.Debug("Replicating batch with quorum for topic %s partition %d (min ISR: %d, messages: %d)", topic, partition, minISR, len(messages))

	if rm.isrManager != nil {
		if !rm.isrManager.HasQuorum(topic, partition, minISR) {
			metrics.QuorumOperations.WithLabelValues("batch_write", "failure").Inc()
			util.Error("Insufficient in-sync replicas for batch on topic %s partition %d (min ISR: %d)", topic, partition, minISR)
			return types.AckResponse{}, fmt.Errorf("not enough in-sync replicas for topic %s partition %d but min ISR %d", topic, partition, minISR)
		}
	}

	batchStart := messages[0].SeqNum
	batchEnd := messages[len(messages)-1].SeqNum

	if acks == "" {
		acks = "-1"
	}

	batchData := types.Batch{
		Topic:      topic,
		Partition:  partition,
		BatchStart: batchStart,
		BatchEnd:   batchEnd,
		Acks:       acks,
		Messages:   messages,
	}

	data, err := json.Marshal(batchData)
	if err != nil {
		util.Error("Failed to marshal batch messages for quorum replication: %v", err)
		return types.AckResponse{}, fmt.Errorf("failed to marshal batch messages: %w", err)
	}

	future := rm.raft.Apply([]byte(fmt.Sprintf("BATCH:%s", string(data))), 5*time.Second)
	if err := future.Error(); err != nil {
		metrics.QuorumOperations.WithLabelValues("batch_write", "failure").Inc()
		util.Error("Failed to replicate batch with quorum for topic %s partition %d: %v", topic, partition, err)
		return types.AckResponse{}, fmt.Errorf("failed to replicate batch with quorum: %w", err)
	}

	ackResponse, ok := future.Response().(types.AckResponse)
	if !ok {
		metrics.QuorumOperations.WithLabelValues("batch_write", "failure").Inc()
		return types.AckResponse{}, fmt.Errorf("raft FSM returned invalid response type for batch")
	}

	metrics.QuorumOperations.WithLabelValues("batch_write", "success").Inc()
	util.Debug("Successfully replicated batch with quorum for topic %s partition %d (Count: %d)", topic, partition, len(messages))
	return ackResponse, nil
}

func (rm *RaftReplicationManager) ApplyResponse(prefix string, data []byte, timeout time.Duration) (types.AckResponse, error) {
	fullCmd := []byte(fmt.Sprintf("%s:%s", prefix, string(data)))

	future := rm.raft.Apply(fullCmd, timeout)
	if err := future.Error(); err != nil {
		util.Error("Raft apply future error: %v", err)
		return types.AckResponse{}, err
	}

	response := future.Response()
	if response == nil {
		return types.AckResponse{}, fmt.Errorf("raft FSM returned nil response")
	}

	resp, ok := response.(types.AckResponse)
	if !ok {
		metrics.QuorumOperations.WithLabelValues("write", "failure").Inc()
		util.Error("FSM returned unexpected type: %T (expected types.AckResponse)", response)
		return types.AckResponse{}, fmt.Errorf("invalid response type from FSM")
	}

	metrics.QuorumOperations.WithLabelValues("write", "success").Inc()
	return resp, nil
}

func (rm *RaftReplicationManager) Shutdown() error {
	if rm.raft != nil {
		if err := rm.raft.Shutdown().Error(); err != nil {
			util.Error("Failed to shutdown raft: %v", err)
			return err
		}
	}
	util.Info("Successfully shutdown RaftReplicationManager")
	return nil
}
