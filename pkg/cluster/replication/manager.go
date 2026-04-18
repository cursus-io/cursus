package replication

import (
	"context"
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
	GetISR(topic string, partition int) []string
	ComputeISR(topic string, partition int) []string
	SetLeader(isLeader bool)
	Start()
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

func NewRaftReplicationManager(ctx context.Context, cfg *config.Config, brokerID string, topicManager *topic.TopicManager, coordinator *coordinator.Coordinator, client client.TCPClusterClient) (*RaftReplicationManager, error) {
	brokerFSM := fsm.NewBrokerFSM(topicManager, coordinator)

	localAddr := fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.RaftPort)
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(brokerID)

	// Raft Security Rule: HeartbeatTimeout must be larger than LeaderLeaseTimeout
	raftCfg.HeartbeatTimeout = 1000 * time.Millisecond
	raftCfg.ElectionTimeout = 2000 * time.Millisecond
	raftCfg.LeaderLeaseTimeout = 800 * time.Millisecond
	raftCfg.CommitTimeout = 50 * time.Millisecond
	raftCfg.LogLevel = "Info"

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
		util.Info("🚀 Raft node %s checking if bootstrap is needed", brokerID)
		if confFuture := r.GetConfiguration(); confFuture.Error() == nil {
			conf := confFuture.Configuration()
			if len(conf.Servers) == 0 {
				util.Info("🚀 No Raft servers found, starting static cluster bootstrap (members=%v)", cfg.StaticClusterMembers)

				var servers []raft.Server
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
						memberID = memberAddr
					}

					util.Info("🔗 Adding Raft voter: ID=%s, Addr=%s", memberID, memberAddr)
					servers = append(servers, raft.Server{
						ID:       raft.ServerID(memberID),
						Address:  raft.ServerAddress(memberAddr),
						Suffrage: raft.Voter,
					})
				}

				if len(servers) > 0 {
					bootstrapConfig := raft.Configuration{Servers: servers}
					if err := r.BootstrapCluster(bootstrapConfig).Error(); err != nil {
						util.Error("❌ Raft bootstrap failed for node %s: %v", brokerID, err)
						return nil, fmt.Errorf("bootstrap failed: %w", err)
					}
					util.Info("✅ Raft cluster bootstrap initiated with %d servers on node %s", len(servers), brokerID)
				}
			} else {
				util.Info("ℹ️ Raft node %s already has %d servers in configuration, skipping bootstrap", brokerID, len(conf.Servers))
			}
		} else {
			util.Error("❌ Failed to get Raft configuration for node %s: %v", brokerID, confFuture.Error())
		}
	}

	rm := &RaftReplicationManager{
		raft:      r,
		fsm:       brokerFSM,
		brokerID:  brokerID,
		localAddr: localAddr,
		peers:     make(map[string]string),
		leaderCh:  make(chan bool, 10),
	}

	rm.isrManager = NewISRManager(ctx, brokerFSM, brokerID, 5*time.Second, rm)
	go rm.isrManager.Start()

	go rm.observeLeadership(notifyCh)

	return rm, nil
}

func (rm *RaftReplicationManager) observeLeadership(notifyCh <-chan bool) {
	for isLeader := range notifyCh {
		rm.isLeader.Store(isLeader)

		if rm.isrManager != nil {
			rm.isrManager.SetLeader(isLeader)
		}

		select {
		case rm.leaderCh <- isLeader:
		default:
		}
	}
}

func (rm *RaftReplicationManager) GetISRManager() ISRManagerInterface {
	return rm.isrManager
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

func (rm *RaftReplicationManager) ReplicateWithQuorum(topic string, partition int, msg types.Message, minISR int, isIdempotent bool, sequenceScope string) (types.AckResponse, error) {
	if rm.isrManager != nil {
		if !rm.isrManager.HasQuorum(topic, partition, minISR) {
			return types.AckResponse{}, fmt.Errorf("insufficient in-sync replicas")
		}
	}

	cmd := types.MessageCommand{
		Topic:         topic,
		Partition:     partition,
		Messages:      []types.Message{msg},
		Acks:          "-1",
		IsIdempotent:  isIdempotent,
		SequenceScope: sequenceScope,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return types.AckResponse{}, err
	}

	return rm.ApplyResponse("MESSAGE", data, 5*time.Second)
}

func (rm *RaftReplicationManager) ReplicateBatchWithQuorum(topic string, partition int, messages []types.Message, minISR int, acks string, isIdempotent bool, sequenceScope string) (types.AckResponse, error) {
	if len(messages) == 0 {
		return types.AckResponse{}, nil
	}

	if rm.isrManager != nil {
		if !rm.isrManager.HasQuorum(topic, partition, minISR) {
			return types.AckResponse{}, fmt.Errorf("insufficient in-sync replicas")
		}
	}

	batchData := types.MessageCommand{
		Topic:         topic,
		Partition:     partition,
		IsIdempotent:  isIdempotent,
		SequenceScope: sequenceScope,
		Messages:      messages,
		Acks:          acks,
	}

	data, err := json.Marshal(batchData)
	if err != nil {
		return types.AckResponse{}, err
	}

	return rm.ApplyResponse("MESSAGE", data, 5*time.Second)
}

func (rm *RaftReplicationManager) ApplyResponse(prefix string, data []byte, timeout time.Duration) (types.AckResponse, error) {
	fullCmd := []byte(fmt.Sprintf("%s:%s", prefix, string(data)))

	future := rm.raft.Apply(fullCmd, timeout)
	if err := future.Error(); err != nil {
		return types.AckResponse{}, err
	}

	response := future.Response()
	if response == nil {
		return types.AckResponse{}, fmt.Errorf("fsm returned nil response")
	}

	resp, ok := response.(types.AckResponse)
	if !ok {
		return types.AckResponse{}, fmt.Errorf("invalid response type")
	}

	return resp, nil
}

func (rm *RaftReplicationManager) Shutdown() error {
	if rm.raft != nil {
		if err := rm.raft.Shutdown().Error(); err != nil {
			return err
		}
	}
	return nil
}
