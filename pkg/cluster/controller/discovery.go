package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/util"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
)

type ServiceDiscovery interface {
	Register() error
	Deregister() error
	DiscoverBrokers() ([]fsm.BrokerInfo, error)
	AddNode(nodeID string, addr string) (string, error)
	RemoveNode(nodeID string) (string, error)
	UpdateHeartbeat(nodeID string)
	HandleHeartbeat(nodeID string, proofs []fsm.ISRCatchupProof) error
	FetchReplicaCatchup(request fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error)
	StartReconciler(ctx context.Context)
	Reconcile()
}

type serviceDiscovery struct {
	rm               RaftManager
	fsm              *fsm.BrokerFSM
	brokerID         string
	addr             string
	clientAddr       string
	incarnationID    string
	heartbeatTimeout time.Duration

	livenessMu  sync.RWMutex
	lastSeen    map[string]time.Time
	leaderSince time.Time
}

func NewServiceDiscoveryImpl(rm RaftManager, brokerID, addr, clientAddr string) *serviceDiscovery {
	sd := &serviceDiscovery{
		rm:               rm,
		brokerID:         brokerID,
		addr:             addr,
		clientAddr:       clientAddr,
		incarnationID:    uuid.NewString(),
		heartbeatTimeout: 5 * time.Second,
		lastSeen:         make(map[string]time.Time),
	}
	if rm != nil {
		sd.fsm = rm.GetFSM()
	}
	return sd
}

// BrokerIncarnationID is carried by leader-directed heartbeats. It is process
// local, while its assigned epoch is part of the replicated BrokerInfo.
func (sd *serviceDiscovery) BrokerIncarnationID() string { return sd.incarnationID }

func NewServiceDiscovery(rm RaftManager, brokerID, addr, clientAddr string) ServiceDiscovery {
	return NewServiceDiscoveryImpl(rm, brokerID, addr, clientAddr)
}

func (sd *serviceDiscovery) Register() error {
	broker := &fsm.BrokerInfo{
		ID:                           sd.brokerID,
		Addr:                         sd.addr,
		ClientAddr:                   sd.clientAddr,
		Status:                       "active",
		LastSeen:                     time.Now(),
		IncarnationID:                sd.incarnationID,
		TransactionCoordinatorShards: sd.transactionCoordinatorShardCount(),
		LifecycleProtocol:            fsm.BrokerProtocolVersionCurrent,
	}

	data, err := json.Marshal(broker)
	if err != nil {
		util.Error("Failed to marshal broker info: %v", err)
		return fmt.Errorf("marshal broker info: %w", err)
	}

	if err := sd.rm.ApplyCommand("REGISTER", data); err != nil {
		util.Error("Failed to register broker %s: %v", sd.brokerID, err)
		return err
	}

	util.Info("Successfully registered broker %s", sd.brokerID)
	return nil
}

func (sd *serviceDiscovery) Deregister() error {
	if sd.fsm == nil {
		return fmt.Errorf("broker_deregistration_fenced broker=%s reason=fsm_unavailable", sd.brokerID)
	}
	broker := sd.fsm.GetBroker(sd.brokerID)
	if broker == nil {
		return fmt.Errorf("broker_deregistration_fenced broker=%s reason=registration_not_found", sd.brokerID)
	}
	if broker.IncarnationID != "" && broker.IncarnationID != sd.incarnationID {
		return fmt.Errorf("broker_deregistration_fenced broker=%s reason=incarnation_mismatch", sd.brokerID)
	}
	data, err := marshalBrokerDeregistration(broker)
	if err != nil {
		util.Error("Failed to marshal payload: %v", err)
		return fmt.Errorf("marshal payload: %w", err)
	}
	if err := sd.rm.ApplyCommand("DEREGISTER", data); err != nil {
		util.Error("Failed to deregister broker %s: %v", sd.brokerID, err)
		return err
	}

	util.Info("Successfully deregistered broker %s via Raft", sd.brokerID)
	return nil
}

func (sd *serviceDiscovery) DiscoverBrokers() ([]fsm.BrokerInfo, error) {
	brokers := sd.fsm.GetBrokers()
	return brokers, nil
}

func (sd *serviceDiscovery) UpdateHeartbeat(nodeID string) {
	sd.UpdateHeartbeatWithIncarnation(nodeID, "")
}

// ValidateHeartbeat rejects stale broker processes before they can refresh ISR
// liveness or submit catch-up proofs.
func (sd *serviceDiscovery) ValidateHeartbeat(nodeID, incarnationID string) error {
	if sd.rm == nil || !sd.rm.IsLeader() || sd.fsm == nil {
		return nil
	}
	broker := sd.fsm.GetBroker(nodeID)
	if broker == nil {
		return fmt.Errorf("broker_heartbeat_fenced broker=%s reason=registration_not_found", nodeID)
	}
	if broker.IncarnationID != "" && incarnationID != broker.IncarnationID {
		return fmt.Errorf("broker_heartbeat_fenced broker=%s reason=incarnation_mismatch", nodeID)
	}
	return nil
}

// UpdateHeartbeatWithIncarnation accepts liveness only on the Raft leader.
// Followers must not create a second membership view from their local socket
// observations. The resulting active/inactive transition is still committed
// through Raft before it becomes visible to coordinator routing.
func (sd *serviceDiscovery) UpdateHeartbeatWithIncarnation(nodeID, incarnationID string) {
	if err := sd.ValidateHeartbeat(nodeID, incarnationID); err != nil {
		util.Warn("Ignoring fenced heartbeat from broker %s: %v", nodeID, err)
		return
	}
	if sd.rm != nil {
		if manager := sd.rm.GetISRManager(); manager != nil {
			manager.UpdateHeartbeat(nodeID)
		}
	}
	if sd.rm == nil || !sd.rm.IsLeader() || sd.fsm == nil {
		return
	}
	broker := sd.fsm.GetBroker(nodeID)
	if broker == nil {
		return
	}
	if broker.IncarnationID != "" && incarnationID != broker.IncarnationID {
		util.Warn("Ignoring fenced heartbeat from broker %s", nodeID)
		return
	}
	sd.livenessMu.Lock()
	sd.lastSeen[nodeID] = time.Now()
	sd.livenessMu.Unlock()
	if broker.Status == "active" {
		return
	}
	broker.Status = "active"
	broker.LastSeen = time.Now()
	broker.TransactionCoordinatorShards = sd.transactionCoordinatorShardCount()
	data, err := json.Marshal(broker)
	if err == nil {
		if err := sd.rm.ApplyCommand("REGISTER", data); err != nil {
			util.Warn("Failed to reactivate broker %s after heartbeat: %v", nodeID, err)
		}
	}
}

func (sd *serviceDiscovery) HandleHeartbeat(nodeID string, proofs []fsm.ISRCatchupProof) error {
	if sd.rm == nil || sd.rm.GetISRManager() == nil {
		return nil
	}
	manager := sd.rm.GetISRManager()
	return manager.SubmitCatchupProofs(nodeID, proofs)
}

func (sd *serviceDiscovery) FetchReplicaCatchup(request fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
	if sd.fsm == nil {
		return fsm.ReplicaCatchupBatch{}, fmt.Errorf("FSM is unavailable")
	}
	metadata := sd.fsm.GetPartitionMetadata(fmt.Sprintf("%s-%d", request.Topic, request.Partition))
	if metadata == nil {
		return fsm.ReplicaCatchupBatch{}, fmt.Errorf("partition metadata not found")
	}
	sourceBroker := request.SourceBroker
	if sourceBroker == "" {
		sourceBroker = request.Leader
	}
	if sourceBroker != sd.brokerID {
		return fsm.ReplicaCatchupBatch{}, fmt.Errorf("broker %s is not selected catch-up source %s", sd.brokerID, sourceBroker)
	}
	return sd.fsm.FetchReplicaCatchup(request)
}

func (sd *serviceDiscovery) AddNode(nodeID string, addr string) (string, error) {
	return sd.AddNodeWithTransactionCoordinatorShards(nodeID, addr, sd.transactionCoordinatorShardCount())
}

func (sd *serviceDiscovery) AddNodeWithTransactionCoordinatorShards(nodeID string, addr string, shardCount int) (string, error) {
	leaderAddr := sd.rm.GetLeaderAddress()
	if !sd.rm.IsLeader() {
		return leaderAddr, fmt.Errorf("not leader; contact leader at %s", leaderAddr)
	}
	clusterShardCount := sd.fsm.TransactionCoordinatorShardCount()
	if shardCount == 0 {
		shardCount = transaction.DefaultCoordinatorShardCount
	}
	if shardCount != clusterShardCount {
		return leaderAddr, fmt.Errorf("transaction coordinator shard count mismatch: broker=%s configured=%d cluster=%d", nodeID, shardCount, clusterShardCount)
	}
	future := sd.rm.GetConfiguration()
	if err := future.Error(); err != nil {
		return leaderAddr, fmt.Errorf("get raft configuration: %w", err)
	}
	for _, server := range future.Configuration().Servers {
		if string(server.ID) != nodeID || server.Suffrage != raft.Voter {
			continue
		}
		if string(server.Address) != addr {
			return leaderAddr, fmt.Errorf("broker %s already belongs to raft at %s, not %s", nodeID, server.Address, addr)
		}
		return leaderAddr, nil
	}

	if err := sd.rm.AddVoter(nodeID, addr); err != nil {
		util.Error("Failed to add Raft voter: %v", err)
		return leaderAddr, err
	}

	broker := &fsm.BrokerInfo{
		ID:                           nodeID,
		Addr:                         addr,
		Status:                       "active",
		LastSeen:                     time.Now(),
		TransactionCoordinatorShards: shardCount,
	}

	data, err := json.Marshal(broker)
	if err != nil {
		util.Error("Marshal failed after AddVoter. Node added to Raft but not to FSM: id=%s err=%v", nodeID, err)
		return leaderAddr, fmt.Errorf("marshal failed after AddVoter: %w", err)
	}

	if err := sd.rm.ApplyCommand("REGISTER", data); err != nil {
		util.Error("REGISTER command failed after AddVoter. Attempting rollback: id=%s err=%v", nodeID, err)
		if rollbackErr := sd.rm.RemoveServer(nodeID); rollbackErr != nil {
			util.Error("CRITICAL: Rollback RemoveServer failed after REGISTER failure: id=%s err=%v", nodeID, rollbackErr)
			return leaderAddr, fmt.Errorf("REGISTER failed and rollback failed: %v (rollback error: %v)", err, rollbackErr)
		} else {
			util.Info("Successfully rolled back AddVoter for node %s", nodeID)
		}
		return leaderAddr, fmt.Errorf("REGISTER command failed (rolled back): %w", err)
	}

	return leaderAddr, nil
}

func (sd *serviceDiscovery) RemoveNode(nodeID string) (string, error) {
	leaderAddr := sd.rm.GetLeaderAddress()

	if !sd.rm.IsLeader() {
		return leaderAddr, fmt.Errorf("not leader")
	}

	if err := sd.rm.RemoveServer(nodeID); err != nil {
		return leaderAddr, err
	}

	data, err := marshalBrokerDeregistration(sd.fsm.GetBroker(nodeID))
	if err != nil {
		util.Error("Failed to marshal payload: %v", err)
		return leaderAddr, fmt.Errorf("marshal payload: %w", err)
	}
	if err := sd.rm.ApplyCommand("DEREGISTER", data); err != nil {
		util.Error("DEREGISTER failed after RemoveServer. FSM contains stale node info: id=%s err=%v", nodeID, err)
		return leaderAddr, fmt.Errorf("DEREGISTER failed: %w", err)
	}

	return leaderAddr, nil
}

func (sd *serviceDiscovery) StartReconciler(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	go func() {
		defer ticker.Stop()
		util.Debug("reconciler started for broker %s", sd.brokerID)

		for {
			select {
			case <-ticker.C:
				if !sd.rm.IsLeader() {
					continue
				}
				sd.ensureClientAddrs()
				sd.Reconcile()
			case <-ctx.Done():
				util.Debug("reconciler stopping for broker %s due to context cancellation", sd.brokerID)
				return
			}
		}
	}()
}

// ensureClientAddrs re-registers self if ClientAddr is missing in FSM.
// Only called on the leader.
func (sd *serviceDiscovery) ensureClientAddrs() {
	if sd.fsm == nil || sd.clientAddr == "" {
		return
	}
	if self := sd.fsm.GetBroker(sd.brokerID); self != nil && self.ClientAddr == "" {
		if err := sd.Register(); err != nil {
			util.Debug("ensureClientAddrs: self re-register failed: %v", err)
		}
	}
}

func (sd *serviceDiscovery) Reconcile() {
	if sd.rm == nil || sd.fsm == nil {
		return
	}
	if !sd.rm.IsLeader() {
		sd.livenessMu.Lock()
		sd.leaderSince = time.Time{}
		sd.livenessMu.Unlock()
		return
	}
	sd.livenessMu.Lock()
	if sd.leaderSince.IsZero() {
		sd.leaderSince = time.Now()
		sd.lastSeen[sd.brokerID] = sd.leaderSince
	}
	sd.livenessMu.Unlock()
	future := sd.rm.GetConfiguration()
	if err := future.Error(); err != nil {
		util.Error("Failed to get Raft configuration: %v", err)
		return
	}
	raftServers := future.Configuration().Servers

	raftMap := make(map[string]string)
	for _, s := range raftServers {
		raftMap[string(s.ID)] = string(s.Address)
	}

	fsmBrokers := sd.fsm.GetBrokers()
	fsmMap := make(map[string]bool)

	for _, b := range fsmBrokers {
		fsmMap[b.ID] = true
		if b.Status == "active" && !sd.brokerAlive(b.ID) {
			util.Warn("Broker %s heartbeat expired; marking inactive", b.ID)
			data, err := marshalBrokerDeregistration(&b)
			if err == nil {
				if err := sd.rm.ApplyCommand("DEREGISTER", data); err != nil {
					util.Error("Failed to mark broker %s inactive: %v", b.ID, err)
				}
			}
			continue
		}
		if _, exists := raftMap[b.ID]; !exists {
			util.Warn("Node %s found in FSM but missing in Raft. Cleaning up...", b.ID)
			data, err := marshalBrokerDeregistration(&b)
			if err != nil {
				util.Error("Failed to marshal payload: %v", err)
				continue
			}
			if err := sd.rm.ApplyCommand("DEREGISTER", data); err != nil {
				util.Error("Failed to apply DEREGISTER for node %s: %v", b.ID, err)
			} else {
				util.Info("Successfully removed stale node %s from FSM", b.ID)
			}
		}
	}

	for id, addr := range raftMap {
		if !fsmMap[id] {
			util.Warn("Node %s found in Raft but missing in FSM. Repairing...", id)
			broker := &fsm.BrokerInfo{
				ID:                           id,
				Addr:                         addr,
				Status:                       "active",
				LastSeen:                     time.Now(),
				TransactionCoordinatorShards: sd.transactionCoordinatorShardCount(),
			}
			if id == sd.brokerID && sd.clientAddr != "" {
				broker.ClientAddr = sd.clientAddr
			}
			data, err := json.Marshal(broker)
			if err != nil {
				util.Error("Failed to marshal broker info for node %s: %v", id, err)
				continue
			}
			if err := sd.rm.ApplyCommand("REGISTER", data); err != nil {
				util.Error("Failed to apply REGISTER repair for node %s: %v", id, err)
			} else {
				util.Info("Successfully repaired FSM for node %s", id)
			}
		}
	}
}

func (sd *serviceDiscovery) transactionCoordinatorShardCount() int {
	if sd.fsm == nil {
		return 0
	}
	return sd.fsm.ConfiguredTransactionCoordinatorShardCount()
}

func (sd *serviceDiscovery) brokerAlive(brokerID string) bool {
	if brokerID == sd.brokerID {
		return true
	}
	sd.livenessMu.RLock()
	defer sd.livenessMu.RUnlock()
	if !sd.leaderSince.IsZero() && time.Since(sd.leaderSince) <= sd.heartbeatTimeout {
		return true
	}
	lastSeen, ok := sd.lastSeen[brokerID]
	return ok && time.Since(lastSeen) <= sd.heartbeatTimeout
}

func marshalBrokerDeregistration(broker *fsm.BrokerInfo) ([]byte, error) {
	if broker == nil || broker.ID == "" {
		return nil, fmt.Errorf("broker deregistration requires current broker metadata")
	}
	payload := struct {
		ID               string `json:"id"`
		IncarnationID    string `json:"incarnation_id,omitempty"`
		IncarnationEpoch uint64 `json:"incarnation_epoch,omitempty"`
	}{
		ID:               broker.ID,
		IncarnationID:    broker.IncarnationID,
		IncarnationEpoch: broker.IncarnationEpoch,
	}
	return json.Marshal(payload)
}
