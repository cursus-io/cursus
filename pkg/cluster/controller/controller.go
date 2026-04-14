package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/hashicorp/raft"
)

type RaftManager interface {
	IsLeader() bool
	GetLeaderAddress() string
	ApplyCommand(prefix string, data []byte) error
	LeaderCh() <-chan bool
	GetFSM() *fsm.BrokerFSM
	GetConfiguration() raft.ConfigurationFuture
	ReplicateWithQuorum(topic string, partition int, msg types.Message, minISR int, isIdempotent bool, sequenceScope string) (types.AckResponse, error)
	ReplicateBatchWithQuorum(topic string, partition int, messages []types.Message, minISR int, acks string, isIdempotent bool, sequenceScope string) (types.AckResponse, error)
	ApplyResponse(prefix string, data []byte, timeout time.Duration) (types.AckResponse, error)
	AddVoter(id string, addr string) error
	RemoveServer(id string) error
	GetISRManager() replication.ISRManagerInterface
}

type ClusterController struct {
	RaftManager RaftManager
	Discovery   ServiceDiscovery
	Election    *ControllerElection
	Router      *ClusterRouter
	brokerID    string
}

func NewClusterController(ctx context.Context, cfg *config.Config, rm RaftManager, sd ServiceDiscovery, brokerID, localAddr string) *ClusterController {
	cc := &ClusterController{
		RaftManager: rm,
		Discovery:   sd,
		Election:    NewControllerElection(rm),
		Router:      NewClusterRouter(brokerID, localAddr, nil, rm, cfg.BrokerPort),
		brokerID:    brokerID,
	}

	return cc
}

func (cc *ClusterController) Start(ctx context.Context) {
	cc.Election.Start()
	cc.Discovery.StartReconciler(ctx)
}

func (cc *ClusterController) SetLocalProcessor(lp LocalProcessor) {
	if lp == nil {
		util.Warn("LocalProcessor is nil, ignoring")
		return
	}
	if cc.Router != nil {
		cc.Router.localProcessor = lp
	}
}

func (cc *ClusterController) GetClusterLeader() (string, error) {
	leader := cc.RaftManager.GetLeaderAddress()
	if leader == "" {
		return "", fmt.Errorf("no cluster leader available")
	}
	return leader, nil
}

func (cc *ClusterController) JoinNewBroker(id, addr string) error {
	_, err := cc.Discovery.AddNode(id, addr)
	return err
}

func (cc *ClusterController) IsLeader() bool {
	if cc.RaftManager != nil {
		return cc.RaftManager.IsLeader()
	}
	util.Warn("RaftManager is nil, assuming non-leader state")
	return false
}

func (cc *ClusterController) IsAuthorized(topic string, partition int) bool {
	if cc.RaftManager == nil {
		return false
	}

	fsm := cc.RaftManager.GetFSM()
	if fsm == nil {
		return false
	}

	partitionKey := fmt.Sprintf("%s-%d", topic, partition)
	meta := fsm.GetPartitionMetadata(partitionKey)
	if meta == nil {
		return false
	}

	return meta.Leader == cc.brokerID
}

func (cc *ClusterController) ReplicateToFollowers(topic string, partition int, msgCmd types.MessageCommand, minISR int) error {
	fsm := cc.RaftManager.GetFSM()
	if fsm == nil {
		return fmt.Errorf("FSM not available")
	}

	partitionKey := fmt.Sprintf("%s-%d", topic, partition)
	meta := fsm.GetPartitionMetadata(partitionKey)
	if meta == nil {
		return fmt.Errorf("partition metadata not found")
	}

	followers := []string{}
	for _, replica := range meta.ISR {
		if replica != cc.brokerID {
			followers = append(followers, replica)
		}
	}

	if len(meta.ISR) < minISR {
		return fmt.Errorf("insufficient in-sync replicas: have %d, want %d", len(meta.ISR), minISR)
	}

	if len(followers) == 0 {
		return nil
	}

	data, err := json.Marshal(msgCmd)
	if err != nil {
		return err
	}
	replicateCmd := fmt.Sprintf("REPLICATE_MESSAGE payload=%s", string(data))

	var wg sync.WaitGroup
	errCh := make(chan error, len(followers))

	for _, followerID := range followers {
		follower := fsm.GetBroker(followerID)
		if follower == nil {
			continue
		}

		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			_, err := cc.Router.forwardWithTimeout(addr, replicateCmd)
			if err != nil {
				errCh <- err
			}
		}(follower.Addr)
	}

	wg.Wait()
	close(errCh)

	// In a real Kafka-like system, we might tolerate some follower failures as long as we have minISR.
	// For now, if acks=-1 was requested (implicit here if calling this), we might want all ISR to succeed.
	// Actually, Kafka only waits for followers in ISR.
	
	// If any error occurred, we should probably return it if we want strict acks.
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}
