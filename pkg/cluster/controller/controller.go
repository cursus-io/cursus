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

	// Fan out to all replicas, not just ISR
	targets := []string{}
	for _, replica := range meta.Replicas {
		if replica != cc.brokerID {
			targets = append(targets, replica)
		}
	}

	data, err := json.Marshal(msgCmd)
	if err != nil {
		return err
	}
	replicateCmd := fmt.Sprintf("REPLICATE_MESSAGE payload=%s", string(data))

	var wg sync.WaitGroup
	var successCount int32 = 1 // Count self (leader)
	var mu sync.Mutex
	errCh := make(chan error, len(targets))

	for _, targetID := range targets {
		broker := fsm.GetBroker(targetID)
		if broker == nil {
			continue
		}

		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			_, err := cc.Router.forwardWithTimeout(addr, replicateCmd)
			if err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()
			} else {
				errCh <- err
			}
		}(broker.Addr)
	}

	wg.Wait()
	close(errCh)

	if int(successCount) < minISR {
		return fmt.Errorf("insufficient successful acknowledgements: got %d, want minISR %d", successCount, minISR)
	}

	return nil
}
