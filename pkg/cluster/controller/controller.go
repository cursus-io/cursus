package controller

import (
	"context"
	"fmt"
	"time"

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
}

type ClusterController struct {
	RaftManager RaftManager
	Discovery   ServiceDiscovery
	Election    *ControllerElection
	Router      *ClusterRouter
	brokerID    string
}

func NewClusterController(ctx context.Context, cfg *config.Config, rm RaftManager, sd ServiceDiscovery) *ClusterController {
	brokerID := fmt.Sprintf("%s-%d", cfg.AdvertisedHost, cfg.BrokerPort)
	localAddr := fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.BrokerPort)

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
		return cc.IsLeader() // fallback to leader (Raft, not partition)
	}

	partitionKey := fmt.Sprintf("%s-%d", topic, partition)
	meta := fsm.GetPartitionMetadata(partitionKey)
	if meta == nil {
		return cc.IsLeader()
	}

	return meta.Leader == cc.brokerID
}
