package controller

import (
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func voterRegistration(state *fsm.BrokerFSM) fsm.BrokerInfo {
	return fsm.BrokerInfo{ID: "node2", Addr: "localhost:9002", ClientAddr: "localhost:9202", Status: "active", IncarnationID: "new-process", LifecycleProtocol: fsm.BrokerProtocolVersionCurrent, RecoveryProtocol: fsm.BootstrapRecoveryProtocolVersion, TransactionCoordinatorShards: state.TransactionCoordinatorShardCount()}
}

func TestNewVoterRejectsIncompleteRegistrationBeforeMutation(t *testing.T) {
	for _, kind := range []string{"missing", "legacy", "process", "identity", "address", "lifecycle", "shards", "inactive"} {
		t.Run(kind, func(t *testing.T) {
			rm := &ComprehensiveMockRaftManager{isLeader: true, mockFSM: fsm.NewBrokerFSM(nil, nil)}
			rm.On("GetLeaderAddress").Return("leader").Once()
			rm.On("GetConfiguration").Return(staticConfigurationFuture{}).Once()
			registration := voterRegistration(rm.mockFSM)
			info := &registration
			switch kind {
			case "missing":
				info = nil
			case "legacy":
				info.RecoveryProtocol = 0
			case "process":
				info.IncarnationID = ""
			case "identity":
				info.ID = "other"
			case "address":
				info.Addr = "other:9002"
			case "lifecycle":
				info.LifecycleProtocol--
			case "shards":
				info.TransactionCoordinatorShards++
			case "inactive":
				info.Status = "inactive"
			}
			sd := NewServiceDiscoveryImpl(rm, "node1", "localhost:9001", "")
			_, err := sd.AddNodeWithRegistration("node2", "localhost:9002", 0, info)
			require.ErrorContains(t, err, "requires current process registration")
			require.Empty(t, rm.mockFSM.GetBrokers())
			rm.AssertExpectations(t) // Any mutation would be an unexpected mock call.
		})
	}
}

func TestNewVoterFailuresNeverRollbackMembership(t *testing.T) {
	for _, stage := range []string{"registration", "configuration"} {
		t.Run(stage, func(t *testing.T) {
			rm := &ComprehensiveMockRaftManager{isLeader: true, mockFSM: fsm.NewBrokerFSM(nil, nil)}
			rm.On("GetLeaderAddress").Return("leader").Once()
			rm.On("GetConfiguration").Return(staticConfigurationFuture{configuration: raft.Configuration{Servers: []raft.Server{{ID: "node1", Address: "localhost:9001", Suffrage: raft.Voter}}}}).Once()
			failure := errors.New("concurrent change")
			if stage == "registration" {
				rm.On("ApplyCommand", "REGISTER", mock.Anything).Return(failure).Once()
			} else {
				rm.On("ApplyCommand", "REGISTER", mock.Anything).Return(nil).Once()
				rm.On("AddVoterAtIndex", "node2", "localhost:9002", uint64(1)).Return(failure).Once()
			}
			sd := NewServiceDiscoveryImpl(rm, "node1", "localhost:9001", "")
			registration := voterRegistration(rm.mockFSM)
			_, err := sd.AddNodeWithRegistration("node2", "localhost:9002", 0, &registration)
			require.ErrorIs(t, err, failure)
			rm.AssertNotCalled(t, "RemoveServer", mock.Anything)
			rm.AssertExpectations(t)
		})
	}
}
