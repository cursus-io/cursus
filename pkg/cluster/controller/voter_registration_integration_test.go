package controller

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/client"
	"github.com/cursus-io/cursus/pkg/cluster/replication"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestVoterRegistrationBootstrapsRealThreeNodeRaft(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var leader *replication.RaftReplicationManager
	var discovery *serviceDiscovery
	for i := 0; i < 3; i++ {
		cfg := config.DefaultConfig()
		cfg.LogDir = t.TempDir()
		cfg.AdvertisedHost = "127.0.0.1"
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		cfg.RaftPort = listener.Addr().(*net.TCPAddr).Port
		require.NoError(t, listener.Close())
		cfg.BootstrapCluster, cfg.BootstrapSoleVoter = i == 0, i == 0
		id, address := fmt.Sprintf("node-%d", i), fmt.Sprintf("127.0.0.1:%d", cfg.RaftPort)
		manager, err := replication.NewRaftReplicationManager(ctx, cfg, id, nil, nil, client.TCPClusterClient{})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, manager.Shutdown()) })
		if i == 0 {
			leader = manager
			require.Eventually(t, leader.IsLeader, 10*time.Second, 10*time.Millisecond)
			discovery = NewServiceDiscoveryImpl(leader, id, address, address)
			require.NoError(t, discovery.Register())
			continue
		}
		registration := fsm.BrokerInfo{ID: id, Addr: address, ClientAddr: address, Status: "active", IncarnationID: id + "-process", LifecycleProtocol: fsm.BrokerProtocolVersionCurrent, RecoveryProtocol: fsm.BootstrapRecoveryProtocolVersion, TransactionCoordinatorShards: cfg.TransactionCoordinatorShards}
		_, err = discovery.AddNodeWithRegistration(id, address, cfg.TransactionCoordinatorShards, &registration)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return manager.GetFSM().GetBroker(id) != nil }, 10*time.Second, 10*time.Millisecond)
	}
	future := leader.GetConfiguration()
	require.NoError(t, future.Error())
	require.Len(t, future.Configuration().Servers, 3)
	index, err := leader.VoterAdmissionIndex(future.Configuration())
	require.NoError(t, err)
	require.Positive(t, index)
}
