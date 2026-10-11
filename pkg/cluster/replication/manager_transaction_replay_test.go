package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/client"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// Exercise the production constructor without a command handler: Raft must
// recover terminal decisions before client handlers are initialized.
func TestRaftRestartRecoversTransactionDecisionBeforeHandler(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		for _, decision := range []transaction.State{transaction.StateAborted, transaction.StateCommitted} {
			t.Run(fmt.Sprintf("snapshot=%t/%s", snapshot, decision), func(t *testing.T) {
				cfg := config.DefaultConfig()
				cfg.LogDir = t.TempDir()
				cfg.AdvertisedHost = "127.0.0.1"
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				require.NoError(t, err)
				cfg.RaftPort = listener.Addr().(*net.TCPAddr).Port
				require.NoError(t, listener.Close())
				cfg.BootstrapCluster = true
				cfg.BootstrapSoleVoter = true
				cfg.TransactionCoordinatorShards = 7
				cfg.MaxTransactions = 2
				start := func() (*RaftReplicationManager, func()) {
					ctx, cancel := context.WithCancel(context.Background())
					rm, err := NewRaftReplicationManager(ctx, cfg, "node-a", nil, nil, client.TCPClusterClient{})
					if err != nil {
						cancel()
						t.Fatal(err)
					}
					return rm, func() { cancel(); require.NoError(t, rm.Shutdown()) }
				}
				rm, stop := start()
				defer func() {
					if stop != nil {
						stop()
					}
				}()
				require.Eventually(t, rm.IsLeader, 10*time.Second, 10*time.Millisecond)
				require.NoError(t, rm.ApplyCommand("REGISTER", []byte(`{"id":"node-a","addr":"127.0.0.1:7000","status":"active"}`)))
				const id = "restart-decision"
				owner, ok := rm.GetFSM().GetTransactionCoordinator(id)
				require.True(t, ok)
				staging := transaction.NewManager()
				producer, epoch, err := staging.InitProducerWithMode(id, transaction.ModeProcessingV1)
				require.NoError(t, err)
				require.NoError(t, staging.SetCoordinatorEpoch(id, owner.Epoch))
				require.NoError(t, staging.Begin(id, producer, epoch))
				if decision == transaction.StateAborted {
					_, err = staging.PrepareAbort(id, producer, epoch)
				} else {
					_, err = staging.PrepareCommit(id, producer, epoch)
				}
				require.NoError(t, err)
				apply := func(state *transaction.Snapshot) {
					data, err := json.Marshal(map[string]interface{}{"transaction": state, "coordinator_owner": owner.Owner, "coordinator_epoch": owner.Epoch})
					require.NoError(t, err)
					require.NoError(t, rm.ApplyCommand("TXN_SYNC", data))
				}
				prepared, ok := staging.Snapshot(id)
				require.True(t, ok)
				apply(prepared)
				if snapshot {
					require.NoError(t, rm.raft.(*raft.Raft).Snapshot().Error())
				}
				var terminal *transaction.Snapshot
				if decision == transaction.StateAborted {
					terminal, err = staging.BuildAbortedSnapshot(id, producer, epoch)
				} else {
					terminal, err = staging.BuildCommittedSnapshot(id)
				}
				require.NoError(t, err)
				apply(terminal)
				lastApplied := rm.GetFSM().AppliedIndex()
				stop()
				stop = nil

				rm, stop = start()
				require.Eventually(t, func() bool { return rm.GetFSM().AppliedIndex() >= lastApplied }, 10*time.Second, 10*time.Millisecond)
				recovered := rm.GetFSM().GetTransactionManager()
				require.NotNil(t, recovered)
				actual, err := recovered.Status(id)
				require.NoError(t, err)
				require.Equal(t, terminal.State, actual.State)
				require.Equal(t, terminal.Revision, actual.Revision)
				require.Equal(t, terminal.CoordinatorEpoch, actual.CoordinatorEpoch)
				require.Equal(t, terminal.Epoch, actual.Epoch)
				_, _, err = recovered.InitProducerWithMode("second-transaction", transaction.ModeProcessingV1)
				require.NoError(t, err)
				_, _, err = recovered.InitProducerWithMode("over-capacity", transaction.ModeProcessingV1)
				require.ErrorContains(t, err, "max_transactions=2", "early initialization must preserve configured admission limits")
			})
		}
	}
}

func TestThreeNodeRaftRestartReplaysTerminalTransactions(t *testing.T) {
	const size = 3
	var configs [size]*config.Config
	var nodes [size]*RaftReplicationManager
	var cancels [size]context.CancelFunc
	stop := func() {
		for i, node := range nodes {
			if node != nil {
				cancels[i]()
				require.NoError(t, node.Shutdown())
				nodes[i] = nil
			}
		}
	}
	defer stop()
	for i := range configs {
		cfg := config.DefaultConfig()
		cfg.LogDir = t.TempDir()
		cfg.AdvertisedHost = "127.0.0.1"
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		cfg.RaftPort = listener.Addr().(*net.TCPAddr).Port
		require.NoError(t, listener.Close())
		cfg.BootstrapCluster, cfg.BootstrapSoleVoter = i == 0, i == 0
		configs[i] = cfg
		ctx, cancel := context.WithCancel(context.Background())
		cancels[i] = cancel
		nodes[i], err = NewRaftReplicationManager(ctx, cfg, fmt.Sprintf("node-%d", i), nil, nil, client.TCPClusterClient{})
		require.NoError(t, err)
		if i == 0 {
			require.Eventually(t, nodes[0].IsLeader, 10*time.Second, 10*time.Millisecond)
		} else {
			require.NoError(t, nodes[0].AddVoter(fmt.Sprintf("node-%d", i), fmt.Sprintf("127.0.0.1:%d", cfg.RaftPort)))
		}
	}
	for i := range nodes {
		require.NoError(t, nodes[0].ApplyCommand("REGISTER", []byte(fmt.Sprintf(`{"id":"node-%d","addr":"127.0.0.1:%d","status":"active"}`, i, configs[i].RaftPort))))
	}
	waitApplied := func(index uint64) {
		for _, node := range nodes {
			require.Eventually(t, func() bool { return node.GetFSM().AppliedIndex() >= index }, 15*time.Second, 10*time.Millisecond)
		}
	}
	staging := transaction.NewManager()
	terminals := make([]*transaction.Snapshot, 0, 2)
	apply := func(state *transaction.Snapshot) {
		owner, ok := nodes[0].GetFSM().GetTransactionCoordinator(state.ID)
		require.True(t, ok)
		data, err := json.Marshal(map[string]interface{}{"transaction": state, "coordinator_owner": owner.Owner, "coordinator_epoch": owner.Epoch})
		require.NoError(t, err)
		require.NoError(t, nodes[0].ApplyCommand("TXN_SYNC", data))
	}
	for _, decision := range []transaction.State{transaction.StateAborted, transaction.StateCommitted} {
		id := "three-node-restart-" + string(decision)
		owner, ok := nodes[0].GetFSM().GetTransactionCoordinator(id)
		require.True(t, ok)
		producer, epoch, err := staging.InitProducerWithMode(id, transaction.ModeProcessingV1)
		require.NoError(t, err)
		require.NoError(t, staging.SetCoordinatorEpoch(id, owner.Epoch))
		require.NoError(t, staging.Begin(id, producer, epoch))
		if decision == transaction.StateAborted {
			_, err = staging.PrepareAbort(id, producer, epoch)
		} else {
			_, err = staging.PrepareCommit(id, producer, epoch)
		}
		require.NoError(t, err)
		prepared, ok := staging.Snapshot(id)
		require.True(t, ok)
		apply(prepared)
		var terminal *transaction.Snapshot
		if decision == transaction.StateAborted {
			terminal, err = staging.BuildAbortedSnapshot(id, producer, epoch)
		} else {
			terminal, err = staging.BuildCommittedSnapshot(id)
		}
		require.NoError(t, err)
		terminals = append(terminals, terminal)
	}
	waitApplied(nodes[0].GetFSM().AppliedIndex())
	for _, node := range nodes {
		require.NoError(t, node.raft.(*raft.Raft).Snapshot().Error())
	}
	// These terminal decisions exist only in post-snapshot Raft logs.
	for _, terminal := range terminals {
		apply(terminal)
	}
	lastApplied := nodes[0].GetFSM().AppliedIndex()
	waitApplied(lastApplied)
	stop()

	// Every constructor waits for committed replay, so restart the quorum
	// concurrently, with the same durable stores and without any handlers.
	var wg sync.WaitGroup
	var startupErrors [size]error
	for i := range nodes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			cancels[i] = cancel
			nodes[i], startupErrors[i] = NewRaftReplicationManager(ctx, configs[i], fmt.Sprintf("node-%d", i), nil, nil, client.TCPClusterClient{})
		}(i)
	}
	wg.Wait()
	for _, err := range startupErrors {
		require.NoError(t, err)
	}
	waitApplied(lastApplied)
	for i, node := range nodes {
		manager := node.GetFSM().GetTransactionManager()
		require.NotNil(t, manager)
		for _, terminal := range terminals {
			actual, ok := manager.Snapshot(terminal.ID)
			require.True(t, ok)
			expectedJSON, err := json.Marshal(terminal)
			require.NoError(t, err)
			actualJSON, err := json.Marshal(actual)
			require.NoError(t, err)
			require.JSONEq(t, string(expectedJSON), string(actualJSON), "node %d must recover the same terminal decision", i)
		}
	}
}
