package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type bootstrapSchedulerProbe struct{ calls int }

func (p *bootstrapSchedulerProbe) RecoverReplicaBootstrap(context.Context, fsm.ReplicaCatchupRequest, fsm.ReplicaCatchupBatch) error {
	p.calls++
	return nil
}

func TestReplicaBootstrapReceiverRefreshLeavesOtherPartitionUntouched(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	t.Cleanup(func() { tm.Stop(); dm.CloseAllHandlers() })
	state := fsm.NewBrokerFSM(tm, nil)
	for _, id := range []string{"source", "receiver"} {
		data, err := json.Marshal(fsm.BrokerInfo{ID: id, Addr: "127.0.0.1:9000", Status: "active"})
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)}))
	}
	def := topic.DefaultDefinition("orders", cfg)
	def.Partitions = 2
	def.ReplicationFactor = 2
	data, err := json.Marshal(fsm.TopicCommand{Definition: &def, LeaderID: "source"})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), data...)}))
	for id := 0; id < 2; id++ {
		key := fmt.Sprintf("orders-%d", id)
		m := state.GetPartitionMetadata(key)
		m.CommittedHWM = 1
		m.CommittedHWMKnown = true
		m.ISR = []string{"source"}
		data, err = json.Marshal(m)
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:"+key+":"), data...)}))
	}
	a, err := tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, a.ReplicaAppendWithMode([]types.Message{{Offset: 0, Payload: "committed"}, {Offset: 1, Payload: "tail"}}, true))
	require.NoError(t, a.ApplyReplicaHWM(1))
	cc := NewClusterController(context.Background(), cfg, &MockRaftManager{mockFSM: state}, nil, "receiver", "127.0.0.1:9000")
	previous := fsm.ReplicaCatchupRequest{Topic: "orders", Partition: 1, BrokerID: "receiver"}
	refreshed, ok := cc.refreshReplicaCatchupRequest(previous)
	require.True(t, ok)
	require.Equal(t, 1, refreshed.Partition)
	require.EqualValues(t, 2, a.NextOffset(), "refresh B must not truncate excluded/completed A")
	require.EqualValues(t, 1, a.GetHWM())
	// A full ordinary builder would mutate A; the preceding assertion is not
	// merely an inert partition fixture.
	_ = state.BuildReplicaCatchupRequests("receiver")
	require.EqualValues(t, 1, a.NextOffset())
}
func (*bootstrapSchedulerProbe) ResumePendingReplicaBootstraps(context.Context) (map[string]bool, error) {
	return nil, nil
}
func (*bootstrapSchedulerProbe) ReplicaBootstrapExcluded(string, int) bool { return false }

type bootstrapSchedulerFetcher struct{ batch fsm.ReplicaCatchupBatch }

func (f bootstrapSchedulerFetcher) FetchReplicaCatchup(context.Context, string, int, fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
	return f.batch, nil
}

func TestReplicaBootstrapReceiverSchedulerAuthenticatesSentinel(t *testing.T) {
	r := fsm.ReplicaCatchupRequest{Topic: "orders", Partition: 0, BrokerID: "receiver", SourceBroker: "source", Leader: "source", LeaderEpoch: 1, LifecycleEpoch: 1, CommittedHWM: 2, RecoveryProtocol: fsm.IndexedRecoveryProtocolVersion}
	b := fsm.ReplicaCatchupBatch{Topic: r.Topic, Partition: r.Partition, BrokerID: r.BrokerID, SourceBroker: r.SourceBroker, Leader: r.Leader, LeaderEpoch: r.LeaderEpoch, LifecycleEpoch: r.LifecycleEpoch, CommittedHWM: r.CommittedHWM, RecoveryProtocol: r.RecoveryProtocol, BootstrapRequired: true}
	sealed, err := fsm.SealReplicaCatchupBatch(b)
	require.NoError(t, err)
	for _, change := range []string{"valid", "digest", "identity", "boundary", "unsealed flag", "malformed"} {
		t.Run(change, func(t *testing.T) {
			batch := sealed
			switch change {
			case "digest":
				batch.CommittedHWM++
			case "identity":
				batch.BrokerID = "wrong"
			case "boundary":
				batch.StartOffset = 1
			case "unsealed flag":
				batch = b
			case "malformed":
				batch.Verified = true
				batch, err = fsm.SealReplicaCatchupBatch(batch)
				require.NoError(t, err)
			}
			probe := &bootstrapSchedulerProbe{}
			cc := &ClusterController{replicaBootstrap: probe}
			err := cc.catchupReplica(context.Background(), bootstrapSchedulerFetcher{batch}, func(context.Context, fsm.ReplicaCatchupRequest, fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
				t.Fatal("bootstrap response reached ordinary apply")
				return nil, nil
			}, r)
			if change == "valid" {
				require.NoError(t, err)
				require.Equal(t, 1, probe.calls)
			} else {
				require.Error(t, err)
				require.Zero(t, probe.calls)
			}
		})
	}
}
