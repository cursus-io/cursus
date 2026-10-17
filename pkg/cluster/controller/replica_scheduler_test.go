package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func recoverySchedulerFixture(t *testing.T, ends ...uint64) (*ClusterController, []*topic.Partition) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	t.Cleanup(func() { tm.Stop(); dm.CloseAllHandlers() })
	state := fsm.NewBrokerFSM(tm, nil)
	apply := func(prefix string, value any) {
		data, err := json.Marshal(value)
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte(prefix), data...)}))
	}
	for _, id := range []string{"source", "receiver"} {
		apply("REGISTER:", fsm.BrokerInfo{ID: id, Addr: "127.0.0.1:9000", Status: "active"})
	}
	def := topic.DefaultDefinition("orders", cfg)
	def.Partitions, def.ReplicationFactor = len(ends), 2
	apply("TOPIC:", fsm.TopicCommand{Definition: &def, LeaderID: "source"})
	var partitions []*topic.Partition
	for id, end := range ends {
		key := fmt.Sprintf("orders-%d", id)
		meta := state.GetPartitionMetadata(key)
		meta.CommittedHWM, meta.CommittedHWMKnown, meta.ISR = end, true, []string{"source"}
		apply("PARTITION:"+key+":", meta)
		p, err := tm.GetTopic("orders").GetPartition(id)
		require.NoError(t, err)
		partitions = append(partitions, p)
	}
	cc := NewClusterController(context.Background(), cfg, &MockRaftManager{mockFSM: state}, nil, "receiver", "127.0.0.1:9000")
	t.Cleanup(cc.StopReplicaCatchup)
	return cc, partitions
}

func TestReplicaSchedulerYieldsLargePartitionAndKeepsDurableProgress(t *testing.T) {
	cc, partitions := recoverySchedulerFixture(t, 100, 1)
	fetcher := &recordingCatchupFetcher{}
	apply := func(ctx context.Context, r fsm.ReplicaCatchupRequest, b fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
		return applySchedulerTestBatch(ctx, partitions[r.Partition], r, b)
	}
	require.NoError(t, cc.RunReplicaCatchupOnceVerified(context.Background(), fetcher, apply))
	require.Len(t, fetcher.requests, replicaCatchupTurnSteps+1)
	require.Equal(t, 1, fetcher.requests[replicaCatchupTurnSteps].Partition, "small partition gets a turn before the large one completes")
	require.EqualValues(t, replicaCatchupTurnSteps, partitions[0].NextOffset())
	require.EqualValues(t, 1, partitions[1].GetHWM())
	require.NoError(t, cc.RunReplicaCatchupOnceVerified(context.Background(), fetcher, apply))
	require.EqualValues(t, 100, partitions[0].NextOffset())
	require.EqualValues(t, 100, partitions[0].GetHWM())
	require.EqualValues(t, replicaCatchupTurnSteps, fetcher.requests[replicaCatchupTurnSteps+1].NextOffset, "next pass resumes at durable progress")
}

type ownedRecoveryFetcher struct {
	started, canceled, release chan struct{}
	calls                      atomic.Int32
}

func (f *ownedRecoveryFetcher) FetchReplicaCatchup(ctx context.Context, _ string, _ int, _ fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
	if f.calls.Add(1) == 1 {
		close(f.started)
	}
	<-ctx.Done()
	close(f.canceled)
	<-f.release
	return fsm.ReplicaCatchupBatch{}, ctx.Err()
}

func TestReplicaSchedulerStopJoinsItsOnlyOwner(t *testing.T) {
	cc, _ := recoverySchedulerFixture(t, 1)
	fetcher := &ownedRecoveryFetcher{started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(fetcher.release) }) }
	defer release()
	apply := func(context.Context, fsm.ReplicaCatchupRequest, fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
		return nil, nil
	}
	cc.StartReplicaCatchupVerified(context.Background(), fetcher, apply)
	select {
	case <-fetcher.started:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery owner did not start")
	}
	cc.StartReplicaCatchupVerified(context.Background(), fetcher, apply)
	require.ErrorContains(t, cc.RunReplicaCatchupOnceVerified(context.Background(), fetcher, apply), "already running")
	stopped := make(chan struct{})
	go func() { cc.StopReplicaCatchup(); close(stopped) }()
	select {
	case <-fetcher.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("owner was not canceled")
	}
	select {
	case <-stopped:
		t.Fatal("stop returned before owned work completed")
	default:
	}
	release()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("owner was not joined")
	}
	cc.StopReplicaCatchup()
	cc.StartReplicaCatchupVerified(context.Background(), fetcher, apply)
	require.EqualValues(t, 1, fetcher.calls.Load())
}
