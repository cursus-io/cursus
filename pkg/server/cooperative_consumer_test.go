package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/sdk"
	"github.com/stretchr/testify/require"
)

func cooperativeWireFixture(t *testing.T, enabled bool) (*sdk.ConsumerConfig, *coordinator.Coordinator, *topic.TopicManager) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.ConsumerCooperativeEnabled = enabled
	cfg.MinInSyncReplicas = 1
	cfg.LingerMS = 1
	_, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	cfg.BrokerPort, _ = strconv.Atoi(port)
	cfg.AdvertisedClientHost = "127.0.0.1"
	cfg.ConsumerSessionTimeoutMS = 2000
	cfg.ConsumerHeartbeatCheckMS = 100
	dm := disk.NewDiskManager(cfg)
	sm := stream.NewStreamManager(100, time.Minute)
	tm := topic.NewTopicManager(cfg, dm, sm)
	ctx, cancel := context.WithCancel(context.Background())
	cd, err := coordinator.NewCoordinatorWithRecovery(ctx, cfg, tm)
	require.NoError(t, err)
	cd.Start()
	ch := controller.NewCommandHandler(tm, cfg, cd, sm, nil)
	require.NoError(t, tm.CreateTopic("cooperative", 6, false, false))
	var connections sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			go func() { defer connections.Done(); handleConn(ctx, conn, ch) }()
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		<-acceptDone
		connections.Wait()
		cd.Stop()
		_ = ch.Close()
		tm.Stop()
		dm.CloseAllHandlers()
	})
	client := sdk.NewDefaultConsumerConfig()
	client.BrokerAddrs = []string{listener.Addr().String()}
	client.CoordinatorAddr = listener.Addr().String()
	client.Topic = "cooperative"
	client.GroupID = "workers"
	client.HeartbeatIntervalMS = 25
	client.RequestTimeoutMS = 1000
	client.PollInterval = time.Millisecond
	client.PollTimeoutMS = 1000
	client.BatchSize = 4
	client.MaxPollRecords = 4
	return client, cd, tm
}

func TestCooperativeGoRejectsUnsupportedBeforeJoin(t *testing.T) {
	cfg, cd, _ := cooperativeWireFixture(t, false)
	consumer, err := sdk.NewCooperativeConsumer(cfg)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = consumer.Run(ctx, func(context.Context, sdk.CooperativeMessage) error { return nil })
	require.Error(t, err)
	require.Nil(t, cd.GetGroup("workers"), "capability rejection must precede eager group mutation")
}

func TestCooperativeGoRetainsWorkersAndWaitsForHandler(t *testing.T) {
	for _, mode := range []sdk.ConsumerMode{sdk.ModePolling, sdk.ModeStreaming} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			cfg, cd, tm := cooperativeWireFixture(t, true)
			cfg.Mode = mode
			const records = 80
			for p := 0; p < 6; p++ {
				for i := 0; i < records; i++ {
					require.NoError(t, tm.PublishToPartitionWithAck("cooperative", p, &types.Message{Payload: strings.Repeat("x", 128)}))
				}
			}
			a, err := sdk.NewCooperativeConsumer(cfg)
			require.NoError(t, err)
			b, err := sdk.NewCooperativeConsumer(cfg)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			var mu sync.Mutex
			counts := make(map[int]int)
			seen := make(map[string]int)
			var old sdk.CooperativeMessage
			blocked := make(chan struct{})
			unblock := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(unblock) })
			doneA := make(chan error, 1)
			doneB := make(chan error, 1)
			handle := func(ctx context.Context, m sdk.CooperativeMessage) error {
				p := m.Ownership.Grant.Partition.Partition
				mu.Lock()
				counts[p]++
				seen[fmt.Sprintf("%d/%d", p, m.Offset)]++
				mu.Unlock()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Millisecond):
					return nil
				}
			}
			go func() {
				doneA <- a.Run(ctx, func(ctx context.Context, m sdk.CooperativeMessage) error {
					if m.Ownership.Grant.Partition.Partition == 5 && m.Offset == 0 {
						old = m
						close(blocked)
						<-unblock
					}
					return handle(ctx, m)
				})
			}()
			select {
			case <-blocked:
			case err := <-doneA:
				t.Fatalf("consumer stopped before blocked handler: %v", err)
			case <-ctx.Done():
				t.Fatal("handler not started")
			}
			start := time.Now()
			go func() { doneB <- b.Run(ctx, handle) }()
			require.Eventually(t, func() bool { return b.Stats().WorkerStarts == 2 }, 5*time.Second, 10*time.Millisecond, "two unblocked transfers should complete before the blocked ACK")
			mu.Lock()
			before := counts[0]
			mu.Unlock()
			require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return counts[0] > before }, time.Second, 5*time.Millisecond)
			require.Equal(t, uint64(6), a.Stats().WorkerStarts, "retained workers must not restart")
			snapshot := cd.ExportGroupState("workers")
			require.NotNil(t, snapshot)
			require.Len(t, snapshot.Cooperative.Current, 6)
			once.Do(func() { close(unblock) })
			require.Eventually(t, func() bool { return b.Stats().WorkerStarts == 3 }, 5*time.Second, 10*time.Millisecond)
			require.Error(t, a.Commit(context.Background(), old, 1), "delayed manual commit cannot acquire a replacement token")
			require.Eventually(t, func() bool {
				for p := 0; p < 6; p++ {
					offset, ok := cd.GetOffset("workers", "cooperative", p)
					if !ok || offset != records {
						return false
					}
				}
				return true
			}, 10*time.Second, 20*time.Millisecond)
			cancel()
			require.True(t, errors.Is(<-doneA, context.Canceled))
			require.True(t, errors.Is(<-doneB, context.Canceled))
			mu.Lock()
			duplicates := 0
			for _, count := range seen {
				if count > 1 {
					duplicates += count - 1
				}
			}
			mu.Unlock()
			statsA, statsB := a.Stats(), b.Stats()
			t.Logf("mode=%s convergence_ms=%d moved_partitions=3 retained_worker_restarts=0 worker_starts=%d fetched_records=%d fetched_payload_bytes=%d duplicate_deliveries=%d duplicate_payload_bytes=%d", mode, time.Since(start).Milliseconds(), statsA.WorkerStarts+statsB.WorkerStarts, statsA.FetchedRecords+statsB.FetchedRecords, statsA.FetchedPayloadBytes+statsB.FetchedPayloadBytes, duplicates, duplicates*128)
		})
	}
}

func TestCooperativeGoTransactionOffsets(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprint(commit), func(t *testing.T) {
			cfg, cd, tm := cooperativeWireFixture(t, true)
			require.NoError(t, cd.RegisterGroup(cfg.Topic, cfg.GroupID, 6))
			parts := make([]coordinator.CooperativePartition, 6)
			for p := range parts {
				parts[p] = coordinator.CooperativePartition{Topic: cfg.Topic, Partition: p, LifecycleEpoch: tm.GetTopic(cfg.Topic).Definition().LifecycleEpoch}
			}
			a, err := cd.ReconcileCooperative(cfg.GroupID, "a", 0, 0, parts, nil, false)
			require.NoError(t, err)
			client, err := sdk.NewConsumerClient(cfg)
			require.NoError(t, err)
			producer, err := client.NewTransactionalProducer("cooperative-txn")
			require.NoError(t, err)
			require.NoError(t, producer.Begin())
			record := sdk.CooperativeMessage{Message: sdk.Message{Topic: cfg.Topic, Partition: 5, Offset: 0}, Group: cfg.GroupID, Ownership: protocol.CooperativeToken{RegistrationEpoch: a.RegistrationEpoch, Grant: a.Grants[5]}}
			require.NoError(t, producer.SendCooperativeOffsets(record, 1))
			if commit {
				require.NoError(t, producer.Commit())
			} else {
				require.NoError(t, producer.Abort())
			}
			offset, found := cd.GetOffset(cfg.GroupID, cfg.Topic, 5)
			require.Equal(t, commit, found)
			if commit {
				require.Equal(t, uint64(1), offset)
			}
			_, err = cd.ReconcileCooperative(cfg.GroupID, "b", 0, 0, parts, nil, false)
			require.NoError(t, err)
			a, err = cd.ReconcileCooperative(cfg.GroupID, "a", a.RegistrationEpoch, a.Session, parts, nil, false)
			require.NoError(t, err)
			var acks []coordinator.CooperativeRevocation
			for _, grant := range a.Grants {
				if grant.RevokeRevision != 0 {
					acks = append(acks, coordinator.CooperativeRevocation{Partition: grant.Partition, GrantEpoch: grant.Epoch, RevokeRevision: grant.RevokeRevision})
				}
			}
			_, err = cd.ReconcileCooperative(cfg.GroupID, "a", a.RegistrationEpoch, a.Session, parts, acks, false)
			require.NoError(t, err)
			stale, err := client.NewTransactionalProducer("cooperative-stale")
			require.NoError(t, err)
			require.NoError(t, stale.Begin())
			require.Error(t, stale.SendCooperativeOffsets(record, 2))
			require.NoError(t, stale.Abort())
		})
	}
}

// A fixed backlog and handler cost make protocol overhead and replay comparable.
// This is a local TCP measurement, not a production capacity benchmark.
func TestCooperativeGoEagerComparison(t *testing.T) {
	for _, cooperative := range []bool{false, true} {
		t.Run(fmt.Sprintf("cooperative=%t", cooperative), func(t *testing.T) {
			cfg, _, tm := cooperativeWireFixture(t, true)
			cfg.Mode = sdk.ModePolling
			cfg.BatchSize = 8
			cfg.MaxPollRecords = 8
			cfg.AutoCommitInterval = 10 * time.Millisecond
			const records = 160
			for p := 0; p < 6; p++ {
				partition, err := tm.GetTopic(cfg.Topic).GetPartition(p)
				require.NoError(t, err)
				batch := make([]types.Message, records)
				for i := range batch {
					batch[i] = types.Message{Payload: strings.Repeat("x", 128)}
				}
				require.NoError(t, partition.EnqueueBatchSync(batch))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var mu sync.Mutex
			seen := map[string]int{}
			counts := map[int]int{}
			last := map[int]time.Time{}
			gaps := map[int]time.Duration{}
			handler := func(m sdk.Message) error {
				mu.Lock()
				counts[m.Partition]++
				seen[fmt.Sprintf("%d/%d", m.Partition, m.Offset)]++
				now := time.Now()
				if !last[m.Partition].IsZero() && now.Sub(last[m.Partition]) > gaps[m.Partition] {
					gaps[m.Partition] = now.Sub(last[m.Partition])
				}
				last[m.Partition] = now
				mu.Unlock()
				time.Sleep(time.Millisecond)
				return nil
			}
			startConsumer := func() (chan error, func()) {
				done := make(chan error, 1)
				copyCfg := *cfg
				if cooperative {
					c, err := sdk.NewCooperativeConsumer(&copyCfg)
					require.NoError(t, err)
					go func() {
						done <- c.Run(ctx, func(_ context.Context, m sdk.CooperativeMessage) error { return handler(m.Message) })
					}()
					return done, func() {
						stats := c.Stats()
						t.Logf("worker_starts=%d fetched_records=%d fetched_payload_bytes=%d", stats.WorkerStarts, stats.FetchedRecords, stats.FetchedPayloadBytes)
					}
				}
				c, err := sdk.NewConsumerWithContext(ctx, &copyCfg)
				require.NoError(t, err)
				go func() { done <- c.Start(handler) }()
				return done, func() {
					if err := c.Close(); err != nil {
						t.Logf("eager shutdown: %v", err)
					}
				}
			}
			started := time.Now()
			doneA, finishA := startConsumer()
			require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return counts[0] >= 16 }, 5*time.Second, 5*time.Millisecond)
			joined := time.Now()
			doneB, finishB := startConsumer()
			require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == 6*records }, 20*time.Second, 10*time.Millisecond)
			elapsed, convergence := time.Since(started), time.Since(joined)
			cancel()
			<-doneA
			<-doneB
			finishA()
			finishB()
			mu.Lock()
			defer mu.Unlock()
			duplicates := 0
			for _, n := range seen {
				duplicates += n - 1
			}
			t.Logf("unique_records=%d records_per_second=%.1f backlog_drain_after_join_ms=%d retained_partition_0_max_gap_ms=%d duplicate_deliveries=%d replay_payload_bytes=%d", len(seen), float64(len(seen))/elapsed.Seconds(), convergence.Milliseconds(), gaps[0].Milliseconds(), duplicates, duplicates*128)
		})
	}
}
