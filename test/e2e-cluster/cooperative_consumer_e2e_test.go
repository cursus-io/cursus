package e2e_cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cooperativeE2EConfig(addrs []string) *sdk.ConsumerConfig {
	cfg := sdk.NewDefaultConsumerConfig()
	cfg.BrokerAddrs = append([]string(nil), addrs...)
	for i, addr := range cfg.BrokerAddrs {
		cfg.BrokerAddrs[i] = strings.Replace(addr, "localhost:", "127.0.0.1:", 1)
	}
	cfg.Topic = "cooperative-live"
	cfg.GroupID = "cooperative-live-workers"
	cfg.HeartbeatIntervalMS = 200
	cfg.PollInterval = 10 * time.Millisecond
	cfg.RequestTimeoutMS = 5000
	cfg.PollTimeoutMS = 2000
	cfg.BatchSize = 8
	cfg.MaxPollRecords = 8
	return cfg
}

// This subprocess intentionally has no graceful shutdown in the crash test.
// Killing it exercises actual Go SDK connection loss and session expiration.
func TestCooperativeCrashClientProcess(t *testing.T) {
	if os.Getenv("CURSUS_COOPERATIVE_CHILD") != "1" {
		t.Skip("crash fixture child")
	}
	consumer, err := sdk.NewCooperativeConsumer(cooperativeE2EConfig(strings.Split(os.Getenv("CURSUS_COOPERATIVE_ADDRS"), ",")))
	require.NoError(t, err)
	go func() {
		for {
			if consumer.Stats().WorkerStarts >= 3 {
				_ = os.WriteFile(os.Getenv("CURSUS_COOPERATIVE_READY"), []byte("ready"), 0o600)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	require.NoError(t, consumer.Run(context.Background(), func(context.Context, sdk.CooperativeMessage) error { return nil }))
}

func TestCooperativeBrokerGoFaultRecovery(t *testing.T) {
	if os.Getenv("RUN_E2E_COOPERATIVE") != "1" {
		t.Skip("set RUN_E2E_COOPERATIVE=1 for isolated broker/Go fault validation")
	}
	// Reuse the tested isolated compose generator; never touch a developer cluster.
	t.Setenv("CURSUS_CLEANUP_PORT_OFFSET", "24000")
	fixture := givenCleanupCluster(t, composeFile, "docker-compose.consumer-cleanup.yml", "docker-compose.cooperative.yml")
	defer fixture.Cleanup()
	addrs := fixture.GetBrokerAddrs()
	// Docker Desktop's IPv6 loopback proxy can stall parallel Wire clients.
	// Pin this isolated host-published fixture to the advertised IPv4 addresses.
	for i, addr := range addrs {
		addrs[i] = strings.Replace(addr, "localhost:", "127.0.0.1:", 1)
	}
	cfg := cooperativeE2EConfig(addrs)
	sendClusterTopicCommand(t, addrs, "CREATE topic="+cfg.Topic+" partitions=6 replication_factor=3 idempotent=true")
	// METADATA is local; DESCRIBE forwards to the leader and cannot prove
	// that the selected group coordinator has applied topic creation yet.
	for node := 1; node <= 3; node++ {
		require.NoError(t, fixture.WhenCluster().WaitForLocalTopicDefinition(node, LocalTopicDefinition{Topic: cfg.Topic, Partitions: 6, CleanupPolicy: "delete"}, true))
	}
	require.Eventually(t, func() bool {
		for _, addr := range addrs {
			admin, err := sdk.NewAdminClient(&sdk.AdminConfig{BrokerAddrs: []string{addr}, RequestTimeoutMS: 1000})
			if err != nil {
				return false
			}
			capabilities, err := admin.Capabilities(context.Background())
			if err != nil {
				return false
			}
			found := false
			for _, feature := range capabilities.Enabled {
				found = found || feature == "consumer_cooperative_v1"
			}
			if !found {
				return false
			}
		}
		return true
	}, 30*time.Second, 200*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publishCtx, stopPublishing := context.WithCancel(ctx)
	defer stopPublishing()
	var published [6]atomic.Uint64
	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		client := e2e.NewBrokerClient(addrs)
		defer client.Close()
		sequence := [6]uint64{1, 1, 1, 1, 1, 1}
		failures := 0
		for publishCtx.Err() == nil {
			for p := 0; p < 6 && publishCtx.Err() == nil; p++ {
				payload := fmt.Sprintf("p%d-%d-%s", p, sequence[p], strings.Repeat("x", 128))
				if err := client.PublishIdempotentToPartition(cfg.Topic, fmt.Sprintf("cooperative-p%d", p), p, sequence[p], 0, payload, "all", true); err == nil {
					published[p].Store(sequence[p])
					sequence[p]++
				} else {
					failures++
					if failures <= 3 {
						t.Logf("publisher partition=%d seq=%d error=%v", p, sequence[p], err)
					}
				}
				select {
				case <-publishCtx.Done():
					return
				case <-time.After(5 * time.Millisecond):
				}
			}
		}
	}()
	t.Cleanup(func() { cancel(); <-publishDone })
	var mu sync.Mutex
	counts := make(map[int]int)
	seen := make(map[string]int)
	last := make(map[int]time.Time)
	gaps := make(map[int]time.Duration)
	var replayBytes uint64
	handler := func(ctx context.Context, m sdk.CooperativeMessage) error {
		p := m.Ownership.Grant.Partition.Partition
		now := time.Now()
		mu.Lock()
		counts[p]++
		key := fmt.Sprintf("%d/%d", p, m.Offset)
		if seen[key] > 0 {
			replayBytes += uint64(len(m.Payload))
		}
		seen[key]++
		if previous := last[p]; !previous.IsZero() && now.Sub(previous) > gaps[p] {
			gaps[p] = now.Sub(previous)
		}
		last[p] = now
		mu.Unlock()
		return nil
	}
	a, err := sdk.NewCooperativeConsumer(cfg)
	require.NoError(t, err)
	doneA := make(chan error, 1)
	go func() {
		err := a.Run(ctx, handler)
		t.Logf("consumer A exit: %v stats=%+v", err, a.Stats())
		doneA <- err
	}()
	require.Eventually(t, func() bool { return a.Stats().WorkerStarts == 6 }, 20*time.Second, 50*time.Millisecond)
	waitProgress := func(phase string) {
		t.Helper()
		mu.Lock()
		before := counts[0]
		mu.Unlock()
		timer := time.NewTimer(60 * time.Second)
		defer timer.Stop()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case err := <-doneA:
				t.Fatalf("%s consumer stopped: %v", phase, err)
			case <-timer.C:
				t.Fatalf("%s made no progress; stats=%+v", phase, a.Stats())
			case <-ticker.C:
				mu.Lock()
				progressed := counts[0] >= before+3
				mu.Unlock()
				if progressed {
					return
				}
			}
		}
	}
	waitProgress("initial")
	b, err := sdk.NewCooperativeConsumer(cfg)
	require.NoError(t, err)
	ctxB, cancelB := context.WithCancel(ctx)
	defer cancelB()
	doneB := make(chan error, 1)
	joinStart := time.Now()
	go func() {
		err := b.Run(ctxB, handler)
		t.Logf("consumer B exit: %v stats=%+v", err, b.Stats())
		doneB <- err
	}()
	joined := assert.Eventually(t, func() bool { return b.Stats().WorkerStarts == 3 && a.Stats().WorkerStops == 3 }, 20*time.Second, 50*time.Millisecond)
	require.True(t, joined, "join stats A=%+v B=%+v", a.Stats(), b.Stats())
	waitProgress("member join")
	require.Equal(t, uint64(6), a.Stats().WorkerStarts)
	t.Logf("phase=join convergence_ms=%d moved_partitions=3 retained_worker_restarts=0", time.Since(joinStart).Milliseconds())
	cancelB()
	require.ErrorIs(t, <-doneB, context.Canceled)
	require.Eventually(t, func() bool { return a.Stats().WorkerStarts == 9 }, 20*time.Second, 50*time.Millisecond)
	waitProgress("member leave")
	ready := filepath.Join(t.TempDir(), "ready")
	childLog, err := os.Create(filepath.Join(t.TempDir(), "child.log"))
	require.NoError(t, err)
	defer func() { _ = childLog.Close() }()
	child := exec.Command(os.Args[0], "-test.run=^TestCooperativeCrashClientProcess$", "-test.timeout=5m")
	child.Env = append(os.Environ(), "CURSUS_COOPERATIVE_CHILD=1", "CURSUS_COOPERATIVE_ADDRS="+strings.Join(addrs, ","), "CURSUS_COOPERATIVE_READY="+ready)
	child.Stdout = childLog
	child.Stderr = childLog
	require.NoError(t, child.Start())
	defer func() {
		if child.ProcessState == nil {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	require.Eventually(t, func() bool { _, err := os.Stat(ready); return err == nil }, 25*time.Second, 100*time.Millisecond)
	crashStart := time.Now()
	require.NoError(t, child.Process.Kill())
	require.Error(t, child.Wait())
	waitProgress("client crash, retained partitions")
	require.Eventually(t, func() bool { return a.Stats().WorkerStarts == 12 }, 40*time.Second, 100*time.Millisecond)
	t.Logf("phase=client_crash recovery_ms=%d rebuilt_workers=3", time.Since(crashStart).Milliseconds())
	oldCoordinator := coordinatorNode(t, findGroupCoordinator(t, 1, cfg.GroupID))
	failoverStart := time.Now()
	cleanupBrokerCommand(t, "stop", oldCoordinator)
	survivors := survivorNodes(oldCoordinator)
	waitForSingleRaftLeader(t, survivors, oldCoordinator)
	waitForAllBrokerReadiness(t, survivors)
	waitProgress("coordinator failover")
	t.Logf("phase=coordinator_failover progress_recovery_ms=%d", time.Since(failoverStart).Milliseconds())
	restartStart := time.Now()
	cleanupBrokerCommand(t, "start", oldCoordinator)
	waitForAllBrokerReadiness(t, []int{1, 2, 3})
	waitProgress("coordinator restart")
	t.Logf("phase=coordinator_restart progress_recovery_ms=%d", time.Since(restartStart).Milliseconds())
	for node := 1; node <= 3; node++ {
		restartStart := time.Now()
		cleanupBrokerCommand(t, "restart", node)
		waitForAllBrokerReadiness(t, []int{1, 2, 3})
		waitProgress(fmt.Sprintf("rolling restart %d", node))
		t.Logf("phase=rolling_restart node=%d progress_recovery_ms=%d", node, time.Since(restartStart).Milliseconds())
	}
	require.Equal(t, uint64(12), a.Stats().WorkerStarts, "broker restarts should retain valid SDK workers")
	stopPublishing()
	<-publishDone
	admin, err := sdk.NewAdminClient(&sdk.AdminConfig{BrokerAddrs: addrs, RequestTimeoutMS: 2000})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		offsets, err := admin.GroupOffsets(ctx, cfg.GroupID, cfg.Topic)
		if err != nil || len(offsets) != 6 {
			return false
		}
		for _, offset := range offsets {
			if offset.Offset != offset.Latest || offset.Offset < published[offset.Partition].Load() {
				return false
			}
		}
		return true
	}, 60*time.Second, 200*time.Millisecond, "all acknowledged records must reach a committed consumer offset")
	t.Log("all_partitions_drained=true committed_consumer_offsets_equal_log_end=true")
	cancel()
	err = <-doneA
	require.True(t, errors.Is(err, context.Canceled), "%v", err)
	stats := a.Stats()
	mu.Lock()
	duplicates := 0
	for _, n := range seen {
		if n > 1 {
			duplicates += n - 1
		}
	}
	t.Logf("scope=parent_and_graceful_peer delivered_records=%d fetched_records=%d fetched_payload_bytes=%d duplicate_deliveries=%d replay_payload_bytes=%d retained_partition_0_max_gap_ms=%d worker_starts=%d", stats.DeliveredRecords, stats.FetchedRecords, stats.FetchedPayloadBytes, duplicates, replayBytes, gaps[0].Milliseconds(), stats.WorkerStarts)
	mu.Unlock()
}
