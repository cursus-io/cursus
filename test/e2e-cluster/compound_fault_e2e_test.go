package e2e_cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const compoundAttemptLimit = 300000
const compoundRoundLimit = 2000

type compoundConfig struct {
	Seed     int64         `json:"seed"`
	Rounds   int           `json:"minimum_rounds"`
	Duration time.Duration `json:"minimum_duration_ns"`
}

// readCompoundConfig rejects partial-coverage runs and unbounded durations.
func readCompoundConfig(getenv func(string) string) (compoundConfig, error) {
	cfg := compoundConfig{Seed: 3271337, Rounds: 3}
	var err error
	if raw := getenv("CURSUS_COMPOUND_SEED"); raw != "" {
		cfg.Seed, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return cfg, fmt.Errorf("invalid compound seed: %w", err)
		}
	}
	if raw := getenv("CURSUS_COMPOUND_ROUNDS"); raw != "" {
		cfg.Rounds, err = strconv.Atoi(raw)
		if err != nil || cfg.Rounds < 3 || cfg.Rounds > compoundRoundLimit {
			return cfg, fmt.Errorf("compound rounds must be between 3 and %d", compoundRoundLimit)
		}
	}
	if raw := getenv("CURSUS_COMPOUND_DURATION"); raw != "" {
		cfg.Duration, err = time.ParseDuration(raw)
		if err != nil || cfg.Duration < 0 || cfg.Duration > 24*time.Hour {
			return cfg, fmt.Errorf("compound duration must be between 0 and 24h")
		}
	}
	return cfg, nil
}

type compoundStep struct {
	Round          int    `json:"round"`
	Fault          string `json:"fault"`
	Peer           int    `json:"peer_index"`
	QuorumHoldSecs int    `json:"quorum_hold_seconds"`
}

// Every three rounds cover all faults. Seeded choices are reproducibility data,
// never production scheduling or cryptographic randomness.
func compoundSchedule(seed int64, count int) []compoundStep {
	random := rand.New(rand.NewSource(seed)) // #nosec G404 -- reproducible test schedule.
	names := []string{"lost-reply-leader-kill", "isolation-quorum-loss", "interrupted-catchup"}
	steps := make([]compoundStep, 0, count)
	for len(steps) < count {
		for _, index := range random.Perm(len(names)) {
			if len(steps) == count {
				break
			}
			steps = append(steps, compoundStep{len(steps) + 1, names[index], random.Intn(2), 2 + random.Intn(4)})
		}
	}
	return steps
}

// compoundProducer has one sequential identity stream across pauses. Unknown
// outcomes retain the same sequence and payload on the next explicit attempt.
type compoundProducer struct {
	node atomic.Int64
	acks atomic.Uint64
	seq  uint64 // accessed only by the active worker, whose predecessor is drained.
}

// start returns a drain function and registers it before any fault can fail.
func (p *compoundProducer) start(t *testing.T, journal *ackFaultJournal, topic string) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var result error
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			message := ackFaultMessage("compound-background", p.seq+1)
			message.Payload += "-" + strings.Repeat("x", 256)
			attempt, _, err := journal.publishAttempt(clusterBrokerAddrs(3)[int(p.node.Load())-1], topic, message)
			if err != nil {
				result = err
				return
			}
			if attempt.Outcome == "acknowledged" {
				p.seq++
				p.acks.Add(1)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}()
	var once sync.Once
	stop := func() {
		once.Do(cancel)
		<-done
		require.NoError(t, result, "background producer evidence must be complete")
	}
	t.Cleanup(stop)
	return stop
}

// TestAcknowledgedWritesSurviveCompoundFaults checks complete acknowledged
// history after seeded overlapping faults on the same retained cluster.
func TestAcknowledgedWritesSurviveCompoundFaults(t *testing.T) {
	if os.Getenv("RUN_E2E_COMPOUND_FAULTS") != "1" {
		t.Skip("set RUN_E2E_COMPOUND_FAULTS=1 for the isolated seeded campaign")
	}
	cfg, err := readCompoundConfig(os.Getenv)
	require.NoError(t, err)
	journal := newAckFaultJournal(t)
	journal.compact = true
	journal.maxAttempts = compoundAttemptLimit
	journal.record("campaign_config", cfg)
	if cfg.Duration > 0 {
		// Metrics and fault milestones stay in the external journal; avoid
		// per-request debug logging dominating a day-long broker log.
		t.Setenv("CURSUS_ACK_FAULT_LOG_LEVEL", "info")
	}
	ctx := newAckFaultClusterMode(t, "ack-compound", true)
	journal.record("campaign_runtime", map[string]any{"driver_go": runtime.Version(), "driver_os": runtime.GOOS, "broker_image": ackFaultDocker(t, "inspect", "--format", "{{.Image}}", ackFaultContainer(1)), "attempt_limit": compoundAttemptLimit, "round_limit": compoundRoundLimit})
	producer := &compoundProducer{}
	started := time.Now()
	steps := compoundSchedule(cfg.Seed, compoundRoundLimit)
	rounds := 0
	for rounds < cfg.Rounds || time.Since(started) < cfg.Duration {
		require.Less(t, rounds, len(steps), "campaign round bound reached before requested duration")
		step := steps[rounds]
		journal.record("fault_plan", step)
		state := waitAckFaultState(t, ctx.GetTopic(), []int{1, 2, 3}, "", 3)
		producer.node.Store(int64(coordinatorNode(t, state.Leader)))
		before := producer.acks.Load()
		stop := producer.start(t, journal, ctx.GetTopic())
		require.Eventually(t, func() bool { return producer.acks.Load() > before }, 45*time.Second, 100*time.Millisecond, "background producer must run before the fault")
		captureAckFaultMetrics(t, journal, fmt.Sprintf("round-%d-before-fault", step.Round))
		faultStarted := time.Now()
		switch step.Fault {
		case "lost-reply-leader-kill":
			compoundLostReply(t, ctx, journal, producer, state, step)
		case "isolation-quorum-loss":
			compoundQuorumLoss(t, ctx, journal, producer, state, step)
		case "interrupted-catchup":
			compoundInterruptedCatchup(t, ctx, journal, state, step)
		default:
			t.Fatalf("unhandled fault %q", step.Fault)
		}
		journal.record("fault_heal_requested", step)
		healed := time.Now()
		afterFault := producer.acks.Load()
		require.Eventually(t, func() bool { return producer.acks.Load() > afterFault }, 2*clusterReadyTimeout, 100*time.Millisecond, "background produce must resume after healing")
		journal.record("post_heal_ack", map[string]any{"round": step.Round, "elapsed_ms": time.Since(healed).Milliseconds()})
		stop()
		verifyAckFaultRecovery(t, ctx, journal)
		rounds++
		journal.record("round_verified", map[string]any{"round": rounds, "fault_and_verification_ms": time.Since(faultStarted).Milliseconds(), "campaign_elapsed_ms": time.Since(started).Milliseconds(), "background_acknowledgements": producer.acks.Load()})
	}
	journal.record("campaign_complete", map[string]any{"seed": cfg.Seed, "rounds": rounds, "elapsed_ms": time.Since(started).Milliseconds(), "minimum_duration_ms": cfg.Duration.Milliseconds(), "attempts": len(journal.snapshotAttempts())})
}

// Live writers can advance HWM between metadata requests. During a fault wait
// only for agreed authority and ISR; after draining, the normal verifier still
// requires identical committed ends and complete content on every replica.
func waitCompoundLeadership(t *testing.T, topic string, nodes []int, excluded string, isr int) ackFaultPartition {
	t.Helper()
	var result ackFaultPartition
	require.NoError(t, eventually(t, "compound live leadership", 2*clusterReadyTimeout, func() (bool, string, error) {
		var first ackFaultPartition
		for index, node := range nodes {
			state, err := ackFaultState(node, topic)
			if err != nil {
				return false, err.Error(), nil
			}
			if state.Leader == "" || len(state.ISR) != isr || state.RecoveryPending || len(state.RecoveryReplicas) != 0 ||
				(excluded != "" && (state.Leader == excluded || slices.Contains(state.ISR, excluded))) {
				return false, fmt.Sprintf("node=%d state=%+v", node, state), nil
			}
			if index == 0 {
				first = state
			} else if first.Leader != state.Leader || first.LeaderEpoch != state.LeaderEpoch {
				return false, "live brokers disagree on partition authority", nil
			}
		}
		result = first
		return true, fmt.Sprintf("%+v", result), nil
	}))
	return result
}

// compoundLostReply overlaps a withheld real ACK with loss of its leader.
func compoundLostReply(t *testing.T, ctx *ClusterTestContext, journal *ackFaultJournal, producer *compoundProducer, state ackFaultPartition, step compoundStep) {
	t.Helper()
	leader := coordinatorNode(t, state.Leader)
	proxy, dropped := dropAckFaultReply(t, ctx.GetBrokerAddrs()[leader-1])
	message := ackFaultMessage(fmt.Sprintf("compound-fault-%d", step.Round), 1)
	attempt, sent := journal.publish(proxy, ctx.GetTopic(), message)
	require.True(t, sent)
	require.Equal(t, "unknown", attempt.Outcome)
	select {
	case err := <-dropped:
		require.NoError(t, err)
	case <-time.After(25 * time.Second):
		t.Fatal("successful publish response was not observed by the drop proxy")
	}
	journal.record("ack_response_dropped", attempt.ID)
	ackFaultDocker(t, "kill", "--signal=KILL", ackFaultContainer(leader))
	require.Equal(t, "false", ackFaultDocker(t, "inspect", "--format", "{{.State.Running}}", ackFaultContainer(leader)))
	journal.record("leader_killed", state.Leader)
	next := waitCompoundLeadership(t, ctx.GetTopic(), ackFaultOtherNodes(leader), state.Leader, 2)
	require.Greater(t, next.LeaderEpoch, state.LeaderEpoch)
	producer.node.Store(int64(coordinatorNode(t, next.Leader)))
	publishAckFaultEventually(t, journal, ctx.GetTopic(), coordinatorNode(t, next.Leader), message)
	ackFaultDocker(t, "start", ackFaultContainer(leader))
}

// compoundQuorumLoss overlaps leader isolation and peer loss, requiring fresh
// write rejection until a majority returns and explicitly healing the firewall.
func compoundQuorumLoss(t *testing.T, ctx *ClusterTestContext, journal *ackFaultJournal, producer *compoundProducer, state ackFaultPartition, step compoundStep) {
	t.Helper()
	leader := coordinatorNode(t, state.Leader)
	heal := isolateAckFaultLeader(t, leader, journal)
	remaining := ackFaultOtherNodes(leader)
	next := waitCompoundLeadership(t, ctx.GetTopic(), remaining, state.Leader, 2)
	producer.node.Store(int64(coordinatorNode(t, next.Leader)))
	probe := ackFaultMessage(fmt.Sprintf("compound-isolated-%d", step.Round), 1)
	attempt, sent := journal.publish(ctx.GetBrokerAddrs()[leader-1], ctx.GetTopic(), probe)
	require.True(t, sent, "isolated old leader must remain client reachable")
	require.NotEqual(t, "acknowledged", attempt.Outcome)
	killed, survivor := remaining[step.Peer], remaining[1-step.Peer]
	ackFaultDocker(t, "kill", "--signal=KILL", ackFaultContainer(killed))
	require.Equal(t, "false", ackFaultDocker(t, "inspect", "--format", "{{.State.Running}}", ackFaultContainer(killed)))
	journal.record("quorum_lost", map[string]any{"isolated": leader, "killed": killed, "survivor": survivor})
	time.Sleep(time.Duration(step.QuorumHoldSecs) * time.Second)
	probe = ackFaultMessage(fmt.Sprintf("compound-no-quorum-%d", step.Round), 1)
	attempt, sent = journal.publish(ctx.GetBrokerAddrs()[survivor-1], ctx.GetTopic(), probe)
	require.True(t, sent, "quorum-loss probe must reach the surviving broker transport")
	require.NotEqual(t, "acknowledged", attempt.Outcome, "fresh write must not succeed without a majority")
	ackFaultDocker(t, "start", ackFaultContainer(killed))
	next = waitCompoundLeadership(t, ctx.GetTopic(), remaining, state.Leader, 2)
	producer.node.Store(int64(coordinatorNode(t, next.Leader)))
	journal.record("firewall_counters", ackFaultDocker(t, "exec", "-u", "0", ackFaultContainer(leader), "iptables", "-L", "-n", "-v", "-x"))
	heal()
}

// compoundInterruptedCatchup crashes a receiver at an observed durable append
// boundary, then restarts the same volume without reusing a stale reached marker.
func compoundInterruptedCatchup(t *testing.T, ctx *ClusterTestContext, journal *ackFaultJournal, state ackFaultPartition, step compoundStep) {
	t.Helper()
	leader := coordinatorNode(t, state.Leader)
	follower := ackFaultOtherNodes(leader)[step.Peer]
	key := ctx.GetTopic() + "-0"
	path := "/run/cursus-e2e-faults/replica-catchup/" + key
	ackFaultDocker(t, "exec", ackFaultContainer(follower), "rm", "-f", path+".active", path+".entered", path+".release", path+".work.json")
	armRecoveryBoundary(t, follower, "replica-catchup", key)
	ackFaultDocker(t, "kill", "--signal=KILL", ackFaultContainer(follower))
	waitCompoundLeadership(t, ctx.GetTopic(), ackFaultOtherNodes(follower), fmt.Sprintf("broker-%d-9000", follower), 2)
	for sequence := uint64(1); sequence <= 64; sequence++ {
		message := ackFaultMessage(fmt.Sprintf("compound-backlog-%d", step.Round), sequence)
		message.Payload += "-" + strings.Repeat("b", 1024)
		publishAckFaultEventually(t, journal, ctx.GetTopic(), leader, message)
	}
	ackFaultDocker(t, "start", ackFaultContainer(follower))
	waitRecoveryBoundary(t, follower, path, journal)
	var work map[string]any
	require.NoError(t, json.Unmarshal([]byte(ackFaultDocker(t, "exec", ackFaultContainer(follower), "cat", path+".work.json")), &work))
	journal.record("interrupted_receiver", map[string]any{"node": follower, "work": work, "process_started_at": ackFaultDocker(t, "inspect", "--format", "{{.State.StartedAt}}", ackFaultContainer(follower))})
	ackFaultDocker(t, "kill", "--signal=KILL", ackFaultContainer(follower))
	require.Equal(t, "false", ackFaultDocker(t, "inspect", "--format", "{{.State.Running}}", ackFaultContainer(follower)))
	ackFaultDocker(t, "start", ackFaultContainer(follower))
}
