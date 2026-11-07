package e2e_cluster

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

// TestAcknowledgedWritesSurviveLostReplyAndLeaderKill retries an unknown write
// after killing its leader and checks that every acknowledged identity survives once.
func TestAcknowledgedWritesSurviveLostReplyAndLeaderKill(t *testing.T) {
	if os.Getenv("RUN_E2E_ACK_FAULTS") != "1" {
		t.Skip("set RUN_E2E_ACK_FAULTS=1 for isolated acknowledged-write faults")
	}
	journal := newAckFaultJournal(t)
	ctx := newAckFaultCluster(t, "ack-lost-reply")
	state := waitAckFaultState(t, ctx.GetTopic(), []int{1, 2, 3}, "", 3)
	leader := coordinatorNode(t, state.Leader)
	for seq := uint64(1); seq <= 8; seq++ {
		publishAckFaultEventually(t, journal, ctx.GetTopic(), leader, ackFaultMessage("producer", seq))
	}
	proxy, dropped := dropAckFaultReply(t, ctx.GetBrokerAddrs()[leader-1])
	message := ackFaultMessage("producer", 9)
	attempt, sent := journal.publish(proxy, ctx.GetTopic(), message)
	require.True(t, sent)
	require.Equal(t, "unknown", attempt.Outcome)
	select {
	case err := <-dropped:
		require.NoError(t, err, "must observe a real successful response before dropping it")
	case <-time.After(25 * time.Second):
		t.Fatal("publish response-drop proxy did not complete")
	}
	journal.record("ack_response_dropped", attempt.ID)
	captureAckFaultMetrics(t, journal, "before-kill")
	ackFaultDocker(t, "kill", "--signal=KILL", ackFaultContainer(leader))
	require.Equal(t, "false", ackFaultDocker(t, "inspect", "--format", "{{.State.Running}}", ackFaultContainer(leader)))
	journal.record("leader_killed", state.Leader)
	remaining := ackFaultOtherNodes(leader)
	next := waitAckFaultState(t, ctx.GetTopic(), remaining, state.Leader, 2)
	require.Greater(t, next.LeaderEpoch, state.LeaderEpoch)
	journal.record("majority_leader", next)
	newLeader := coordinatorNode(t, next.Leader)
	publishAckFaultEventually(t, journal, ctx.GetTopic(), newLeader, message)
	for seq := uint64(10); seq <= 16; seq++ {
		publishAckFaultEventually(t, journal, ctx.GetTopic(), newLeader, ackFaultMessage("producer", seq))
	}
	ackFaultDocker(t, "start", ackFaultContainer(leader))
	journal.record("leader_restarted", state.Leader)
	verifyAckFaultRecovery(t, ctx, journal)
}

// TestAcknowledgedWritesFenceIsolatedLeader requires majority progress and rejects
// fresh acknowledgements from the isolated leader before comparing healed replicas.
func TestAcknowledgedWritesFenceIsolatedLeader(t *testing.T) {
	if os.Getenv("RUN_E2E_ACK_FAULTS") != "1" {
		t.Skip("set RUN_E2E_ACK_FAULTS=1 for isolated acknowledged-write faults")
	}
	journal := newAckFaultJournal(t)
	ctx := newAckFaultCluster(t, "ack-isolated-leader")
	state := waitAckFaultState(t, ctx.GetTopic(), []int{1, 2, 3}, "", 3)
	leader := coordinatorNode(t, state.Leader)
	for seq := uint64(1); seq <= 8; seq++ {
		publishAckFaultEventually(t, journal, ctx.GetTopic(), leader, ackFaultMessage("producer", seq))
	}
	captureAckFaultMetrics(t, journal, "before-isolation")
	startedAt := ackFaultDocker(t, "inspect", "--format", "{{.State.StartedAt}}", ackFaultContainer(leader))
	heal := isolateAckFaultLeader(t, leader, journal)
	next := waitAckFaultState(t, ctx.GetTopic(), ackFaultOtherNodes(leader), state.Leader, 2)
	require.Greater(t, next.LeaderEpoch, state.LeaderEpoch)
	journal.record("majority_leader", next)
	for seq := uint64(9); seq <= 11; seq++ {
		publishAckFaultEventually(t, journal, ctx.GetTopic(), coordinatorNode(t, next.Leader), ackFaultMessage("producer", seq))
		attempt, sent := journal.publish(ctx.GetBrokerAddrs()[leader-1], ctx.GetTopic(), ackFaultMessage("isolated", seq-8))
		require.True(t, sent, "old leader client handshake and request write must remain available")
		require.NotEqual(t, "acknowledged", attempt.Outcome, "isolated old leader acknowledged a fresh write")
	}
	journal.record("firewall_counters", ackFaultDocker(t, "exec", "-u", "0", ackFaultContainer(leader), "iptables", "-L", "-n", "-v", "-x"))
	require.Equal(t, startedAt, ackFaultDocker(t, "inspect", "--format", "{{.State.StartedAt}}", ackFaultContainer(leader)), "isolated broker must not restart")
	heal()
	verifyAckFaultRecovery(t, ctx, journal)
}

func ackFaultMessage(producer string, sequence uint64) types.Message {
	return types.Message{ProducerID: producer, SeqNum: sequence, Epoch: 0,
		Payload: fmt.Sprintf("%s-message-%04d", producer, sequence)}
}

// ackFaultOtherNodes returns the surviving peer numbers for a three-broker fixture.
func ackFaultOtherNodes(excluded int) []int {
	var nodes []int
	for node := 1; node <= 3; node++ {
		if node != excluded {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// publishAckFaultEventually retries the same identity while journaling every outcome.
func publishAckFaultEventually(t *testing.T, journal *ackFaultJournal, topic string, node int, message types.Message) {
	t.Helper()
	require.NoError(t, eventually(t, "acknowledgement for "+writeIdentity(message), 2*clusterReadyTimeout, func() (bool, string, error) {
		attempt, _ := journal.publish(clusterBrokerAddrs(3)[node-1], topic, message)
		return attempt.Outcome == "acknowledged", attempt.Error, nil
	}))
}

type ackFaultPartition struct {
	Topic            string   `json:"topic"`
	Partition        int      `json:"partition"`
	Leader           string   `json:"leader"`
	LeaderEpoch      int      `json:"leader_epoch"`
	CommittedHWM     uint64   `json:"committed_hwm"`
	ISR              []string `json:"isr"`
	RecoveryPending  bool     `json:"recovery_pending"`
	RecoveryReplicas []string `json:"recovery_replicas"`
}

// ackFaultState reads one broker's metadata view of the test partition.
func ackFaultState(node int, topic string) (ackFaultPartition, error) {
	response, _, err := ackFaultRoundTrip(clusterBrokerAddrs(3)[node-1], "CLUSTER_STATUS")
	if err != nil {
		return ackFaultPartition{}, err
	}
	var status struct {
		Partitions []ackFaultPartition `json:"partitions"`
	}
	if err := json.Unmarshal([]byte(strings.TrimPrefix(string(response), "OK cluster=")), &status); err != nil {
		return ackFaultPartition{}, err
	}
	for _, partition := range status.Partitions {
		if partition.Topic == topic && partition.Partition == 0 {
			return partition, nil
		}
	}
	return ackFaultPartition{}, fmt.Errorf("partition %s not found on broker %d", topic, node)
}

// waitAckFaultState waits for the selected brokers to agree on leadership and HWM,
// with the required ISR size and no unresolved replica recovery.
func waitAckFaultState(t *testing.T, topic string, nodes []int, excluded string, expectedISR int) ackFaultPartition {
	t.Helper()
	return waitAckFaultStateWithin(t, topic, nodes, excluded, expectedISR, 2*clusterReadyTimeout)
}

// waitAckFaultStateWithin waits within the supplied deadline for a stable leader, ISR, and committed watermark.
func waitAckFaultStateWithin(t *testing.T, topic string, nodes []int, excluded string, expectedISR int, timeout time.Duration) ackFaultPartition {
	t.Helper()
	var result ackFaultPartition
	require.NoError(t, eventually(t, "agreed partition leadership and ISR", timeout, func() (bool, string, error) {
		var first ackFaultPartition
		for index, node := range nodes {
			state, err := ackFaultState(node, topic)
			if err != nil {
				return false, fmt.Sprintf("broker-%d: %v", node, err), nil
			}
			if state.Leader == "" || (excluded != "" && (state.Leader == excluded || slices.Contains(state.ISR, excluded))) || len(state.ISR) != expectedISR || state.RecoveryPending || len(state.RecoveryReplicas) != 0 {
				return false, fmt.Sprintf("broker-%d: %+v", node, state), nil
			}
			if index == 0 {
				first = state
			} else if first.Leader != state.Leader || first.LeaderEpoch != state.LeaderEpoch || first.CommittedHWM != state.CommittedHWM {
				return false, "local partition observations disagree", nil
			}
		}
		result = first
		return true, fmt.Sprintf("%+v", result), nil
	}))
	return result
}

// verifyAckFaultRecovery requires sustained full ISR, checks every replica against
// the write journal, and independently compares the public consumer's offsets and payloads.
func verifyAckFaultRecovery(t *testing.T, ctx *ClusterTestContext, journal *ackFaultJournal) {
	t.Helper()
	converged := waitAckFaultState(t, ctx.GetTopic(), []int{1, 2, 3}, "", 3)
	journal.record("isr_converged", converged)
	require.NoError(t, eventuallyStable(t, "stable ISR after acknowledged-write recovery", 3*time.Minute, 65*time.Second,
		func() (bool, string, error) { return fullISRAndZeroUnderReplicated(ctx) }))
	state := waitAckFaultState(t, ctx.GetTopic(), []int{1, 2, 3}, "", 3)
	journal.record("full_isr", state)
	captureAckFaultMetrics(t, journal, "recovered-before-readback")
	var canonical []types.Message
	attempts := journal.snapshotAttempts()
	var payloadBytes int64
	for _, attempt := range attempts {
		payloadBytes += 2 * int64(len(attempt.Message.Payload)+256)
	}
	// Bound long-campaign inspection by recorded payload work, including
	// framing allowance, rather than applying the tiny fixture's fixed 30s.
	readbackTimeout := 30*time.Second + time.Duration(payloadBytes/(512<<10))*time.Second
	for node := 1; node <= 3; node++ {
		records := readRecoveryHistoryFrom(t, ctx.GetTopic(), node, node%3+1, coordinatorNode(t, state.Leader), state.LeaderEpoch, state.CommittedHWM, readbackTimeout)
		require.NoError(t, checkWriteHistory(attempts, records), "replica %d", node)
		if node == 1 {
			canonical = records
		} else {
			require.Equal(t, canonical, records, "replicas disagree on full decoded records")
		}
	}
	// Also verify the public consumer view, independent of internal catch-up.
	const group = "ack-fault-reader"
	reader, generation, member := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), group)
	defer reader.Close()
	consumer := e2e.NewBrokerClient([]string{ctx.GetBrokerAddrs()[coordinatorNode(t, state.Leader)-1]})
	defer consumer.Close()
	var consumed uint64
	lastHeartbeat := time.Now()
	for consumed < state.CommittedHWM {
		if time.Since(lastHeartbeat) >= 2*time.Second {
			heartbeatGroup(t, reader, ctx.GetTopic(), group, generation, member)
			lastHeartbeat = time.Now()
		}
		payloads, offsets, err := consumer.ConsumeMessagesAtOffsetWithOffsets(ctx.GetTopic(), 0, consumed, group, member, generation, 5*time.Second)
		require.NoError(t, err)
		require.NotEmpty(t, payloads, "consumer stopped before the captured committed end")
		require.Len(t, offsets, len(payloads))
		for index, payload := range payloads {
			require.Less(t, consumed, uint64(len(canonical)))
			require.Equal(t, canonical[consumed].Offset, offsets[index])
			require.Equal(t, canonical[consumed].Payload, payload)
			consumed++
		}
	}
	require.Equal(t, state.CommittedHWM, consumed)
	if journal.compact {
		digest := sha256.New()
		encoder := json.NewEncoder(digest)
		for _, record := range canonical {
			require.NoError(t, encoder.Encode(record))
		}
		journal.record("verified_history", map[string]any{"records": len(canonical), "attempts": len(attempts), "sha256_json_lines": fmt.Sprintf("%x", digest.Sum(nil))})
	} else {
		journal.record("verified_records", canonical)
	}
	leave, err := reader.SendCommand("", fmt.Sprintf("LEAVE_GROUP topic=%s group=%s member=%s generation=%d", ctx.GetTopic(), group, member, generation), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(leave, "OK"), leave)
	captureAckFaultMetrics(t, journal, "after-readback")
}

// Raw counter samples retain process start time. A restarted process is a new
// interval, not a negative delta or a claim that its previous work was zero.
func captureAckFaultMetrics(t *testing.T, journal *ackFaultJournal, phase string) {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second}
	for node := 1; node <= 3; node++ {
		url := fmt.Sprintf("http://127.0.0.1:%d/metrics", baseExporterPort+ackFaultPortShift+node)
		response, err := client.Get(url) // #nosec G107 -- isolated loopback test ports.
		require.NoError(t, err)
		data, readErr := io.ReadAll(io.LimitReader(response.Body, 8<<20))
		_ = response.Body.Close()
		require.NoError(t, readErr)
		require.Equal(t, http.StatusOK, response.StatusCode)
		var lines []string
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "cursus_recovery_work_total{") || strings.HasPrefix(line, "cursus_recovery_attempts_total{") ||
				strings.HasPrefix(line, "process_start_time_seconds ") || strings.HasPrefix(line, "process_resident_memory_bytes ") ||
				strings.HasPrefix(line, "process_cpu_seconds_total ") || strings.HasPrefix(line, "go_memstats_heap_alloc_bytes ") {
				lines = append(lines, line)
			}
		}
		journal.record("recovery_metrics", map[string]any{"phase": phase, "node": node, "samples": lines})
	}
}
