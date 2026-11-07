package e2e_cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The runner starts a fresh, isolated three-broker cluster. This test deliberately
// refuses the default developer container names and never runs compose down.
func TestRecoveryBudgetMixedLoad(t *testing.T) {
	output := os.Getenv("CURSUS_RECOVERY_E2E_RESULT")
	if output == "" {
		t.Skip("set CURSUS_RECOVERY_E2E_RESULT and use the isolated recovery runner")
	}
	prefix := os.Getenv("CURSUS_E2E_CONTAINER_PREFIX")
	require.True(t, strings.HasPrefix(prefix, "cursus-recovery-"), "isolated recovery containers required")
	require.GreaterOrEqual(t, clusterPortShift(), 10000)
	ctx := GivenCluster(t).WithTopic("c-large").WithPartitions(1).WithAcks("all")
	ctx.WhenCluster().StartCluster()
	runRecoveryBudgetMixedLoad(t, ctx, prefix, output, false)
}

func TestRecoveryBudgetInterruptedMixedLoad(t *testing.T) {
	if os.Getenv("RUN_E2E_RECOVERY_BOUNDARIES") != "1" {
		t.Skip("set RUN_E2E_RECOVERY_BOUNDARIES=1 for deterministic recovery faults")
	}
	ctx := newAckFaultClusterMode(t, "c-large", true)
	dir := os.Getenv("CURSUS_ACK_FAULT_RESULTS")
	if dir == "" {
		dir = t.TempDir()
	}
	require.NoError(t, os.MkdirAll(dir, 0o750))
	output := filepath.Join(dir, t.Name()+".json")
	runRecoveryBudgetMixedLoad(t, ctx, ackFaultProject+"-", output, true)
}

func runRecoveryBudgetMixedLoad(t *testing.T, ctx *ClusterTestContext, prefix, output string, interrupt bool) {
	t.Helper()
	addrs := ctx.GetBrokerAddrs()
	admin := e2e.NewBrokerClient(addrs[:2])
	defer admin.Close()
	names := []string{"a-unresponsive", "b-slow", "c-large", "z-small"}
	for _, name := range names {
		if !interrupt || name != ctx.GetTopic() {
			sendClusterTopicCommand(t, addrs, "CREATE topic="+name+" partitions=1 replication_factor=3 idempotent=true")
		}
		requireReplicaOffsetsEventually(t, addrs, name, 0)
		require.NoError(t, admin.PublishIdempotentToPartition(name, "seed", 0, 1, 0, "seed", "all", true))
	}
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "recovery budget fixture")
	var backlogPayloadBytes int64
	for i, name := range names {
		leader := 1
		if i >= 2 {
			leader = 2
		}
		sendClusterTopicCommand(t, addrs, fmt.Sprintf("ELECT_LEADER topic=%s partition=0 broker=broker-%d-9000", name, leader))
	}
	var journal *ackFaultJournal
	var boundary string
	if interrupt {
		journal = newAckFaultJournal(t)
		boundary = armRecoveryBoundary(t, 3, "replica-catchup", "c-large-0")
		captureAckFaultMetrics(t, journal, "before-recovery-backlog")
	}
	recoveryDocker(t, "stop", prefix+"broker-3")
	waitForBrokerEvictedFromISR(t, ctx, 3)
	// The live topic has only the two remaining brokers, so its successful
	// requests exercise healthy ISR replication while the third broker repairs.
	const live, group = "live-traffic", "recovery-budget-reader"
	sendClusterTopicCommand(t, addrs[:2], "CREATE topic="+live+" partitions=1 replication_factor=2 idempotent=true")
	requireReplicaOffsetsEventually(t, addrs[:2], live, 0)
	sendClusterTopicCommand(t, addrs[:2], "ELECT_LEADER topic="+live+" partition=0 broker=broker-1-9000")
	for i, name := range names {
		count := []int{1, 128, 4096, 2}[i]
		for start := 0; start < count; start += 128 {
			messages := make([]types.Message, min(128, count-start))
			for j := range messages {
				messages[j] = types.Message{ProducerID: "seed", SeqNum: uint64(start + j + 2), Payload: recoveryBudgetPayload(name, uint64(start+j+1))}
				backlogPayloadBytes += int64(len(messages[j].Payload))
			}
			payload, err := util.EncodeBatchMessages(name, 0, "all", true, messages)
			require.NoError(t, err)
			leader := 0
			if i >= 2 {
				leader = 1
			}
			conn, err := net.DialTimeout("tcp", addrs[leader], 3*time.Second)
			require.NoError(t, err)
			require.NoError(t, conn.SetDeadline(time.Now().Add(30*time.Second)))
			client, err := wire.NewClientConn(conn, "none")
			require.NoError(t, err)
			require.NoError(t, client.Send(payload))
			response, err := client.Receive()
			_ = conn.Close()
			require.NoError(t, err, string(response))
			var ack struct {
				Status string `json:"status"`
			}
			require.NoError(t, json.Unmarshal(response, &ack))
			require.Equal(t, "OK", ack.Status, string(response))
		}
	}
	for node := 1; node <= 2; node++ {
		recoveryDocker(t, "exec", prefix+fmt.Sprintf("broker-%d", node), "mkdir", "-p", "/run/cursus-e2e-faults/recovery-source")
	}
	recoveryDocker(t, "exec", prefix+"broker-1", "sh", "-c", "printf 2m > /run/cursus-e2e-faults/recovery-source/a-unresponsive")
	recoveryDocker(t, "exec", prefix+"broker-1", "sh", "-c", "printf 200ms > /run/cursus-e2e-faults/recovery-source/b-slow")
	// Join after backlog creation: slow seeding must not consume the member's
	// session timeout before the measured heartbeat loop starts.
	reader, generation, member := joinRecoveryBudgetGroup(t, addrs[:2], live, group)
	defer reader.Close()
	consumer := e2e.NewBrokerClient(addrs[:1])
	defer consumer.Close()
	before := recoveryBudgetResources(t, prefix, 2)
	started := time.Now()
	recoveryDocker(t, "start", prefix+"broker-3")
	var produce, consume, commit []int64
	errorsByClass := map[string]int{}
	completion := -time.Millisecond
	var seq, consumed uint64
	var emptyReads int
	nextProbe := started
	interrupted := false
	commitsBeforeCrash := 0
	successfulCommits := 0
	for time.Since(started) < 50*time.Second {
		time.Sleep(5 * time.Millisecond)
		if time.Now().After(nextProbe) {
			if interrupt && !interrupted && successfulCommits >= 10 {
				entered := ackFaultDocker(t, "exec", ackFaultContainer(3), "sh", "-c", "if test -f "+boundary+".entered; then echo entered; else echo waiting; fi")
				if entered == "entered" {
					journal.record("recovery_interrupted_after_durable_append", boundary)
					var interruptedWork map[string]any
					require.NoError(t, json.Unmarshal([]byte(ackFaultDocker(t, "exec", ackFaultContainer(3), "cat", boundary+".work.json")), &interruptedWork))
					journal.record("interrupted_process_work", interruptedWork)
					journal.record("interrupted_process_started_at", ackFaultDocker(t, "inspect", "--format", "{{.State.StartedAt}}", ackFaultContainer(3)))
					journal.record("process_resources_before_interruption", recoveryBudgetResources(t, prefix, 3))
					ackFaultDocker(t, "kill", "--signal=KILL", ackFaultContainer(3))
					sendClusterTopicCommand(t, addrs[:2], "ELECT_LEADER topic=c-large partition=0 broker=broker-1-9000")
					journal.record("recovery_source_changed", "broker-2-9000 -> broker-1-9000")
					ackFaultDocker(t, "start", ackFaultContainer(3))
					commitsBeforeCrash = successfulCommits
					interrupted = true
				}
			}
			if interrupt {
				response, err := reader.SendCommand("", fmt.Sprintf("HEARTBEAT topic=%s group=%s member=%s generation=%d", live, group, member, generation), 3*time.Second)
				if err != nil || !strings.HasPrefix(response, "OK") {
					errorsByClass["heartbeat"]++
					journal.record("foreground_heartbeat_error", map[string]any{"response": response, "error": fmt.Sprint(err), "elapsed_ms": time.Since(started).Milliseconds()})
				}
			} else {
				heartbeatGroup(t, reader, live, group, generation, member)
			}
			response, err := admin.SendCommand("", "DESCRIBE topic=z-small", 3*time.Second)
			if err == nil && completion < 0 {
				var meta topicMetadata
				if json.Unmarshal([]byte(response), &meta) == nil && len(meta.Partitions) == 1 && containsRecoveryMember(meta.Partitions[0].ISR, "broker-3-9000") {
					completion = time.Since(started)
				}
			}
			nextProbe = time.Now().Add(100 * time.Millisecond)
		}
		begin := time.Now()
		err := admin.PublishIdempotentToPartition(live, "live-producer", 0, seq+1, 0, fmt.Sprintf("live-%d", seq), "all", true)
		produce = append(produce, time.Since(begin).Microseconds())
		if err != nil {
			errorsByClass["produce"]++
			continue
		}
		seq++
		begin = time.Now()
		messages, offsets, err := consumer.ConsumeMessagesAtOffsetWithOffsets(live, 0, consumed, group, member, generation, 3*time.Second)
		consume = append(consume, time.Since(begin).Microseconds())
		if err != nil {
			if errorsByClass["consume"] == 0 {
				t.Logf("first consume error: %v", err)
			}
			errorsByClass["consume"]++
			continue
		}
		if len(messages) == 0 || len(offsets) == 0 {
			emptyReads++
			continue
		}
		require.Len(t, offsets, len(messages))
		for i, payload := range messages {
			require.Equal(t, consumed+uint64(i), offsets[i], "foreground offsets must not skip or repeat")
			require.Equal(t, fmt.Sprintf("live-%d", offsets[i]), payload)
		}
		consumed = offsets[len(offsets)-1] + 1
		begin = time.Now()
		response, err := reader.SendCommand("", fmt.Sprintf("COMMIT_OFFSET topic=%s partition=0 group=%s offset=%d generation=%d member=%s", live, group, consumed, generation, member), 3*time.Second)
		commit = append(commit, time.Since(begin).Microseconds())
		if err != nil || !strings.HasPrefix(response, "OK") {
			if interrupt {
				var brokerError *wire.BrokerError
				_ = errors.As(err, &brokerError)
				journal.record("foreground_commit_error", map[string]any{"offset": consumed, "response": response, "error": fmt.Sprint(err), "broker_error": brokerError, "elapsed_ms": time.Since(started).Milliseconds(), "interrupted": interrupted})
			}
			errorsByClass["commit"]++
		} else {
			successfulCommits++
		}
	}
	percentiles := func(samples []int64) map[string]int64 {
		if len(samples) == 0 {
			return nil
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		return map[string]int64{"count": int64(len(samples)), "p95_us": samples[(len(samples)-1)*95/100], "p99_us": samples[(len(samples)-1)*99/100]}
	}
	firstText := recoveryDocker(t, "exec", prefix+"broker-2", "cat", "/run/cursus-e2e-faults/recovery-source/z-small.entered")
	first, err := strconv.ParseInt(firstText, 10, 64)
	require.NoError(t, err)
	finalISR := make(map[string][]string)
	for _, name := range names {
		response, err := admin.SendCommand("", "DESCRIBE topic="+name, 3*time.Second)
		require.NoError(t, err)
		var metadata topicMetadata
		require.NoError(t, json.Unmarshal([]byte(response), &metadata))
		require.Len(t, metadata.Partitions, 1)
		finalISR[name] = metadata.Partitions[0].ISR
	}
	resources := recoveryBudgetResources(t, prefix, 3)
	result := map[string]any{"window_seconds": 50, "small_first_ms": time.Unix(0, first).Sub(started).Milliseconds(), "small_exact_isr_ms": completion.Milliseconds(), "final_isr": finalISR, "produce": percentiles(produce), "consume": percentiles(consume), "offset_commit": percentiles(commit), "empty_reads": emptyReads, "consumed_records": consumed, "errors": errorsByClass, "resources_before": before, "resources_after": resources}
	result["successful_offset_commits"] = successfulCommits
	data, err := json.MarshalIndent(result, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(output, data, 0600))
	t.Log(string(data))
	require.Greater(t, completion, time.Duration(0), "small replica never rejoined exact ISR")
	// Keep failures visible while collecting post-heal data and recovery work.
	assert.Empty(t, errorsByClass)
	assert.Greater(t, successfulCommits, 100, "foreground must exercise successful offset commits")
	if interrupt {
		require.True(t, interrupted, "large recovery must actually reach its durable append boundary")
		assert.Greater(t, successfulCommits-commitsBeforeCrash, 10, "foreground must progress after interruption")
		for node := 1; node <= 3; node++ {
			for _, command := range []string{"CLUSTER_STATUS", "LIST_OFFSETS topic=__consumer_offsets"} {
				response, _, err := ackFaultRoundTrip(addrs[node-1], command)
				journal.record("post_window_state", map[string]any{"node": node, "command": command, "response": string(response), "error": fmt.Sprint(err)})
			}
		}
		for node := 1; node <= 2; node++ {
			ackFaultDocker(t, "exec", ackFaultContainer(node), "rm", "-f", "/run/cursus-e2e-faults/recovery-source/a-unresponsive", "/run/cursus-e2e-faults/recovery-source/b-slow")
		}
		// The fixture uses default quotas: its >32 MiB backlog alone needs
		// over 64 seconds at 512 KiB/s. The generic 60-second metadata wait
		// cannot cover that transfer plus an in-flight stall/backoff. Allow
		// twice payload bytes for wire/proof overhead and one shared control
		// allowance; keep the 50-second foreground assertions unchanged.
		rate := config.DefaultRecoveryConfig().SourceBytesPerSecond
		healTimeout := 2*clusterReadyTimeout + time.Duration((2*backlogPayloadBytes+rate-1)/rate)*time.Second
		healDeadline := time.Now().Add(healTimeout)
		journal.record("post_heal_budget", map[string]any{"payload_bytes": backlogPayloadBytes, "wire_allowance_bytes": 2 * backlogPayloadBytes, "source_bytes_per_second": rate, "timeout_ms": healTimeout.Milliseconds()})
		for _, name := range names {
			remaining := time.Until(healDeadline)
			require.Positive(t, remaining, "shared post-heal recovery deadline exhausted")
			waitAckFaultStateWithin(t, name, []int{1, 2, 3}, "", 3, remaining)
		}
		captureAckFaultMetrics(t, journal, "interrupted-recovery-before-readback")
		for i, name := range names {
			state := waitAckFaultState(t, name, []int{1, 2, 3}, "", 3)
			// Raw verification is also charged to source quotas. Reuse the
			// byte-sized allowance, not the small-fixture 30-second reader.
			records := readRecoveryHistoryFrom(t, name, 3, 1, coordinatorNode(t, state.Leader), state.LeaderEpoch, state.CommittedHWM, healTimeout)
			require.Len(t, records, []int{2, 129, 4097, 3}[i])
			for offset, record := range records {
				require.Equal(t, uint64(offset), record.Offset)
				require.Equal(t, uint64(offset+1), record.SeqNum)
				require.Equal(t, "seed", record.ProducerID)
				want := "seed"
				if offset > 0 {
					want = recoveryBudgetPayload(name, uint64(offset))
				}
				require.Equal(t, want, record.Payload)
			}
			journal.record("recovered_topic_verified", map[string]any{"topic": name, "records": len(records)})
		}
		liveState := waitAckFaultState(t, live, []int{1, 2}, "", 2)
		require.Equal(t, seq, liveState.CommittedHWM)
		for node := 1; node <= 2; node++ {
			records := readRecoveryHistoryFrom(t, live, node, 3-node, coordinatorNode(t, liveState.Leader), liveState.LeaderEpoch, liveState.CommittedHWM, 30*time.Second)
			require.Len(t, records, int(seq))
			for offset, record := range records {
				require.Equal(t, uint64(offset), record.Offset)
				require.Equal(t, uint64(offset+1), record.SeqNum)
				require.Equal(t, "live-producer", record.ProducerID)
				require.Equal(t, fmt.Sprintf("live-%d", offset), record.Payload)
			}
		}
		journal.record("foreground_acknowledged_records_verified", seq)
		require.Equal(t, consumed, fetchCommittedOffset(t, reader, live, group))
		captureAckFaultMetrics(t, journal, "after-interrupted-recovery-readback")
	}
}

func recoveryBudgetPayload(topic string, offset uint64) string {
	return fmt.Sprintf("%s-%05d-", topic, offset) + strings.Repeat("x", 8192)
}

func recoveryBudgetResources(t *testing.T, prefix string, nodes int) map[string]string {
	t.Helper()
	resources := map[string]string{}
	client := &http.Client{Timeout: 3 * time.Second}
	for node := 1; node <= nodes; node++ {
		id := fmt.Sprintf("broker-%d", node)
		// proc works on both cgroup v1 and v2 Docker hosts. CPU is in CLK_TCK
		// units; VmHWM is process-lifetime peak, not a resettable window maximum.
		resources[id+"_proc"] = recoveryDocker(t, "exec", prefix+id, "sh", "-c", "getconf CLK_TCK; cat /proc/1/stat /proc/1/status /proc/1/io")
		var response *http.Response
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			response, err = client.Get(fmt.Sprintf("http://localhost:%d/metrics", baseExporterPort+clusterPortShift()+node))
			if err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			resources[id+"_metrics_error"] = err.Error()
			continue
		}
		data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
		_ = response.Body.Close()
		if err != nil {
			resources[id+"_metrics_error"] = err.Error()
			continue
		}
		var selected []string
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "cursus_recovery_") || strings.HasPrefix(line, "cursus_cluster_under_replicated_") || strings.HasPrefix(line, "process_cpu_") || strings.HasPrefix(line, "process_resident_") || strings.HasPrefix(line, "process_start_time_seconds ") {
				selected = append(selected, line)
			}
		}
		resources[id+"_metrics"] = strings.Join(selected, "\n")
	}
	return resources
}

func joinRecoveryBudgetGroup(t *testing.T, addrs []string, topicName, group string) (*e2e.BrokerClient, int, string) {
	t.Helper()
	client := e2e.NewBrokerClient(addrs)
	var generation int
	var member string
	// Broker eviction and metadata coordinator election are separate transitions.
	// Retry only explicit pre-admission unavailability, outside the measured window.
	require.NoError(t, eventually(t, "metadata coordinator ready", clusterReadyTimeout, func() (bool, string, error) {
		var err error
		generation, member, err = client.JoinGroup(topicName, group)
		if err != nil && strings.Contains(err.Error(), "coordinator_not_available") {
			return false, err.Error(), nil
		}
		if err != nil && strings.Contains(err.Error(), "write succeeded but read failed") {
			generation, member = recoverAmbiguousGroupMember(t, client, group, client.GetMemberID())
			client.SetMemberID(member)
			return true, "recovered admitted join", nil
		}
		return err == nil, fmt.Sprintf("join: %v", err), err
	}))
	require.NoError(t, eventually(t, "sync measurement group", clusterReadyTimeout, func() (bool, string, error) {
		_, err := client.SyncGroup(topicName, group, generation, member)
		return err == nil, fmt.Sprintf("sync: %v", err), nil
	}))
	return client, generation, member
}

func containsRecoveryMember(members []string, id string) bool {
	for _, member := range members {
		if member == id {
			return true
		}
	}
	return false
}
