package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// This opt-in local experiment exercises the real scheduler, request builder,
// disk apply, proof/witness and foreground partition storage. Transport is
// controlled. Metadata appends below are NOT client offset-commit RPCs; the
// separate cluster experiment is required for end-to-end claims.
type workloadFetcher struct {
	mu    sync.Mutex
	start time.Time
	first map[int]time.Duration
	bytes int64
	calls map[int]int
}

func (f *workloadFetcher) FetchReplicaCatchup(ctx context.Context, _ string, _ int, r fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
	f.mu.Lock()
	if _, ok := f.first[r.Partition]; !ok {
		f.first[r.Partition] = time.Since(f.start)
	}
	f.calls[r.Partition]++
	f.mu.Unlock()
	if r.Partition == 0 {
		<-ctx.Done()
		return fsm.ReplicaCatchupBatch{}, ctx.Err()
	}
	if r.Partition == 1 {
		timer := time.NewTimer(25 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return fsm.ReplicaCatchupBatch{}, ctx.Err()
		case <-timer.C:
		}
	}
	end := min(r.NextOffset+1, r.CommittedHWM)
	b := fsm.ReplicaCatchupBatch{Topic: r.Topic, Partition: r.Partition, BrokerID: r.BrokerID, StartOffset: r.NextOffset, EndOffset: end, CommittedHWM: r.CommittedHWM, Leader: r.Leader, SourceBroker: r.SourceBroker, LeaderEpoch: r.LeaderEpoch, LifecycleEpoch: r.LifecycleEpoch, Verified: end == r.CommittedHWM}
	if end > r.NextOffset {
		b.Messages = []types.Message{{Offset: r.NextOffset, Payload: strings.Repeat("x", 8192), LeaderEpoch: int64(r.LeaderEpoch)}}
	}
	b, err := fsm.SealReplicaCatchupBatch(b)
	data, _ := json.Marshal(b)
	f.mu.Lock()
	f.bytes += int64(len(data))
	f.mu.Unlock()
	return b, err
}

func TestRecoveryMixedLocalWorkload(t *testing.T) {
	output := os.Getenv("CURSUS_RECOVERY_RESULT")
	if output == "" {
		t.Skip("set CURSUS_RECOVERY_RESULT for controlled local scheduler measurement")
	}
	util.SetLevel(util.LogLevelError)
	t.Cleanup(func() { util.SetLevel(util.LogLevelDebug) })
	cc, p := recoverySchedulerFixture(t, 1, 10000, 10000, 2, 0, 0)
	if raw := os.Getenv("CURSUS_RECOVERY_TEST_RATE"); raw != "" {
		rate, err := strconv.ParseInt(raw, 10, 64)
		require.NoError(t, err)
		cc.Config.Recovery.BytesPerSecond = rate
		cc.Config.Recovery.SourceBytesPerSecond = rate / 2
		cc.Config.Recovery.BurstBytes = rate / 2
		cc.Config.Recovery.SourceBurstBytes = rate / 4
		cc.Config.Recovery.QuantumBytes = min(65536, rate/4)
	}
	state := cc.RaftManager.GetFSM()
	for id := range p {
		name := fmt.Sprintf("source-%d", id)
		data, err := json.Marshal(fsm.BrokerInfo{ID: name, Addr: name + ":9000", Status: "active"})
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)}))
		meta := state.GetPartitionMetadata(fmt.Sprintf("orders-%d", id))
		meta.Leader, meta.Replicas, meta.ISR = name, []string{name, "receiver"}, []string{name}
		meta.LeaderEpoch++
		if id >= 4 {
			meta.ISR = []string{name, "receiver"}
		}
		data, err = json.Marshal(meta)
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte(fmt.Sprintf("PARTITION:orders-%d:", id)), data...)}))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var appendUS, readUS, metadataUS []int64
	foregroundDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for seq := uint64(0); ; seq++ {
			select {
			case <-ctx.Done():
				foregroundDone <- nil
				return
			case <-ticker.C:
			}
			started := time.Now()
			if err := p[4].EnqueueSync(types.Message{Payload: "live-produce", ProducerID: "live", SeqNum: seq + 1}); err != nil {
				foregroundDone <- err
				return
			}
			appendTime := time.Since(started).Microseconds()
			p[4].SetHWM(seq + 1)
			started = time.Now()
			if _, err := p[4].ReadCommitted(seq, 1); err != nil {
				foregroundDone <- err
				return
			}
			readTime := time.Since(started).Microseconds()
			started = time.Now()
			if err := p[5].EnqueueSync(types.Message{Payload: fmt.Sprintf("offset=%d", seq), ProducerID: "metadata", SeqNum: seq + 1}); err != nil {
				foregroundDone <- err
				return
			}
			mu.Lock()
			appendUS = append(appendUS, appendTime)
			readUS = append(readUS, readTime)
			metadataUS = append(metadataUS, time.Since(started).Microseconds())
			mu.Unlock()
		}
	}()
	f := &workloadFetcher{start: time.Now(), first: make(map[int]time.Duration), calls: make(map[int]int)}
	completed := make(chan time.Duration, 1)
	var once sync.Once
	cc.SetReplicaCatchupProofSubmitter(func(ctx context.Context, proof fsm.ISRCatchupProof) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := json.Marshal(proof)
		if err != nil {
			return err
		}
		if result := state.Apply(&raft.Log{Data: append([]byte("ISR_CATCHUP:"), data...)}); result != nil {
			return result.(error)
		}
		if proof.Partition == 3 {
			once.Do(func() { completed <- time.Since(f.start) })
		}
		return nil
	})
	apply := func(ctx context.Context, r fsm.ReplicaCatchupRequest, b fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
		return applySchedulerTestBatch(ctx, p[r.Partition], r, b)
	}
	cc.StartReplicaCatchupVerified(ctx, f, apply)
	var completion time.Duration
	// Equal observation windows are essential: stopping on small completion
	// would leave the improved scheduler with too few foreground samples.
	<-time.After(40 * time.Second)
	cancel()
	cc.StopReplicaCatchup()
	select {
	case completion = <-completed:
	default:
		t.Error("small recovery did not rejoin ISR within experiment window")
	}
	require.NoError(t, <-foregroundDone)
	percentiles := func(values []int64) map[string]int64 {
		if len(values) == 0 {
			return nil
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		return map[string]int64{"count": int64(len(values)), "p95_us": values[(len(values)-1)*95/100], "p99_us": values[(len(values)-1)*99/100]}
	}
	result := map[string]any{"scope": "controlled transport, local durable storage and exact FSM ISR admission; not client RPC", "window_seconds": 40, "small_complete_ms": completion.Milliseconds(), "small_first_ms": f.first[3].Milliseconds(), "transfer_bytes": f.bytes, "calls": f.calls, "produce_storage": percentiles(appendUS), "consume_storage": percentiles(readUS), "metadata_storage": percentiles(metadataUS)}
	data, err := json.MarshalIndent(result, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(output, data, 0600))
	t.Log(string(data))
}
