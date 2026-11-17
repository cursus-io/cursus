package fsm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// Bounded characterization of real retained work and unrelated commit progress.
type preparedScanStorage struct {
	*disk.DiskHandler
	entered, release             chan struct{}
	cancel                       context.CancelFunc
	scans, records, payloadBytes int
	scanDuration                 time.Duration
}

func (s *preparedScanStorage) TruncateToIfGenerationContext(ctx context.Context, n uint64, g disk.ContentGeneration) error {
	err := s.DiskHandler.TruncateToIfGenerationContext(ctx, n, g)
	if err == nil && s.cancel != nil {
		s.cancel()
	}
	return err
}
func (s *preparedScanStorage) ScanRetainedRange(ctx context.Context, a, b uint64, visit func(types.Message) error) error {
	s.scans++
	if s.entered != nil {
		close(s.entered)
		<-s.release
	}
	started := time.Now()
	defer func() { s.scanDuration += time.Since(started) }()
	return s.DiskHandler.ScanRetainedRange(ctx, a, b, func(m types.Message) error {
		s.records++
		s.payloadBytes += len(m.Payload)
		return visit(m)
	})
}

type preparedScanProvider struct {
	*disk.DiskManager
	storage *preparedScanStorage
}

func (p *preparedScanProvider) GetHandlerWithPolicy(name string, id int, cleanup string, hours int, bytes int64) (types.StorageHandler, error) {
	h, err := p.DiskManager.GetHandlerWithPolicy(name, id, cleanup, hours, bytes)
	if err != nil || name != "orders" {
		return h, err
	}
	p.storage = &preparedScanStorage{DiskHandler: h.(*disk.DiskHandler)}
	return p.storage, nil
}

func TestPreparedRollbackWorkAndCrossPartitionCommit(t *testing.T) {
	for _, mib := range []int{1, 16, 64} {
		t.Run(fmt.Sprintf("%dMiB", mib), func(t *testing.T) {
			count := mib * 1024 * 1024 / (16 * 1024)
			records := make([]types.Message, count+1)
			payload := strings.Repeat("x", 16*1024)
			for i := range records {
				records[i] = types.Message{Offset: uint64(i), Payload: payload}
			}
			var provider *preparedScanProvider
			state, p := indexedRecoveryFSMWithProvider(t, records, func(dm *disk.DiskManager) topic.HandlerProvider {
				provider = &preparedScanProvider{DiskManager: dm}
				return provider
			})
			s := provider.storage
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.cancel = cancel
			require.NoError(t, state.tm.CreateTopic("other", 1, false, false))
			other, err := state.tm.GetTopic("other").GetPartition(0)
			require.NoError(t, err)
			t.Cleanup(other.Close)
			def := state.tm.GetTopic("other").Definition()
			state.topicState["other"] = &def
			state.partitionMetadata["other-0"] = &PartitionMetadata{Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: def.LifecycleEpoch, CommittedHWMKnown: true, Replicas: []string{"node-1", "node-2"}, ISR: []string{"node-1"}}
			s.entered = make(chan struct{})
			s.release = make(chan struct{})
			// Exclude partition initialization scans from measured repair work.
			s.scans, s.records, s.payloadBytes, s.scanDuration = 0, 0, 0, 0
			released := false
			defer func() {
				if !released {
					close(s.release)
				}
			}()
			offset := uint64(count)
			batch := ReplicaCatchupBatch{Topic: "orders", BrokerID: "node-2", SourceBroker: "node-1", Leader: "node-1", LeaderEpoch: 4, StartOffset: offset + 1, EndOffset: offset + 1, CommittedHWM: offset + 1, TruncateTo: &offset}
			// Use the metadata lifecycle rather than any receiver-provided authority.
			batch.LifecycleEpoch = state.GetPartitionMetadata("orders-0").LifecycleEpoch
			done := make(chan error, 1)
			started := time.Now()
			go func() {
				prepared, err := p.PrepareReplicaRollbackContext(ctx, offset, offset+1, nil, offset+1)
				if err != nil {
					done <- err
					return
				}
				done <- state.WithReplicaRecoveryFence(batch, func() error {
					return p.ApplyPreparedReplicaRollbackChecked(ctx, prepared, offset, offset+1, nil, offset+1, nil)
				})
			}()
			select {
			case <-s.entered:
			case err := <-done:
				t.Fatalf("rollback ended before scan: %v", err)
			case <-time.After(20 * time.Second):
				t.Fatal("scan not reached")
			}
			require.NoError(t, ctx.Err())
			appendStarted := time.Now()
			require.NoError(t, other.ReplicaAppend([]types.Message{{Offset: 0, Payload: "other-partition"}}))
			appendElapsed := time.Since(appendStarted)
			data, err := json.Marshal(partitionCommitCommand{Topic: "other", Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: def.LifecycleEpoch, HWM: 1})
			require.NoError(t, err)
			applied := make(chan interface{}, 1)
			attempting := make(chan struct{})
			go func() {
				close(attempting)
				applied <- state.Apply(&raft.Log{Data: append([]byte("PARTITION_COMMIT:"), data...)})
			}()
			<-attempting
			commitStarted := time.Now()
			select {
			case result := <-applied:
				require.Nil(t, result)
			case <-time.After(5 * time.Second):
				t.Fatal("unrelated commit blocked during preparation")
			}
			commitElapsed := time.Since(commitStarted)
			require.Equal(t, uint64(1), other.GetHWM())
			time.Sleep(500 * time.Millisecond)
			close(s.release)
			released = true
			require.NoError(t, <-done)
			repairElapsed := time.Since(started)
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			require.Equal(t, uint64(1), other.GetHWM())
			require.Equal(t, offset, p.NextOffset())
			require.Equal(t, offset, p.GetHWM())
			require.NoError(t, p.RecoveryError())
			require.Equal(t, 1, s.scans)
			require.Equal(t, count, s.records)
			require.Equal(t, mib*1024*1024, s.payloadBytes)
			t.Logf("PREPARED retained_mib=%d scans=%d records=%d payload_read_bytes=%d scan_ms=%d injected_hold_ms=500 repair_ms=%d other_append_ms=%d other_raft_commit_wait_ms=%d", mib, s.scans, s.records, s.payloadBytes, s.scanDuration.Milliseconds(), repairElapsed.Milliseconds(), appendElapsed.Milliseconds(), commitElapsed.Milliseconds())
		})
	}
}
