package transaction

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func assertDecisionGenerationChange(t *testing.T, m *Manager, changed bool, mutate func()) {
	t.Helper()
	before, ok := m.CaptureTransactionDecisionGeneration()
	require.True(t, ok)
	require.True(t, m.CheckTransactionDecisionGeneration(before))
	mutate()
	require.Equal(t, !changed, m.CheckTransactionDecisionGeneration(before))
	after, ok := m.CaptureTransactionDecisionGeneration()
	require.True(t, ok)
	require.True(t, m.CheckTransactionDecisionGeneration(after))
	if changed {
		require.Greater(t, after, before)
	} else {
		require.Equal(t, before, after)
	}
}

func decisionGenerationSnapshot(id string) *Snapshot {
	return &Snapshot{ID: id, Producer: "producer", Epoch: 7, CoordinatorEpoch: 19,
		Mode: ModeProcessingV1, State: StateOpen, Revision: 1,
		CreatedAt: time.Unix(100, 0), UpdatedAt: time.Unix(100, 0)}
}

func TestDecisionGenerationLifecycle(t *testing.T) {
	for _, mode := range []Mode{ModeLegacy, ModeProcessingV1} {
		t.Run(string(mode), func(t *testing.T) {
			m := NewManager()
			var producer string
			var epoch int64
			assertDecisionGenerationChange(t, m, true, func() {
				var err error
				producer, epoch, err = m.InitProducerWithMode("tx", mode)
				require.NoError(t, err)
			})
			assertDecisionGenerationChange(t, m, false, func() {
				require.Error(t, m.Begin("tx", "wrong-owner", epoch))
				require.Error(t, m.Begin("tx", producer, epoch+1))
				require.Error(t, m.Commit("missing"))
				_, _, err := m.InitProducer("")
				require.Error(t, err)
			})
			assertDecisionGenerationChange(t, m, true, func() {
				require.NoError(t, m.BeginWithDeadline("tx", producer, epoch, time.Now().Add(time.Hour)))
			})
			assertDecisionGenerationChange(t, m, false, func() {
				require.NoError(t, m.AddMessage("tx", producer, epoch, MessageOperation{Topic: "topic"}))
				require.Error(t, m.Begin("tx", producer, epoch))
				if mode == ModeProcessingV1 {
					_, _, err := m.InitProducerWithMode("tx", mode)
					require.Error(t, err)
					require.NoError(t, m.AddParticipant("tx", producer, epoch, Participant{Topic: "topic"}, time.Now()))
					_, _, err = m.ResolveRequestAssignment("tx", producer, epoch, "topic", 0, 1, 0, "payload")
					require.NoError(t, err)
				}
			})
			assertDecisionGenerationChange(t, m, true, func() {
				_, err := m.PrepareCommit("tx", producer, epoch)
				require.NoError(t, err)
			})
			assertDecisionGenerationChange(t, m, false, func() {
				_, err := m.PrepareCommit("tx", producer, epoch)
				require.NoError(t, err)
				_, err = m.BuildCommittedSnapshot("tx")
				require.NoError(t, err)
				require.Error(t, m.Abort("tx", producer, epoch))
				_, err = m.PrepareAbort("tx", producer, epoch)
				require.Error(t, err)
			})
			assertDecisionGenerationChange(t, m, true, func() { require.NoError(t, m.Commit("tx")) })
			assertDecisionGenerationChange(t, m, false, func() {
				_, err := m.PrepareCommit("tx", producer, epoch)
				require.NoError(t, err)
				require.Error(t, m.Commit("tx"))
				require.NoError(t, m.MarkOffsetsMaterialized("tx"))
				_, err = m.PruneTopicReferences("topic")
				require.NoError(t, err)
			})
			assertDecisionGenerationChange(t, m, true, func() {
				var err error
				producer, epoch, err = m.InitProducerWithMode("tx", mode)
				require.NoError(t, err)
			})
			require.NoError(t, m.Begin("tx", producer, epoch))
			assertDecisionGenerationChange(t, m, true, func() { require.NoError(t, m.Abort("tx", producer, epoch)) })
			assertDecisionGenerationChange(t, m, false, func() { require.NoError(t, m.Abort("tx", producer, epoch)) })
			producer, epoch, err := m.InitProducerWithMode("tx", mode)
			require.NoError(t, err)
			require.NoError(t, m.Begin("tx", producer, epoch))
			assertDecisionGenerationChange(t, m, true, func() {
				_, err := m.PrepareAbort("tx", producer, epoch)
				require.NoError(t, err)
			})
			var aborted *Snapshot
			assertDecisionGenerationChange(t, m, false, func() {
				_, err := m.PrepareAbort("tx", producer, epoch)
				require.NoError(t, err)
				aborted, err = m.BuildAbortedSnapshot("tx", producer, epoch)
				require.NoError(t, err)
			})
			assertDecisionGenerationChange(t, m, true, func() { require.NoError(t, m.ApplyReplicatedSnapshot(aborted)) })
			assertDecisionGenerationChange(t, m, false, func() { require.NoError(t, m.ApplyReplicatedSnapshot(aborted)) })
		})
	}
}

func TestDecisionGenerationRestore(t *testing.T) {
	for _, apply := range []string{"local", "replicated", "import"} {
		t.Run(apply, func(t *testing.T) {
			m := NewManager()
			install := func(snap *Snapshot) {
				switch apply {
				case "local":
					m.ApplySnapshot(snap)
				case "replicated":
					require.NoError(t, m.ApplyReplicatedSnapshot(snap))
				case "import":
					require.NoError(t, m.ImportState(map[string]*Snapshot{snap.ID: snap}))
				}
			}
			snap := decisionGenerationSnapshot("tx")
			assertDecisionGenerationChange(t, m, true, func() { install(snap) })
			assertDecisionGenerationChange(t, m, false, func() { install(snap) })
			for _, change := range []struct {
				name   string
				mutate func(*Snapshot)
			}{
				{"epoch", func(s *Snapshot) { s.Epoch++ }},
				{"coordinator epoch", func(s *Snapshot) { s.CoordinatorEpoch++ }},
				{"state", func(s *Snapshot) { s.State = StateCommitted }},
				{"mode changes effective epoch", func(s *Snapshot) { s.Mode = ModeLegacy }},
				{"expired", func(s *Snapshot) { s.Expired = true }},
			} {
				t.Run(change.name, func(t *testing.T) {
					change.mutate(snap)
					snap.Revision++
					assertDecisionGenerationChange(t, m, true, func() { install(snap) })
				})
			}
			assertDecisionGenerationChange(t, m, false, func() {
				snap.CoordinatorEpoch++ // Legacy decisions expose zero.
				snap.Revision++
				snap.Ready = true
				snap.UpdatedAt = snap.UpdatedAt.Add(time.Second)
				snap.Messages = []MessageOperation{{Topic: "payload"}}
				snap.SequenceByPartition = map[string]uint64{"payload:0": 8}
				install(snap)
			})
			assertDecisionGenerationChange(t, m, true, func() { m.Delete("tx") })
			assertDecisionGenerationChange(t, m, false, func() { m.Delete("tx") })
			assertDecisionGenerationChange(t, m, true, func() { install(snap) })
			assertDecisionGenerationChange(t, m, true, func() { require.NoError(t, m.ImportState(nil)) })
			assertDecisionGenerationChange(t, m, false, func() { require.NoError(t, m.ImportState(nil)) })
		})
	}
}

func TestDecisionGenerationRejectedSnapshotsAndBookkeeping(t *testing.T) {
	m := NewManager()
	snap := decisionGenerationSnapshot("tx")
	m.ApplySnapshot(snap)
	assertDecisionGenerationChange(t, m, false, func() {
		m.ApplySnapshot(nil)
		m.ApplySnapshot(&Snapshot{})
		require.Error(t, m.ImportState(map[string]*Snapshot{"tx": nil}))
		require.Error(t, m.ApplyReplicatedSnapshot(nil))
		stale := *snap
		stale.State = StateAborted
		require.Error(t, m.ApplyReplicatedSnapshot(&stale))
		require.NoError(t, m.RestoreProducerEpochWatermark(100))
	})
	// Switching modes with an effective epoch of zero preserves resolver answers.
	snap.CoordinatorEpoch = 0
	m.ApplySnapshot(snap)
	assertDecisionGenerationChange(t, m, false, func() {
		snap.Mode = ModeLegacy
		m.ApplySnapshot(snap)
		snap.Mode = ""
		require.NoError(t, m.ImportState(map[string]*Snapshot{"tx": snap}))
	})
}

func TestDecisionGenerationOffsetBookkeeping(t *testing.T) {
	m := NewManager()
	snap := decisionGenerationSnapshot("tx")
	m.ApplySnapshot(snap)
	assertDecisionGenerationChange(t, m, false, func() {
		require.NoError(t, m.AddOffsets("tx", snap.Producer, snap.Epoch, []OffsetOperation{{Topic: "input", Group: "group", Offset: 10}}))
		require.NoError(t, m.AddStream("tx", snap.Producer, snap.Epoch, StreamOperation{Topic: "output", Key: "key", ExpectedVersion: 1}))
		_, err := m.BeginOffsetReservations("tx", snap.Producer, snap.Epoch)
		require.NoError(t, err)
	})
	_, err := m.PrepareCommit("tx", snap.Producer, snap.Epoch)
	require.NoError(t, err)
	prepared, ok := m.Snapshot("tx")
	require.True(t, ok)
	committed, err := m.BuildCommittedSnapshot("tx")
	require.NoError(t, err)
	require.NoError(t, m.ApplyReplicatedSnapshot(committed))
	assertDecisionGenerationChange(t, m, false, func() {
		// A retry of the prepared predecessor cannot regress a terminal decision.
		require.NoError(t, m.ApplyReplicatedSnapshot(prepared))
		resolved, err := m.BuildOffsetReservationsResolvedSnapshot("tx")
		require.NoError(t, err)
		require.NoError(t, m.ApplyReplicatedSnapshot(resolved))
		require.NoError(t, m.ImportState(m.ExportState()))
		_, err = m.PruneTopicReferences("output")
		require.NoError(t, err)
	})
}

func TestDecisionGenerationCoordinatorEpochs(t *testing.T) {
	m := NewManagerWithExpirationAndShards(time.Hour, 1)
	for _, state := range []State{StateOpen, StateCommitted, StateAborted} {
		snap := decisionGenerationSnapshot(string(state))
		snap.State = state
		m.ApplySnapshot(snap)
	}
	assertDecisionGenerationChange(t, m, false, func() {
		require.NoError(t, m.SetCoordinatorEpoch(string(StateOpen), 19))
		require.Error(t, m.SetCoordinatorEpoch(string(StateOpen), 0))
		require.Error(t, m.SetCoordinatorEpoch("missing", 20))
		m.ReconcileCoordinatorEpochs(map[int]int64{-1: 20, 0: 19, 1: 20}, 1)
	})
	assertDecisionGenerationChange(t, m, true, func() { require.NoError(t, m.SetCoordinatorEpoch(string(StateOpen), 20)) })
	assertDecisionGenerationChange(t, m, true, func() { m.ReconcileCoordinatorEpochs(map[int]int64{0: 21}, 1) })
	for _, state := range []State{StateCommitted, StateAborted} {
		decision, epoch, ok := m.TransactionDecisionWithCoordinatorEpoch(string(state), 7)
		require.True(t, ok)
		require.Equal(t, string(state), decision)
		require.Equal(t, int64(19), epoch)
	}
	assertDecisionGenerationChange(t, m, false, func() { m.ReconcileCoordinatorEpochs(map[int]int64{0: 21}, 1) })
	m.Delete(string(StateOpen))
	assertDecisionGenerationChange(t, m, false, func() { m.ReconcileCoordinatorEpochs(map[int]int64{0: 22}, 1) })
	snap := decisionGenerationSnapshot("legacy")
	snap.Mode = ModeLegacy
	m.ApplySnapshot(snap)
	assertDecisionGenerationChange(t, m, false, func() { require.NoError(t, m.SetCoordinatorEpoch("legacy", 25)) })
}

func TestDecisionGenerationExpiration(t *testing.T) {
	m := NewManagerWithExpiration(time.Hour)
	_, epoch, err := m.InitProducer("tx")
	require.NoError(t, err)
	now := time.Now()
	assertDecisionGenerationChange(t, m, false, func() { require.Zero(t, m.PruneExpired(now)) })
	assertDecisionGenerationChange(t, m, true, func() { require.Equal(t, 1, m.PruneExpired(now.Add(2*time.Hour))) })
	_, known := m.TransactionDecision("tx", epoch)
	require.False(t, known)
	assertDecisionGenerationChange(t, m, true, func() { require.Equal(t, 1, m.PruneExpired(now.Add(4*time.Hour))) })
	assertDecisionGenerationChange(t, m, false, func() { require.Zero(t, m.PruneExpired(now.Add(6*time.Hour))) })
}

func TestDecisionGenerationExpirationBeforeInitError(t *testing.T) {
	m := NewManagerWithExpiration(time.Hour)
	snap := decisionGenerationSnapshot("old")
	snap.State = StateAborted
	m.ApplySnapshot(snap)
	require.NoError(t, m.RestoreProducerEpochWatermark(uint64(math.MaxInt64)+1))
	assertDecisionGenerationChange(t, m, true, func() {
		_, _, err := m.InitProducer("old")
		require.ErrorContains(t, err, "exhausted")
	})
	_, known := m.TransactionDecision("old", snap.Epoch)
	require.False(t, known)
	assertDecisionGenerationChange(t, m, false, func() {
		_, _, err := m.InitProducer("old")
		require.ErrorContains(t, err, "exhausted")
		_, _, err = m.InitProducer("new")
		require.ErrorContains(t, err, "exhausted")
	})
}

func TestDecisionGenerationOverflowAndNil(t *testing.T) {
	var absent *Manager
	_, ok := absent.CaptureTransactionDecisionGeneration()
	require.False(t, ok)
	require.False(t, absent.CheckTransactionDecisionGeneration(0))
	m := NewManager()
	m.decisionGeneration.Store(math.MaxUint64 - 1)
	token, ok := m.CaptureTransactionDecisionGeneration()
	require.True(t, ok)
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() { m.ApplySnapshot(decisionGenerationSnapshot(fmt.Sprintf("tx-%d", i))) })
	}
	wg.Wait()
	require.Equal(t, uint64(math.MaxUint64), m.decisionGeneration.Load())
	require.False(t, m.CheckTransactionDecisionGeneration(token))
	require.NoError(t, m.ImportState(nil))
	m.ApplySnapshot(decisionGenerationSnapshot("still-functional"))
	_, known := m.TransactionDecision("still-functional", 7)
	require.True(t, known)
	_, ok = m.CaptureTransactionDecisionGeneration()
	require.False(t, ok)
	require.False(t, m.CheckTransactionDecisionGeneration(math.MaxUint64))
}

func TestDecisionGenerationWaitsForOverlappingShardWriters(t *testing.T) {
	for _, check := range []bool{false, true} {
		t.Run(fmt.Sprintf("check=%t", check), func(t *testing.T) {
			m := NewManagerWithExpirationAndShards(time.Hour, 2)
			ids := make([]string, 2)
			for i := 0; ids[0] == "" || ids[1] == ""; i++ {
				id := fmt.Sprintf("tx-%d", i)
				ids[CoordinatorShardForCount(id, 2)] = id
			}
			for _, id := range ids {
				m.ApplySnapshot(decisionGenerationSnapshot(id))
			}
			token, ok := m.CaptureTransactionDecisionGeneration()
			require.True(t, ok)
			started := make(chan struct{}, 2)
			release := []chan struct{}{make(chan struct{}), make(chan struct{})}
			done := []chan struct{}{make(chan struct{}), make(chan struct{})}
			for i := range 2 {
				go func() {
					s := &m.shards[i]
					s.mu.Lock()
					before := transactionDecisionTuple(s.txns[ids[i]])
					s.txns[ids[i]].State = StateCommitted
					started <- struct{}{}
					<-release[i]
					m.recordDecisionChangeLocked(s, ids[i], before)
					s.mu.Unlock()
					close(done[i])
				}()
			}
			<-started
			<-started
			result := make(chan uint64, 1)
			go func() {
				if check {
					if m.CheckTransactionDecisionGeneration(token) {
						result <- token
					} else {
						result <- math.MaxUint64
					}
				} else {
					generation, _ := m.CaptureTransactionDecisionGeneration()
					result <- generation
				}
			}()
			close(release[0])
			<-done[0]
			// The only remaining contender for shard zero is capture/check. Once
			// it holds that lock, writer one is still paused before its increment.
			t.Cleanup(func() { close(release[1]); <-done[1] })
			timeout := time.NewTimer(10 * time.Second)
			defer timeout.Stop()
			for m.shards[0].mu.TryLock() {
				m.shards[0].mu.Unlock()
				select {
				case <-result:
					t.Fatal("certification returned before acquiring all shard locks")
				case <-timeout.C:
					t.Fatal("certification did not acquire the first shard lock")
				default:
					runtime.Gosched()
				}
			}
			select {
			case <-result:
				t.Fatal("certification returned while a shard mutation was unfinished")
			default:
			}
			// Let the second mutation finish without closing the cleanup channel.
			release[1] <- struct{}{}
			<-done[1]
			if check {
				require.Equal(t, uint64(math.MaxUint64), <-result)
			} else {
				require.Equal(t, token+2, <-result)
			}
		})
	}
}
