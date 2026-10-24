package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type stableOffsetResult struct {
	offset uint64
	found  bool
	err    error
}

func readStableAsync(c *Coordinator, partition int) <-chan stableOffsetResult {
	done := make(chan stableOffsetResult, 1)
	go func() {
		offset, found, err := c.GetStableOffset("workers", "orders", partition)
		done <- stableOffsetResult{offset, found, err}
	}()
	return done
}

func awaitStable(t *testing.T, done <-chan stableOffsetResult) stableOffsetResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(time.Second):
		t.Fatal("stable offset read waited for the in-flight append")
		return stableOffsetResult{}
	}
}

func TestCooperativeOffsetReadWindowDurabilityAndRecovery(t *testing.T) {
	for _, outcome := range []string{"success", "rejected", "lost-response"} {
		t.Run(outcome, func(t *testing.T) {
			c, parts, records := runtimeCooperativeFixture(t, 2)
			a := runtimeJoin(t, c, parts, "a")
			grant := a.Grants[0]
			require.NoError(t, c.CommitCooperative("workers", a.RegistrationEpoch, grant, 8))
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
				close(entered)
				<-release
				if outcome != "rejected" {
					*records = append(*records, record)
				}
				if outcome != "success" {
					return errors.New(outcome)
				}
				return nil
			})
			done := make(chan error, 1)
			go func() { done <- c.CommitCooperative("workers", a.RegistrationEpoch, grant, 16) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("append did not start")
			}
			require.Equal(t, stableOffsetResult{offset: 8, found: true}, awaitStable(t, readStableAsync(c, 0)))
			require.Equal(t, stableOffsetResult{}, awaitStable(t, readStableAsync(c, 1)))
			stale := grant
			stale.Epoch++
			require.ErrorContains(t, c.ValidateCooperativeRead("workers", a.RegistrationEpoch, &stale), "cooperative_fenced")
			select {
			case <-done:
				t.Fatal("commit acknowledged before durable append completed")
			default:
			}
			unblock()
			select {
			case err := <-done:
				if outcome == "success" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, outcome)
				}
			case <-time.After(time.Second):
				t.Fatal("commit did not finish")
			}
			expected := uint64(8)
			if outcome == "success" {
				expected = 16
			}
			require.Equal(t, stableOffsetResult{offset: expected, found: true}, awaitStable(t, readStableAsync(c, 0)))
			require.False(t, c.groups["workers"].offsetReadWindow.active)
			require.Nil(t, c.groups["workers"].offsetReadWindow.offsets)

			var messages []types.Message
			recoveryBytes := 0
			for i, record := range *records {
				message := encodedMetadataMessage(t, record, uint64(i))
				recoveryBytes += len(message.Payload)
				messages = append(messages, message)
			}
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution = true
			recovered, err := NewCoordinatorWithRecovery(context.Background(), cfg, &metadataReplayHandler{messages: map[int][]types.Message{0: messages}})
			require.NoError(t, err)
			t.Cleanup(recovered.Stop)
			if outcome == "lost-response" {
				expected = 16 // Durable log, rather than the failed acknowledgement, wins on replay.
			}
			require.Equal(t, stableOffsetResult{offset: expected, found: true}, awaitStable(t, readStableAsync(recovered, 0)))
			t.Logf("recovery_records=%d recovery_payload_bytes=%d recovered_offset=%d", len(messages), recoveryBytes, expected)
		})
	}
}

type blockingWindowResolver struct {
	entered chan struct{}
	release chan struct{}
}

func (r *blockingWindowResolver) CommittedOffset(_, _ string, _ int, _ uint64) (uint64, bool) {
	close(r.entered)
	<-r.release
	return 9, true
}

func TestCooperativeOffsetReadWindowDrainsResolverBeforePublication(t *testing.T) {
	c, parts, _ := runtimeCooperativeFixture(t, 2)
	a := runtimeJoin(t, c, parts, "a")
	resolver := &blockingWindowResolver{make(chan struct{}), make(chan struct{})}
	c.SetTransactionalOffsetResolver(resolver)
	entered, release := make(chan struct{}), make(chan struct{})
	var appendOnce, readOnce sync.Once
	unblockAppend := func() { appendOnce.Do(func() { close(release) }) }
	unblockRead := func() { readOnce.Do(func() { close(resolver.release) }) }
	t.Cleanup(unblockAppend)
	t.Cleanup(unblockRead)
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { close(entered); <-release; return nil })
	done := make(chan error, 1)
	go func() { done <- c.CommitCooperative("workers", a.RegistrationEpoch, a.Grants[0], 16) }()
	<-entered
	read := readStableAsync(c, 0)
	select {
	case <-resolver.entered:
	case <-time.After(time.Second):
		t.Fatal("read did not reach transaction resolver during append")
	}
	unblockAppend()
	select {
	case <-done:
		t.Fatal("commit published while a read still held the reservation fence")
	case <-time.After(30 * time.Millisecond):
	}
	unblockRead()
	require.Equal(t, stableOffsetResult{offset: 9, found: true}, awaitStable(t, read))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("commit did not finish after reader drained")
	}
}

func TestCooperativeOffsetReadWindowPreservesReservationBarrier(t *testing.T) {
	c, parts, _ := runtimeCooperativeFixture(t, 2)
	a := runtimeJoin(t, c, parts, "a")
	reservation := TransactionOffsetReservation{TransactionalID: "tx", ProducerID: "p", ProducerEpoch: 1, MemberID: "a", Offsets: []ReservedTransactionOffset{{Topic: "orders", Partition: 1, Offset: 9}}, Grants: []CooperativeGrant{a.Grants[1]}}
	require.NoError(t, c.PrepareOffsetReservation("workers", a.RegistrationEpoch, reservation))
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { close(entered); <-release; return nil })
	done := make(chan error, 1)
	go func() { done <- c.CommitCooperative("workers", a.RegistrationEpoch, a.Grants[0], 16) }()
	<-entered
	read := readStableAsync(c, 1)
	select {
	case result := <-read:
		t.Fatalf("reserved group bypassed the existing locked path: %+v", result)
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	require.NoError(t, <-done)
	require.ErrorContains(t, awaitStable(t, read).err, "unstable_offset_commit")
}

func TestCooperativeOffsetReadWindowDoesNotDelayUnexpiredSessionScan(t *testing.T) {
	for _, distributed := range []bool{false, true} {
		t.Run(fmt.Sprint(distributed), func(t *testing.T) {
			c, parts, _ := runtimeCooperativeFixture(t, 2)
			a := runtimeJoin(t, c, parts, "a")
			c.standalone = !distributed
			c.groupOwnerChecker = func(string) bool { return true }
			c.ownershipSince["workers"] = time.Now().Add(-time.Hour)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { close(entered); <-release; return nil })
			done := make(chan error, 1)
			go func() { done <- c.CommitCooperative("workers", a.RegistrationEpoch, a.Grants[0], 16) }()
			<-entered
			scanned := make(chan struct{})
			go func() { c.checkSingleGroupTimeout("workers", time.Minute); close(scanned) }()
			select {
			case <-scanned:
			case <-time.After(time.Second):
				t.Fatal("read-only timeout scan queued a writer behind durable append")
			}
			require.Equal(t, stableOffsetResult{}, awaitStable(t, readStableAsync(c, 0)))
			unblock()
			require.NoError(t, <-done)
		})
	}
}

func TestCooperativeOffsetReadWindowCannotOutliveNewReservation(t *testing.T) {
	c, parts, _ := runtimeCooperativeFixture(t, 2)
	a := runtimeJoin(t, c, parts, "a")
	grant := a.Grants[0]
	require.NoError(t, c.CommitCooperative("workers", a.RegistrationEpoch, grant, 8))
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
		if record.Type == ConsumerMetadataRecordOffsetSnapshot {
			close(entered)
			<-release
		}
		return nil
	})
	committed := make(chan error, 1)
	go func() { committed <- c.CommitCooperative("workers", a.RegistrationEpoch, grant, 16) }()
	<-entered
	reserved := make(chan error, 1)
	go func() {
		reserved <- c.PrepareOffsetReservation("workers", a.RegistrationEpoch, TransactionOffsetReservation{
			TransactionalID: "new-tx", ProducerID: "p", ProducerEpoch: 1, MemberID: "a",
			Offsets: []ReservedTransactionOffset{{Topic: "orders", Partition: 0, Offset: 17}}, Grants: []CooperativeGrant{grant},
		})
	}()
	select {
	case err := <-reserved:
		t.Fatalf("reservation passed the in-flight mutation fence: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.Equal(t, stableOffsetResult{offset: 8, found: true}, awaitStable(t, readStableAsync(c, 0)))
	unblock()
	require.NoError(t, <-committed)
	require.NoError(t, <-reserved)
	require.ErrorContains(t, awaitStable(t, readStableAsync(c, 0)).err, "unstable_offset_commit")
}
