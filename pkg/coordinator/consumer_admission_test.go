package coordinator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func awaitAdmission[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(10 * time.Second):
		t.Fatal("admission test timed out")
		var zero T
		return zero
	}
}

func waitRecoveryClaims(t *testing.T, c *Coordinator, count int) {
	t.Helper()
	for {
		g := &c.consumerAdmission
		g.mu.Lock()
		if g.recoveries == count {
			g.mu.Unlock()
			return
		}
		if g.changed == nil {
			g.changed = make(chan struct{})
		}
		changed := g.changed
		g.mu.Unlock()
		awaitAdmission(t, changed)
	}
}

func startAdmissionRecovery(t *testing.T, c *Coordinator) <-chan func() {
	t.Helper()
	result := make(chan func(), 1)
	go func() {
		release, err := c.BeginConsumerMetadataRecovery(context.Background())
		if err != nil {
			t.Error(err)
		}
		result <- release
	}()
	return result
}

func TestConsumerAdmissionDrainsWriteBeforeRecoveryLocks(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.RegisterGroupFromFSM("events", "g", 1))
	entered, apply := make(chan struct{}), make(chan struct{})
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { close(entered); <-apply; return nil })
	done := make(chan error, 1)
	go func() { done <- c.CommitOffset("g", "events", 0, 9) }()
	awaitAdmission(t, entered)
	recovery := startAdmissionRecovery(t, c)
	waitRecoveryClaims(t, c, 1)
	require.ErrorIs(t, c.ConsumerOperationsReadinessError(), ErrConsumerMetadataRecovering)
	// A later request must fail before acquiring even a lifecycle/controller-like lock.
	c.lifecycleMu.Lock()
	require.ErrorIs(t, c.RegisterGroup("events", "later", 1), ErrConsumerMetadataRecovering)
	c.lifecycleMu.Unlock()
	select {
	case <-recovery:
		t.Fatal("recovery acquired before write drained")
	default:
	}
	// Simulated Raft completion needs no recovery-owned lifecycle/FSM lock.
	close(apply)
	require.NoError(t, awaitAdmission(t, done))
	release := awaitAdmission(t, recovery)
	require.Equal(t, uint64(9), c.ExportState()["g"].Offsets["events"][0])
	// Historical apply can complete during the exclusive interval without publishing.
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { t.Error("historical apply published"); return nil })
	require.NoError(t, c.JoinGroupFromFSM("historical", "m", "events", 1))
	require.NoError(t, c.ApplyFencedOffsetUpdateFromFSM("historical", "events", "m", 1, []OffsetItem{{Partition: 0, Offset: 4}}))
	release()
	release()
	require.NoError(t, c.ConsumerOperationsReadinessError())
	require.Equal(t, uint64(4), c.ExportState()["historical"].Offsets["events"][0])
}

type admissionOffsetResolver struct{ entered, resume chan struct{} }

func (r *admissionOffsetResolver) CommittedOffset(string, string, int, uint64) (uint64, bool) {
	close(r.entered)
	<-r.resume
	return 7, true
}

func TestConsumerAdmissionHoldsCallbacksAndResolvers(t *testing.T) {
	for _, name := range []string{"offset", "stable", "reservation", "observation"} {
		t.Run(name, func(t *testing.T) {
			c := newAuthorityTestCoordinator(t)
			require.NoError(t, c.JoinGroupFromFSM("g", "m", "events", 1))
			c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { return nil })
			entered, resume := make(chan struct{}), make(chan struct{})
			c.SetTransactionalOffsetResolver(&admissionOffsetResolver{entered, resume})
			c.SetGroupObservationBatchResolver(func([]string) (map[string]bool, error) {
				close(entered)
				<-resume
				return map[string]bool{"g": true}, nil
			})
			done := make(chan struct{})
			go func() {
				defer close(done)
				switch name {
				case "offset":
					c.GetOffset("g", "events", 0)
				case "stable":
					_, _, err := c.GetStableOffset("g", "events", 0)
					if err != nil {
						t.Error(err)
					}
				case "reservation":
					if err := c.PrepareOffsetReservation("g", 1, TransactionOffsetReservation{TransactionalID: "tx", ProducerID: "p", ProducerEpoch: 1, MemberID: "m", Generation: 1, Offsets: []ReservedTransactionOffset{{Topic: "events", Partition: 0, Offset: 9}}}); err != nil {
						t.Error(err)
					}
				case "observation":
					c.ObserveConsumerGroups()
				}
			}()
			awaitAdmission(t, entered)
			recovery := startAdmissionRecovery(t, c)
			waitRecoveryClaims(t, c, 1)
			select {
			case <-recovery:
				t.Fatal("callback was not counted")
			default:
			}
			require.ErrorIs(t, c.CommitOffset("g", "events", 0, 11), ErrConsumerMetadataRecovering)
			close(resume)
			awaitAdmission(t, done)
			release := awaitAdmission(t, recovery)
			release()
		})
	}
}

func TestConsumerAdmissionCancellationAndExclusiveWaiters(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.BeginConsumerMetadataRecovery(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, c.ConsumerOperationsReadinessError())
	admitted, err := c.admitConsumerOperation()
	require.NoError(t, err)
	ctx, cancel = context.WithCancel(context.Background())
	cancelled := make(chan error, 1)
	go func() { _, err := c.BeginConsumerMetadataRecovery(ctx); cancelled <- err }()
	waitRecoveryClaims(t, c, 1)
	second := startAdmissionRecovery(t, c)
	waitRecoveryClaims(t, c, 2)
	cancel()
	require.ErrorIs(t, awaitAdmission(t, cancelled), context.Canceled)
	require.ErrorIs(t, c.ConsumerOperationsReadinessError(), ErrConsumerMetadataRecovering)
	admitted()
	admitted()
	owner := awaitAdmission(t, second)
	third := startAdmissionRecovery(t, c)
	waitRecoveryClaims(t, c, 2)
	ctx, cancel = context.WithCancel(context.Background())
	go func() { _, err := c.BeginConsumerMetadataRecovery(ctx); cancelled <- err }()
	waitRecoveryClaims(t, c, 3)
	cancel()
	require.ErrorIs(t, awaitAdmission(t, cancelled), context.Canceled)
	owner()
	owner()
	require.ErrorIs(t, c.ConsumerOperationsReadinessError(), ErrConsumerMetadataRecovering)
	next := awaitAdmission(t, third)
	next()
	next()
	require.NoError(t, c.ConsumerOperationsReadinessError())
	// Cancellation with no successor must reopen even while the old operation runs.
	admitted, err = c.admitConsumerOperation()
	require.NoError(t, err)
	ctx, cancel = context.WithCancel(context.Background())
	go func() { _, err := c.BeginConsumerMetadataRecovery(ctx); cancelled <- err }()
	waitRecoveryClaims(t, c, 1)
	cancel()
	require.ErrorIs(t, awaitAdmission(t, cancelled), context.Canceled)
	require.NoError(t, c.ConsumerOperationsReadinessError())
	admitted()
}

func TestConsumerAdmissionAllOrdinaryEntrypoints(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.JoinGroupFromFSM("g", "m", "events", 1))
	before := c.ExportState()
	release, err := c.BeginConsumerMetadataRecovery(context.Background())
	require.NoError(t, err)
	defer release()
	mutations := map[string]func() error{
		"register": func() error { return c.RegisterGroup("events", "g", 1) },
		"subscription": func() error {
			return c.RegisterGroupSubscription("g", []string{"events"}, "", map[string]int{"events": 1})
		},
		"delete":               func() error { return c.DeleteGroup("g") },
		"delete-topic":         func() error { _, err := c.DeleteInactiveGroupsForTopic("events"); return err },
		"add":                  func() error { _, err := c.AddConsumer("g", "m"); return err },
		"remove":               func() error { return c.RemoveConsumer("g", "m") },
		"remove-generation":    func() error { return c.RemoveConsumerForGeneration("g", "m", 1) },
		"expire":               func() error { return c.ExpireConsumers("g", 1, []string{"m"}) },
		"heartbeat":            func() error { return c.RecordHeartbeat("g", "m") },
		"heartbeat-generation": func() error { return c.RecordHeartbeatForGeneration("g", "m", 1) },
		"resume":               func() error { _, err := c.ResumeConsumer("g", "m", 1); return err },
		"resume-topics":        func() error { _, err := c.ResumeConsumerTopicAssignments("g", "m", 1); return err },
		"commit":               func() error { return c.CommitOffset("g", "events", 0, 1) },
		"commit-bulk":          func() error { return c.CommitOffsetsBulk("g", "events", nil) },
		"validate-commit":      func() error { return c.ValidateAndCommit("g", "events", 0, 1, 1, "m") },
		"validate-bulk":        func() error { return c.ValidateAndCommitOffsetsBulk("g", "events", "m", 1, nil) },
		"validate-topics":      func() error { return c.ValidateAndCommitTopicOffsetsBulk("g", "m", 1, nil) },
		"validate-epoch":       func() error { return c.ValidateAndCommitTopicOffsetsBulkForEpoch("g", "m", 1, 1, nil) },
		"materialize":          func() error { return c.MaterializeCommittedTransactionOffsets("g", 1, nil) },
		"prepare":              func() error { return c.PrepareOffsetReservation("g", 1, TransactionOffsetReservation{}) },
		"resolve":              func() error { return c.ResolveOffsetReservation("g", 1, "tx", "p", 1, true) },
		"fence": func() error {
			return c.WithOwnershipFence("g", "m", 1, nil, func() error { t.Error("blocked callback"); return nil })
		},
		"topic-fence": func() error {
			return c.WithTopicOwnershipFence("g", "m", 1, nil, func() error { t.Error("blocked callback"); return nil })
		},
		"reload": c.ReloadDistributedConsumerMetadata,
		"status": func() error { _, err := c.GetGroupStatus("g"); return err },
		"stable": func() error { _, _, err := c.GetStableOffset("g", "events", 0); return err },
	}
	for name, operation := range mutations {
		t.Run(name, func(t *testing.T) { require.ErrorIs(t, operation(), ErrConsumerMetadataRecovering) })
	}
	require.Empty(t, c.GetAssignments("g"))
	require.Empty(t, c.GetMemberAssignments("g", "m"))
	require.Empty(t, c.GetMemberTopicAssignments("g", "m"))
	require.Empty(t, c.ListGroups())
	require.Nil(t, c.GetGroup("g"))
	require.False(t, c.IsSubscriptionGroup("g"))
	require.Zero(t, c.GetGeneration("g"))
	require.Zero(t, c.GetRegistrationEpoch("g"))
	require.Empty(t, c.TopicGroupReferences("events"))
	require.Empty(t, c.ObserveConsumerGroups())
	offset, found := c.GetOffset("g", "events", 0)
	require.Zero(t, offset)
	require.False(t, found)
	require.False(t, c.ValidateOwnershipAtomic("g", "m", 1, 0))
	require.Equal(t, ErrConsumerMetadataRecovering.Error(), c.ValidateMemberGeneration("g", "m", 1))
	require.Equal(t, ErrConsumerMetadataRecovering.Error(), c.ValidateOwnershipFailure("g", "m", 1, 0))
	require.Equal(t, ErrConsumerMetadataRecovering.Error(), c.ValidateTopicPartitionOwnershipFailure("g", "m", 1, "events", 0))
	c.Rebalance("g")
	require.Equal(t, before, c.ExportState())
}

func TestConsumerAdmissionDeletionRetainsOneAdmission(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.RegisterGroupFromFSM("events", "a", 1))
	require.NoError(t, c.RegisterGroupFromFSM("events", "b", 1))
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { once.Do(func() { close(entered); <-resume }); return nil })
	done := make(chan error, 1)
	go func() {
		deleted, err := c.DeleteInactiveGroupsForTopic("events")
		if err == nil && len(deleted) != 2 {
			t.Errorf("deleted %v", deleted)
		}
		done <- err
	}()
	awaitAdmission(t, entered)
	recovery := startAdmissionRecovery(t, c)
	waitRecoveryClaims(t, c, 1)
	close(resume)
	require.NoError(t, awaitAdmission(t, done))
	release := awaitAdmission(t, recovery)
	release()
	require.Empty(t, c.ListGroups())
}

func TestConsumerAdmissionHeartbeatCallbacksAndTimers(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.JoinGroupFromFSM("g", "m", "events", 1))
	old := time.Now().Add(-time.Hour)
	c.SetGroupSessionCallbacks(func(string) bool { return false }, nil)
	c.mu.Lock()
	c.ownershipSince["g"] = old
	c.groups["g"].Members["m"].LastHeartbeat = old
	c.mu.Unlock()
	release, err := c.BeginConsumerMetadataRecovery(context.Background())
	require.NoError(t, err)
	require.False(t, c.ownsGroupSession("g", time.Now(), time.Second))
	c.checkSingleGroupTimeout("g", time.Second)
	c.checkAllGroupsTimeout()
	require.Equal(t, old, c.ownershipSince["g"])
	release()
	// Owner resolution may acquire recovery itself: it must not hold admission.
	c.SetGroupSessionCallbacks(func(string) bool {
		release, err := c.BeginConsumerMetadataRecovery(context.Background())
		if err != nil {
			t.Error(err)
		}
		release()
		return true
	}, func(group string, generation int, members []string) error {
		// Expiration callbacks also run after inspection's admission is released.
		release, err := c.BeginConsumerMetadataRecovery(context.Background())
		if err != nil {
			return err
		}
		release()
		return c.ExpireConsumers(group, generation, members)
	})
	c.mu.Lock()
	c.ownershipSince["g"] = old
	c.mu.Unlock()
	done := make(chan struct{})
	go func() { c.checkSingleGroupTimeout("g", time.Second); close(done) }()
	awaitAdmission(t, done)
	require.Empty(t, c.ExportState()["g"].Members)
}

func TestConsumerAdmissionStandaloneRecoveryRejected(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = false
	c := NewCoordinator(context.Background(), cfg, &countingPublisher{})
	t.Cleanup(c.Stop)
	_, err := c.BeginConsumerMetadataRecovery(context.Background())
	require.ErrorIs(t, err, ErrConsumerMetadataRecoveryStandalone)
	require.Zero(t, c.consumerAdmission.recoveries)
	require.NoError(t, c.ConsumerOperationsReadinessError())
	require.NoError(t, c.RegisterGroup("events", "g", 1))
	_, err = c.AddConsumer("g", "m")
	require.NoError(t, err)
	require.NoError(t, c.RecordHeartbeat("g", "m"))
	require.NoError(t, c.WithOwnershipFence("g", "m", c.GetGeneration("g"), nil, func() error { return c.RecordHeartbeat("g", "m") }))
	require.NotNil(t, c.GetGroup("g"))
}
