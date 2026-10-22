package replication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/stretchr/testify/require"
)

type scheduledCleanup struct {
	issues   map[string]fsm.TopicMaterializationIssue
	step     func(string) error
	replicas int
}

func (s *scheduledCleanup) TopicMaterializationIssues() []fsm.TopicMaterializationIssue {
	var issues []fsm.TopicMaterializationIssue
	for _, issue := range s.issues {
		issues = append(issues, issue)
	}
	return issues
}
func (s *scheduledCleanup) ReconcileTopicMaterializationContext(_ context.Context, name string) error {
	return s.step(name)
}
func (s *scheduledCleanup) ReconcileReplicaMaterializations(string) error { s.replicas++; return nil }

func TestMaterializationSchedulerContinuationFairnessAndBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Unix(1, 0)
	start := now
	s := &scheduledCleanup{issues: map[string]fsm.TopicMaterializationIssue{}}
	for _, name := range []string{"a-long", "b-short", "c-error", "d-wait"} {
		s.issues[name] = fsm.TopicMaterializationIssue{Topic: name, Operation: "delete"}
	}
	counts := make(map[string]int)
	var errorTimes, waitTimes []time.Duration
	var shortFinished time.Duration
	s.step = func(name string) error {
		counts[name]++
		switch name {
		case "a-long":
			if counts[name] == 25000 {
				cancel()
				return nil
			}
			return &coordinator.TopicCleanupPending{State: coordinator.CleanupContinue}
		case "b-short":
			shortFinished = now.Sub(start)
			delete(s.issues, name)
			return nil
		case "c-error":
			errorTimes = append(errorTimes, now.Sub(start))
			return errors.New("disk unavailable")
		default:
			waitTimes = append(waitTimes, now.Sub(start))
			return &coordinator.TopicCleanupPending{State: coordinator.CleanupWait}
		}
	}
	waits := 0
	runMaterializations(ctx, s, "broker", 5*time.Second, materializationClock{
		now: func() time.Time { return now },
		wait: func(ctx context.Context, d time.Duration) bool {
			require.Positive(t, d)
			now = now.Add(d)
			waits++
			require.Less(t, waits, 30000)
			return ctx.Err() == nil
		},
	})
	require.Less(t, shortFinished, 10*time.Millisecond)
	require.Equal(t, 25000, counts["a-long"])
	require.GreaterOrEqual(t, s.replicas, 5)
	require.Len(t, errorTimes, 3)
	require.GreaterOrEqual(t, errorTimes[1]-errorTimes[0], 5*time.Second)
	require.GreaterOrEqual(t, errorTimes[2]-errorTimes[1], 10*time.Second)
	for i := 1; i < len(waitTimes); i++ {
		require.GreaterOrEqual(t, waitTimes[i]-waitTimes[i-1], 5*time.Second)
	}
}

func TestMaterializationScheduler54164HistoryDoesNotAccumulatePollingDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Unix(0, 0)
	start := now
	remaining := 54164
	batches := 0
	s := &scheduledCleanup{issues: map[string]fsm.TopicMaterializationIssue{"history": {Topic: "history", Operation: "delete"}}}
	s.step = func(string) error {
		batches++
		// Alternate record-limited and large-record byte-limited batches.
		count := 128
		if batches%2 == 0 {
			count = 16
		}
		remaining -= min(remaining, count)
		if remaining == 0 {
			cancel()
			return nil
		}
		return &coordinator.TopicCleanupPending{State: coordinator.CleanupContinue}
	}
	runMaterializations(ctx, s, "broker", 5*time.Second, materializationClock{func() time.Time { return now }, func(ctx context.Context, d time.Duration) bool { now = now.Add(d); return ctx.Err() == nil }})
	require.Greater(t, batches, 424)
	require.Less(t, now.Sub(start), time.Second)
	t.Logf("records=54164 batches=%d virtual_wait=%s old_fixed_poll_wait=%s", batches, now.Sub(start), time.Duration(batches)*5*time.Second)
}

func TestMaterializationSchedulerCanceledWaitDoesNotLaunchWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &scheduledCleanup{issues: map[string]fsm.TopicMaterializationIssue{}}
	s.step = func(string) error { t.Fatal("unexpected work"); return nil }
	runMaterializations(ctx, s, "broker", 5*time.Second, materializationClock{time.Now, func(context.Context, time.Duration) bool { cancel(); return false }})
	require.Equal(t, 1, s.replicas)
}
