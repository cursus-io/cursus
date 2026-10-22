package replication

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/util"
)

const materializationYield = time.Millisecond

type materializationReconciler interface {
	TopicMaterializationIssues() []fsm.TopicMaterializationIssue
	ReconcileTopicMaterializationContext(context.Context, string) error
	ReconcileReplicaMaterializations(string) error
}
type materializationClock struct {
	now  func() time.Time
	wait func(context.Context, time.Duration) bool
}
type materializationRetry struct {
	due             time.Time
	failures        uint
	operation       string
	revision, epoch uint64
	since           time.Time
}

func waitMaterialization(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// One worker executes one bounded topic batch, then yields. Failed/waiting
// topics have independent deadlines; another topic's progress never pulls
// those retries (or replica inventories) onto the fast path.
func runMaterializations(ctx context.Context, f materializationReconciler, broker string, interval time.Duration, clock materializationClock) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	retries := make(map[string]materializationRetry)
	var last string
	replicaDue := clock.now()
	for ctx.Err() == nil {
		now := clock.now()
		if !now.Before(replicaDue) {
			if err := f.ReconcileReplicaMaterializations(broker); err != nil {
				util.Warn("Replica materialization reconcile pending on broker %s: %v", broker, err)
			}
			replicaDue = clock.now().Add(interval)
		}
		issues := f.TopicMaterializationIssues()
		sort.Slice(issues, func(i, j int) bool { return issues[i].Topic < issues[j].Topic })
		live := make(map[string]bool, len(issues))
		for _, issue := range issues {
			live[issue.Topic] = true
		}
		for name := range retries {
			if !live[name] {
				delete(retries, name)
			}
		}
		wake := replicaDue
		selected := -1
		start := sort.Search(len(issues), func(i int) bool { return issues[i].Topic > last })
		for n := range issues {
			i := (start + n) % len(issues)
			issue := issues[i]
			r := retries[issue.Topic]
			if r.operation != issue.Operation || r.revision != issue.Revision || r.epoch != issue.LifecycleEpoch || r.since != issue.PendingSince {
				r = materializationRetry{}
				delete(retries, issue.Topic)
			}
			if !now.Before(r.due) {
				selected = i
				break
			}
			if r.due.Before(wake) {
				wake = r.due
			}
		}
		if selected < 0 {
			if !clock.wait(ctx, max(materializationYield, wake.Sub(clock.now()))) {
				return
			}
			continue
		}
		issue := issues[selected]
		last = issue.Topic
		if ctx.Err() != nil {
			return
		}
		err := f.ReconcileTopicMaterializationContext(ctx, issue.Topic)
		if ctx.Err() != nil {
			return
		}
		r := retries[issue.Topic]
		r.operation, r.revision, r.epoch, r.since = issue.Operation, issue.Revision, issue.LifecycleEpoch, issue.PendingSince
		delay := interval
		var pending *coordinator.TopicCleanupPending
		switch {
		case err == nil:
			r.failures = 0
		case errors.As(err, &pending) && pending.State == coordinator.CleanupContinue:
			delay, r.failures = materializationYield, 0
		case errors.As(err, &pending) && pending.State != coordinator.CleanupError:
			r.failures = 0
		default:
			delay = min(time.Minute, interval*time.Duration(1<<min(r.failures, 4)))
			r.failures++
			util.Warn("Topic materialization reconcile pending on broker %s: %v", broker, err)
		}
		r.due = clock.now().Add(delay)
		retries[issue.Topic] = r
		if !clock.wait(ctx, materializationYield) {
			return
		}
	}
}
