package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/recovery"
	"github.com/cursus-io/cursus/util"
)

type scheduledReplica struct {
	item      recovery.Item
	candidate fsm.ReplicaCatchupCandidate
	turn      replicaTurn
	cancel    context.CancelFunc
	since     time.Time
}
type replicaDispatch struct {
	owner   *scheduledReplica
	ctx     context.Context
	lease   *recovery.Lease
	started time.Time
}
type replicaCompletion struct {
	dispatch replicaDispatch
	result   replicaTurnResult
}

// runReplicaScheduler owns the queue and all worker lifetimes. A task's mutable
// continuation is passed to one worker and cannot be observed/replaced until
// that worker returns it on completed. Cancellation never releases a live owner.
func (cc *ClusterController) runReplicaScheduler(ctx context.Context, fetcher ReplicaCatchupFetcher, apply VerifiedReplicaCatchupApplier) {
	cc.catchupRunMu.Lock()
	defer cc.catchupRunMu.Unlock()
	settings := cc.Config.RecoverySettings()
	limits, err := settings.Limits()
	if err != nil {
		util.Error("recovery budget: %v", err)
		return
	}
	workers, quantum := settings.Workers, settings.QuantumBytes
	budget := cc.recoveryBudget
	if budget == nil {
		budget, err = recovery.NewBudget(limits, nil)
		if err != nil {
			util.Error("recovery budget: %v", err)
			return
		}
	}
	queue := recovery.NewQueue(settings.QueueEntries)
	owners := make(map[string]*scheduledReplica)
	candidates := make(map[string]fsm.ReplicaCatchupCandidate)
	maintenance := make(map[string]bool)
	jobs := make(chan replicaDispatch)
	completed := make(chan replicaCompletion, workers+1)
	var wg sync.WaitGroup
	for n := 0; n < workers+1; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if job.lease == nil {
					err := cc.RaftManager.GetFSM().AdvanceReplicaISRWatermarkContext(job.ctx, cc.brokerID, job.owner.candidate)
					completed <- replicaCompletion{job, replicaTurnResult{done: err == nil, err: err}}
					continue
				}
				measured, meter := recovery.Measure(job.ctx)
				result := cc.advanceReplicaTurn(measured, job.owner.candidate, &job.owner.turn, fetcher, apply)
				result.bytes = max(result.bytes, meter.Bytes())
				completed <- replicaCompletion{job, result}
			}
		}()
	}
	active := 0
	controlActive := false
	finish := func(c replicaCompletion) {
		owner := c.dispatch.owner
		owner.cancel()
		owner.cancel = nil
		// Wire bounds cap an indivisible first unit. Charge its complete cost
		// even when it exceeds the ordinary quantum; never discard valid data
		// merely to satisfy the quantum. The bounded worker pool caps overshoot.
		if err := c.dispatch.lease.Finish(c.result.bytes); err != nil {
			util.Debug("recovery oversized quantum accounted: %v", err)
		}
		queue.Complete(owner.item, time.Now(), c.result.done, c.result.err != nil, c.result.sourceFailure)
		if !queue.Contains(owner.item) {
			delete(owners, owner.item.Key)
		}
		active--
		if c.dispatch.lease == nil {
			controlActive = false
			return
		}
		metrics.ObserveRecoveryTurn(c.dispatch.started)
		metrics.RecoveryRunning.Dec()
		if c.result.done && c.result.err == nil && owner.turn.witness != nil {
			metrics.RecoveryResidence.Observe(time.Since(owner.since).Seconds())
		}
	}
	defer func() {
		for _, owner := range owners {
			if owner.cancel != nil {
				owner.cancel()
			}
		}
		close(jobs)
		for active > 0 {
			finish(<-completed)
		}
		wg.Wait()
		metrics.RecoveryQueue.Set(0)
		metrics.RecoveryOldest.Set(0)
	}()
	discover := func() {
		state := cc.RaftManager.GetFSM()
		if cc.replicaBootstrap != nil {
			if _, err := cc.replicaBootstrap.ResumePendingReplicaBootstraps(ctx); err != nil {
				util.Debug("bootstrap recovery pending: %v", err)
			}
		}
		// Materialization owns its own reconciliation cadence. Discovery only
		// inspects metadata and bootstrap exclusion, never runs a storage scan.
		items := make([]recovery.Item, 0)
		clear(candidates)
		clear(maintenance)
		for _, candidate := range state.ReplicaCatchupCandidates(cc.brokerID) {
			if cc.replicaBootstrap != nil && cc.replicaBootstrap.ReplicaBootstrapExcluded(candidate.Topic, candidate.Partition) {
				continue
			}
			source, generation, control, ok := state.ReplicaSchedulingIdentity(cc.brokerID, candidate)
			if !ok {
				continue
			}
			key := fmt.Sprintf("%s/%d", candidate.Topic, candidate.Partition)
			items = append(items, recovery.Item{Key: key, Source: source, Generation: generation})
			candidates[key] = candidate
			maintenance[key] = control
		}
		for _, key := range queue.Reconcile(items, time.Now()) {
			if owner := owners[key]; owner != nil && owner.cancel != nil {
				owner.cancel()
			}
		}
		for key, owner := range owners {
			if owner.cancel == nil && !queue.Contains(owner.item) {
				delete(owners, key)
			}
		}
		metrics.RecoveryOldest.Set(time.Since(queue.OldestSince(time.Now())).Seconds())
	}
	discover()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	inventory := time.NewTicker(time.Second)
	defer inventory.Stop()
	for ctx.Err() == nil {
		metrics.RecoveryQueue.Set(float64(queue.Len()))
		for active < workers+1 {
			var lease *recovery.Lease
			entry, ok := queue.Next(time.Now(), func(item recovery.Item) bool {
				if maintenance[item.Key] {
					if controlActive {
						return false
					}
					controlActive, lease = true, nil
					return true
				}
				var err error
				lease, _, err = budget.TryAcquireEstimated(ctx, item.Source, quantum)
				if err != nil {
					metrics.RecoveryDeferred.Inc()
				}
				return err == nil
			})
			if !ok {
				break
			}
			owner := owners[entry.Key]
			if owner == nil {
				owner = &scheduledReplica{item: entry.Item, candidate: candidates[entry.Key], since: entry.Since}
				if lease != nil {
					metrics.RecoveryFirstService.Observe(time.Since(entry.Since).Seconds())
				}
				owners[entry.Key] = owner
			}
			turnCtx, cancel := context.WithTimeout(ctx, time.Duration(settings.TurnTimeoutMS)*time.Millisecond)
			owner.cancel = cancel
			active++
			if lease != nil {
				metrics.RecoveryRunning.Inc()
			}
			jobs <- replicaDispatch{owner, turnCtx, lease, time.Now()}
		}
		select {
		case <-ctx.Done():
			return
		case c := <-completed:
			finish(c)
		case <-inventory.C:
			discover()
		case <-tick.C:
		}
	}
}
