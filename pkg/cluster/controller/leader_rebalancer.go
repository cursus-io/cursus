package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/util"
	"github.com/hashicorp/raft"
)

const maxInternalLeaderMovesPerCycle = 1

type LeaderRebalanceReason string

const (
	LeaderRebalanceEligible             LeaderRebalanceReason = "eligible"
	LeaderRebalanceAlreadyPreferred     LeaderRebalanceReason = "already_preferred"
	LeaderRebalanceMissingReplica       LeaderRebalanceReason = "missing_replica"
	LeaderRebalanceAssignmentInProgress LeaderRebalanceReason = "assignment_in_progress"
	LeaderRebalancePreferredInactive    LeaderRebalanceReason = "preferred_inactive"
	LeaderRebalancePreferredOutOfISR    LeaderRebalanceReason = "preferred_out_of_isr"
	LeaderRebalanceRecoveryPending      LeaderRebalanceReason = "recovery_pending"
	LeaderRebalanceInvalidMetadata      LeaderRebalanceReason = "invalid_metadata"
)

type LeaderRebalancePartition struct {
	Key                string
	Topic              string
	Partition          int
	CurrentLeader      string
	PreferredLeader    string
	LeaderEpoch        int
	LifecycleEpoch     uint64
	DefinitionRevision uint64
	ExpectedReplicas   []string
	Internal           bool
	Eligible           bool
	Reason             LeaderRebalanceReason
}

type LeaderRebalancePlan struct {
	Partitions            []LeaderRebalancePartition
	Candidates            []LeaderRebalancePartition
	ImbalancedPartitions  int
	EligiblePartitions    int
	LeaderCounts          map[string]int
	PreferredLeaderCounts map[string]int
}

type LeaderRebalanceMoveResult struct {
	Candidate LeaderRebalancePartition
	Result    string
	Reason    string
	Err       error
}

type LeaderRebalanceCycleResult struct {
	Plan     LeaderRebalancePlan
	Moves    []LeaderRebalanceMoveResult
	Skipped  string
	Duration time.Duration
}

func PlanPreferredLeaderRebalance(snapshot fsm.LeaderRebalanceSnapshot, maxMoves int) LeaderRebalancePlan {
	plan := LeaderRebalancePlan{
		LeaderCounts:          make(map[string]int),
		PreferredLeaderCounts: make(map[string]int),
	}
	topicNames := make([]string, 0, len(snapshot.Topics))
	for name := range snapshot.Topics {
		topicNames = append(topicNames, name)
	}
	sort.Strings(topicNames)

	var internalCandidates []LeaderRebalancePartition
	var applicationCandidates []LeaderRebalancePartition
	for _, topicName := range topicNames {
		definition := snapshot.Topics[topicName]
		for partition := 0; partition < definition.Partitions; partition++ {
			key := topicName + "-" + strconv.Itoa(partition)
			assessment := assessLeaderRebalancePartition(key, topicName, partition, definition, snapshot)
			plan.Partitions = append(plan.Partitions, assessment)
			if assessment.CurrentLeader != "" {
				plan.LeaderCounts[assessment.CurrentLeader]++
			}
			if assessment.PreferredLeader != "" {
				plan.PreferredLeaderCounts[assessment.PreferredLeader]++
			}
			if assessment.PreferredLeader != "" && assessment.CurrentLeader != assessment.PreferredLeader {
				plan.ImbalancedPartitions++
			}
			if !assessment.Eligible {
				continue
			}
			plan.EligiblePartitions++
			if assessment.Internal {
				internalCandidates = append(internalCandidates, assessment)
			} else {
				applicationCandidates = append(applicationCandidates, assessment)
			}
		}
	}

	if maxMoves <= 0 {
		return plan
	}
	if len(internalCandidates) > 0 {
		limit := min(maxInternalLeaderMovesPerCycle, maxMoves, len(internalCandidates))
		plan.Candidates = append(plan.Candidates, internalCandidates[:limit]...)
	}
	remaining := maxMoves - len(plan.Candidates)
	if remaining > 0 {
		plan.Candidates = append(plan.Candidates, applicationCandidates[:min(remaining, len(applicationCandidates))]...)
	}
	return plan
}

func assessLeaderRebalancePartition(key, topicName string, partition int, definition topic.Definition, snapshot fsm.LeaderRebalanceSnapshot) LeaderRebalancePartition {
	assessment := LeaderRebalancePartition{
		Key: key, Topic: topicName, Partition: partition, Internal: isInternalTopic(topicName),
		Reason: LeaderRebalanceMissingReplica,
	}
	metadata, found := snapshot.Partitions[key]
	if !found || len(metadata.Replicas) == 0 {
		return assessment
	}
	assessment.CurrentLeader = metadata.Leader
	assessment.PreferredLeader = metadata.Replicas[0]
	assessment.LeaderEpoch = metadata.LeaderEpoch
	assessment.LifecycleEpoch = metadata.LifecycleEpoch
	assessment.DefinitionRevision = definition.Revision
	assessment.ExpectedReplicas = append([]string(nil), metadata.Replicas...)
	if definition.LifecycleEpoch == 0 || metadata.LifecycleEpoch != definition.LifecycleEpoch || metadata.PartitionCount != definition.Partitions || !validLeaderRebalanceAssignment(metadata) || !leaderRebalanceReplicasKnown(metadata.Replicas, snapshot.Brokers) {
		assessment.Reason = LeaderRebalanceInvalidMetadata
		return assessment
	}
	if len(metadata.Replicas) != definition.ReplicationFactor {
		assessment.Reason = LeaderRebalanceAssignmentInProgress
		return assessment
	}
	if metadata.Leader == assessment.PreferredLeader {
		assessment.Reason = LeaderRebalanceAlreadyPreferred
		return assessment
	}
	if len(metadata.RecoveryReplicas) > 0 {
		assessment.Reason = LeaderRebalanceRecoveryPending
		return assessment
	}
	preferred, found := snapshot.Brokers[assessment.PreferredLeader]
	if !found || !strings.EqualFold(preferred.Status, "active") {
		assessment.Reason = LeaderRebalancePreferredInactive
		return assessment
	}
	if !containsLeaderRebalanceBroker(metadata.ISR, assessment.PreferredLeader) {
		assessment.Reason = LeaderRebalancePreferredOutOfISR
		return assessment
	}
	assessment.Eligible = true
	assessment.Reason = LeaderRebalanceEligible
	return assessment
}

func isInternalTopic(name string) bool { return strings.HasPrefix(name, "__") }

func validLeaderRebalanceAssignment(metadata fsm.PartitionMetadata) bool {
	if metadata.Leader == "" || metadata.LeaderEpoch <= 0 || metadata.LifecycleEpoch == 0 || len(metadata.Replicas) == 0 {
		return false
	}
	replicas := make(map[string]struct{}, len(metadata.Replicas))
	for _, replica := range metadata.Replicas {
		if strings.TrimSpace(replica) == "" {
			return false
		}
		if _, duplicate := replicas[replica]; duplicate {
			return false
		}
		replicas[replica] = struct{}{}
	}
	if _, configured := replicas[metadata.Leader]; !configured {
		return false
	}
	isr := make(map[string]struct{}, len(metadata.ISR))
	for _, replica := range metadata.ISR {
		if strings.TrimSpace(replica) == "" {
			return false
		}
		if _, duplicate := isr[replica]; duplicate {
			return false
		}
		if _, configured := replicas[replica]; !configured {
			return false
		}
		isr[replica] = struct{}{}
	}
	return true
}

func leaderRebalanceReplicasKnown(replicas []string, brokers map[string]fsm.BrokerInfo) bool {
	for _, replica := range replicas {
		if _, found := brokers[replica]; !found {
			return false
		}
	}
	return true
}

func containsLeaderRebalanceBroker(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (cc *ClusterController) RunLeaderRebalanceOnce(ctx context.Context) (result LeaderRebalanceCycleResult) {
	started := time.Now()
	defer func() {
		result.Duration = time.Since(started)
		successful := 0
		for _, move := range result.Moves {
			if move.Result == "success" {
				successful++
			}
		}
		metrics.ObserveLeaderRebalanceCycle(result.Duration, successful)
	}()
	if ctx == nil {
		ctx = context.Background()
	}
	if cc == nil || cc.RaftManager == nil || cc.Config == nil || !cc.Config.EnabledDistribution || !cc.Config.LeaderRebalanceEnabled {
		result.Skipped = "disabled"
		metrics.ClearLeaderRebalancePlan()
		return result
	}
	if !cc.RaftManager.IsLeader() {
		result.Skipped = "not_leader"
		metrics.ClearLeaderRebalancePlan()
		return result
	}
	state := cc.RaftManager.GetFSM()
	if state == nil {
		result.Skipped = "fsm_unavailable"
		metrics.ClearLeaderRebalancePlan()
		return result
	}
	result.Plan = PlanPreferredLeaderRebalance(state.GetLeaderRebalanceSnapshot(), cc.Config.LeaderRebalanceMaxMovesPerCycle)
	metrics.SetLeaderRebalancePlan(
		result.Plan.ImbalancedPartitions,
		result.Plan.EligiblePartitions,
		result.Plan.LeaderCounts,
		result.Plan.PreferredLeaderCounts,
	)
	for _, candidate := range result.Plan.Candidates {
		if err := ctx.Err(); err != nil {
			result.Skipped = "canceled"
			break
		}
		if !cc.RaftManager.IsLeader() {
			result.Skipped = "not_leader"
			break
		}
		command := fsm.LeaderElectionCommand{
			Topic: candidate.Topic, Partition: candidate.Partition, Broker: candidate.PreferredLeader,
			ExpectedLeaderEpoch: candidate.LeaderEpoch, TopologyFenceVersion: fsm.LeaderElectionTopologyFenceVersionCurrent,
			ExpectedLifecycleEpoch: candidate.LifecycleEpoch, ExpectedDefinitionRevision: candidate.DefinitionRevision,
			ExpectedReplicas: append([]string(nil), candidate.ExpectedReplicas...),
		}
		payload, err := json.Marshal(command)
		if err == nil {
			err = cc.RaftManager.ApplyCommand("LEADER_ELECTION", payload)
		}
		move := LeaderRebalanceMoveResult{Candidate: candidate, Result: "success", Reason: "applied", Err: err}
		if err != nil {
			move.Result = "failure"
			move.Reason = "rejected"
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				move.Reason = "canceled"
				result.Skipped = "canceled"
			} else if errors.Is(err, raft.ErrNotLeader) {
				move.Reason = "not_leader"
				result.Skipped = "not_leader"
			}
		}
		result.Moves = append(result.Moves, move)
		metrics.RecordLeaderRebalanceMove(move.Result, move.Reason)
		if result.Skipped != "" {
			break
		}
	}
	return result
}

func (cc *ClusterController) StartLeaderRebalancer(ctx context.Context) func() {
	if cc == nil || cc.Config == nil || !cc.Config.EnabledDistribution || !cc.Config.LeaderRebalanceEnabled || cc.RaftManager == nil {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	interval := time.Duration(cc.Config.LeaderRebalanceIntervalMS) * time.Millisecond
	wait := cc.leaderRebalanceWait
	if wait == nil {
		wait = waitLeaderRebalanceInterval
	}
	go func() {
		defer close(done)
		for wait(ctx, interval) {
			cycle := cc.RunLeaderRebalanceOnce(ctx)
			if cycle.Skipped != "" && cycle.Skipped != "not_leader" && cycle.Skipped != "disabled" {
				util.Debug("preferred leader rebalance skipped=%s", cycle.Skipped)
			}
			for _, move := range cycle.Moves {
				if move.Err != nil {
					util.Debug("preferred leader rebalance topic=%q partition=%d reason=%s: %v", move.Candidate.Topic, move.Candidate.Partition, move.Reason, move.Err)
				}
			}
		}
	}()
	return func() { cancel(); <-done }
}

func waitLeaderRebalanceInterval(ctx context.Context, interval time.Duration) bool {
	if interval <= 0 {
		return false
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (p LeaderRebalancePartition) String() string {
	return fmt.Sprintf("%s-%d:%s->%s", p.Topic, p.Partition, p.CurrentLeader, p.PreferredLeader)
}
