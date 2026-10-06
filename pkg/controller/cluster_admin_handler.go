package controller

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/cluster/replication"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
)

type clusterBrokerStatus struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	Addr       string `json:"addr"`
	ClientAddr string `json:"client_addr,omitempty"`
}

type clusterPartitionStatus struct {
	Key                string   `json:"key"`
	Topic              string   `json:"topic"`
	Partition          int      `json:"partition"`
	Leader             string   `json:"leader"`
	LeaderEpoch        int      `json:"leader_epoch"`
	Replicas           []string `json:"replicas"`
	ISR                []string `json:"isr"`
	CommittedHWM       uint64   `json:"committed_hwm"`
	ExpectedReplicas   int      `json:"expected_replicas"`
	ActiveReplicas     int      `json:"active_replicas"`
	InactiveReplicas   int      `json:"inactive_replicas"`
	InSyncReplicas     int      `json:"in_sync_replicas"`
	MinInSyncReplicas  int      `json:"min_in_sync_replicas"`
	LeaderAvailable    bool     `json:"leader_available"`
	AssignmentComplete bool     `json:"assignment_complete"`
	UnderReplicated    bool     `json:"under_replicated"`
	MinISRUnsatisfied  bool     `json:"min_isr_unsatisfied"`
	Healthy            bool     `json:"healthy"`
	Reasons            []string `json:"reasons,omitempty"`
}

type clusterStatus struct {
	InternalCredentialGeneration string                   `json:"internal_credential_generation,omitempty"`
	RaftLeader                   string                   `json:"raft_leader"`
	RaftState                    string                   `json:"raft_state"`
	RaftAppliedIndex             uint64                   `json:"raft_applied_index"`
	RaftCommitIndex              uint64                   `json:"raft_commit_index"`
	RaftLastLogIndex             uint64                   `json:"raft_last_log_index"`
	RaftLastSnapshotIndex        uint64                   `json:"raft_last_snapshot_index"`
	RaftLastSnapshotTerm         uint64                   `json:"raft_last_snapshot_term"`
	BrokerCount                  int                      `json:"broker_count"`
	ActiveBrokers                int                      `json:"active_brokers"`
	InactiveBrokers              int                      `json:"inactive_brokers"`
	PartitionCount               int                      `json:"partition_count"`
	Leaderless                   int                      `json:"leaderless_partitions"`
	UnderReplicated              int                      `json:"under_replicated_partitions"`
	AssignmentDeficient          int                      `json:"assignment_deficient_partitions"`
	InactiveReplicaPartitions    int                      `json:"inactive_replica_partitions"`
	InactiveReplicas             int                      `json:"inactive_replicas"`
	MinISRUnsatisfied            int                      `json:"min_isr_unsatisfied_partitions"`
	Healthy                      bool                     `json:"healthy"`
	Brokers                      []clusterBrokerStatus    `json:"brokers"`
	Partitions                   []clusterPartitionStatus `json:"partitions"`
}

// handleListCluster processes the read-only LIST_CLUSTER command.
func (ch *CommandHandler) handleListCluster() string {
	if !ch.isDistributed() {
		return "ERROR: distribution_not_enabled"
	}
	state := ch.Cluster.RaftManager.GetFSM()
	if state == nil {
		return "ERROR: fsm_not_available"
	}

	brokers := state.GetBrokers()
	data, err := json.Marshal(brokers)
	if err != nil {
		return fmt.Sprintf("ERROR: marshal_brokers_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK brokers=%s", string(data))
}

func (ch *CommandHandler) handleClusterStatus() string {
	if !ch.isDistributed() {
		return "ERROR: distribution_required command=CLUSTER_STATUS"
	}
	state := ch.Cluster.RaftManager.GetFSM()
	if state == nil {
		return "ERROR: fsm_not_available command=CLUSTER_STATUS"
	}

	defaultMinISR := 1
	if ch.Config != nil {
		defaultMinISR = ch.Config.MinInSyncReplicas
	}
	status := buildClusterStatus(state, ch.Cluster.RaftManager.GetLeaderAddress(), defaultMinISR)
	if ch.Config != nil {
		status.InternalCredentialGeneration = ch.Config.InternalAuthGeneration
	}
	if provider, ok := ch.Cluster.RaftManager.(interface {
		GetRaftStatus() (replication.RaftStatus, error)
	}); ok {
		raftStatus, err := provider.GetRaftStatus()
		if err != nil {
			status.RaftState = "unavailable"
		} else {
			status.RaftState = raftStatus.State
			status.RaftAppliedIndex = raftStatus.AppliedIndex
			status.RaftCommitIndex = raftStatus.CommitIndex
			status.RaftLastLogIndex = raftStatus.LastLogIndex
			status.RaftLastSnapshotIndex = raftStatus.LastSnapshotIndex
			status.RaftLastSnapshotTerm = raftStatus.LastSnapshotTerm
		}
	}
	data, err := json.Marshal(status)
	if err != nil {
		return fmt.Sprintf("ERROR: marshal_cluster_status_failed reason=%q", err.Error())
	}
	return "OK cluster=" + string(data)
}

func buildClusterStatus(state *fsm.BrokerFSM, raftLeader string, defaultMinISR int) clusterStatus {
	status := clusterStatus{RaftLeader: raftLeader}
	brokers := state.GetBrokers()
	sort.Slice(brokers, func(i, j int) bool { return brokers[i].ID < brokers[j].ID })
	for _, broker := range brokers {
		isActive := strings.EqualFold(broker.Status, "active")
		if isActive {
			status.ActiveBrokers++
		} else {
			status.InactiveBrokers++
		}
		status.Brokers = append(status.Brokers, clusterBrokerStatus{
			ID: broker.ID, Status: broker.Status, Addr: broker.Addr, ClientAddr: broker.ClientAddr,
		})
	}
	status.BrokerCount = len(status.Brokers)

	topology := state.EvaluateTopology(defaultMinISR)
	status.PartitionCount = topology.PartitionCount
	status.Leaderless = topology.Offline
	status.UnderReplicated = topology.UnderReplicated
	status.AssignmentDeficient = topology.AssignmentDeficient
	status.InactiveReplicaPartitions = topology.InactiveReplicaPartitions
	status.InactiveReplicas = topology.InactiveReplicas
	status.MinISRUnsatisfied = topology.MinISRUnsatisfied
	status.Healthy = topology.Healthy
	for _, partition := range topology.Partitions {
		status.Partitions = append(status.Partitions, clusterPartitionStatus{
			Key: partition.Key, Topic: partition.Topic, Partition: partition.Partition,
			Leader: partition.Leader, LeaderEpoch: partition.LeaderEpoch,
			Replicas: append([]string(nil), partition.Replicas...), ISR: append([]string(nil), partition.ISR...),
			CommittedHWM:     partition.CommittedHWM,
			ExpectedReplicas: partition.ExpectedReplicas, ActiveReplicas: partition.ActiveReplicas,
			InactiveReplicas: partition.InactiveReplicas,
			InSyncReplicas:   partition.InSyncReplicas, MinInSyncReplicas: partition.MinInSyncReplicas,
			LeaderAvailable: partition.LeaderAvailable, AssignmentComplete: partition.AssignmentComplete,
			UnderReplicated: partition.UnderReplicated, MinISRUnsatisfied: partition.MinISRUnsatisfied,
			Healthy: partition.Healthy, Reasons: append([]string(nil), partition.Reasons...),
		})
	}
	return status
}

func (ch *CommandHandler) handleElectLeader(cmd string, ctx ...*ClientContext) string {
	requestCtx := firstClientContext(ctx).RequestContext()
	if !ch.isDistributed() {
		return "ERROR: distribution_required command=ELECT_LEADER"
	}
	if resp, forwarded, _ := ch.isLeaderAndForwardContext(requestCtx, cmd); forwarded {
		return resp
	}

	args := parseKeyValueArgs(cmd[len("ELECT_LEADER "):])
	topicName := strings.TrimSpace(args["topic"])
	if topicName == "" {
		return "ERROR: missing_topic command=ELECT_LEADER"
	}
	partition, err := strconv.Atoi(args["partition"])
	if err != nil || partition < 0 {
		return "ERROR: invalid_partition command=ELECT_LEADER"
	}
	brokerID := strings.TrimSpace(args["broker"])
	if brokerID == "" {
		return "ERROR: missing_broker command=ELECT_LEADER"
	}

	state := ch.Cluster.RaftManager.GetFSM()
	if state == nil {
		return "ERROR: fsm_not_available command=ELECT_LEADER"
	}
	key := fmt.Sprintf("%s-%d", topicName, partition)
	metadata := state.GetPartitionMetadata(key)
	if metadata == nil {
		return fmt.Sprintf("ERROR: partition_not_found topic=%s partition=%d", topicName, partition)
	}

	result, err := ch.applyAndWaitContext(requestCtx, "LEADER_ELECTION", map[string]interface{}{
		"topic":                 topicName,
		"partition":             partition,
		"broker":                brokerID,
		"expected_leader_epoch": metadata.LeaderEpoch,
	})
	if err != nil {
		return fmt.Sprintf("ERROR: leader_election_rejected topic=%s partition=%d broker=%s reason=%q", topicName, partition, brokerID, err.Error())
	}

	election, ok := result.(fsm.LeaderElectionResult)
	if !ok {
		updated := state.GetPartitionMetadata(key)
		if updated == nil || updated.Leader != brokerID {
			return fmt.Sprintf("ERROR: leader_election_result_unavailable topic=%s partition=%d", topicName, partition)
		}
		election = fsm.LeaderElectionResult{
			Topic: topicName, Partition: partition, PreviousLeader: metadata.Leader,
			Leader: updated.Leader, LeaderEpoch: updated.LeaderEpoch, Changed: metadata.Leader != updated.Leader,
		}
	}
	return fmt.Sprintf(
		"OK topic=%s partition=%d previous_leader=%s leader=%s leader_epoch=%d changed=%t",
		election.Topic, election.Partition, election.PreviousLeader, election.Leader, election.LeaderEpoch, election.Changed,
	)
}

func (ch *CommandHandler) handleReassignPartition(cmd string, ctx ...*ClientContext) string {
	requestCtx := firstClientContext(ctx).RequestContext()
	if !ch.isDistributed() {
		return "ERROR: distribution_required command=REASSIGN_PARTITION"
	}
	if resp, forwarded, _ := ch.isLeaderAndForwardContext(requestCtx, cmd); forwarded {
		return resp
	}
	args := parseKeyValueArgs(cmd[len("REASSIGN_PARTITION "):])
	topicName := strings.TrimSpace(args["topic"])
	partition, err := strconv.Atoi(args["partition"])
	if topicName == "" || err != nil || partition < 0 {
		return "ERROR: invalid_partition_identity command=REASSIGN_PARTITION"
	}
	target := splitCSV(args["replicas"])
	if len(target) == 0 {
		return "ERROR: missing_replicas command=REASSIGN_PARTITION"
	}
	state := ch.Cluster.RaftManager.GetFSM()
	if state == nil {
		return "ERROR: fsm_not_available command=REASSIGN_PARTITION"
	}
	key := fmt.Sprintf("%s-%d", topicName, partition)
	metadata := state.GetPartitionMetadata(key)
	if metadata == nil {
		return fmt.Sprintf("ERROR: partition_not_found topic=%s partition=%d", topicName, partition)
	}
	_, err = ch.applyAndWaitContext(requestCtx, "REPLICA_REASSIGN", map[string]interface{}{
		"topic": topicName, "partition": partition,
		"lifecycle_epoch": metadata.LifecycleEpoch,
		"leader":          metadata.Leader, "leader_epoch": metadata.LeaderEpoch,
		"expected_replicas": append([]string(nil), metadata.Replicas...),
		"target_replicas":   target,
	})
	if err != nil {
		return fmt.Sprintf("ERROR: replica_reassignment_rejected topic=%s partition=%d reason=%q", topicName, partition, err.Error())
	}
	return fmt.Sprintf("OK topic=%s partition=%d replicas=%s", topicName, partition, strings.Join(target, ","))
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			result = append(result, part)
		}
	}
	return result
}
