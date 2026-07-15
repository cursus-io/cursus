package controller

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
)

type clusterBrokerStatus struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	Addr       string `json:"addr"`
	ClientAddr string `json:"client_addr,omitempty"`
}

type clusterPartitionStatus struct {
	Key             string   `json:"key"`
	Topic           string   `json:"topic"`
	Partition       int      `json:"partition"`
	Leader          string   `json:"leader"`
	LeaderEpoch     int      `json:"leader_epoch"`
	Replicas        []string `json:"replicas"`
	ISR             []string `json:"isr"`
	CommittedHWM    uint64   `json:"committed_hwm"`
	LeaderAvailable bool     `json:"leader_available"`
	UnderReplicated bool     `json:"under_replicated"`
}

type clusterStatus struct {
	RaftLeader      string                   `json:"raft_leader"`
	BrokerCount     int                      `json:"broker_count"`
	ActiveBrokers   int                      `json:"active_brokers"`
	InactiveBrokers int                      `json:"inactive_brokers"`
	PartitionCount  int                      `json:"partition_count"`
	Leaderless      int                      `json:"leaderless_partitions"`
	UnderReplicated int                      `json:"under_replicated_partitions"`
	Brokers         []clusterBrokerStatus    `json:"brokers"`
	Partitions      []clusterPartitionStatus `json:"partitions"`
}

func (ch *CommandHandler) handleClusterStatus() string {
	if !ch.isDistributed() {
		return "ERROR: distribution_required command=CLUSTER_STATUS"
	}
	state := ch.Cluster.RaftManager.GetFSM()
	if state == nil {
		return "ERROR: fsm_not_available command=CLUSTER_STATUS"
	}

	status := buildClusterStatus(state, ch.Cluster.RaftManager.GetLeaderAddress())
	data, err := json.Marshal(status)
	if err != nil {
		return fmt.Sprintf("ERROR: marshal_cluster_status_failed reason=%q", err.Error())
	}
	return "OK cluster=" + string(data)
}

func buildClusterStatus(state *fsm.BrokerFSM, raftLeader string) clusterStatus {
	status := clusterStatus{RaftLeader: raftLeader}
	brokers := state.GetBrokers()
	sort.Slice(brokers, func(i, j int) bool { return brokers[i].ID < brokers[j].ID })
	active := make(map[string]bool, len(brokers))
	for _, broker := range brokers {
		isActive := strings.EqualFold(broker.Status, "active")
		active[broker.ID] = isActive
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

	keys := state.GetAllPartitionKeys()
	sort.Strings(keys)
	for _, key := range keys {
		metadata := state.GetPartitionMetadata(key)
		if metadata == nil {
			continue
		}
		topicName, partition := splitPartitionMetadataKey(key)
		leaderAvailable := metadata.Leader != "" && active[metadata.Leader]
		underReplicated := len(metadata.ISR) < len(metadata.Replicas)
		if !leaderAvailable {
			status.Leaderless++
		}
		if underReplicated {
			status.UnderReplicated++
		}
		status.Partitions = append(status.Partitions, clusterPartitionStatus{
			Key:             key,
			Topic:           topicName,
			Partition:       partition,
			Leader:          metadata.Leader,
			LeaderEpoch:     metadata.LeaderEpoch,
			Replicas:        append([]string(nil), metadata.Replicas...),
			ISR:             append([]string(nil), metadata.ISR...),
			CommittedHWM:    metadata.CommittedHWM,
			LeaderAvailable: leaderAvailable,
			UnderReplicated: underReplicated,
		})
	}
	status.PartitionCount = len(status.Partitions)
	return status
}

func splitPartitionMetadataKey(key string) (string, int) {
	idx := strings.LastIndexByte(key, '-')
	if idx < 1 || idx+1 >= len(key) {
		return key, -1
	}
	partition, err := strconv.Atoi(key[idx+1:])
	if err != nil {
		return key, -1
	}
	return key[:idx], partition
}

func (ch *CommandHandler) handleElectLeader(cmd string) string {
	if !ch.isDistributed() {
		return "ERROR: distribution_required command=ELECT_LEADER"
	}
	if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
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

	result, err := ch.applyAndWait("LEADER_ELECTION", map[string]interface{}{
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
