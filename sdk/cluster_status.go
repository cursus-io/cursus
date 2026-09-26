package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// ClusterStatus is a read-only snapshot of broker membership and partition
// safety. It is intentionally separate from topic metadata: a caller can
// render operational health without guessing it from client endpoints.
type ClusterStatus struct {
	RaftLeader      string             `json:"raft_leader"`
	RaftState       string             `json:"raft_state"`
	BrokerCount     int                `json:"broker_count"`
	ActiveBrokers   int                `json:"active_brokers"`
	InactiveBrokers int                `json:"inactive_brokers"`
	PartitionCount  int                `json:"partition_count"`
	Leaderless      int                `json:"leaderless_partitions"`
	UnderReplicated int                `json:"under_replicated_partitions"`
	Brokers         []ClusterBroker    `json:"brokers"`
	Partitions      []ClusterPartition `json:"partitions"`
}

type ClusterBroker struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	Addr       string `json:"addr"`
	ClientAddr string `json:"client_addr"`
}

type ClusterPartition struct {
	Key             string   `json:"key"`
	Topic           string   `json:"topic"`
	Partition       int      `json:"partition"`
	Leader          string   `json:"leader"`
	LeaderEpoch     int      `json:"leader_epoch"`
	Replicas        []string `json:"replicas"`
	ISR             []string `json:"isr"`
	LeaderAvailable bool     `json:"leader_available"`
	UnderReplicated bool     `json:"under_replicated"`
}

// ErrStandaloneBroker reports a successful broker connection whose
// distribution mode is disabled. Callers can render it as one local node.
type ErrStandaloneBroker struct{}

func (ErrStandaloneBroker) Error() string { return "broker distribution is not enabled" }

// ClusterStatus asks a broker for its replicated membership and partition
// safety snapshot. A SASL-enabled broker authorizes CLUSTER_STATUS as an
// admin operation; an auth-disabled local broker may answer it without a
// credential. Callers must still keep the result behind their own boundary.
func (c *AdminClient) ClusterStatus(ctx context.Context) (*ClusterStatus, error) {
	response, err := c.execute(ctx, "CLUSTER_STATUS", true)
	if err != nil {
		if brokerErr, ok := err.(*BrokerError); ok && strings.EqualFold(brokerErr.Code, "distribution_required") {
			return nil, ErrStandaloneBroker{}
		}
		return nil, err
	}
	return parseClusterStatus(response)
}

func parseClusterStatus(response string) (*ClusterStatus, error) {
	payload, ok := strings.CutPrefix(strings.TrimSpace(response), "OK cluster=")
	if !ok || payload == "" {
		return nil, fmt.Errorf("invalid cluster status response")
	}
	var status ClusterStatus
	if err := json.Unmarshal([]byte(payload), &status); err != nil {
		return nil, fmt.Errorf("decode cluster status: %w", err)
	}
	if status.BrokerCount < 0 || status.ActiveBrokers < 0 || status.InactiveBrokers < 0 || status.PartitionCount < 0 || status.Leaderless < 0 || status.UnderReplicated < 0 {
		return nil, fmt.Errorf("invalid negative cluster status count")
	}
	if status.BrokerCount != len(status.Brokers) || status.ActiveBrokers+status.InactiveBrokers != status.BrokerCount || status.PartitionCount != len(status.Partitions) {
		return nil, fmt.Errorf("inconsistent cluster status counts")
	}
	for _, broker := range status.Brokers {
		if broker.ID == "" || (broker.Status != "active" && broker.Status != "inactive") {
			return nil, fmt.Errorf("invalid cluster broker status")
		}
	}
	return &status, nil
}
