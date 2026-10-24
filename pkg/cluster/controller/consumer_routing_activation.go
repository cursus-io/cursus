package controller

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/hashicorp/raft"
)

// ActivateConsumerMetadataRouting waits for every registered process and voter
// to report recovered strict authority. Apply revalidates the complete process
// set, so a delayed registration/restart cannot reuse another process's report.
func ActivateConsumerMetadataRouting(rm RaftManager) error {
	if rm == nil || rm.GetFSM() == nil || !rm.IsLeader() {
		return nil
	}
	state := rm.GetFSM()
	if state.ConsumerMetadataRoutingEnabled() {
		return nil
	}
	brokers := state.GetBrokers()
	if len(brokers) == 0 {
		return nil
	}
	capable := make(map[string]bool, len(brokers))
	processes := make([]fsm.BootstrapProcessIdentity, 0, len(brokers))
	for _, broker := range brokers {
		if broker.RecoveryProtocol < fsm.BootstrapRecoveryProtocolVersion || broker.LifecycleProtocol < fsm.ReplicaGapRecoveryProtocolVersion || broker.IncarnationID == "" || broker.IncarnationEpoch == 0 || broker.Status != "active" || !broker.ConsumerRoutingReady {
			return nil
		}
		capable[broker.ID] = true
		processes = append(processes, fsm.BootstrapProcessIdentity{BrokerID: broker.ID, IncarnationID: broker.IncarnationID, IncarnationEpoch: broker.IncarnationEpoch})
	}
	configuration := rm.GetConfiguration()
	if configuration == nil {
		return fmt.Errorf("voter configuration unavailable")
	}
	if err := configuration.Error(); err != nil {
		return err
	}
	sort.Slice(processes, func(i, j int) bool { return processes[i].BrokerID < processes[j].BrokerID })
	command := fsm.ConsumerRoutingActivation{Processes: processes}
	for _, server := range configuration.Configuration().Servers {
		if server.Suffrage != raft.Voter {
			continue
		}
		if !capable[string(server.ID)] {
			return nil
		}
		command.Voters = append(command.Voters, string(server.ID))
	}
	if len(command.Voters) == 0 {
		return nil
	}
	sort.Strings(command.Voters)
	data, err := json.Marshal(command)
	if err != nil {
		return err
	}
	return rm.ApplyCommand("CONSUMER_ROUTING_ACTIVATE", data)
}
