package controller

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/hashicorp/raft"
)

// ActivateRetentionTime checks every registered process, including
// inactive ones, and every current voter. New voter admission requires the same
// capability before membership changes, so a delayed JOIN cannot downgrade it.
func ActivateRetentionTime(rm RaftManager) error {
	if rm == nil || rm.GetFSM() == nil || !rm.IsLeader() {
		return nil
	}
	state := rm.GetFSM()
	if state.RetentionTimeEnabled() {
		return nil
	}
	brokers := state.GetBrokers()
	if len(brokers) == 0 {
		return nil
	}
	capable := make(map[string]bool, len(brokers))
	for _, broker := range brokers {
		if broker.RecoveryProtocol < fsm.BootstrapRecoveryProtocolVersion || broker.LifecycleProtocol < fsm.RetentionTimeProtocolVersion || broker.IncarnationID == "" || broker.IncarnationEpoch == 0 {
			return nil
		}
		capable[broker.ID] = true
	}
	configuration := rm.GetConfiguration()
	if configuration == nil {
		return fmt.Errorf("voter configuration unavailable")
	}
	if err := configuration.Error(); err != nil {
		return err
	}
	command := fsm.RetentionTimeActivation{}
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
	return rm.ApplyCommand("RETENTION_TIME_ACTIVATE", data)
}
