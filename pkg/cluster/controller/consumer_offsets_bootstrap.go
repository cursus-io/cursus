package controller

import (
	"encoding/json"
	"fmt"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
)

const consumerOffsetsPartitionCount = 4

// BootstrapConsumerOffsetsTopic installs the broker-owned offset log's
// topology through the normal Raft TOPIC command. Offset records themselves
// are deliberately not controller metadata: they stay in the replicated
// __consumer_offsets partitions.
//
// The operation is idempotent. It only writes when the durable FSM has no
// partition-zero record, so a later controller election cannot rewrite the
// topic's assignment from node-local state.
func BootstrapConsumerOffsetsTopic(rm RaftManager, cfg *config.Config) error {
	if rm == nil || rm.GetFSM() == nil {
		return fmt.Errorf("consumer offsets bootstrap requires raft FSM")
	}
	if !rm.IsLeader() {
		return fmt.Errorf("consumer offsets bootstrap requires raft leader")
	}
	if rm.GetFSM().GetPartitionMetadata(config.ConsumerOffsetsTopicName+"-0") != nil {
		return nil
	}
	configuration := rm.GetConfiguration()
	if err := configuration.Error(); err != nil {
		return fmt.Errorf("read raft voter configuration: %w", err)
	}
	for _, voter := range configuration.Configuration().Servers {
		if voter.Suffrage != raft.Voter {
			continue
		}
		broker := rm.GetFSM().GetBroker(string(voter.ID))
		if broker == nil || broker.Status != "active" {
			return fmt.Errorf("waiting for durable active registration of raft voter %s", voter.ID)
		}
	}

	replicationFactor := 3
	if cfg != nil && cfg.DefaultReplicationFactor > 0 {
		replicationFactor = cfg.DefaultReplicationFactor
	}
	definition := topic.DefaultDefinition(config.ConsumerOffsetsTopicName, cfg)
	definition.Partitions = consumerOffsetsPartitionCount
	definition.ReplicationFactor = replicationFactor
	definition.Policy = topic.ConsumerMetadataPolicy()
	payload, err := json.Marshal(fsm.TopicCommand{Definition: &definition})
	if err != nil {
		return fmt.Errorf("encode consumer offsets topology: %w", err)
	}
	if err := rm.ApplyCommand("TOPIC", payload); err != nil {
		return fmt.Errorf("commit consumer offsets topology: %w", err)
	}
	return nil
}
