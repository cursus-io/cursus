package controller

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type bootstrapRaftManager struct {
	*MockRaftManager
	applyCount    int
	configuration raft.Configuration
}

func (m *bootstrapRaftManager) ApplyCommand(prefix string, data []byte) error {
	m.applyCount++
	m.mockFSM.Apply(&raft.Log{Index: uint64(m.applyCount), Data: append([]byte(prefix+":"), data...)})
	return nil
}

func (m *bootstrapRaftManager) GetConfiguration() raft.ConfigurationFuture {
	return staticConfigurationFuture{configuration: m.configuration}
}

func TestBootstrapConsumerOffsetsTopicCommitsOnlyDurableTopology(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	for index, id := range []string{"n1", "n2", "n3"} {
		state.Apply(&raft.Log{Index: uint64(index + 1), Data: []byte(`REGISTER:{"id":"` + id + `","addr":"localhost:7001","status":"active","lifecycle_protocol":2}`)})
	}
	manager := &bootstrapRaftManager{
		MockRaftManager: &MockRaftManager{isLeader: true, mockFSM: state},
		configuration: raft.Configuration{Servers: []raft.Server{
			{ID: "n1", Suffrage: raft.Voter},
			{ID: "n2", Suffrage: raft.Voter},
			{ID: "n3", Suffrage: raft.Voter},
		}},
	}

	require.NoError(t, BootstrapConsumerOffsetsTopic(manager, config.DefaultConfig()))
	metadata := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
	require.NotNil(t, metadata)
	require.Contains(t, []string{"n1", "n2", "n3"}, metadata.Leader)
	require.Equal(t, 1, manager.applyCount)

	require.NoError(t, BootstrapConsumerOffsetsTopic(manager, config.DefaultConfig()))
	require.Equal(t, 1, manager.applyCount, "existing durable topology must not be rewritten")
}

func TestBootstrapConsumerOffsetsTopicWaitsForEveryRaftVoter(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	state.Apply(&raft.Log{Index: 1, Data: []byte(`REGISTER:{"id":"n1","addr":"localhost:7001","status":"active","lifecycle_protocol":2}`)})
	manager := &bootstrapRaftManager{
		MockRaftManager: &MockRaftManager{isLeader: true, mockFSM: state},
		configuration: raft.Configuration{Servers: []raft.Server{
			{ID: "n1", Suffrage: raft.Voter},
			{ID: "n2", Suffrage: raft.Voter},
		}},
	}

	require.Error(t, BootstrapConsumerOffsetsTopic(manager, config.DefaultConfig()))
	require.Equal(t, 0, manager.applyCount)
	require.Nil(t, state.GetPartitionMetadata(config.ConsumerOffsetsTopicName+"-0"))
}
