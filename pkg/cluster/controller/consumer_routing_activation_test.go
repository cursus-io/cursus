package controller

import (
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestConsumerRoutingActivationWaitsForEveryVoterAndInactiveBroker(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	register := func(id, status string, version int) {
		require.Nil(t, state.Apply(&raft.Log{Data: []byte(fmt.Sprintf(`REGISTER:{"id":%q,"status":%q,"incarnation_id":%q,"lifecycle_protocol":6,"recovery_protocol":%d}`, id, status, id+"-process", version))}))
	}
	manager := &bootstrapRaftManager{MockRaftManager: &MockRaftManager{isLeader: true, mockFSM: state}, configuration: raft.Configuration{Servers: []raft.Server{{ID: "a", Suffrage: raft.Voter}, {ID: "b", Suffrage: raft.Voter}}}}
	register("a", "active", 2)
	require.NoError(t, ActivateConsumerMetadataRouting(manager))
	require.Zero(t, manager.applyCount)
	register("b", "active", 2)
	register("old", "inactive", 0)
	require.NoError(t, ActivateConsumerMetadataRouting(manager))
	require.Zero(t, manager.applyCount)
	register("old", "inactive", 2)
	require.NoError(t, ActivateConsumerMetadataRouting(manager))
	require.True(t, state.ConsumerMetadataRoutingEnabled())
	require.Equal(t, 1, manager.applyCount)
	require.NoError(t, ActivateConsumerMetadataRouting(manager))
	require.Equal(t, 1, manager.applyCount)
}
