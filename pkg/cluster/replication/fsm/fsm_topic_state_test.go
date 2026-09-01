package fsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestBrokerFSMSnapshotRestoresTopicDefinition(t *testing.T) {
	f := newTestFSM()
	registerActiveBroker(t, f, "broker-1")
	policy := topic.Policy{
		CleanupPolicy:  "delete",
		Partitioner:    topic.PartitionerRoundRobin,
		AuthPolicy:     topic.AuthPolicyACL,
		ReadACL:        []string{"reader"},
		WriteACL:       []string{"writer"},
		RetentionHours: 48,
		RetentionBytes: 8192,
	}
	command := TopicCommand{
		Name:              "orders",
		Partitions:        2,
		Idempotent:        true,
		EventSourcing:     false,
		ReplicationFactor: 1,
		Policy:            policy,
	}
	data, err := json.Marshal(command)
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte("TOPIC:" + string(data)), Index: 2}))

	snapshot, err := f.Snapshot()
	require.NoError(t, err)
	buffer := new(bytes.Buffer)
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: buffer}))
	var persisted BrokerFSMState
	require.NoError(t, json.Unmarshal(buffer.Bytes(), &persisted))
	require.Equal(t, 7, persisted.Version, "first-generation snapshots stay rolling-upgrade compatible")

	restored := newTestFSM()
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buffer.Bytes()))))
	restoredTopic := restored.tm.GetTopic("orders")
	require.NotNil(t, restoredTopic)
	require.Len(t, restoredTopic.Partitions, 2)
	require.True(t, restoredTopic.IsIdempotent)
	require.False(t, restoredTopic.IsEventSourcing)
	require.Equal(t, topic.PartitionerRoundRobin, restoredTopic.Policy.Partitioner)
	require.Equal(t, topic.AuthPolicyACL, restoredTopic.Policy.AuthPolicy)
	require.Equal(t, []string{"reader"}, restoredTopic.Policy.ReadACL)
	require.Equal(t, []string{"writer"}, restoredTopic.Policy.WriteACL)
	require.Equal(t, 48, restoredTopic.Policy.RetentionHours)
	require.Equal(t, int64(8192), restoredTopic.Policy.RetentionBytes)
	require.Equal(t, 1, restoredTopic.ReplicationFactor)
	require.Equal(t, uint64(1), restoredTopic.Revision)
	require.Equal(t, uint64(topic.InitialLifecycleEpoch), restoredTopic.LifecycleEpoch)
}

func TestBrokerFSMPatchCommandsMergeAgainstSerializedAuthoritativeState(t *testing.T) {
	f := newTestFSM()
	registerActiveBroker(t, f, "broker-1")
	defaults := topic.DefaultDefinition("orders", nil)
	partitions := 1
	retentionHours := 24
	readACL := []string{"reader"}
	initialPatch := topic.DefinitionPatch{
		Partitions:     &partitions,
		RetentionHours: &retentionHours,
		ReadACL:        &readACL,
	}
	applyTopicPatch(t, f, 2, defaults, initialPatch)

	retentionBytes := int64(8192)
	applyTopicPatch(t, f, 3, defaults, topic.DefinitionPatch{RetentionBytes: &retentionBytes})
	writeACL := []string{"writer"}
	applyTopicPatch(t, f, 4, defaults, topic.DefinitionPatch{WriteACL: &writeACL})

	definition := f.tm.GetTopic("orders").Definition()
	require.Equal(t, uint64(3), definition.Revision)
	require.Equal(t, topic.DefaultReplicationFactor, definition.ReplicationFactor)
	require.Equal(t, 24, definition.Policy.RetentionHours)
	require.Equal(t, int64(8192), definition.Policy.RetentionBytes)
	require.Equal(t, []string{"reader"}, definition.Policy.ReadACL)
	require.Equal(t, []string{"writer"}, definition.Policy.WriteACL)
	require.Len(t, f.GetPartitionMetadata("orders-0").Replicas, 1)

	applyTopicPatch(t, f, 5, defaults, topic.DefinitionPatch{})
	require.Equal(t, uint64(3), f.tm.GetTopic("orders").Definition().Revision)

	retentionHours = 0
	readACL = []string{}
	applyTopicPatch(t, f, 6, defaults, topic.DefinitionPatch{RetentionHours: &retentionHours, ReadACL: &readACL})
	definition = f.tm.GetTopic("orders").Definition()
	require.Equal(t, uint64(4), definition.Revision)
	require.Zero(t, definition.Policy.RetentionHours)
	require.Empty(t, definition.Policy.ReadACL)
	require.Equal(t, []string{"writer"}, definition.Policy.WriteACL)
}

func applyTopicPatch(t *testing.T, f *BrokerFSM, index uint64, defaults topic.Definition, patch topic.DefinitionPatch) {
	t.Helper()
	payload, err := json.Marshal(TopicCommand{Definition: &defaults, Patch: &patch})
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte("TOPIC:" + string(payload)), Index: index}))
}

func TestBrokerFSMRepeatedCreatePreservesExistingPartitionState(t *testing.T) {
	f := newTestFSM()
	registerActiveBroker(t, f, "broker-1")
	initial := TopicCommand{Name: "orders", Partitions: 1, ReplicationFactor: 1, Policy: topic.DefaultPolicy()}
	data, err := json.Marshal(initial)
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte("TOPIC:" + string(data)), Index: 2}))

	f.mu.Lock()
	f.partitionMetadata["orders-0"].CommittedHWM = 41
	f.partitionMetadata["orders-0"].LeaderEpoch = 7
	f.mu.Unlock()

	updated := initial
	updated.Partitions = 2
	updated.Policy.RetentionHours = 24
	data, err = json.Marshal(updated)
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte("TOPIC:" + string(data)), Index: 3}))

	existing := f.GetPartitionMetadata("orders-0")
	require.Equal(t, uint64(41), existing.CommittedHWM)
	require.Equal(t, 7, existing.LeaderEpoch)
	require.Equal(t, 2, existing.PartitionCount)
	require.NotNil(t, f.GetPartitionMetadata("orders-1"))
	require.Equal(t, 24, f.tm.GetTopic("orders").Policy.RetentionHours)
}

func TestBrokerFSMRestoreMigratesLegacyPartitionMetadata(t *testing.T) {
	state := BrokerFSMState{
		Version: 5,
		PartitionMetadata: map[string]*PartitionMetadata{
			"legacy-topic-0": {PartitionCount: 2, Idempotent: true},
			"legacy-topic-1": {PartitionCount: 2, Idempotent: true},
		},
	}
	data, err := json.Marshal(state)
	require.NoError(t, err)

	restored := newTestFSM()
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(data))))
	restoredTopic := restored.tm.GetTopic("legacy-topic")
	require.NotNil(t, restoredTopic)
	require.Len(t, restoredTopic.Partitions, 2)
	require.True(t, restoredTopic.IsIdempotent)
	require.Equal(t, topic.DefaultPolicy(), restoredTopic.Policy)
}

func TestBrokerFSMRestoreVersionSixInfersDefinitionFields(t *testing.T) {
	state := BrokerFSMState{
		Version: 6,
		TopicState: map[string]*topic.Definition{
			"orders": {Name: "orders", Partitions: 1, Policy: topic.DefaultPolicy()},
		},
		PartitionMetadata: map[string]*PartitionMetadata{
			"orders-0": {PartitionCount: 1, Replicas: []string{"broker-1", "broker-2"}},
		},
	}
	data, err := json.Marshal(state)
	require.NoError(t, err)

	restored := newTestFSM()
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(data))))
	definition := restored.tm.GetTopic("orders").Definition()
	require.Equal(t, uint64(1), definition.Revision)
	require.Equal(t, 2, definition.ReplicationFactor)
}

func TestBrokerFSMRestoreVersionSevenRequiresDefinitionFields(t *testing.T) {
	for _, missing := range []string{"revision", "replication_factor"} {
		t.Run(missing, func(t *testing.T) {
			definition := &topic.Definition{
				Name: "orders", Revision: 1, Partitions: 1, ReplicationFactor: 3, Policy: topic.DefaultPolicy(),
			}
			if missing == "revision" {
				definition.Revision = 0
			} else {
				definition.ReplicationFactor = 0
			}
			state := BrokerFSMState{
				Version:    7,
				TopicState: map[string]*topic.Definition{"orders": definition},
				PartitionMetadata: map[string]*PartitionMetadata{
					"orders-0": {PartitionCount: 1},
				},
			}
			data, err := json.Marshal(state)
			require.NoError(t, err)

			err = newTestFSM().Restore(io.NopCloser(bytes.NewReader(data)))
			require.ErrorContains(t, err, "missing "+missing)
		})
	}
}

func TestBrokerFSMRestoreRejectsVersionSixWithoutTopicState(t *testing.T) {
	state := BrokerFSMState{
		Version: 6,
		PartitionMetadata: map[string]*PartitionMetadata{
			"orders-0": {PartitionCount: 1},
		},
	}
	data, err := json.Marshal(state)
	require.NoError(t, err)

	err = newTestFSM().Restore(io.NopCloser(bytes.NewReader(data)))
	require.ErrorContains(t, err, "missing topic state")
}

func TestBrokerFSMRestoreRejectsMissingPartitionMetadata(t *testing.T) {
	state := BrokerFSMState{
		Version: 6,
		TopicState: map[string]*topic.Definition{
			"orders": {
				Name:       "orders",
				Partitions: 2,
				Policy:     topic.DefaultPolicy(),
			},
		},
		PartitionMetadata: map[string]*PartitionMetadata{
			"orders-0": {PartitionCount: 2},
		},
	}
	data, err := json.Marshal(state)
	require.NoError(t, err)

	err = newTestFSM().Restore(io.NopCloser(bytes.NewReader(data)))
	require.ErrorContains(t, err, "missing partition metadata 1")
}

func TestBrokerFSMRestoreRejectsPartitionWithoutTopicDefinition(t *testing.T) {
	state := BrokerFSMState{
		Version: 6,
		TopicState: map[string]*topic.Definition{
			"orders": {
				Name:       "orders",
				Partitions: 1,
				Policy:     topic.DefaultPolicy(),
			},
		},
		PartitionMetadata: map[string]*PartitionMetadata{
			"orders-0": {PartitionCount: 1},
			"audit-0":  {PartitionCount: 1},
		},
	}
	data, err := json.Marshal(state)
	require.NoError(t, err)

	err = newTestFSM().Restore(io.NopCloser(bytes.NewReader(data)))
	require.ErrorContains(t, err, "has no topic definition")
}

func registerActiveBroker(t *testing.T, f *BrokerFSM, id string) {
	t.Helper()
	payload, err := json.Marshal(BrokerInfo{ID: id, Addr: "127.0.0.1:9000", Status: "active"})
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("REGISTER:%s", payload)), Index: 1}))
}
