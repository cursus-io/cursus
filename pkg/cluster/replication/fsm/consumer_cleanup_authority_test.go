package fsm

import (
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConsumerCleanupAuthorityFencesReplacementWithoutBlockingApply(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	f.topicState[config.ConsumerOffsetsTopicName] = &topic.Definition{Name: config.ConsumerOffsetsTopicName, InstanceID: "metadata", LifecycleEpoch: 1, Partitions: 1}
	p := &PartitionMetadata{PartitionCount: 1, LifecycleEpoch: 1, CommittedHWMKnown: true, CommittedHWM: 17, Leader: "source", Replicas: []string{"source"}, ISR: []string{"source"}}
	f.partitionMetadata[config.ConsumerOffsetsTopicName+"-0"] = p
	f.brokers["source"] = &BrokerInfo{ID: "source", Status: "active", IncarnationID: "process", IncarnationEpoch: 3, RecoveryProtocol: BootstrapRecoveryProtocolVersion}
	expected := []PartitionMetadata{*p}
	processes := []BootstrapProcessIdentity{{BrokerID: "source", IncarnationID: "process", IncarnationEpoch: 3}}
	check := func() error { return f.CheckConsumerCleanupAuthority("metadata", expected, processes) }
	require.NoError(t, check())
	p.CommittedHWM++
	require.NoError(t, check(), "ordinary suffix commits preserve the certified prefix")
	p.CommittedHWM = 16
	require.Error(t, check())
	p.CommittedHWM = 17
	f.mu.Lock()
	require.Error(t, check(), "Apply must not wait for its own state lock")
	f.mu.Unlock()
	f.brokers["source"].IncarnationEpoch++
	require.Error(t, check())
	f.brokers["source"].IncarnationEpoch--
	f.topicState[config.ConsumerOffsetsTopicName].InstanceID = "replacement"
	require.Error(t, check())
	f.topicState[config.ConsumerOffsetsTopicName].InstanceID = "metadata"
	f.recoveryReplayPending = true
	require.Error(t, check(), "restore invalidates detached cleanup authority")
}
