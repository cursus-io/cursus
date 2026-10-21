package sdk

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdminClientRetriesLostTruncateInstanceReply(t *testing.T) {
	first, firstResult := startAdminCommandDropServer(t)
	second, secondResult := startAdminTestServer(t, "OK topic=orders truncated=true revision=8 lifecycle_epoch=3 leo=0 hwm=0 instance_id=instance-original-1234 replayed=true")
	client, err := NewAdminClient(&AdminConfig{BrokerAddrs: []string{first, second}, MaxRetries: 1, RequestTimeoutMS: 1000})
	require.NoError(t, err)
	result, err := client.TruncateTopic("orders", TruncateTopicOptions{ExpectedRevision: 7, ExpectedInstance: "instance-original-1234"})
	require.NoError(t, err)
	require.True(t, result.Replayed)
	command := "TRUNCATE_INSTANCE topic=orders expected_revision=7 expected_instance=instance-original-1234"
	require.Equal(t, command, receiveAdminTestCommand(t, firstResult))
	require.Equal(t, command, receiveAdminTestCommand(t, secondResult))
}

func TestAdminTruncateInstanceRejectsMismatchedReceipt(t *testing.T) {
	addr, commands := startAdminTestServer(t, "OK topic=orders truncated=true revision=8 lifecycle_epoch=3 leo=0 hwm=0 instance_id=instance-recreated-5678 replayed=true")
	client, err := NewAdminClient(&AdminConfig{BrokerAddrs: []string{addr}})
	require.NoError(t, err)
	_, err = client.TruncateTopic("orders", TruncateTopicOptions{ExpectedRevision: 7, ExpectedInstance: "instance-original-1234"})
	require.ErrorContains(t, err, "does not match")
	_ = receiveAdminTestCommand(t, commands)
}
