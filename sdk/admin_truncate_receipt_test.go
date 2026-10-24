package sdk

import (
	"fmt"
	"testing"
	"time"

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

func TestAdminTruncateInstanceRejectsInvalidSuccessReceipt(t *testing.T) {
	for _, response := range []string{
		"OK topic=another-topic truncated=true revision=8 lifecycle_epoch=3 leo=0 hwm=0",
		"OK topic=orders truncated=false revision=8 lifecycle_epoch=3 leo=0 hwm=0",
		"OK topic=orders truncated=true revision=8 lifecycle_epoch=0 leo=0 hwm=0",
		"OK topic=orders truncated=true revision=8 lifecycle_epoch=1 leo=0 hwm=0",
		"OK topic=orders truncated=true revision=8 lifecycle_epoch=3 leo=1 hwm=0",
		"OK topic=orders truncated=true revision=8 lifecycle_epoch=3 leo=0 hwm=1",
		"OK topic=orders truncated=true revision=9 lifecycle_epoch=3 leo=0 hwm=0",
	} {
		t.Run(response, func(t *testing.T) {
			addr, commands := startAdminTestServer(t, response+" instance_id=instance-original-1234 replayed=true")
			client, err := NewAdminClient(&AdminConfig{BrokerAddrs: []string{addr}, RequestTimeoutMS: 1000})
			require.NoError(t, err)
			_, err = client.TruncateTopic("orders", TruncateTopicOptions{ExpectedRevision: 7, ExpectedInstance: "instance-original-1234"})
			require.ErrorContains(t, err, "truncate receipt does not match")
			_ = receiveAdminTestCommand(t, commands)
		})
	}
}

func TestAdminTruncateInstanceLostReplyRetryBound(t *testing.T) {
	for _, retries := range []int{0, 2} {
		t.Run(fmt.Sprintf("retries_%d", retries), func(t *testing.T) {
			var addresses []string
			var commands []<-chan adminTestResult
			for i := 0; i <= retries; i++ {
				address, received := startAdminCommandDropServer(t)
				addresses = append(addresses, address)
				commands = append(commands, received)
			}
			unused, unexpected := startAdminTestServer(t, "OK topic=orders truncated=true revision=8 lifecycle_epoch=3 leo=0 hwm=0 instance_id=instance-original-1234 replayed=true")
			addresses = append(addresses, unused)
			client, err := NewAdminClient(&AdminConfig{BrokerAddrs: addresses, MaxRetries: retries, RequestTimeoutMS: 1000})
			require.NoError(t, err)
			_, err = client.TruncateTopic("orders", TruncateTopicOptions{ExpectedRevision: 7, ExpectedInstance: "instance-original-1234"})
			require.Error(t, err, "exhausted ambiguous replies must not manufacture success")
			for _, received := range commands {
				require.Equal(t, "TRUNCATE_INSTANCE topic=orders expected_revision=7 expected_instance=instance-original-1234", receiveAdminTestCommand(t, received))
			}
			select {
			case result := <-unexpected:
				t.Fatalf("request exceeded MaxRetries+1 attempts: %+v", result)
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}

func TestAdminTruncateInstanceRejectsMismatchedReceipt(t *testing.T) {
	addr, commands := startAdminTestServer(t, "OK topic=orders truncated=true revision=8 lifecycle_epoch=3 leo=0 hwm=0 instance_id=instance-recreated-5678 replayed=true")
	client, err := NewAdminClient(&AdminConfig{BrokerAddrs: []string{addr}})
	require.NoError(t, err)
	_, err = client.TruncateTopic("orders", TruncateTopicOptions{ExpectedRevision: 7, ExpectedInstance: "instance-original-1234"})
	require.ErrorContains(t, err, "does not match")
	_ = receiveAdminTestCommand(t, commands)
}
