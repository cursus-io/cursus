package e2e_cluster

import (
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

func TestValidateTopicMetadataAcceptsCommittedQuorum(t *testing.T) {
	meta := topicMetadata{
		Topic: "orders",
		Partitions: []partitionMetadata{
			{ID: 1, Leader: "localhost:9002", Replicas: []string{"n1", "n2", "n3"}, ISR: []string{"n1", "n2"}, LEO: 5, HWM: 5},
			{ID: 0, Leader: "localhost:9001", Replicas: []string{"n1", "n2", "n3"}, ISR: []string{"n1", "n2"}, LEO: 5, HWM: 5},
		},
	}

	require.NoError(t, validateTopicMetadata(meta, "orders", 2, 3, 2, 5))
	require.NoError(t, validateISRContract(3, 2, 2, false))
}

func TestValidateTopicMetadataRejectsUncommittedOrUnexpectedRecords(t *testing.T) {
	meta := validTopicMetadata()
	meta.Partitions[0].HWM = 4
	require.ErrorContains(t, validateTopicMetadata(meta, "orders", 1, 3, 3, 5), "leo=5 hwm=4")

	meta = validTopicMetadata()
	meta.Partitions[0].LEO = 6
	require.ErrorContains(t, validateTopicMetadata(meta, "orders", 1, 3, 3, 5), "leo=6 hwm=5")
}

func TestValidateTopicMetadataRejectsUnsafeISR(t *testing.T) {
	meta := validTopicMetadata()
	meta.Partitions[0].ISR = []string{"n1"}
	require.ErrorContains(t, validateTopicMetadata(meta, "orders", 1, 3, 2, 5), "exactly 2")
	require.ErrorContains(t, validateISRContract(3, 1, 2, false), "unsafe replication expectation")
	require.NoError(t, validateISRContract(3, 1, 2, true))

	meta = validTopicMetadata()
	meta.Partitions[0].ISR = []string{"n1", "n2", "unknown"}
	require.ErrorContains(t, validateTopicMetadata(meta, "orders", 1, 3, 3, 5), "is not assigned")
}

func TestValidateTopicMetadataRejectsMalformedTopology(t *testing.T) {
	meta := validTopicMetadata()
	meta.Partitions[0].ID = 2
	require.ErrorContains(t, validateTopicMetadata(meta, "orders", 1, 3, 3, 5), "contiguous ids")

	meta = validTopicMetadata()
	meta.Partitions[0].Replicas = []string{"n1", "n1", "n3"}
	require.ErrorContains(t, validateTopicMetadata(meta, "orders", 1, 3, 3, 5), "duplicate replica")

	require.ErrorContains(t, validateISRContract(1, 2, 2, false), "invalid replication expectation")
}

func TestValidateISRContractRejectsWrongSideOfMinISR(t *testing.T) {
	require.ErrorContains(t, validateISRContract(3, 2, 2, true), "invalid below-minISR expectation")
	require.ErrorContains(t, validateISRContract(3, 1, 2, false), "unsafe replication expectation")
}

func TestPublishRejectedBelowMinISRRequiresQuorumFailure(t *testing.T) {
	ctx := e2e.Given(t)
	ctx.SetLastError(wire.NewBrokerError(wire.ErrorPayload{
		Code:      "insufficient_in_sync_replicas",
		Class:     wire.ErrorClassAvailability,
		Retryable: true,
		Message:   "insufficient in-sync replicas",
		Fields:    map[string]string{"current": "2", "required": "3"},
	}))
	require.NoError(t, PublishRejectedBelowMinISR(3)(ctx))

	ctx.SetLastError(wire.NewBrokerError(wire.ErrorPayload{
		Code: "insufficient_in_sync_replicas", Class: wire.ErrorClassAvailability,
		Fields: map[string]string{"current": "2", "required": "2"},
	}))
	require.ErrorContains(t, PublishRejectedBelowMinISR(3)(ctx), "required=\"2\", want 3")

	ctx.SetLastError(errors.New("connection reset"))
	require.ErrorContains(t, PublishRejectedBelowMinISR(3)(ctx), "want insufficient_in_sync_replicas required=3")

	ctx.SetLastError(errors.New("ERROR: insufficient_in_sync_replicas current=2 required=3"))
	require.NoError(t, PublishRejectedBelowMinISR(3)(ctx))
}

func validTopicMetadata() topicMetadata {
	return topicMetadata{
		Topic: "orders",
		Partitions: []partitionMetadata{{
			ID: 0, Leader: "localhost:9001",
			Replicas: []string{"n1", "n2", "n3"}, ISR: []string{"n1", "n2", "n3"},
			LEO: 5, HWM: 5,
		}},
	}
}
