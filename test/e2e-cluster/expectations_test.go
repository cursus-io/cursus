package e2e_cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateTopicMetadataAcceptsCommittedQuorum(t *testing.T) {
	meta := topicMetadata{
		Topic: "orders",
		Partitions: []partitionMetadata{
			{ID: 1, Leader: "localhost:9002", Replicas: []string{"n1", "n2", "n3"}, ISR: []string{"n1", "n2"}, LEO: 5, HWM: 5},
			{ID: 0, Leader: "localhost:9001", Replicas: []string{"n1", "n2", "n3"}, ISR: []string{"n1", "n2", "n3"}, LEO: 5, HWM: 5},
		},
	}

	require.NoError(t, validateTopicMetadata(meta, "orders", 2, 3, 2, 5))
}

func TestValidateTopicMetadataRejectsUncommittedOrUnexpectedRecords(t *testing.T) {
	meta := validTopicMetadata()
	meta.Partitions[0].HWM = 4
	require.ErrorContains(t, validateTopicMetadata(meta, "orders", 1, 3, 2, 5), "leo=5 hwm=4")

	meta = validTopicMetadata()
	meta.Partitions[0].LEO = 6
	require.ErrorContains(t, validateTopicMetadata(meta, "orders", 1, 3, 2, 5), "leo=6 hwm=5")
}

func TestValidateTopicMetadataRejectsUnsafeISR(t *testing.T) {
	meta := validTopicMetadata()
	meta.Partitions[0].ISR = []string{"n1"}
	require.ErrorContains(t, validateTopicMetadata(meta, "orders", 1, 3, 2, 5), "at least 2")

	meta = validTopicMetadata()
	meta.Partitions[0].ISR = []string{"n1", "unknown"}
	require.ErrorContains(t, validateTopicMetadata(meta, "orders", 1, 3, 2, 5), "is not assigned")
}

func TestValidateTopicMetadataRejectsMalformedTopology(t *testing.T) {
	meta := validTopicMetadata()
	meta.Partitions[0].ID = 2
	require.ErrorContains(t, validateTopicMetadata(meta, "orders", 1, 3, 2, 5), "contiguous ids")

	meta = validTopicMetadata()
	meta.Partitions[0].Replicas = []string{"n1", "n1", "n3"}
	require.ErrorContains(t, validateTopicMetadata(meta, "orders", 1, 3, 2, 5), "duplicate replica")

	require.ErrorContains(t, validateTopicMetadata(validTopicMetadata(), "orders", 1, 1, 2, 5), "invalid expectation")
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
