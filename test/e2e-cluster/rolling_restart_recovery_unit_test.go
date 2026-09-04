package e2e_cluster

import (
	"strings"
	"testing"
)

func TestBrokerEvictedFromISRRequiresTargetAbsentWithRemainingReplicas(t *testing.T) {
	metadata := topicMetadata{Partitions: []partitionMetadata{{
		ID:       0,
		Replicas: []string{"broker-1-9000", "broker-2-9000", "broker-3-9000"},
		ISR:      []string{"broker-1-9000", "broker-2-9000"},
	}}}

	ready, detail := brokerEvictedFromISR(metadata, "broker-3-9000", 3)
	if !ready {
		t.Fatalf("expected broker eviction to be ready: %s", detail)
	}
}

func TestBrokerEvictedFromISRRejectsTargetStillPresent(t *testing.T) {
	metadata := topicMetadata{Partitions: []partitionMetadata{{
		ID:       0,
		Replicas: []string{"broker-1-9000", "broker-2-9000", "broker-3-9000"},
		ISR:      []string{"broker-1-9000", "broker-2-9000", "broker-3-9000"},
	}}}

	ready, detail := brokerEvictedFromISR(metadata, "broker-3-9000", 3)
	if ready || !strings.Contains(detail, "still present") {
		t.Fatalf("expected target-present detail, got ready=%v detail=%q", ready, detail)
	}
}

func TestBrokerEvictedFromISRRejectsAdditionalReplicaLoss(t *testing.T) {
	metadata := topicMetadata{Partitions: []partitionMetadata{{
		ID:       0,
		Replicas: []string{"broker-1-9000", "broker-2-9000", "broker-3-9000"},
		ISR:      []string{"broker-1-9000"},
	}}}

	ready, detail := brokerEvictedFromISR(metadata, "broker-3-9000", 3)
	if ready || !strings.Contains(detail, "want=2") {
		t.Fatalf("expected remaining-ISR detail, got ready=%v detail=%q", ready, detail)
	}
}
