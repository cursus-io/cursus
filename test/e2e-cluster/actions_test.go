package e2e_cluster

import (
	"errors"
	"reflect"
	"testing"
)

func TestLeaderNodeFromDescribeUsesLeaderField(t *testing.T) {
	resp := `{
		"partitions": [{
			"leader": "broker-2:9001",
			"replicas": ["broker-1-9000", "broker-2-9000", "broker-3-9000"]
		}]
	}`

	got, err := leaderNodeFromDescribe(resp, 3)
	if err != nil {
		t.Fatalf("leaderNodeFromDescribe() error = %v", err)
	}
	if got != 2 {
		t.Fatalf("leaderNodeFromDescribe() = %d, want 2", got)
	}
}

func TestLeaderNodeFromDescribeRejectsUnknownLeader(t *testing.T) {
	resp := `{"partitions":[{"leader":"broker-4:9001"}]}`

	if _, err := leaderNodeFromDescribe(resp, 3); err == nil {
		t.Fatal("leaderNodeFromDescribe() error = nil, want out-of-range error")
	}
}

func TestValidateNodeHealthURLRestrictsLoopbackEndpoint(t *testing.T) {
	if err := validateNodeHealthURL(2, "http://localhost:9082/ready"); err != nil {
		t.Fatalf("expected cluster health URL to be accepted: %v", err)
	}
	for _, candidate := range []string{
		"http://example.com:9082/ready",
		"http://localhost:9081/ready",
		"http://localhost:9082/other",
	} {
		if err := validateNodeHealthURL(2, candidate); err == nil {
			t.Fatalf("validateNodeHealthURL(%q) error = nil, want rejection", candidate)
		}
	}
}

func TestTopicMetadataConvergedRequiresEveryBroker(t *testing.T) {
	addrs := []string{"broker-1", "broker-2", "broker-3"}
	responses := map[string]string{
		"broker-1": "OK topic=orders partitions=1",
		"broker-2": "ERROR: topic_not_found topic=orders",
		"broker-3": "OK topic=orders partitions=1",
	}
	var probed []string

	ready, detail, err := topicMetadataConverged(addrs, "orders", func(addr string) (string, error) {
		probed = append(probed, addr)
		return responses[addr], nil
	})
	if err != nil {
		t.Fatalf("topicMetadataConverged() error = %v", err)
	}
	if ready {
		t.Fatal("topicMetadataConverged() ready = true while one broker is missing the topic")
	}
	if detail != "broker-2: ERROR: topic_not_found topic=orders" {
		t.Fatalf("topicMetadataConverged() detail = %q", detail)
	}
	if want := []string{"broker-1", "broker-2"}; !reflect.DeepEqual(probed, want) {
		t.Fatalf("topicMetadataConverged() probed = %v, want %v", probed, want)
	}

	responses["broker-2"] = "OK topic=orders partitions=1"
	ready, detail, err = topicMetadataConverged(addrs, "orders", func(addr string) (string, error) {
		return responses[addr], nil
	})
	if err != nil || !ready {
		t.Fatalf("topicMetadataConverged() = (%v, %q, %v), want ready", ready, detail, err)
	}
}

func TestTopicMetadataConvergedReportsProbeFailure(t *testing.T) {
	ready, detail, err := topicMetadataConverged([]string{"broker-1"}, "orders", func(string) (string, error) {
		return "", errors.New("dial failed")
	})
	if err != nil || ready || detail != "broker-1: dial failed" {
		t.Fatalf("topicMetadataConverged() = (%v, %q, %v)", ready, detail, err)
	}
}

func TestMetadataResponseMatchesExactTopic(t *testing.T) {
	if !metadataResponseMatchesTopic("OK topic=orders partitions=1", "orders") {
		t.Fatal("expected matching metadata response")
	}
	for _, response := range []string{
		"ERROR: topic_not_found topic=orders",
		"OK topic=orders-archive partitions=1",
		"OK partitions=1",
	} {
		if metadataResponseMatchesTopic(response, "orders") {
			t.Fatalf("metadataResponseMatchesTopic(%q) = true", response)
		}
	}
}
