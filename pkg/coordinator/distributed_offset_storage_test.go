package coordinator

import (
	"context"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
)

type countingPublisher struct{ publishes int }

func (publisher *countingPublisher) Publish(string, *types.Message) error {
	publisher.publishes++
	return nil
}

func (*countingPublisher) CreateTopic(string, int, bool, bool) error { return nil }

func TestDistributedOffsetsDoNotAppendStandaloneCompatibilityLog(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	publisher := &countingPublisher{}
	coordinator := NewCoordinator(context.Background(), cfg, publisher)
	if err := coordinator.RegisterGroup("events", "workers", 2); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CommitOffset("workers", "events", 0, 7); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CommitOffsetsBulk("workers", "events", []OffsetItem{{Partition: 0, Offset: 8}, {Partition: 1, Offset: 3}}); err != nil {
		t.Fatal(err)
	}
	if publisher.publishes != 0 {
		t.Fatalf("distributed commits appended %d local offset records", publisher.publishes)
	}
	if offset, found := coordinator.GetOffset("workers", "events", 0); !found || offset != 8 {
		t.Fatalf("in-memory Raft-applied offset = %d, found=%v", offset, found)
	}
}
