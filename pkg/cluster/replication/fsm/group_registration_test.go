package fsm

import (
	"context"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
)

func TestBrokerFSMGroupRegistrationIsReplicatedAndIdempotent(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	tm := topic.NewTopicManager(cfg, &MockHandlerProvider{}, nil)
	if err := tm.CreateTopic("events", 2, false, false); err != nil {
		t.Fatal(err)
	}
	cd := coordinator.NewCoordinator(context.Background(), cfg, tm)
	fsm := NewBrokerFSM(tm, cd)
	record := &raft.Log{Data: []byte(`GROUP_SYNC:{"type":"REGISTER","group":"workers","topic":"events","partition_count":2}`)}

	for attempt := 0; attempt < 2; attempt++ {
		if result := fsm.Apply(record); result != nil {
			t.Fatalf("registration attempt %d failed: %v", attempt+1, result)
		}
	}
	status, err := cd.GetGroupStatus("workers")
	if err != nil {
		t.Fatal(err)
	}
	if status.TopicName != "events" || status.PartitionCount != 2 || status.Generation != 0 {
		t.Fatalf("unexpected restored registration: %+v", status)
	}
}

func TestBrokerFSMGroupJoinAtomicallyRegistersGroup(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	tm := topic.NewTopicManager(cfg, &MockHandlerProvider{}, nil)
	if err := tm.CreateTopic("events", 2, false, false); err != nil {
		t.Fatal(err)
	}
	cd := coordinator.NewCoordinator(context.Background(), cfg, tm)
	fsm := NewBrokerFSM(tm, cd)

	result := fsm.Apply(&raft.Log{Data: []byte(
		`GROUP_SYNC:{"type":"JOIN","group":"workers","topic":"events","member":"member-1","partition_count":2}`,
	)})
	if result != nil {
		t.Fatalf("atomic group join failed: %v", result)
	}
	status, err := cd.GetGroupStatus("workers")
	if err != nil {
		t.Fatal(err)
	}
	if status.TopicName != "events" || status.PartitionCount != 2 || status.MemberCount != 1 {
		t.Fatalf("unexpected joined group: %+v", status)
	}
}
