package sdk

import (
	"sync"
	"testing"
)

func TestNewProducerClient_BasicConstruction(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	client := NewProducerClient(cfg)

	if client == nil {
		t.Fatal("expected non-nil ProducerClient")
	}
	if client.ID == "" {
		t.Error("expected non-empty ID")
	}
	if client.config != cfg {
		t.Error("expected config to be stored")
	}
	if client.Epoch == 0 {
		t.Error("expected non-zero Epoch")
	}

	// Leader should be initialized with empty addr
	info := client.leader.Load()
	if info == nil {
		t.Fatal("expected non-nil leader info")
	}
	if info.addr != "" {
		t.Errorf("expected empty leader addr, got %q", info.addr)
	}
}

func TestNewProducerClient_UniqueIDs(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	p1 := NewProducerClient(cfg)
	p2 := NewProducerClient(cfg)

	if p1.ID == p2.ID {
		t.Error("expected different IDs for different clients")
	}
}

func TestNextSeqNum_GlobalIncrement(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.EnableIdempotence = false
	client := NewProducerClient(cfg)

	s1 := client.NextSeqNum(0)
	s2 := client.NextSeqNum(0)
	s3 := client.NextSeqNum(1) // different partition, same global counter

	if s1 != 1 {
		t.Errorf("expected first seq=1, got %d", s1)
	}
	if s2 != 2 {
		t.Errorf("expected second seq=2, got %d", s2)
	}
	if s3 != 3 {
		t.Errorf("expected third seq=3, got %d", s3)
	}
}

func TestNextSeqNum_PerPartitionWithIdempotence(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.EnableIdempotence = true
	client := NewProducerClient(cfg)

	// Partition 0
	s1 := client.NextSeqNum(0)
	s2 := client.NextSeqNum(0)
	// Partition 1
	s3 := client.NextSeqNum(1)

	if s1 != 1 {
		t.Errorf("expected partition 0 first seq=1, got %d", s1)
	}
	if s2 != 2 {
		t.Errorf("expected partition 0 second seq=2, got %d", s2)
	}
	if s3 != 1 {
		t.Errorf("expected partition 1 first seq=1, got %d", s3)
	}
}

func TestNextSeqNum_ConcurrentSafety(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	cfg.EnableIdempotence = false
	client := NewProducerClient(cfg)

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			client.NextSeqNum(0)
		}()
	}
	wg.Wait()

	// After 100 increments, next should be 101
	next := client.NextSeqNum(0)
	if next != uint64(goroutines+1) {
		t.Errorf("expected seq=%d after %d concurrent calls, got %d", goroutines+1, goroutines, next)
	}
}
