package sdk

import (
	"testing"
)

func TestNewConsumerClient_BasicConstruction(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	client := NewConsumerClient(cfg)

	if client == nil {
		t.Fatal("expected non-nil ConsumerClient")
	}
	if client.ID == "" {
		t.Error("expected non-empty ID")
	}
	if client.config != cfg {
		t.Error("expected config to be stored")
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

func TestNewConsumerClient_UniqueIDs(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	c1 := NewConsumerClient(cfg)
	c2 := NewConsumerClient(cfg)

	if c1.ID == c2.ID {
		t.Error("expected different IDs for different clients")
	}
}
