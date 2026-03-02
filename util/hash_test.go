package util_test

import (
	"testing"

	"github.com/cursus-io/cursus/util"
)

func TestGenerateID(t *testing.T) {
	payload1 := "hello world"
	payload2 := "hello Go"

	id1 := util.GenerateID(payload1)
	id2 := util.GenerateID(payload1)
	id3 := util.GenerateID(payload2)

	if id1 != id2 {
		t.Errorf("Expected same ID for same payload, got %d and %d", id1, id2)
	}

	if id1 == id3 {
		t.Errorf("Expected different IDs for different payloads, got %d", id1)
	}
}

func TestHashDeterministic(t *testing.T) {
	key := "my-key"
	hash1 := util.Hash(key)
	hash2 := util.Hash(key)

	if hash1 != hash2 {
		t.Errorf("Hash should be deterministic, got %v and %v", hash1, hash2)
	}
}

func TestHashDifferentKeys(t *testing.T) {
	key1 := "key-one"
	key2 := "key-two"

	if util.Hash(key1) == util.Hash(key2) {
		t.Errorf("Hash should produce different results for different keys")
	}
}

func TestPartitionIndex(t *testing.T) {
	partitions := 5
	keys := []string{"a", "b", "c", "d", "e"}

	for _, key := range keys {
		index := util.Hash(key) % partitions
		if index < 0 {
			t.Errorf("Partition index out of bounds: %v", index)
		}
		if index >= partitions {
			t.Errorf("Partition index out of bounds: %v", index)
		}
	}
}
