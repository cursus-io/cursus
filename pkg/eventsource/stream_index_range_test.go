package eventsource

import "testing"

func TestStreamIndexLookupRangeCopiesOnlyRequestedPage(t *testing.T) {
	index, err := NewStreamIndex(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	for version := uint64(1); version <= 5; version++ {
		if err := index.Append("order-1", version, version*10, version); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := index.LookupRange("order-1", 3, 5, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].AggregateVersion != 3 || entries[1].AggregateVersion != 4 {
		t.Fatalf("unexpected range: %#v", entries)
	}
}
