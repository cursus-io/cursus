package recovery

import (
	"fmt"
	"testing"
	"time"
)

func TestQueueContinuousArrivalsAndBoundedAdmissionScan(t *testing.T) {
	q := NewQueue(4096)
	now := time.Unix(1, 0)
	items := make([]Item, 1024)
	for i := range items {
		items[i] = Item{fmt.Sprint(i), "busy", "1"}
	}
	items[len(items)-1].Source = "ready"
	seen := false
	for round := 0; round < 8; round++ {
		items = append(items, Item{fmt.Sprintf("arrival-%d", round), "busy", "1"})
		q.Reconcile(items, now)
		probes := 0
		e, ok := q.Next(now, func(i Item) bool { probes++; return i.Source == "ready" })
		if probes > 256 {
			t.Fatalf("unbounded wakeup work: %d", probes)
		}
		if ok {
			seen = true
			q.Complete(e.Item, now, false, false, false)
			break
		}
	}
	if !seen {
		t.Fatal("new arrivals reset the cursor and starved ready work")
	}
}

func TestQueueRotationNewArrivalsAndIndependentBackoff(t *testing.T) {
	q := NewQueue(8)
	now := time.Unix(1, 0)
	items := []Item{{"a", "slow", "1"}, {"b", "fast", "1"}, {"c", "fast", "1"}}
	q.Reconcile(items, now)
	admit := func(Item) bool { return true }
	a, _ := q.Next(now, admit)
	q.Complete(a.Item, now, false, true, true)
	for n := 0; n < 100; n++ {
		// Reconciliation must not restart at the beginning of the inventory.
		q.Reconcile(items, now)
		e, ok := q.Next(now, admit)
		if !ok || e.Key != []string{"b", "c"}[n%2] {
			t.Fatalf("unfair selection: %+v %v", e, ok)
		}
		q.Complete(e.Item, now, false, false, false)
	}
	items = append(items, Item{"d", "fast", "1"})
	q.Reconcile(items, now)
	seen := map[string]bool{}
	for n := 0; n < 3; n++ {
		e, _ := q.Next(now, admit)
		seen[e.Key] = true
		q.Complete(e.Item, now, false, false, false)
	}
	if len(seen) != 3 || seen["a"] {
		t.Fatalf("arrival disrupted fairness: %v", seen)
	}
	now = now.Add(100 * time.Millisecond)
	seen = map[string]bool{}
	for n := 0; n < 4; n++ {
		e, _ := q.Next(now, admit)
		seen[e.Key] = true
		q.Complete(e.Item, now, false, false, false)
	}
	if !seen["a"] {
		t.Fatal("backoff did not expire")
	}
}

func TestQueueGenerationChangeJoinsBeforeReplacement(t *testing.T) {
	q := NewQueue(1)
	now := time.Unix(1, 0)
	old := Item{"a", "old-source", "1"}
	fresh := Item{"a", "new-source", "2"}
	q.Reconcile([]Item{old}, now)
	e, _ := q.Next(now, func(Item) bool { return true })
	canceled := q.Reconcile([]Item{fresh}, now)
	if len(canceled) != 1 || canceled[0] != "a" {
		t.Fatalf("missing cancellation: %v", canceled)
	}
	if _, ok := q.Next(now, func(Item) bool { return true }); ok {
		t.Fatal("replacement raced old owner")
	}
	q.Complete(e.Item, now, false, false, false)
	q.Reconcile([]Item{fresh}, now)
	e, ok := q.Next(now, func(Item) bool { return true })
	if !ok || e.Item != fresh {
		t.Fatalf("replacement missing: %+v", e)
	}
}

func TestQueueRemovalPreservesSharedSourceBackoff(t *testing.T) {
	q := NewQueue(3)
	now := time.Unix(1, 0)
	items := []Item{{"a", "shared", "1"}, {"b", "shared", "1"}, {"c", "other", "1"}}
	admit := func(Item) bool { return true }
	q.Reconcile(items, now)
	a, _ := q.Next(now, admit)
	q.Complete(a.Item, now, false, true, true)
	q.Reconcile(items[1:], now)
	c, ok := q.Next(now, admit)
	if !ok || c.Key != "c" {
		t.Fatal("removing a partition erased its peer's source backoff", c)
	}
	q.Complete(c.Item, now, true, false, false)
	if _, ok := q.Next(now, admit); ok {
		t.Fatal("remaining source ran before its retry deadline")
	}
	items[1].Generation = "2"
	q.Reconcile(items[1:2], now)
	b, ok := q.Next(now, admit)
	if !ok || b.Key != "b" {
		t.Fatal("replacement generation inherited obsolete source backoff", b)
	}
	q.Complete(b.Item, now, true, false, false)
	q.Reconcile(nil, now)
	if len(q.sources) != 0 {
		t.Fatal("removed sources retained retry state")
	}
}

func TestQueueCapacityDiscoveryAndNonblockingAdmission(t *testing.T) {
	q := NewQueue(2)
	now := time.Unix(1, 0)
	items := []Item{{"a", "blocked", "1"}, {"b", "b", "1"}, {"c", "c", "1"}}
	q.Reconcile(items, now)
	if q.Len() != 2 {
		t.Fatal("capacity")
	}
	e, ok := q.Next(now, func(i Item) bool { return i.Source != "blocked" })
	if !ok || e.Key != "b" {
		t.Fatal("blocked source prevented unrelated dispatch")
	}
	q.Complete(e.Item, now, true, false, false)
	q.Reconcile(items, now)
	e, ok = q.Next(now, func(i Item) bool { return i.Source != "blocked" })
	if !ok || e.Key != "c" {
		t.Fatal("discovery cursor restarted")
	}
}
