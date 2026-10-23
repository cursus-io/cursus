package recovery

import "time"

// Item is metadata only. Generation must change on source incarnation,
// leadership or lifecycle changes, but not merely because committed HWM moves.
type Item struct{ Key, Source, Generation string }
type Entry struct {
	Item
	Due, Since time.Time
	Failures   uint
	Running    bool
	Retired    bool
}

// Queue is owned by one event loop. Workers receive immutable copies and return
// completions to that owner. Neither discovery nor retry creates a goroutine.
type Queue struct {
	capacity  int
	entries   map[string]*Entry
	ready     []string
	cursor    int
	discovery string
	sources   map[string]retry
}
type retry struct {
	due      time.Time
	failures uint
}

func NewQueue(capacity int) *Queue {
	return &Queue{capacity: max(1, min(capacity, 1000000)), entries: make(map[string]*Entry), sources: make(map[string]retry)}
}

// Reconcile returns running keys whose old owners must be canceled. They remain
// charged/running until Complete acknowledges that the owner has actually left.
// Items are in stable inventory order. The discovery cursor advances even when
// a full queue cannot admit more work, so new arrivals cannot reset priority.
func (q *Queue) Reconcile(items []Item, now time.Time) []string {
	live := make(map[string]Item, len(items))
	for _, item := range items {
		live[item.Key] = item
	}
	var cancel []string
	for key, e := range q.entries {
		item, exists := live[key]
		if !exists || e.Item != item {
			// A changed generation must not inherit a dead incarnation's retry
			// delay. Byte debt remains in Budget, keyed by stable broker ID.
			delete(q.sources, e.Source)
			if e.Running {
				e.Retired = true
				cancel = append(cancel, key)
			} else {
				delete(q.entries, key)
			}
		}
	}
	q.compact()
	start := 0
	for i, item := range items {
		if item.Key == q.discovery {
			start = (i + 1) % len(items)
			break
		}
	}
	for n := 0; n < len(items) && len(q.entries) < q.capacity; n++ {
		item := items[(start+n)%len(items)]
		q.discovery = item.Key
		if q.entries[item.Key] == nil {
			q.entries[item.Key] = &Entry{Item: item, Since: now}
			q.ready = append(q.ready, item.Key)
		}
	}
	// Retry state is bounded by admitted sources, never historical identities.
	sources := make(map[string]bool)
	for _, e := range q.entries {
		sources[e.Source] = true
	}
	for source := range q.sources {
		if !sources[source] {
			delete(q.sources, source)
		}
	}
	return cancel
}

func (q *Queue) compact() {
	// Preserve rotation while removing retired entries.
	ordered := make([]string, 0, len(q.entries))
	for n := range q.ready {
		key := q.ready[(q.cursor+n)%len(q.ready)]
		if q.entries[key] != nil {
			ordered = append(ordered, key)
		}
	}
	q.ready, q.cursor = ordered, 0
}

// Next rotates over ready entries. admit must be nonblocking; quota/source slot
// waits skip that entry without preventing another source from running. Bound
// each scan as well as storage: an exhausted quota on 100,000 partitions must
// not turn the scheduler's wakeup into an inventory-sized CPU busy loop.
func (q *Queue) Next(now time.Time, admit func(Item) bool) (Entry, bool) {
	for n := 0; n < min(256, len(q.ready)); n++ {
		key := q.ready[q.cursor]
		q.cursor = (q.cursor + 1) % len(q.ready)
		e := q.entries[key]
		if e == nil || e.Running || e.Retired || now.Before(e.Due) || now.Before(q.sources[e.Source].due) {
			continue
		}
		if !admit(e.Item) {
			continue
		}
		e.Running = true
		return *e, true
	}
	return Entry{}, false
}

func backoff(failures uint) time.Duration {
	return min(30*time.Second, 100*time.Millisecond*time.Duration(1<<min(failures, 9)))
}

// Complete rotates successful continuations immediately. done retires state;
// sourceFailure only applies to transport failures, not partition validation.
func (q *Queue) Complete(item Item, now time.Time, done, failed, sourceFailure bool) {
	e := q.entries[item.Key]
	if e == nil || e.Item != item || !e.Running {
		return
	}
	e.Running = false
	if e.Retired || done {
		delete(q.entries, item.Key)
		// Leave a tombstone until the next inventory reconciliation. Compacting
		// the entire queue for every small completion would make a large fleet's
		// recovery quadratic and monopolize the event loop.
		return
	}
	if failed {
		e.Due = now.Add(backoff(e.Failures))
		e.Failures = min(e.Failures+1, 10)
	} else {
		e.Failures = 0
		e.Due = now
	}
	if sourceFailure {
		r := q.sources[item.Source]
		r.due = now.Add(backoff(r.failures))
		r.failures = min(r.failures+1, 10)
		q.sources[item.Source] = r
	} else if !failed {
		delete(q.sources, item.Source)
	}
}

func (q *Queue) Len() int { return len(q.entries) }

func (q *Queue) OldestSince(now time.Time) time.Time {
	for _, entry := range q.entries {
		if entry.Since.Before(now) {
			now = entry.Since
		}
	}
	return now
}

func (q *Queue) Contains(item Item) bool {
	e := q.entries[item.Key]
	return e != nil && !e.Retired && e.Item == item
}
