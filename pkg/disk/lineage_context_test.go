package disk

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type contextualLineage interface {
	MatchLineageCandidatesContext(context.Context, []LineagePrefix, uint64) (*LineagePrefix, error)
	CaptureContentGenerationContext(context.Context) (ContentGeneration, error)
	CheckContentGenerationContext(context.Context, ContentGeneration) error
}

func TestLineageContextCancelsQueuedLocks(t *testing.T) {
	d, _ := newLineageHandler(t)
	api, ok := any(d).(contextualLineage)
	if !ok {
		t.Fatal("disk does not expose cancellable lineage and generation authority")
	}
	token, err := d.CaptureContentGeneration()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		lock *sync.Mutex
		call func(context.Context) error
	}{
		{"lineage builder", &d.lineageMu, func(ctx context.Context) error { _, err := api.MatchLineageCandidatesContext(ctx, nil, 0); return err }},
		{"lineage maintenance", &d.maintenanceMu, func(ctx context.Context) error { _, err := api.MatchLineageCandidatesContext(ctx, nil, 0); return err }},
		{"generation capture", &d.mu, func(ctx context.Context) error { _, err := api.CaptureContentGenerationContext(ctx); return err }},
		{"generation check", &d.mu, func(ctx context.Context) error { return api.CheckContentGenerationContext(ctx, token) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.lock.Lock()
			defer tc.lock.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if err := tc.call(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("want deadline, got %v", err)
			}
		})
	}
}

// Counting cancellation deterministically interrupts the initial authoritative
// scan between records, without injecting a disk implementation hook.
type scanCancelContext struct {
	context.Context
	calls, limit int
}

func (c *scanCancelContext) Err() error {
	c.calls++
	if c.calls >= c.limit {
		return context.Canceled
	}
	return nil
}
func TestLineageCanceledInitialScanLeavesRecoverableCache(t *testing.T) {
	d, _ := newLineageHandler(t)
	for i := 0; i < 300; i++ {
		appendLineageRecords(t, d, "record")
	}
	api, ok := any(d).(contextualLineage)
	if !ok {
		t.Fatal("disk does not expose cancellable lineage authority")
	}
	token, err := d.CaptureContentGeneration()
	if err != nil {
		t.Fatal(err)
	}
	ctx := &scanCancelContext{Context: context.Background(), limit: 40}
	_, err = api.MatchLineageCandidatesContext(ctx, nil, 300)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want scan cancellation, got %v", err)
	}
	if d.lineage != nil {
		t.Fatal("canceled scan left trusted partial cache")
	}
	proof := lineageTail(t, d, 300)
	got, err := api.MatchLineageCandidatesContext(context.Background(), []LineagePrefix{proof}, 300)
	if err != nil || got == nil || *got != proof {
		t.Fatalf("recovery: %v %v", got, err)
	}
	if err := d.CheckContentGeneration(token); err != nil {
		t.Fatal("sidecar rebuild changed content generation", err)
	}
}
func TestLineageContextCancelsInternalReadLocks(t *testing.T) {
	for _, name := range []string{"availability", "compaction metadata"} {
		t.Run(name, func(t *testing.T) {
			d, _ := newLineageHandler(t)
			lock := &d.writeFailureMu
			if name == "compaction metadata" {
				lock = &d.compactionMu
			}
			lock.Lock()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := d.MatchLineageCandidatesContext(ctx, nil, 0); result <- err }()
			select {
			case err := <-result:
				lock.Unlock()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("want deadline, got %v", err)
				}
			case <-time.After(200 * time.Millisecond):
				lock.Unlock()
				<-result
				t.Fatal("lineage proof waited for internal read lock after deadline")
			}
		})
	}
}
func TestFirstOffsetContextCancelsQueuedDiskMutex(t *testing.T) {
	d, _ := newLineageHandler(t)
	api, ok := any(d).(interface {
		GetFirstOffsetContext(context.Context) (uint64, error)
	})
	if !ok {
		t.Fatal("retained start must expose cancellable disk-lock acquisition")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := api.GetFirstOffsetContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline, got %v", err)
	}
}
