package recovery

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestEstimatedFirstUnitChargesFullDebt(t *testing.T) {
	b, clock := fixture(t)
	l, _, err := b.TryAcquireEstimated(context.Background(), "a", 20)
	if err != nil {
		t.Fatal(err)
	}
	finish(t, l, 200)
	if _, delay, err := b.TryAcquireEstimated(context.Background(), "a", 20); !errors.Is(err, ErrBusy) || delay != 3400*time.Millisecond {
		t.Fatalf("delay=%v err=%v", delay, err)
	}
	clock.advance(3400 * time.Millisecond)
	l, _, err = b.TryAcquireEstimated(context.Background(), "a", 20)
	if err != nil {
		t.Fatal(err)
	}
	finish(t, l, 20)
}

func TestWorkReservationRetainsSlotUntilOwnerReleases(t *testing.T) {
	b, _ := fixture(t)
	release, err := b.AcquireWork(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	if b.Usage().Active != 1 {
		t.Fatal("work did not reserve broker capacity")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.AcquireWork(ctx, "b"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if b.Usage().Active != 1 || b.Usage().Waiters != 0 {
		t.Fatal("canceled waiter leaked or released owner")
	}
	// Transfer can use the broker's other slot while atomic work is active.
	finish(t, acquire(t, b, "b", 10), 10)
	release()
	release()
	if b.Usage().Active != 0 {
		t.Fatal("work slot leaked")
	}
}

func TestAtomicWorkReservesNextSourceSlotAgainstReadyTransfers(t *testing.T) {
	b, clock := fixture(t)
	transfer := acquire(t, b, "a", 10)
	waiting, wake := make(chan struct{}), make(chan struct{})
	clock.wait = func(ctx context.Context, _ time.Duration) error {
		close(waiting)
		select {
		case <-wake:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan func(), 1)
	go func() {
		release, err := b.AcquireWork(ctx, "a")
		if err != nil {
			result <- nil
			return
		}
		result <- release
	}()
	<-waiting
	finish(t, transfer, 10)
	if l, _, err := b.TryAcquire(ctx, "a", 1); !errors.Is(err, ErrBusy) {
		if l != nil {
			_ = l.Finish(0)
		}
		t.Fatalf("ready transfer stole atomic work's source slot: %v", err)
	}
	// An unrelated source can still make progress while the waiter is asleep.
	finish(t, acquire(t, b, "b", 10), 10)
	close(wake)
	release := <-result
	if release == nil {
		t.Fatal("atomic work failed admission")
	}
	release()
	if got := b.Usage(); got.Active != 0 || got.Waiters != 0 {
		t.Fatalf("atomic work leaked: %+v", got)
	}
}
