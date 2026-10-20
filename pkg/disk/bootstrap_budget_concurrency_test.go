package disk

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBootstrapHeadroomWaitDoesNotHoldGlobalBudget(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	owner := bootstrapByteOwner{owned: true, limits: DefaultBootstrapLimits()}
	other := bootstrapByteOwner{owned: true, limits: DefaultBootstrapLimits()}
	defer owner.release()
	defer other.release()
	// reserve holds this gate across the real filesystem capacity query. Blocking
	// here exercises the same lock order without replacing capacity accounting.
	d.headroom.mu.Lock()
	released := false
	defer func() {
		if !released {
			d.headroom.mu.Unlock()
		}
	}()
	entered, done := make(chan struct{}), make(chan error, 1)
	go func() { close(entered); done <- owner.grow(d, 1) }()
	<-entered
	// Give the blocked owner time to enter reserve before unrelated admission.
	time.Sleep(20 * time.Millisecond)
	unrelated := make(chan error, 1)
	go func() { unrelated <- other.growWithHeadroom(newDiskHeadroomGuard(t.TempDir(), 1, .1), 1) }()
	select {
	case err := <-unrelated:
		require.NoError(t, err)
	case <-time.After(time.Second):
		d.headroom.mu.Unlock()
		released = true
		<-done
		<-unrelated
		t.Fatal("headroom wait blocked unrelated global byte accounting")
	}
	d.headroom.mu.Unlock()
	released = true
	require.NoError(t, <-done)
}

func TestBootstrapBudgetRejectionKeepsConservativeHeadroomReservation(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, func() error { _, err := d.headroom.reserve(0, true); return err }())
	bootstrapBudget.Lock()
	before := bootstrapBudget.bytes
	bootstrapBudget.Unlock()
	owner := bootstrapByteOwner{owned: true, limits: BootstrapLimits{MaxTransferBytes: 10, MaxTotalBytes: before}}
	d.headroom.mu.Lock()
	reserved := d.headroom.reserved
	d.headroom.mu.Unlock()
	err := owner.grow(d, 1)
	var limit *BootstrapLimitError
	require.ErrorAs(t, err, &limit)
	require.Zero(t, owner.reserved)
	d.headroom.mu.Lock()
	require.Equal(t, reserved+1, d.headroom.reserved)
	d.headroom.mu.Unlock()
	bootstrapBudget.Lock()
	require.Equal(t, before, bootstrapBudget.bytes)
	bootstrapBudget.Unlock()
}
