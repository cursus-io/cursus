package disk

import (
	"context"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestRollbackRawScanCancellationWhileMaintenanceHeld(t *testing.T) {
	d, _ := newLineageHandler(t)
	d.maintenanceMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.ScanRetainedRange(ctx, 0, 0, func(types.Message) error { return nil }) }()
	cancel()
	select {
	case err := <-done:
		d.maintenanceMu.Unlock()
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		d.maintenanceMu.Unlock()
		<-done
		t.Fatal("raw rollback rebuild cannot cancel while maintenance is held")
	}
}
