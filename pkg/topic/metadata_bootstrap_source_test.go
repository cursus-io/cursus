package topic

import (
	"context"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/stretchr/testify/require"
)

func TestMetadataBootstrapSourceRequiresReadyAuthority(t *testing.T) {
	for _, state := range []string{"unknown zero", "closed", "bootstrap pending", "snapshot pending", "known zero"} {
		t.Run(state, func(t *testing.T) {
			p := &Partition{distributed: true, hwmAuthoritative: true}
			switch state {
			case "unknown zero":
				p.hwmAuthoritative = false
			case "closed":
				p.closed = true
			case "bootstrap pending":
				p.bootstrapPending.Store(true)
			case "snapshot pending":
				p.snapshotRecovery = true
			}
			hwm, err := p.MetadataBootstrapSourceHWMContext(context.Background())
			if state == "known zero" {
				require.NoError(t, err)
				require.Zero(t, hwm)
			} else {
				require.Error(t, err)
			}
		})
	}
	p := &Partition{}
	p.mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err := p.MetadataBootstrapSourceHWMContext(ctx)
	cancel()
	p.mu.Unlock()
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestMetadataBootstrapSourceOptionalStorage(t *testing.T) {
	p := &Partition{}
	export, err := p.ExportMetadataBootstrapContext(context.Background(), disk.ContentGeneration{}, 0, 0, 0, disk.BootstrapFence{})
	require.ErrorIs(t, err, ErrReplicaSourceUnsupported)
	require.Nil(t, export)
}
