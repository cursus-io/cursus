package topic

import (
	"context"
	"fmt"
)

// ConsumerReplayIndexIdentity is a durable local prefix fence. The caller must
// additionally bind topic instance/lifecycle and committed bounds. Read views
// exclude replacement; disk generation changes invalidate prefix rewrites.
func (v *PartitionReadView) ConsumerReplayIndexIdentity(ctx context.Context) (string, error) {
	if err := v.check(); err != nil {
		return "", err
	}
	if v.offline || v.snapshotCut != nil {
		return "", fmt.Errorf("replay index requires ordinary read view")
	}
	storage, ok := v.p.dh.(interface {
		CleanupGenerationContext(context.Context) (string, error)
	})
	if !ok {
		return "", fmt.Errorf("durable replay index generation unavailable")
	}
	return storage.CleanupGenerationContext(ctx)
}

func (v *PartitionReadView) SyncConsumerReplayIndex(ctx context.Context) error {
	if err := v.check(); err != nil {
		return err
	}
	storage, ok := v.p.dh.(interface{ SyncCleanupCheckpointContext(context.Context) error })
	if !ok {
		return fmt.Errorf("durable replay index sync unavailable")
	}
	return storage.SyncCleanupCheckpointContext(ctx)
}
