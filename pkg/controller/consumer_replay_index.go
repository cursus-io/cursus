package controller

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
)

func (r *consumerRecoveryReader) ConsumerMetadataIndexIdentity(p int) (string, error) {
	if p < 0 || p >= len(r.views) || p >= len(r.metadata) || r.views[p] == nil {
		return "", fmt.Errorf("local replay index identity unavailable")
	}
	token, err := r.views[p].ConsumerReplayIndexIdentity(r.ctx)
	if err != nil {
		return "", err
	}
	if token == "" {
		return "", fmt.Errorf("local replay index durable generation unavailable")
	}
	// Auto-created internal topics predate InstanceID. Their durable disk
	// generation still identifies the physical incarnation and changes on
	// destructive replacement; never substitute a name-only cache identity.
	return fmt.Sprintf("%s:%s:%d:%d:%s", config.ConsumerOffsetsTopicName, r.indexTopicInstance, r.metadata[p].LifecycleEpoch, p, token), nil
}

func (r *consumerRecoveryReader) ReadRawConsumerMetadataIndex(p int, offset uint64, max int) ([]types.Message, error) {
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	if p < 0 || p >= len(r.views) || r.views[p] == nil {
		return nil, fmt.Errorf("local replay index view unavailable")
	}
	messages, err := r.views[p].ReadMessages(offset, max)
	for i, m := range messages {
		if m.Offset >= r.ends[p] {
			return messages[:i], err
		}
	}
	return messages, err
}

func (r *consumerRecoveryReader) SyncConsumerMetadataIndex() error {
	for _, v := range r.views {
		if v == nil {
			return fmt.Errorf("local replay index view unavailable")
		}
		if err := v.SyncConsumerReplayIndex(r.ctx); err != nil {
			return err
		}
	}
	return nil
}

func (r *consumerRecoveryReader) ConsumerMetadataIndexTransactionVisible(p int, m types.Message) (bool, error) {
	if err := r.ctx.Err(); err != nil {
		return false, err
	}
	if p < 0 || p >= len(r.views) || r.views[p] == nil {
		return false, fmt.Errorf("local replay index view unavailable")
	}
	return r.views[p].ConsumerReplayIndexTransactionVisible(m, r.ends[p])
}
