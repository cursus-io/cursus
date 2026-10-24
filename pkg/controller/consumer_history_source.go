package controller

import (
	"context"
	"fmt"

	"github.com/cursus-io/cursus/pkg/types"
)

// History proofs can use freshly certified adopted raw generations. Recovery
// indexes still require durable LOCAL prefix identities: never pass this adapter
// to startup replay or persist its identity in the replay index.
type stagedConsumerHistory interface {
	ConsumerHistoryIdentity(int) (string, error)
	ReadRawConsumerHistory(int, uint64, int) ([]types.Message, error)
}

type consumerHistorySource struct{ *consumerRecoveryReader }

func (r consumerHistorySource) ConsumerMetadataIndexIdentity(p int) (string, error) {
	if p < 0 || p >= len(r.views) {
		return "", ErrConsumerRecoveryPending
	}
	if r.views[p] != nil {
		return r.consumerRecoveryReader.ConsumerMetadataIndexIdentity(p)
	}
	stage, ok := r.staged.(stagedConsumerHistory)
	if !ok {
		return "", fmt.Errorf("staged raw history proof unavailable")
	}
	identity, err := stage.ConsumerHistoryIdentity(p)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("history:%s:%d:%d:%s", r.indexTopicInstance, r.metadata[p].LifecycleEpoch, p, identity), nil
}

func (r consumerHistorySource) ReadRawConsumerMetadataIndex(p int, next uint64, max int) ([]types.Message, error) {
	if p < 0 || p >= len(r.views) {
		return nil, ErrConsumerRecoveryPending
	}
	if r.views[p] != nil {
		return r.consumerRecoveryReader.ReadRawConsumerMetadataIndex(p, next, max)
	}
	stage, ok := r.staged.(stagedConsumerHistory)
	if !ok {
		return nil, ErrConsumerRecoveryPending
	}
	return stage.ReadRawConsumerHistory(p, next, max)
}

func (r consumerHistorySource) SyncConsumerMetadataIndex() error {
	for p, v := range r.views {
		if v != nil {
			if err := v.SyncConsumerReplayIndex(r.ctx); err != nil {
				return err
			}
		} else if _, err := r.ConsumerMetadataIndexIdentity(p); err != nil {
			return err
		}
	}
	// Adoption already fsynced the raw/index files and publication directory.
	return r.ctx.Err()
}

// Refresh outside every local read view/admission/FSM fence. A source prefix
// replacement forces a full transfer and a new adopted generation, invalidating
// the proof even when its HWM and broker incarnation are unchanged.
func (ch *CommandHandler) refreshConsumerHistorySources(ctx context.Context, plan *consumerRecoveryPlan) error {
	if len(plan.missing) == 0 {
		return nil
	}
	ch.consumerRecoveryProviderMu.RLock()
	provider := ch.consumerRecoveryProvider
	ch.consumerRecoveryProviderMu.RUnlock()
	if provider == nil {
		return ErrConsumerRecoveryPending
	}
	fresh, err := provider(ctx, ConsumerMetadataRecoveryStageRequest{RequiredGeneration: plan.generation, Metadata: cloneConsumerRecoveryMetadata(plan.metadata), Partitions: append([]int(nil), plan.missing...)})
	if fresh != nil {
		defer func() { _ = fresh.Close() }()
	}
	if err != nil {
		return err
	}
	before, ok := plan.staged.(stagedConsumerHistory)
	after, freshOK := fresh.(stagedConsumerHistory)
	if !ok || !freshOK {
		return ErrConsumerRecoveryPending
	}
	for _, p := range plan.missing {
		old, err := before.ConsumerHistoryIdentity(p)
		if err != nil {
			return err
		}
		current, err := after.ConsumerHistoryIdentity(p)
		if err != nil || old != current || fresh.Sources()[p] != plan.sources[p] {
			return fmt.Errorf("consumer history staged source replaced")
		}
	}
	return nil
}
