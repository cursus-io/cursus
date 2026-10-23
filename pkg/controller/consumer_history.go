package controller

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

type consumerHistoryCursor struct {
	partition int
	first     uint64
	valid     bool
}

// compactConsumerHistory performs at most one atomic segment replacement. The
// full certification reads ordinary local views outside consumer admission and
// FSM exclusion; only final validation/publication takes those gates.
func (ch *CommandHandler) compactConsumerHistory(ctx context.Context, cursor *consumerHistoryCursor) (result disk.CompactionResult, err error) {
	defer func() {
		outcome := "completed"
		if err != nil {
			outcome = "deferred"
		} else if result.SkippedReason != "" {
			outcome = "skipped"
		}
		metrics.ConsumerHistoryRuns.WithLabelValues(outcome).Inc()
		metrics.ConsumerHistoryRemovedRecords.Add(float64(result.RecordsRemoved))
		if err == nil && result.BytesBefore > result.BytesAfter {
			metrics.ConsumerHistoryRemovedBytes.Add(float64(result.BytesBefore - result.BytesAfter))
		}
	}()
	if !ch.isDistributed() || ch.Coordinator == nil || ch.TopicManager == nil || !ch.Config.ConsumerHistoryCompactionEnabled {
		result.SkippedReason = "disabled"
		return result, nil
	}
	state := ch.Cluster.RaftManager.GetFSM()
	if state == nil || !state.ConsumerHistoryEnabled() || !state.ConsumerMetadataRoutingEnabled() {
		result.SkippedReason = "activation_pending"
		return result, nil
	}
	if err = ch.consumerReadinessError(); err != nil {
		return result, err
	}
	var work *coordinator.ConsumerHistoryWork
	var plan *consumerRecoveryPlan
	phaseStart := time.Now()
	metadata, err := state.ConsumerMetadataRecoverySnapshot()
	if err == nil {
		plan, err = ch.localConsumerHistoryPlan(metadata)
	}
	if err == nil {
		work, err = ch.Coordinator.TryCaptureConsumerHistoryWork()
	}
	metrics.ConsumerHistoryPhaseSeconds.WithLabelValues("capture").Observe(time.Since(phaseStart).Seconds())
	if err != nil {
		return result, err
	}
	var certificate *coordinator.ConsumerHistoryCompactionPlan
	var selected disk.ConsumerHistorySegment
	target := -1
	phaseStart = time.Now()
	err = ch.withConsumerRecoveryPlan(ctx, plan, -1, nil, func(reader *consumerRecoveryReader) error {
		type candidate struct {
			partition int
			segment   disk.ConsumerHistorySegment
		}
		var candidates []candidate
		for p, v := range reader.views {
			if allowed, _ := ch.distributedCompactionAllowed(config.ConsumerOffsetsTopicName, p); !allowed {
				continue
			}
			segments, e := v.ConsumerHistorySegments(ctx, reader.ends[p])
			if e != nil {
				return e
			}
			for _, segment := range segments {
				candidates = append(candidates, candidate{p, segment})
			}
		}
		if len(candidates) == 0 {
			return nil
		}
		chosen := candidates[0]
		for _, candidate := range candidates {
			first, _ := candidate.segment.Range()
			if !cursor.valid || candidate.partition > cursor.partition || candidate.partition == cursor.partition && first > cursor.first {
				chosen = candidate
				break
			}
		}
		target, selected = chosen.partition, chosen.segment
		first, end := selected.Range()
		*cursor = consumerHistoryCursor{target, first, true}
		var e error
		counted := &consumerHistoryCountingReader{ConsumerMetadataIndexReader: reader}
		defer func() {
			metrics.ConsumerHistoryReadRecords.Add(float64(counted.records))
			metrics.ConsumerHistoryReadPayloadBytes.Add(float64(counted.bytes))
		}()
		certificate, e = work.Prepare(counted, target, first, end)
		return e
	})
	metrics.ConsumerHistoryPhaseSeconds.WithLabelValues("certify").Observe(time.Since(phaseStart).Seconds())
	if err != nil {
		return result, err
	}
	if target < 0 {
		result.SkippedReason = "no_eligible_segments"
		return result, nil
	}
	if len(certificate.RemovedRecords()) == 0 && !selected.Empty() {
		result.SkippedReason = "no_superseded_records"
		return result, nil
	}
	phaseStart = time.Now()
	err = ch.withConsumerHistoryFence(ctx, func(metadata []fsm.PartitionMetadata) error {
		if !state.ConsumerHistoryEnabled() || !state.ConsumerMetadataRoutingEnabled() {
			return fmt.Errorf("consumer history activation changed")
		}
		current, e := ch.localConsumerHistoryPlan(metadata)
		if e != nil {
			return e
		}
		if len(metadata) != len(plan.metadata) {
			return fmt.Errorf("consumer history topology changed")
		}
		for p, m := range metadata {
			old := plan.metadata[p]
			if m.CommittedHWM < old.CommittedHWM || current.local[p] != plan.local[p] {
				return fmt.Errorf("consumer history source replaced")
			}
			m.CommittedHWM = old.CommittedHWM
			if !reflect.DeepEqual(m, old) {
				return fmt.Errorf("consumer history authority changed")
			}
		}
		if allowed, reason := ch.distributedCompactionAllowed(config.ConsumerOffsetsTopicName, target); !allowed {
			return fmt.Errorf("consumer history replica gate: %s", reason)
		}
		return current.local[target].WithConsumerHistoryInstallation(ctx, func(view *topic.PartitionReadView, install func(disk.ConsumerHistorySegment, []types.Message) (disk.CompactionResult, error)) error {
			return ch.withConsumerRecoveryPlan(ctx, current, target, view, func(reader *consumerRecoveryReader) error {
				reader.ends[target] = metadata[target].CommittedHWM
				if e := ch.Coordinator.ValidateConsumerHistoryCompaction(reader, certificate); e != nil {
					return e
				}
				var e error
				result, e = install(selected, certificate.RemovedRecords())
				return e
			})
		})
	})
	metrics.ConsumerHistoryPhaseSeconds.WithLabelValues("publish").Observe(time.Since(phaseStart).Seconds())
	return result, err
}

type consumerHistoryCountingReader struct {
	coordinator.ConsumerMetadataIndexReader
	records, bytes int64
}

func (r *consumerHistoryCountingReader) observe(messages []types.Message) {
	r.records += int64(len(messages))
	for _, m := range messages {
		r.bytes += int64(len(m.Key) + len(m.Payload))
	}
}
func (r *consumerHistoryCountingReader) ReadCommittedConsumerMetadata(topic string, p int, offset uint64, max int) ([]types.Message, error) {
	messages, err := r.ConsumerMetadataIndexReader.ReadCommittedConsumerMetadata(topic, p, offset, max)
	r.observe(messages)
	return messages, err
}
func (r *consumerHistoryCountingReader) ReadRawConsumerMetadataIndex(p int, offset uint64, max int) ([]types.Message, error) {
	messages, err := r.ConsumerMetadataIndexReader.ReadRawConsumerMetadataIndex(p, offset, max)
	r.observe(messages)
	return messages, err
}

func (ch *CommandHandler) withConsumerHistoryFence(ctx context.Context, fn func([]fsm.PartitionMetadata) error) error {
	attempt, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	release, err := ch.Coordinator.BeginConsumerMetadataRecovery(attempt)
	if err != nil {
		return err
	}
	defer release()
	if !ch.groupRecoveryReloadMu.TryLock() {
		return ErrConsumerRecoveryPending
	}
	defer ch.groupRecoveryReloadMu.Unlock()
	if err := ch.consumerHistoryRecoveryReady(); err != nil {
		return err
	}
	return ch.Cluster.RaftManager.GetFSM().WithConsumerMetadataRecoveryFence(fn)
}

func (ch *CommandHandler) consumerHistoryRecoveryReady() error {
	r := ch.Coordinator.ConsumerRecoveryRequirement()
	if r.Pending() || r.Installing || r.Failure != nil {
		return ErrConsumerRecoveryPending
	}
	return nil
}

func (ch *CommandHandler) localConsumerHistoryPlan(metadata []fsm.PartitionMetadata) (*consumerRecoveryPlan, error) {
	t := ch.TopicManager.GetTopic(config.ConsumerOffsetsTopicName)
	if t == nil || t.Definition().Partitions != len(metadata) {
		return nil, ErrConsumerRecoveryPending
	}
	plan := &consumerRecoveryPlan{metadata: cloneConsumerRecoveryMetadata(metadata), local: make([]*topic.Partition, len(metadata)), ends: make([]uint64, len(metadata))}
	for p, m := range metadata {
		if !m.CommittedHWMKnown || m.LifecycleEpoch != t.Definition().LifecycleEpoch {
			return nil, ErrConsumerRecoveryPending
		}
		partition, err := t.GetPartition(p)
		if err != nil {
			return nil, err
		}
		// Missing coverage is a hold, never a reason to compact a partial view.
		if partition.BootstrapPending() || partition.SnapshotRecoveryPending() || !partition.HWMKnown() || partition.GetHWM() < m.CommittedHWM {
			return nil, ErrConsumerRecoveryPending
		}
		plan.local[p], plan.ends[p] = partition, m.CommittedHWM
	}
	return plan, nil
}

func (ch *CommandHandler) startConsumerHistoryMonitor(ctx context.Context) error {
	if !ch.Config.ConsumerHistoryCompactionEnabled {
		return nil
	}
	ctx, finish, err := ch.beginMonitor(ctx, "consumer-history")
	if err != nil {
		return err
	}
	go func() {
		defer finish()
		interval := time.Duration(ch.Config.CompactionCheckIntervalMS) * time.Millisecond
		if interval < time.Second {
			interval = time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		cursor := consumerHistoryCursor{}
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				attempt, cancel := context.WithTimeout(ctx, time.Minute)
				start := time.Now()
				result, err := ch.compactConsumerHistory(attempt, &cursor)
				cancel()
				if err != nil {
					util.Warn("Consumer history compaction deferred: %v", err)
				} else if result.RecordsRemoved > 0 {
					util.Info("Consumer history compaction removed=%d bytes_before=%d bytes_after=%d elapsed=%s", result.RecordsRemoved, result.BytesBefore, result.BytesAfter, time.Since(start))
				}
			}
		}
	}()
	return nil
}
