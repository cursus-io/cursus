package controller

import (
	"context"
	"errors"
	"fmt"
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

// compactConsumerHistory certifies one partition's closed prefix, then publishes
// separately fenced atomic segments. Unsafe subsequences use one segment. Full
// scans and remote transfers never hold consumer admission or the FSM fence.
func (ch *CommandHandler) compactConsumerHistory(ctx context.Context, cursor *consumerHistoryCursor) (result disk.CompactionResult, err error) {
	defer func() {
		outcome := "completed"
		if err != nil {
			outcome = "deferred"
		} else if result.SkippedReason != "" {
			outcome = "skipped"
		}
		metrics.ConsumerHistoryRuns.WithLabelValues(outcome).Inc()
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
		plan, err = ch.prepareConsumerRecoveryPlan(ctx, metadata, -1, false)
	}
	if plan != nil {
		defer plan.close()
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
	var segments []disk.ConsumerHistorySegment
	var sweep *coordinator.ConsumerHistorySweep
	target := -1
	phaseStart = time.Now()
	err = ch.withConsumerRecoveryPlan(ctx, plan, -1, nil, func(reader *consumerRecoveryReader) error {
		type candidate struct {
			partition int
			segment   disk.ConsumerHistorySegment
		}
		var candidates []candidate
		for p, v := range reader.views {
			if v == nil {
				continue
			}
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
		for _, candidate := range candidates {
			start, stop := candidate.segment.Range()
			if candidate.partition == target && start >= first {
				segments = append(segments, candidate.segment)
				end = stop
			}
		}
		*cursor = consumerHistoryCursor{target, first, true}
		var e error
		counted := &consumerHistoryCountingReader{ConsumerMetadataIndexReader: consumerHistorySource{reader}}
		defer func() {
			metrics.ConsumerHistoryReadRecords.Add(float64(counted.records))
			metrics.ConsumerHistoryReadPayloadBytes.Add(float64(counted.bytes))
		}()
		certificate, e = work.Prepare(counted, target, first, end)
		if e == nil && len(segments) > 1 {
			var safe bool
			sweep, safe = certificate.Sweep()
			if !safe {
				// Replay equivalence of a whole prefix does not imply safe
				// intermediate publications. Fall back to the original proof.
				segments = segments[:1]
				_, end = selected.Range()
				certificate, e = work.Prepare(counted, target, first, end)
			}
		}
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
	if sweep == nil && len(certificate.RemovedRecords()) == 0 && !selected.Empty() {
		result.SkippedReason = "no_superseded_records"
		return result, nil
	}
	if err = ch.refreshConsumerHistorySources(ctx, plan); err != nil {
		return result, err
	}
	phaseStart = time.Now()
	var previousPublication time.Duration
	for _, segment := range segments {
		if err = waitConsumerHistoryPublication(ctx, previousPublication); err != nil {
			break
		}
		first, end := segment.Range()
		if sweep != nil {
			certificate, err = sweep.Segment(first, end)
			if err != nil {
				break
			}
		}
		var part disk.CompactionResult
		publicationStart := time.Now()
		part, err = ch.publishConsumerHistorySegment(ctx, plan, target, segment, certificate, sweep)
		previousPublication = time.Since(publicationStart)
		// A later segment can defer after earlier durable publications. Account
		// those successes even when the overall sweep returns an error. Failed
		// rewrites return no removed records and cannot certify saved bytes.
		metrics.ConsumerHistoryRemovedRecords.Add(float64(part.RecordsRemoved))
		if part.RecordsRemoved > 0 && part.BytesBefore > part.BytesAfter {
			metrics.ConsumerHistoryRemovedBytes.Add(float64(part.BytesBefore - part.BytesAfter))
		}
		result.SegmentsScanned += part.SegmentsScanned
		result.SegmentsRewritten += part.SegmentsRewritten
		result.RecordsScanned += part.RecordsScanned
		result.RecordsRemoved += part.RecordsRemoved
		result.BytesBefore += part.BytesBefore
		result.BytesAfter += part.BytesAfter
		if err != nil {
			break
		}
		*cursor = consumerHistoryCursor{target, first, true}
	}
	metrics.ConsumerHistoryPhaseSeconds.WithLabelValues("publish").Observe(time.Since(phaseStart).Seconds())
	return result, err
}

// An ordered sweep must not immediately reacquire exclusive admission for each
// segment. Give requests and ownership recovery at least as much time as the
// preceding publication used, with a floor above the ordinary retry interval.
// The certificate is still validated after the pause; no lock or lease is held.
func waitConsumerHistoryPublication(ctx context.Context, previous time.Duration) error {
	if previous == 0 {
		return ctx.Err()
	}
	if previous < 25*time.Millisecond {
		previous = 25 * time.Millisecond
	}
	timer := time.NewTimer(previous)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (ch *CommandHandler) publishConsumerHistorySegment(ctx context.Context, plan *consumerRecoveryPlan, target int, selected disk.ConsumerHistorySegment, certificate *coordinator.ConsumerHistoryCompactionPlan, sweep *coordinator.ConsumerHistorySweep) (result disk.CompactionResult, err error) {
	state := ch.Cluster.RaftManager.GetFSM()
	err = retryConsumerHistoryFence(ctx, func() (bool, error) {
		entered := false
		e := ch.withConsumerHistoryFence(ctx, func(metadata []fsm.PartitionMetadata) error {
			entered = true
			if !state.ConsumerHistoryEnabled() || !state.ConsumerMetadataRoutingEnabled() {
				return fmt.Errorf("consumer history activation changed")
			}
			if certificate.HasTransactionRemovals() && !state.ConsumerTransactionHistoryEnabled() {
				return fmt.Errorf("consumer transaction history activation pending")
			}
			if e := ch.validateConsumerRecoveryPlan(plan, metadata); e != nil {
				return e
			}
			if allowed, reason := ch.distributedCompactionAllowed(config.ConsumerOffsetsTopicName, target); !allowed {
				return fmt.Errorf("consumer history replica gate: %s", reason)
			}
			return plan.local[target].WithConsumerHistoryInstallation(ctx, func(view *topic.PartitionReadView, install func(disk.ConsumerHistorySegment, disk.ConsumerHistoryRemovalCertificate) (disk.CompactionResult, error)) error {
				return ch.withConsumerRecoveryPlan(ctx, plan, target, view, func(reader *consumerRecoveryReader) error {
					reader.ends[target] = metadata[target].CommittedHWM
					if e := ch.Coordinator.ValidateConsumerHistoryCompaction(consumerHistorySource{reader}, certificate); e != nil {
						return e
					}
					var e error
					if len(certificate.RemovedRecords()) != 0 || selected.Empty() {
						selected, e = view.RefreshConsumerHistorySegment(ctx, selected)
						if e != nil {
							return e
						}
						result, e = install(selected, certificate)
						if e != nil {
							return e
						}
					}
					if sweep != nil {
						identity, e := (consumerHistorySource{reader}).ConsumerMetadataIndexIdentity(target)
						if e != nil {
							return e
						}
						return sweep.Published(certificate, identity)
					}
					return nil
				})
			})
		})
		if errors.Is(e, topic.ErrConsumerHistoryBusy) {
			// The partition callback never ran. Release both admission and the
			// FSM fence before retrying so an in-flight replication decision can
			// apply and release its partition mutation gate.
			return false, fsm.ErrConsumerMetadataFencePending
		}
		return entered, e
	})
	return result, err
}

// Reuse the scanned certificate only when publication never entered the FSM
// fence. Each retry releases admission first. Once validation or storage starts,
// any failure discards the plan; no partial publication is retried blindly.
func retryConsumerHistoryFence(ctx context.Context, attempt func() (bool, error)) error {
	var err error
	for tries := 0; tries < 16; tries++ {
		if e := ctx.Err(); e != nil {
			return e
		}
		var entered bool
		entered, err = attempt()
		if entered || !errors.Is(err, fsm.ErrConsumerMetadataFencePending) || tries == 15 {
			return err
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
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
		timer := time.NewTimer(interval)
		defer timer.Stop()
		cursor := consumerHistoryCursor{}
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				start := time.Now()
				result, err := ch.compactConsumerHistory(ctx, &cursor)
				if err != nil {
					util.Warn("Consumer history compaction deferred: %v", err)
				} else if result.RecordsRemoved > 0 {
					util.Info("Consumer history compaction removed=%d bytes_before=%d bytes_after=%d elapsed=%s", result.RecordsRemoved, result.BytesBefore, result.BytesAfter, time.Since(start))
				}
				// A long pass must not consume an already queued ticker event and
				// immediately start another exclusive publication pass.
				timer.Reset(interval)
			}
		}
	}()
	return nil
}
