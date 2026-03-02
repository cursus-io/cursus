package bench

import (
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/util"
)

const sep = "========================================"

type Phase string

const (
	PhaseInitial    Phase = "initial"
	PhaseRebalanced Phase = "rebalanced"
)

type PartitionMetrics struct {
	ID        int
	mu        sync.RWMutex
	firstSeen time.Time
	lastSeen  time.Time
	totalMsgs int64
}

func (pm *PartitionMetrics) record(count int) {
	now := time.Now()
	pm.mu.Lock()
	if pm.firstSeen.IsZero() {
		pm.firstSeen = now
	}
	pm.lastSeen = now
	pm.mu.Unlock()

	atomic.AddInt64(&pm.totalMsgs, int64(count))
}

func (pm *PartitionMetrics) TPS() float64 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if pm.firstSeen.IsZero() || pm.lastSeen.Equal(pm.firstSeen) {
		return 0
	}
	elapsed := pm.lastSeen.Sub(pm.firstSeen).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(atomic.LoadInt64(&pm.totalMsgs)) / elapsed
}

type PhaseMetrics struct {
	startTime  time.Time
	endTime    time.Time
	mu         sync.RWMutex
	totalMsgs  int64
	partitions map[int]*PartitionMetrics
}

func (pm *PhaseMetrics) TPS() float64 {
	if pm.startTime.IsZero() {
		return 0
	}

	end := pm.endTime
	if end.IsZero() {
		end = time.Now()
	}

	d := end.Sub(pm.startTime).Seconds()
	if d <= 0 {
		return 0
	}
	return float64(atomic.LoadInt64(&pm.totalMsgs)) / d
}

type RebalanceEvent struct {
	Start time.Time
	End   time.Time
}

type ConsumerMetrics struct {
	startTime time.Time
	started   bool

	totalMsgs  int64
	uniqueMsgs int64

	currentPhase Phase
	phases       map[Phase]*PhaseMetrics

	enableCorrectness bool
	expectedTotal     int64
	seenOffsetFilter  *BloomFilter // partition+offset
	seenIDFilter      *BloomFilter // messageID
	dupCount          int64
	dupOffsetCount    int64
	missingCount      int64

	mu        sync.RWMutex
	rebalance *RebalanceEvent
}

func NewConsumerMetrics(expected int64, enableCorrectness bool) *ConsumerMetrics {
	var offsetBF, idBF *BloomFilter
	if enableCorrectness {
		offsetBF = NewBloomFilter(uint64(expected), 0.0001)
		idBF = NewBloomFilter(uint64(expected), 0.0001)
	}

	return &ConsumerMetrics{
		currentPhase: PhaseInitial,
		phases: map[Phase]*PhaseMetrics{
			PhaseInitial: {partitions: make(map[int]*PartitionMetrics)},
		},
		expectedTotal:     expected,
		enableCorrectness: enableCorrectness,
		seenOffsetFilter:  offsetBF,
		seenIDFilter:      idBF,
	}
}

func (m *ConsumerMetrics) RecordBatch(partition int, count int) {
	m.mu.Lock()
	if !m.started {
		now := time.Now()
		m.startTime = now
		m.started = true
		if ph, ok := m.phases[m.currentPhase]; ok && ph.startTime.IsZero() {
			ph.startTime = now
		}
	}
	currentPhase := m.currentPhase
	ph := m.phases[currentPhase]
	m.mu.Unlock()

	ph.mu.Lock()
	pm, ok := ph.partitions[partition]
	if !ok {
		pm = &PartitionMetrics{ID: partition}
		ph.partitions[partition] = pm
	}
	ph.mu.Unlock()

	atomic.AddInt64(&m.totalMsgs, int64(count))
	atomic.AddInt64(&ph.totalMsgs, int64(count))
	pm.record(count)
}

func (m *ConsumerMetrics) RecordMessage(partition int, offset int64, producerID string, seqNum uint64) {
	if !m.enableCorrectness {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	offsetKey := encodeOffset(partition, offset)
	if m.seenOffsetFilter.Add(offsetKey) {
		atomic.AddInt64(&m.dupOffsetCount, 1)
		util.Debug("duplicated offset. (partition: %d, offset: %d)", partition, offset)
	}

	messageKey := encodeMessageID(partition, producerID, seqNum)
	if !m.seenIDFilter.Add(messageKey) {
		atomic.AddInt64(&m.uniqueMsgs, 1)
	} else {
		atomic.AddInt64(&m.dupCount, 1)
		util.Debug("duplicated message. (partition: %d, offset: %d, seqNum: %d)", partition, offset, seqNum)
	}
}

func (m *ConsumerMetrics) RebalanceStart() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	m.rebalance = &RebalanceEvent{Start: now}

	if _, ok := m.phases[PhaseRebalanced]; !ok {
		m.phases[PhaseRebalanced] = &PhaseMetrics{
			partitions: make(map[int]*PartitionMetrics),
		}
	}
}

func (m *ConsumerMetrics) OnFirstConsumeAfterRebalance() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.rebalance == nil || !m.rebalance.End.IsZero() {
		return
	}

	now := time.Now()
	m.rebalance.End = now

	if pm := m.phases[PhaseInitial]; pm != nil && pm.endTime.IsZero() {
		pm.endTime = now
	}

	reb := m.phases[PhaseRebalanced]
	reb.startTime = now
	m.currentPhase = PhaseRebalanced
}

func (m *ConsumerMetrics) IsFullyConsumed(expectedTotal int64) (bool, string) {
	currentTotal := atomic.LoadInt64(&m.totalMsgs)
	currentUnique := atomic.LoadInt64(&m.uniqueMsgs)

	if currentTotal < expectedTotal {
		return false, fmt.Sprintf("receiving... (%d/%d) unique=%d total=%d missing=%d", currentTotal, expectedTotal, currentUnique, currentTotal, expectedTotal-currentTotal)
	}
	return true, ""
}

func (m *ConsumerMetrics) Finalize() {
	consumed := atomic.LoadInt64(&m.totalMsgs)
	unique := atomic.LoadInt64(&m.uniqueMsgs)

	if consumed < m.expectedTotal {
		atomic.StoreInt64(&m.missingCount, m.expectedTotal-consumed)
	} else {
		atomic.StoreInt64(&m.missingCount, 0)
	}

	if m.enableCorrectness && unique < m.expectedTotal && consumed >= m.expectedTotal {
		util.Warn("benchmark reached target total but unique count is lower (%d/%d). this is likely due to BloomFilter false positives.", unique, m.expectedTotal)
	}
}

// nearest rank, not interpolation
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

func (m *ConsumerMetrics) PrintSummaryTo(w io.Writer) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	elapsed := time.Since(m.startTime).Seconds()
	total := atomic.LoadInt64(&m.totalMsgs)

	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintln(w, sep)
	_, _ = fmt.Fprintln(w, "📊 CONSUMER BENCHMARK SUMMARY")

	_, _ = fmt.Fprintf(w, "Total Messages       : %d\n", total)
	_, _ = fmt.Fprintf(w, "Elapsed Time         : %.2fs\n", elapsed)

	overallTPS := 0.0
	if elapsed > 0 {
		overallTPS = float64(total) / elapsed
	}
	_, _ = fmt.Fprintf(w, "Overall TPS          : %.2f msg/s\n", overallTPS)

	if m.enableCorrectness {
		_, _ = fmt.Fprintf(w, "Duplicate (MessageID) : %d (fp possible)\n", atomic.LoadInt64(&m.dupCount))
		_, _ = fmt.Fprintf(w, "Duplicate (Offset)    : %d (fp possible)\n", atomic.LoadInt64(&m.dupOffsetCount))
		_, _ = fmt.Fprintf(w, "Message missing       : %d\n", atomic.LoadInt64(&m.missingCount))
	} else {
		_, _ = fmt.Fprintf(w, "Correctness Check     : disabled (no Bloom filter)\n")
	}

	if m.rebalance != nil && !m.rebalance.End.IsZero() {
		_, _ = fmt.Fprintf(w, "Rebalancing Cost      : %d ms\n", m.rebalance.End.Sub(m.rebalance.Start).Milliseconds())
	}

	var phaseKeys []string
	for k := range m.phases {
		phaseKeys = append(phaseKeys, string(k))
	}
	sort.Strings(phaseKeys)

	for _, k := range phaseKeys {
		phase := Phase(k)
		pm := m.phases[phase]

		var partitionTPS []float64
		var pIDs []int
		for id := range pm.partitions {
			pIDs = append(pIDs, id)
		}
		sort.Ints(pIDs)

		for _, id := range pIDs {
			partitionTPS = append(partitionTPS, pm.partitions[id].TPS())
		}

		sort.Float64s(partitionTPS)

		_, _ = fmt.Fprintln(w)
		_, _ = fmt.Fprintf(w, "▶ Phase: %s\n", phase)
		_, _ = fmt.Fprintf(w, "  Phase Total TPS       : %.2f\n", pm.TPS())
		_, _ = fmt.Fprintf(w, "  p95 Partition Avg TPS : %.2f\n", percentile(partitionTPS, 0.95))
		_, _ = fmt.Fprintf(w, "  p99 Partition Avg TPS : %.2f\n", percentile(partitionTPS, 0.99))

		for _, id := range pIDs {
			p := pm.partitions[id]
			_, _ = fmt.Fprintf(w, "    #%-2d total=%-8d avgTPS=%.1f\n", p.ID, atomic.LoadInt64(&p.totalMsgs), p.TPS())
		}
	}

	_, _ = fmt.Fprintln(w, sep)
}

func (m *ConsumerMetrics) PrintSummary() {
	util.Info("🎉 Benchmark completed successfully!")
	m.PrintSummaryTo(os.Stdout)
}
