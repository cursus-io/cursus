package coordinator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"github.com/cursus-io/cursus/pkg/types"
)

// ConsumerMetadataIndexReader is optional. All methods run inside the same
// committed read views and FSM fence as strict replay. Identity must include
// topic incarnation/lifecycle and a DURABLE prefix-mutation generation. Raw
// reads must include invisible transactions, but exclude offsets at/after end.
// Neither a remote transfer cache nor a process-local generation suffices.
type ConsumerMetadataIndexReader interface {
	ConsumerMetadataCommittedReader
	ConsumerMetadataIndexIdentity(partition int) (string, error)
	ReadRawConsumerMetadataIndex(partition int, offset uint64, max int) ([]types.Message, error)
	SyncConsumerMetadataIndex() error
}

const consumerReplayIndexLimit = 32 << 20

var errConsumerReplayIndexFallback = errors.New("consumer replay index requires full history")

type consumerReplayIndexPartition struct {
	Identity   string
	First, End uint64
}

type consumerReplayIndexRecord struct {
	Partition int
	Message   types.Message
}

type consumerReplayIndex struct {
	Version         int
	Topic, Baseline string
	Partitions      []consumerReplayIndexPartition
	Records         []consumerReplayIndexRecord
	// Version 2 retains raw version-7 transaction evidence. Visibility is resolved
	// again from the current fixed read view; cached decisions are never authority.
	Transactions []consumerReplayIndexRecord `json:",omitempty"`
	Sources      map[string]map[int]struct{}
}

type replayIndexKey struct{ group, kind, topic string }

type indexedMetadataReader struct {
	*boundedMetadataReader
	source                 ConsumerMetadataIndexReader
	path                   string
	index                  consumerReplayIndex
	cached                 *consumerReplayIndex
	selected               map[replayIndexKey]consumerReplayIndexRecord
	epochs                 map[string]uint64
	eligible               bool
	readRecords, readBytes int
	cacheBytes             int
	transactionChecks      int
	cachedRecords          []consumerReplayIndexRecord
	visibleTransactions    []consumerReplayIndexRecord
	ordinarySources        map[string]map[int]struct{}
}

func (c *Coordinator) newIndexedMetadataReader(reader ConsumerMetadataCommittedReader, bounded *boundedMetadataReader, baseline map[string]*GroupStateSnapshot, load bool) *indexedMetadataReader {
	source, ok := reader.(ConsumerMetadataIndexReader)
	if !ok || c.cfg == nil || !c.cfg.ConsumerRecoveryIndexEnabled || c.cfg.LogDir == "" {
		return nil
	}
	r := &indexedMetadataReader{boundedMetadataReader: bounded, source: source, eligible: true, selected: make(map[replayIndexKey]consumerReplayIndexRecord), epochs: make(map[string]uint64), ordinarySources: make(map[string]map[int]struct{})}
	r.path = filepath.Join(c.cfg.LogDir, ".consumer-replay-index.json")
	data, err := json.Marshal(baseline)
	if err != nil {
		return nil
	}
	sum := sha256.Sum256(data)
	r.index = consumerReplayIndex{Version: 1, Topic: c.offsetTopic, Baseline: hex.EncodeToString(sum[:])}
	for p := 0; p < len(bounded.bounds); p++ {
		identity, err := source.ConsumerMetadataIndexIdentity(p)
		if err != nil || identity == "" {
			return nil
		}
		b := bounded.bounds[p]
		r.index.Partitions = append(r.index.Partitions, consumerReplayIndexPartition{identity, b.first, b.end})
	}
	if load {
		r.load()
	}
	return r
}

func (r *indexedMetadataReader) load() {
	f, err := os.Open(r.path) // #nosec G304 -- fixed broker-local derived cache path.
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, consumerReplayIndexLimit+1))
	if err != nil || len(data) > consumerReplayIndexLimit {
		return
	}
	r.cacheBytes = len(data)
	var envelope cleanupCheckpointEnvelope
	if json.Unmarshal(data, &envelope) != nil {
		return
	}
	sum := sha256.Sum256(envelope.Payload)
	if hex.EncodeToString(sum[:]) != envelope.SHA256 {
		return
	}
	var saved consumerReplayIndex
	if json.Unmarshal(envelope.Payload, &saved) != nil || (saved.Version != 1 && saved.Version != 2) || saved.Version == 1 && len(saved.Transactions) != 0 || saved.Version == 2 && len(saved.Transactions) == 0 || saved.Topic != r.index.Topic || saved.Baseline != r.index.Baseline || len(saved.Partitions) != len(r.index.Partitions) {
		return
	}
	for p, current := range r.index.Partitions {
		old := saved.Partitions[p]
		if old.Identity != current.Identity || old.First != current.First || old.End < old.First || old.End > current.End {
			return
		}
	}
	last := make(map[int]uint64)
	for name, partitions := range saved.Sources {
		if name == "" {
			return
		}
		for p := range partitions {
			if p < 0 || p >= len(saved.Partitions) {
				return
			}
		}
	}
	for _, entry := range saved.Records {
		if entry.Partition < 0 || entry.Partition >= len(saved.Partitions) {
			return
		}
		b := saved.Partitions[entry.Partition]
		m := entry.Message
		if m.Offset < b.First || m.Offset >= b.End {
			return
		}
		if previous, ok := last[entry.Partition]; ok && m.Offset <= previous {
			return
		}
		last[entry.Partition] = m.Offset
		record, versioned, err := decodeConsumerMetadataRecord(m.Payload)
		if err != nil || !versioned || m.Key != consumerMetadataRecordKey(record) || indexTransaction(m) {
			return
		}
		if _, ok := saved.Sources[record.Group][entry.Partition]; !ok {
			return
		}
	}
	for _, entry := range saved.Records {
		_ = r.observe(entry.Partition, []types.Message{entry.Message}, false)
	}
	if !r.loadTransactions(&saved) {
		r.selected = make(map[replayIndexKey]consumerReplayIndexRecord)
		r.epochs = make(map[string]uint64)
		r.ordinarySources = make(map[string]map[int]struct{})
		return
	}
	r.ordinarySources = saved.Sources
	r.cached = &saved
}

func indexTransaction(m types.Message) bool {
	return m.TransactionalID != "" || m.TransactionState != "" || m.TransactionMarker != "" || m.ControlBatchType != "" || m.ControlBatchVersion != 0 || m.ControlBatchCoordinatorEpoch != 0 || len(m.ControlBatchKey) != 0 || len(m.ControlBatchValue) != 0
}

// certifyRaw avoids certifying an omission caused by the current transaction
// decision view. Version 2 retains every version-7 raw intent/control and resolves
// visibility again on reuse. Legacy transaction formats still require full replay.
func (r *indexedMetadataReader) certifyRaw() bool {
	for p, b := range r.index.Partitions {
		next := b.First
		if r.cached != nil {
			next = r.cached.Partitions[p].End
		}
		for next < b.End {
			messages, err := r.source.ReadRawConsumerMetadataIndex(p, next, 1024)
			if err != nil {
				return false
			}
			if len(messages) == 0 {
				break
			}
			for _, m := range messages {
				if m.Offset < next || m.Offset >= b.End {
					return false
				}
				r.readRecords++
				r.readBytes += len(m.Payload) + len(m.Key)
				if indexTransaction(m) {
					if !indexHistoryTransaction(m) {
						return false
					}
					r.index.Version = 2
					r.index.Transactions = append(r.index.Transactions, consumerReplayIndexRecord{p, m})
				}
				next = m.Offset + 1
			}
		}
	}
	return true
}

func replayIndexRecordKey(record ConsumerMetadataRecord) replayIndexKey {
	kind := record.Type
	if kind == ConsumerMetadataRecordTombstone {
		kind = ConsumerMetadataRecordRegistration
	}
	key := replayIndexKey{group: record.Group, kind: kind}
	if kind == ConsumerMetadataRecordOffsetSnapshot {
		key.topic = record.Topic
	}
	return key
}

func (r *indexedMetadataReader) observe(p int, messages []types.Message, suffix bool) error {
	for _, m := range messages {
		if indexHistoryTransaction(m) {
			if m.TransactionMarker == types.TransactionMarkerNone {
				r.visibleTransactions = append(r.visibleTransactions, consumerReplayIndexRecord{p, m})
			}
			continue
		}
		record, versioned, err := decodeConsumerMetadataRecord(m.Payload)
		if err != nil || !versioned || indexTransaction(m) || record.Type == ConsumerMetadataRecordTransactionalOffsetSnapshot {
			r.eligible = false
			if r.cached != nil {
				return errConsumerReplayIndexFallback
			}
			continue
		}
		if r.ordinarySources == nil {
			r.ordinarySources = make(map[string]map[int]struct{})
		}
		if r.ordinarySources[record.Group] == nil {
			r.ordinarySources[record.Group] = make(map[int]struct{})
		}
		r.ordinarySources[record.Group][p] = struct{}{}
		key := replayIndexRecordKey(record)
		if suffix && r.cached != nil && record.Epoch < r.epochs[record.Group] {
			return errConsumerReplayIndexFallback
		}
		if record.Epoch > r.epochs[record.Group] {
			r.epochs[record.Group] = record.Epoch
		}
		previous, exists := r.selected[key]
		if exists {
			old, _, _ := decodeConsumerMetadataRecord(previous.Message.Payload)
			if suffix && r.cached != nil && (record.Epoch < old.Epoch || record.Epoch == old.Epoch && record.Revision <= old.Revision) {
				return errConsumerReplayIndexFallback
			}
			if record.Epoch < old.Epoch || record.Epoch == old.Epoch && record.Revision < old.Revision {
				continue
			}
			if record.Epoch == old.Epoch && record.Revision == old.Revision {
				if record.Type != ConsumerMetadataRecordLifecycleSnapshot || sameConsumerMetadataRecord(record, old) || (!sameEmptyLifecycleState(record, old) && (previous.Partition != p || previous.Message.Offset >= m.Offset)) {
					continue
				}
			}
		}
		r.selected[key] = consumerReplayIndexRecord{p, m}
	}
	return nil
}

func (r *indexedMetadataReader) ReadTopicPartition(topic string, p int, next uint64, max int) ([]types.Message, error) {
	if r.cached != nil && next < r.cached.Partitions[p].End {
		var out []types.Message
		for _, entry := range r.cachedRecords {
			if entry.Partition == p && entry.Message.Offset >= next && len(out) < max {
				out = append(out, entry.Message)
			}
		}
		if len(out) != 0 {
			return out, nil
		}
		next = r.cached.Partitions[p].End
	}
	messages, err := r.boundedMetadataReader.ReadTopicPartition(topic, p, next, max)
	if err != nil {
		return nil, err
	}
	r.readRecords += len(messages)
	for _, m := range messages {
		r.readBytes += len(m.Payload) + len(m.Key)
	}
	return messages, r.observe(p, messages, true)
}

func (r *indexedMetadataReader) projection() *boundedMetadataReader {
	r.index.Records = nil
	for _, entry := range r.selected {
		r.index.Records = append(r.index.Records, entry)
	}
	sortReplayIndexRecords(r.index.Records)
	projected := append(append([]consumerReplayIndexRecord(nil), r.index.Records...), r.visibleTransactions...)
	sortReplayIndexRecords(projected)
	return &boundedMetadataReader{bounds: r.bounds, read: func(_ string, p int, next uint64, max int) ([]types.Message, error) {
		var out []types.Message
		for _, entry := range projected {
			if entry.Partition == p && entry.Message.Offset >= next && len(out) < max {
				out = append(out, entry.Message)
			}
		}
		return out, nil
	}}
}

func (r *indexedMetadataReader) save(baseline map[string]*GroupStateSnapshot, scratch *Coordinator) (int, error) {
	if !r.eligible {
		return 0, nil
	}
	if r.cached != nil && reflect.DeepEqual(r.cached.Partitions, r.index.Partitions) {
		return 0, nil
	}
	for _, g := range scratch.groups {
		if g.RegistrationInferred || g.RegistrationEpoch == 0 {
			return 0, nil
		}
	}
	for name, epoch := range r.epochs {
		if epoch > scratch.groupEpochs[name] {
			return 0, nil
		}
	}
	check := newCompatibilityCoordinator()
	if err := check.importStateValidated(baseline); err != nil {
		return 0, err
	}
	check.offsetTopic, check.offsetTopicPartitionCount = r.index.Topic, len(r.index.Partitions)
	if _, err := check.recoverConsumerMetadataWithOptions(r.projection(), true); err != nil {
		return 0, err
	}
	if !reflect.DeepEqual(check.ExportState(), scratch.ExportState()) {
		return 0, fmt.Errorf("replay projection differs from full replay")
	}
	r.index.Sources = r.ordinarySources
	sortReplayIndexRecords(r.index.Transactions)
	if err := r.source.SyncConsumerMetadataIndex(); err != nil {
		return 0, err
	}
	for p, b := range r.index.Partitions {
		identity, err := r.source.ConsumerMetadataIndexIdentity(p)
		if err != nil || identity != b.Identity {
			return 0, fmt.Errorf("replay index source changed")
		}
	}
	payload, err := json.Marshal(r.index)
	if err != nil {
		return 0, err
	}
	sum := sha256.Sum256(payload)
	data, err := json.Marshal(cleanupCheckpointEnvelope{payload, hex.EncodeToString(sum[:])})
	if err != nil {
		return 0, err
	}
	if len(data) > consumerReplayIndexLimit {
		return 0, fmt.Errorf("replay index exceeds size limit")
	}
	return writeConsumerReplayIndex(r.path, data, nil)
}

// One fixed temporary file bounds leftovers across repeated process crashes.
// The recovery lease and broker log-directory lock serialize publication.
func writeConsumerReplayIndex(path string, data []byte, after func(string) error) (int, error) {
	if len(data) > consumerReplayIndexLimit {
		return 0, fmt.Errorf("replay index exceeds size limit")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return 0, err
	}
	f, err := os.OpenFile(path+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600) // #nosec G304 -- fixed broker-owned cache temporary path.
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	n, err := f.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil && after != nil {
		err = after("written")
	}
	if err == nil {
		err = f.Sync()
	}
	if err == nil && after != nil {
		err = after("synced")
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return 0, err
	}
	if err = replaceCleanupCheckpoint(f.Name(), path); err != nil {
		return 0, err
	}
	if after != nil {
		if err = after("published"); err != nil {
			return len(data), err
		}
	}
	return len(data), nil
}

func (r *indexedMetadataReader) validateIdentity() error {
	for p, b := range r.index.Partitions {
		identity, err := r.source.ConsumerMetadataIndexIdentity(p)
		if err != nil || identity != b.Identity {
			return fmt.Errorf("consumer replay index source changed during replay")
		}
	}
	return nil
}
