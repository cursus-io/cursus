package coordinator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/cursus-io/cursus/pkg/types"
)

const cleanupCheckpointLimit = 1 << 20

type cleanupCheckpointTarget struct {
	BeforeLive, DeletedEpoch uint64
	RegistrationAfter        bool
	Epoch                    uint64
	Generation               int
	EvidenceEpoch, LiveEpoch uint64
	Kind, Topic              string
	Digest                   [32]byte
}
type cleanupCheckpointPartition struct {
	Generation       string
	Start, End, Next uint64
	Cursor           types.ReplayCursor
}
type cleanupCheckpoint struct {
	Boundary   *TopicCleanupBoundary
	Version    int
	Topic      string
	Targets    map[string]cleanupCheckpointTarget
	Partitions []cleanupCheckpointPartition
}
type cleanupCheckpointEnvelope struct {
	Payload json.RawMessage
	SHA256  string
}

func (c *Coordinator) cleanupCheckpointPath(topic string) string {
	sum := sha256.Sum256([]byte(topic))
	return filepath.Join(c.cfg.LogDir, ".consumer-cleanup", hex.EncodeToString(sum[:])+".json")
}

// Checkpoints are disposable local evidence caches, not authoritative metadata.
// Missing, torn, old-version or stale caches always restart the validated scan.
// All callers hold cleanupMu and lifecycleMu and pin the partition read view.
func (c *Coordinator) loadCleanupCheckpoint(scan *topicCleanupScan, partitions []ConsumerCleanupPartition) {
	if scan.checkpointLoaded {
		return
	}
	scan.checkpointLoaded = true
	if len(partitions) == 0 {
		return
	}
	for _, p := range partitions {
		if p.DurableGeneration == "" {
			return
		}
	}
	path := c.cleanupCheckpointPath(scan.status.Topic)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > cleanupCheckpointLimit {
		return
	}
	// #nosec G304 -- path is a SHA256 name under the configured broker log root.
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var envelope cleanupCheckpointEnvelope
	if json.Unmarshal(data, &envelope) != nil {
		return
	}
	sum := sha256.Sum256(envelope.Payload)
	if hex.EncodeToString(sum[:]) != envelope.SHA256 {
		return
	}
	var cp cleanupCheckpoint
	if json.Unmarshal(envelope.Payload, &cp) != nil || cp.Version != 2 || !reflect.DeepEqual(cp.Boundary, scan.boundary) || cp.Topic != scan.status.Topic || len(cp.Targets) != len(scan.targets) || len(cp.Partitions) != len(partitions) {
		return
	}
	for name, target := range scan.targets {
		saved, ok := cp.Targets[name]
		if !ok || saved.Epoch != target.epoch || saved.Generation != target.generation {
			return
		}
	}
	for i, p := range partitions {
		saved := cp.Partitions[i]
		if saved.Generation != p.DurableGeneration || saved.Start != p.Start || saved.End > p.End || saved.Next < p.Start || saved.Next > saved.End {
			return
		}
		if saved.Next > p.Start && !saved.Cursor.Initialized {
			return
		}
	}
	scan.partitions = append([]ConsumerCleanupPartition(nil), partitions...)
	scan.next = make([]uint64, len(partitions))
	scan.cursors = make([]types.ReplayCursor, len(partitions))
	for i, saved := range cp.Partitions {
		scan.next[i], scan.cursors[i] = saved.Next, saved.Cursor
	}
	for name, saved := range cp.Targets {
		scan.targets[name].evidence = cleanupEvidence{beforeLive: saved.BeforeLive, deletedEpoch: saved.DeletedEpoch, registrationAfter: saved.RegistrationAfter, epoch: saved.EvidenceEpoch, liveEpoch: saved.LiveEpoch, kind: saved.Kind, topic: saved.Topic, digest: saved.Digest}
	}
}

func (c *Coordinator) saveCleanupCheckpoint(scan *topicCleanupScan, view ConsumerCleanupView) error {
	if len(scan.partitions) == 0 || scan.status.Records == scan.checkpointRecords {
		return nil
	}
	if scan.checkpointRecords != 0 && scan.status.Records-scan.checkpointRecords < 1024 && scan.status.Bytes-scan.checkpointBytes < 8<<20 && !scan.matches(scan.partitions) {
		return nil
	}
	cp := cleanupCheckpoint{Version: 2, Boundary: scan.boundary.Clone(), Topic: scan.status.Topic, Targets: make(map[string]cleanupCheckpointTarget)}
	for i, p := range scan.partitions {
		if p.DurableGeneration == "" {
			return nil
		}
		cp.Partitions = append(cp.Partitions, cleanupCheckpointPartition{p.DurableGeneration, p.Start, p.End, scan.next[i], scan.cursors[i]})
	}
	prepare, ok := view.(interface{ PrepareCleanupCheckpoint() error })
	if !ok {
		return nil
	}
	if err := prepare.PrepareCleanupCheckpoint(); err != nil {
		return err
	}
	for name, target := range scan.targets {
		e := target.evidence
		cp.Targets[name] = cleanupCheckpointTarget{BeforeLive: e.beforeLive, DeletedEpoch: e.deletedEpoch, RegistrationAfter: e.registrationAfter, Epoch: target.epoch, Generation: target.generation, EvidenceEpoch: e.epoch, LiveEpoch: e.liveEpoch, Kind: e.kind, Topic: e.topic, Digest: e.digest}
	}
	payload, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	data, err := json.Marshal(cleanupCheckpointEnvelope{payload, hex.EncodeToString(sum[:])})
	if err != nil {
		return err
	}
	if len(data) > cleanupCheckpointLimit {
		return fmt.Errorf("cleanup checkpoint exceeds size limit")
	}
	path := c.cleanupCheckpointPath(cp.Topic)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".cleanup-*")
	if err != nil {
		return err
	}
	temp := f.Name()
	defer func() { _ = os.Remove(temp) }()
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := replaceCleanupCheckpoint(temp, path); err != nil {
		return err
	}
	// Directory loss can only restore an older validated checkpoint or lose it.
	scan.checkpointRecords, scan.checkpointBytes = scan.status.Records, scan.status.Bytes
	return nil
}

func (c *Coordinator) removeCleanupCheckpoint(topic string) {
	if c.cfg != nil {
		_ = os.Remove(c.cleanupCheckpointPath(topic))
	}
}
