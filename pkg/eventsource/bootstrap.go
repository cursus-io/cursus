package eventsource

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
)

// Bootstrap catalogs are complete inventories, never incremental snapshot updates.
// Pointer identity fields distinguish an absent field from a legitimate zero.
type bootstrapCatalog struct {
	Version        int                 `json:"version"`
	Topic          string              `json:"topic"`
	Partition      *int                `json:"partition"`
	LifecycleEpoch *uint64             `json:"lifecycle_epoch"`
	Snapshots      []bootstrapSnapshot `json:"snapshots"`
}

// SnapshotStore treats keys and payloads as bytes held in Go strings. The wire
// schema uses explicit base64 fields so JSON never replaces invalid UTF-8 bytes.
type bootstrapSnapshot struct {
	Key     string
	Version uint64
	Payload string
}

type bootstrapSnapshotJSON struct {
	KeyBase64     *string `json:"key_base64"`
	Version       uint64  `json:"version"`
	PayloadBase64 *string `json:"payload_base64"`
}

func (s bootstrapSnapshot) MarshalJSON() ([]byte, error) {
	key := base64.StdEncoding.EncodeToString([]byte(s.Key))
	payload := base64.StdEncoding.EncodeToString([]byte(s.Payload))
	return json.Marshal(bootstrapSnapshotJSON{KeyBase64: &key, Version: s.Version, PayloadBase64: &payload})
}

func (s *bootstrapSnapshot) UnmarshalJSON(data []byte) error {
	var record bootstrapSnapshotJSON
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return err
	}
	if record.KeyBase64 == nil || record.PayloadBase64 == nil {
		return fmt.Errorf("missing bootstrap snapshot byte field")
	}
	key, err := decodeBootstrapBytes(*record.KeyBase64, 65535)
	if err != nil {
		return fmt.Errorf("bootstrap snapshot key: %w", err)
	}
	payload, err := decodeBootstrapBytes(*record.PayloadBase64, maxSnapshotPayloadBytes)
	if err != nil {
		return fmt.Errorf("bootstrap snapshot payload: %w", err)
	}
	*s = bootstrapSnapshot{Key: key, Version: record.Version, Payload: payload}
	return nil
}

func decodeBootstrapBytes(encoded string, maximum int) (string, error) {
	if len(encoded) > base64.StdEncoding.EncodedLen(maximum) {
		return "", fmt.Errorf("decoded byte limit exceeded")
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("invalid base64: %w", err)
	}
	// Strict decoding still permits CR/LF. Accept only the canonical spelling so
	// one byte identity has one representation in an authoritative inventory.
	if len(decoded) > maximum || base64.StdEncoding.EncodeToString(decoded) != encoded {
		return "", fmt.Errorf("noncanonical or oversized base64")
	}
	return string(decoded), nil
}

func decodeBootstrapCatalog(data []byte, name string, partition int, lifecycle uint64) (bootstrapCatalog, error) {
	var catalog bootstrapCatalog
	if len(data) > disk.BootstrapCatalogBytes {
		return catalog, fmt.Errorf("bootstrap catalog exceeds byte limit")
	}
	if err := rejectDuplicateJSONKeys(json.NewDecoder(bytes.NewReader(data)), false); err != nil {
		return catalog, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&catalog); err != nil {
		return catalog, fmt.Errorf("decode bootstrap catalog: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return catalog, fmt.Errorf("trailing bootstrap catalog data")
	}
	if catalog.Version != 1 || catalog.Topic != name || catalog.Partition == nil || *catalog.Partition != partition || catalog.LifecycleEpoch == nil || *catalog.LifecycleEpoch != lifecycle || catalog.Snapshots == nil {
		return catalog, fmt.Errorf("bootstrap catalog version or identity mismatch")
	}
	seen := make(map[string]bool, len(catalog.Snapshots))
	for _, snap := range catalog.Snapshots {
		if snap.Key == "" || len(snap.Key) > 65535 || snap.Version == 0 || len(snap.Payload) > maxSnapshotPayloadBytes || seen[snap.Key] {
			return catalog, fmt.Errorf("invalid or duplicate bootstrap snapshot key %q", snap.Key)
		}
		seen[snap.Key] = true
	}
	return catalog, nil
}

// JSON's usual last-key-wins decoding is unsafe for an authoritative inventory.
func rejectDuplicateJSONKeys(d *json.Decoder, snapshot bool) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	if token == nil {
		return fmt.Errorf("null bootstrap catalog field")
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			token, err := d.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok || seen[key] {
				return fmt.Errorf("duplicate JSON field %q", token)
			}
			seen[key] = true
			allowed := key == "version" || key == "topic" || key == "partition" || key == "lifecycle_epoch" || key == "snapshots"
			if snapshot {
				allowed = key == "key_base64" || key == "version" || key == "payload_base64"
			}
			if !allowed {
				return fmt.Errorf("unknown bootstrap catalog field %q", key)
			}
			if err := rejectDuplicateJSONKeys(d, key == "snapshots"); err != nil {
				return err
			}
		}
		expectedFields := 5
		if snapshot {
			expectedFields = 3
		}
		if len(seen) != expectedFields {
			return fmt.Errorf("missing bootstrap catalog field")
		}
	case '[':
		for d.More() {
			if err := rejectDuplicateJSONKeys(d, snapshot); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("invalid JSON delimiter")
	}
	_, err = d.Token()
	return err
}

type capturedBootstrapCatalog struct {
	data     []byte
	validate func(context.Context) error
}

// CaptureBootstrapCatalog requires the active exclusive source and fixed cut.
func (h *Handler) CaptureBootstrapCatalog(ctx context.Context, name string, partition int, source *topic.BootstrapSource, lifecycle uint64) ([]byte, error) {
	captured, err := h.captureBootstrapCatalog(ctx, name, partition, source, lifecycle)
	if err != nil {
		return nil, err
	}
	return captured.data, nil
}
func (h *Handler) captureBootstrapCatalog(ctx context.Context, name string, partition int, source *topic.BootstrapSource, lifecycle uint64) (*capturedBootstrapCatalog, error) {
	if source == nil {
		return nil, fmt.Errorf("bootstrap catalog requires exclusive callback source")
	}
	view := source.View()
	if err := view.ValidateBootstrapSource(name, partition, lifecycle); err != nil {
		return nil, err
	}
	cut, err := source.SnapshotReadCut()
	if err != nil {
		return nil, err
	}
	h.indexSyncMu.Lock()
	defer h.indexSyncMu.Unlock()
	proof, err := h.certifySnapshotIndex(ctx, name, partition, cut)
	if err != nil {
		return nil, err
	}

	ss, err := h.getSnapshotWithView(name, partition, view)
	if err != nil {
		return nil, err
	}
	catalog := bootstrapCatalog{Version: 1, Topic: name, Partition: &partition, LifecycleEpoch: &lifecycle, Snapshots: []bootstrapSnapshot{}}
	prefix, err := json.Marshal(catalog)
	if err != nil {
		return nil, err
	}
	size := len(prefix)
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	// Even empty payloads require at least this many encoded bytes per record.
	// Reject before allocating a key inventory larger than any valid catalog.
	const minimumRecordBytes = len(`{"key_base64":"YQ==","version":1,"payload_base64":""}`)
	if len(ss.index) > (disk.BootstrapCatalogBytes-size)/minimumRecordBytes {
		return nil, fmt.Errorf("bootstrap catalog exceeds byte limit")
	}
	keys := make([]string, 0, len(ss.index))
	for key := range ss.index {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		snap, err := ss.readAt(ss.index[key])
		if err != nil {
			return nil, err
		}
		current, err := proof.idx.VersionBefore(key, cut.LastStableOffset())
		if err != nil {
			return nil, err
		}
		if snap.Version > current {
			return nil, fmt.Errorf("snapshot_version_exceeds_stream key=%s version=%d current=%d", key, snap.Version, current)
		}
		record := bootstrapSnapshot{Key: key, Version: snap.Version, Payload: snap.Payload}
		encoded, err := json.Marshal(record)
		if err != nil {
			return nil, err
		}
		size += len(encoded)
		if len(catalog.Snapshots) > 0 {
			size++
		}
		if size > disk.BootstrapCatalogBytes {
			return nil, fmt.Errorf("bootstrap catalog exceeds byte limit")
		}
		catalog.Snapshots = append(catalog.Snapshots, record)
	}
	data, err := json.Marshal(catalog)
	if err != nil {
		return nil, err
	}
	if _, err := decodeBootstrapCatalog(data, name, partition, lifecycle); err != nil {
		return nil, err
	}
	guard := func(ctx context.Context) error {
		err := h.validateSnapshotIndex(ctx, snapshotCacheKey(name, partition), cut, proof, ss)
		if err != nil {
			h.retireIndex(snapshotCacheKey(name, partition), proof.idx, err)
		}
		return err
	}
	if err := guard(ctx); err != nil {
		return nil, err
	}
	return &capturedBootstrapCatalog{data: data, validate: guard}, nil
}

// DetachPartitionForBootstrap requires the partition's exclusive bootstrap
// session. Close all handles before Windows replaces the generation's files.
func (h *Handler) DetachPartitionForBootstrap(name string, partition int) error {
	key := name + ":" + strconv.Itoa(partition)
	h.mu.Lock()
	idx, ss := h.indexes[key], h.snapshots[key]
	delete(h.indexes, key)
	delete(h.snapshots, key)
	delete(h.indexedHWM, key)
	delete(h.replicaIndexGenerations, key)
	h.mu.Unlock()
	var errs []error
	if idx != nil {
		errs = append(errs, idx.Close())
	}
	if ss != nil {
		errs = append(errs, ss.Close())
	}
	return errors.Join(errs...)
}

var syncBootstrapIndex = (*StreamIndex).Sync
var syncBootstrapSnapshots = (*SnapshotStore).syncFile

// RebuildBootstrapState runs only in BootstrapSession.Complete with its offline
// view. Pending state protects readers throughout replacement and retries. The
// immutable staged catalog, rather than partly rebuilt local files, is authority.
func (h *Handler) RebuildBootstrapState(name string, partition int, view *topic.PartitionReadView, data []byte, lifecycle uint64) error {
	catalog, err := decodeBootstrapCatalog(data, name, partition, lifecycle)
	if err != nil {
		return err
	}
	if err := view.ValidateBootstrapRecovery(name, partition, lifecycle); err != nil {
		return err
	}
	end := view.LastStableOffset()
	if view.FirstOffset() > end {
		return fmt.Errorf("retention floor exceeds stable tail")
	}
	if view.FirstOffset() > 0 && len(catalog.Snapshots) == 0 {
		return fmt.Errorf("cannot recover retained event stream without a snapshot checkpoint")
	}
	if err := h.DetachPartitionForBootstrap(name, partition); err != nil {
		return err
	}
	dir := h.tm.GetLogDir(name, partition)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	for _, suffix := range []string{"stream.idx", "stream_keys.dat", "snapshots.dat", "snapshots_v2.dat"} {
		if err := os.Remove(filepath.Join(dir, fmt.Sprintf("partition_%d_%s", partition, suffix))); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	ss, err := NewSnapshotStore(dir, partition)
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			_ = ss.Close()
		}
	}()
	for _, snap := range catalog.Snapshots {
		if err := ss.Save(snap.Key, snap.Version, snap.Payload); err != nil {
			return err
		}
	}
	if err := syncBootstrapSnapshots(ss); err != nil {
		return fmt.Errorf("sync bootstrap snapshots: %w", err)
	}
	idx, err := NewStreamIndex(dir, partition)
	if err != nil {
		return err
	}
	defer func() {
		if !published {
			_ = idx.Close()
		}
	}()
	if view.FirstOffset() > 0 {
		for _, snap := range catalog.Snapshots {
			if err := idx.SeedVersion(snap.Key, snap.Version); err != nil {
				return err
			}
		}
	}
	if err := h.replayIndex(idx, view, view.FirstOffset(), end); err != nil {
		return err
	}
	if err := syncBootstrapIndex(idx); err != nil {
		return fmt.Errorf("sync bootstrap stream index: %w", err)
	}
	if err := syncSnapshotCreationDirectory(dir); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return fmt.Errorf("handler is closed")
	}
	key := name + ":" + strconv.Itoa(partition)
	h.indexes[key], h.snapshots[key], h.indexedHWM[key] = idx, ss, end
	published = true
	return nil
}
