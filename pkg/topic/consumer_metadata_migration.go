package topic

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
)

const consumerMetadataMigrationVersion = 1

type ConsumerMetadataRecordSelector struct {
	SegmentState string `json:"segment_state"`
	LogPartition int    `json:"log_partition"`
	SegmentBase  uint64 `json:"segment_base"`
	RecordOffset uint64 `json:"record_offset"`
}

type ConsumerMetadataGroupSelection struct {
	Group          string                           `json:"group"`
	Topic          string                           `json:"topic"`
	PartitionCount int                              `json:"partition_count"`
	Deleted        bool                             `json:"deleted,omitempty"`
	Records        []ConsumerMetadataRecordSelector `json:"records"`
}

type ConsumerMetadataSelection struct {
	Version int                              `json:"version"`
	Groups  []ConsumerMetadataGroupSelection `json:"groups"`
}

type consumerMetadataMigrationManifest struct {
	Version         int                                  `json:"version"`
	Authoritative   bool                                 `json:"authoritative"`
	InventorySHA256 string                               `json:"inventory_sha256"`
	Records         []coordinator.ConsumerMetadataRecord `json:"records"`
}

type ConsumerMetadataMigrationResult struct {
	Changed         bool                                 `json:"changed"`
	Committed       bool                                 `json:"committed"`
	InventorySHA256 string                               `json:"inventory_sha256"`
	Records         []coordinator.ConsumerMetadataRecord `json:"records"`
	Inventory       StorageInventory                     `json:"inventory"`
}

func ReadConsumerMetadataSelection(path string) (ConsumerMetadataSelection, error) {
	file, err := os.Open(path) // #nosec G304 -- operator supplied maintenance input.
	if err != nil {
		return ConsumerMetadataSelection{}, fmt.Errorf("open consumer metadata selection: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return ConsumerMetadataSelection{}, fmt.Errorf("stat consumer metadata selection: %w", err)
	}
	if info.Size() > maxTopicMetadataBytes {
		return ConsumerMetadataSelection{}, fmt.Errorf("consumer metadata selection exceeds %d bytes", maxTopicMetadataBytes)
	}
	decoder := json.NewDecoder(io.LimitReader(file, maxTopicMetadataBytes+1))
	decoder.DisallowUnknownFields()
	var selection ConsumerMetadataSelection
	if err := decoder.Decode(&selection); err != nil {
		return ConsumerMetadataSelection{}, fmt.Errorf("decode consumer metadata selection: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ConsumerMetadataSelection{}, fmt.Errorf("consumer metadata selection has trailing content")
	}
	if selection.Version != consumerMetadataMigrationVersion {
		return ConsumerMetadataSelection{}, fmt.Errorf("unsupported consumer metadata selection version %d", selection.Version)
	}
	return selection, nil
}

func CreateConsumerMetadataMigration(logDir string, selection ConsumerMetadataSelection, dryRun bool) (ConsumerMetadataMigrationResult, error) {
	inventory, err := InspectStandaloneStorage(logDir)
	if err != nil {
		return ConsumerMetadataMigrationResult{}, err
	}
	result := ConsumerMetadataMigrationResult{Inventory: inventory}
	if len(inventory.Problems) != 0 {
		return result, fmt.Errorf("persisted storage has %d validation problem(s)", len(inventory.Problems))
	}
	records, err := selectedConsumerMetadataRecords(selection, inventory)
	if err != nil {
		return result, err
	}
	fingerprint, err := inventoryFingerprint(inventory)
	if err != nil {
		return result, err
	}
	result.InventorySHA256 = fingerprint
	result.Records = records

	root, err := safeStorageRoot(logDir)
	if err != nil {
		return result, err
	}
	path := filepath.Join(root, config.ConsumerMetadataMigrationFileName)
	existing, present, err := readConsumerMetadataMigration(path)
	if err != nil {
		return result, err
	}
	if present {
		if existing.Authoritative && reflect.DeepEqual(existing.Records, records) {
			return result, nil
		}
		return result, fmt.Errorf("consumer metadata migration already exists with different selection")
	}
	if dryRun {
		return result, nil
	}

	second, err := InspectStandaloneStorage(root)
	if err != nil {
		return result, err
	}
	if !reflect.DeepEqual(inventory, second) {
		return result, fmt.Errorf("persisted storage changed during consumer metadata validation")
	}
	manifest := consumerMetadataMigrationManifest{
		Version:         consumerMetadataMigrationVersion,
		Authoritative:   true,
		InventorySHA256: fingerprint,
		Records:         records,
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return result, fmt.Errorf("marshal consumer metadata migration: %w", err)
	}
	data = append(data, '\n')
	committed, err := installManifestExclusive(root, path, data)
	result.Committed = committed
	result.Changed = committed
	return result, err
}

func selectedConsumerMetadataRecords(selection ConsumerMetadataSelection, inventory StorageInventory) ([]coordinator.ConsumerMetadataRecord, error) {
	if selection.Version != consumerMetadataMigrationVersion {
		return nil, fmt.Errorf("unsupported consumer metadata selection version %d", selection.Version)
	}
	available := make(map[string]PersistedConsumerMetadataRecord, len(inventory.ConsumerMetadataRecords))
	for _, record := range inventory.ConsumerMetadataRecords {
		available[consumerMetadataSelectorKey(ConsumerMetadataRecordSelector{
			SegmentState: record.SegmentState,
			LogPartition: record.LogPartition,
			SegmentBase:  record.SegmentBase,
			RecordOffset: record.RecordOffset,
		})] = record
	}
	topicPartitions := make(map[string]int, len(inventory.Topics))
	for _, persistedTopic := range inventory.Topics {
		topicPartitions[persistedTopic.Name] = len(persistedTopic.Partitions)
	}

	seenGroups := make(map[string]struct{}, len(selection.Groups))
	usedSelectors := make(map[string]struct{})
	records := make([]coordinator.ConsumerMetadataRecord, 0, len(selection.Groups))
	for _, groupSelection := range selection.Groups {
		if groupSelection.Group == "" || groupSelection.Topic == "" || groupSelection.PartitionCount <= 0 {
			return nil, fmt.Errorf("consumer metadata group selection requires group, topic, and positive partition_count")
		}
		if _, exists := seenGroups[groupSelection.Group]; exists {
			return nil, fmt.Errorf("duplicate consumer metadata group selection %q", groupSelection.Group)
		}
		seenGroups[groupSelection.Group] = struct{}{}
		if persistedCount, exists := topicPartitions[groupSelection.Topic]; !exists {
			return nil, fmt.Errorf("selected group %q references topic %q absent from inventory", groupSelection.Group, groupSelection.Topic)
		} else if persistedCount != groupSelection.PartitionCount {
			return nil, fmt.Errorf("selected group %q partition_count=%d but topic inventory has %d", groupSelection.Group, groupSelection.PartitionCount, persistedCount)
		}

		offsets := make(map[int]uint64)
		for _, selector := range groupSelection.Records {
			if selector.SegmentState != "active" && selector.SegmentState != "deleted" {
				return nil, fmt.Errorf("selector for group %q has invalid segment_state %q", groupSelection.Group, selector.SegmentState)
			}
			key := consumerMetadataSelectorKey(selector)
			if _, duplicate := usedSelectors[key]; duplicate {
				return nil, fmt.Errorf("consumer metadata record selector %s is used more than once", key)
			}
			usedSelectors[key] = struct{}{}
			selected, exists := available[key]
			if !exists {
				return nil, fmt.Errorf("selected consumer metadata record %s was not found", key)
			}
			if selected.RecordType != "legacy_offset" {
				return nil, fmt.Errorf("selected consumer metadata record %s is not a legacy offset record", key)
			}
			if selected.Group != groupSelection.Group || selected.Topic != groupSelection.Topic {
				return nil, fmt.Errorf("selected record %s belongs to group=%q topic=%q", key, selected.Group, selected.Topic)
			}
			for _, item := range selected.Offsets {
				if item.Partition < 0 || item.Partition >= groupSelection.PartitionCount {
					return nil, fmt.Errorf("selected record %s partition %d exceeds partition_count=%d", key, item.Partition, groupSelection.PartitionCount)
				}
				if current, exists := offsets[item.Partition]; exists && item.Offset < current {
					return nil, fmt.Errorf("selected records regress group=%s topic=%s partition=%d from %d to %d", groupSelection.Group, groupSelection.Topic, item.Partition, current, item.Offset)
				}
				offsets[item.Partition] = item.Offset
			}
		}

		recordType := coordinator.ConsumerMetadataRecordRegistration
		initial := make([]coordinator.TopicOffsetSnapshot, 0, 1)
		if groupSelection.Deleted {
			recordType = coordinator.ConsumerMetadataRecordTombstone
			if len(groupSelection.Records) != 0 {
				return nil, fmt.Errorf("deleted group %q cannot select live offset records", groupSelection.Group)
			}
		} else if len(offsets) != 0 {
			items := make([]coordinator.OffsetItem, 0, len(offsets))
			for partition, offset := range offsets {
				items = append(items, coordinator.OffsetItem{Partition: partition, Offset: offset})
			}
			sort.Slice(items, func(i, j int) bool { return items[i].Partition < items[j].Partition })
			initial = append(initial, coordinator.TopicOffsetSnapshot{Topic: groupSelection.Topic, Offsets: items})
		}
		record := coordinator.ConsumerMetadataRecord{
			Version:        coordinator.ConsumerMetadataRecordVersion,
			Type:           recordType,
			Group:          groupSelection.Group,
			Topic:          groupSelection.Topic,
			PartitionCount: groupSelection.PartitionCount,
			Epoch:          1,
			InitialOffsets: initial,
			Timestamp:      time.Unix(0, 0).UTC(),
		}
		if recordType == coordinator.ConsumerMetadataRecordTombstone {
			record.PartitionCount = 0
			record.InitialOffsets = nil
		}
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Group < records[j].Group })
	return records, nil
}

func consumerMetadataSelectorKey(selector ConsumerMetadataRecordSelector) string {
	return fmt.Sprintf("%s:P%d:S%d:O%d", selector.SegmentState, selector.LogPartition, selector.SegmentBase, selector.RecordOffset)
}

func inventoryFingerprint(inventory StorageInventory) (string, error) {
	data, err := json.Marshal(inventory)
	if err != nil {
		return "", fmt.Errorf("marshal storage inventory fingerprint: %w", err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func readConsumerMetadataMigration(path string) (consumerMetadataMigrationManifest, bool, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path is broker-owned or supplied by the maintenance root.
	if errors.Is(err, os.ErrNotExist) {
		return consumerMetadataMigrationManifest{}, false, nil
	}
	if err != nil {
		return consumerMetadataMigrationManifest{}, false, fmt.Errorf("read consumer metadata migration: %w", err)
	}
	if len(data) > maxTopicMetadataBytes {
		return consumerMetadataMigrationManifest{}, false, fmt.Errorf("consumer metadata migration exceeds %d bytes", maxTopicMetadataBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest consumerMetadataMigrationManifest
	if err := decoder.Decode(&manifest); err != nil {
		return consumerMetadataMigrationManifest{}, false, fmt.Errorf("decode consumer metadata migration: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return consumerMetadataMigrationManifest{}, false, fmt.Errorf("consumer metadata migration has trailing content")
	}
	if manifest.Version != consumerMetadataMigrationVersion || !manifest.Authoritative {
		return consumerMetadataMigrationManifest{}, false, fmt.Errorf("unsupported or non-authoritative consumer metadata migration")
	}
	fingerprint, fingerprintErr := hex.DecodeString(manifest.InventorySHA256)
	if fingerprintErr != nil || len(fingerprint) != sha256.Size {
		return consumerMetadataMigrationManifest{}, false, fmt.Errorf("consumer metadata migration has invalid inventory_sha256")
	}
	seen := make(map[string]struct{}, len(manifest.Records))
	for _, record := range manifest.Records {
		payload, err := json.Marshal(record)
		if err != nil {
			return consumerMetadataMigrationManifest{}, false, err
		}
		decoded, versioned, err := coordinator.DecodeConsumerMetadataRecord(string(payload))
		if err != nil {
			return consumerMetadataMigrationManifest{}, false, fmt.Errorf("invalid migrated consumer metadata record for group %q: %w", record.Group, err)
		}
		if !versioned {
			return consumerMetadataMigrationManifest{}, false, fmt.Errorf("migrated consumer metadata record for group %q is not versioned", record.Group)
		}
		if _, exists := seen[decoded.Group]; exists {
			return consumerMetadataMigrationManifest{}, false, fmt.Errorf("duplicate migrated consumer group %q", decoded.Group)
		}
		seen[decoded.Group] = struct{}{}
	}
	return manifest, true, nil
}

// ConsumerMetadataMigrationRecords returns an immutable authoritative baseline
// selected by the operator during pre-manifest migration.
func (tm *TopicManager) ConsumerMetadataMigrationRecords() ([]coordinator.ConsumerMetadataRecord, bool, error) {
	if tm == nil || tm.cfg == nil || tm.cfg.EnabledDistribution || tm.cfg.LogDir == "" {
		return nil, false, nil
	}
	manifest, present, err := readConsumerMetadataMigration(filepath.Join(tm.cfg.LogDir, config.ConsumerMetadataMigrationFileName))
	if err != nil || !present {
		return nil, present, err
	}
	records := append([]coordinator.ConsumerMetadataRecord(nil), manifest.Records...)
	return records, true, nil
}
