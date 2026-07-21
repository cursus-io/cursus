package topic

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/util"
)

const (
	maxPersistedRecordBytes          = 16 << 20
	persistedCompactionMarkerVersion = "cursus-compaction-v1\n"
)

var persistedSegmentName = regexp.MustCompile(`^partition_(0|[1-9][0-9]*)_segment_([0-9]{20})\.log$`)

// StorageProblem describes persisted storage that cannot safely be adopted.
type StorageProblem struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

// PersistedSegment is a read-only description of one log segment.
type PersistedSegment struct {
	BaseOffset       uint64 `json:"base_offset"`
	Path             string `json:"path"`
	Size             int64  `json:"size"`
	ModifiedUnixNano int64  `json:"modified_unix_nano"`
}

// PersistedPartition is a read-only description of one topic partition.
type PersistedPartition struct {
	ID       int                `json:"id"`
	Segments []PersistedSegment `json:"segments"`
}

// PersistedTopic is a read-only description of one topic directory.
type PersistedTopic struct {
	Name       string               `json:"name"`
	Partitions []PersistedPartition `json:"partitions"`
}

// StorageInventory reports standalone topic storage without opening handlers.
type StorageInventory struct {
	ManifestPresent bool             `json:"manifest_present"`
	ManifestTopics  []string         `json:"manifest_topics,omitempty"`
	Topics          []PersistedTopic `json:"topics"`
	Problems        []StorageProblem `json:"problems,omitempty"`
}

// ManifestMigrationResult reports whether a manifest was published.
type ManifestMigrationResult struct {
	Changed   bool             `json:"changed"`
	Committed bool             `json:"committed"`
	Inventory StorageInventory `json:"inventory"`
}

// ArchiveResult reports one atomic orphan-directory move.
type ArchiveResult struct {
	Changed     bool             `json:"changed"`
	Committed   bool             `json:"committed"`
	Source      string           `json:"source"`
	Destination string           `json:"destination"`
	Problems    []StorageProblem `json:"problems,omitempty"`
}

// InspectStandaloneStorage inventories persisted topic logs without opening a DiskHandler.
func InspectStandaloneStorage(logDir string) (StorageInventory, error) {
	root, err := safeStorageRoot(logDir)
	if err != nil {
		return StorageInventory{}, err
	}

	inventory := StorageInventory{Topics: make([]PersistedTopic, 0)}
	manifestPath := filepath.Join(root, TopicMetadataFileName)
	definitions, err := readStandaloneManifestIfPresent(manifestPath)
	if err != nil {
		return StorageInventory{}, err
	}
	if definitions != nil {
		inventory.ManifestPresent = true
		for _, definition := range definitions {
			inventory.ManifestTopics = append(inventory.ManifestTopics, definition.Name)
		}
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		return StorageInventory{}, fmt.Errorf("read log directory: %w", err)
	}
	for _, entry := range entries {
		if entry.Name() == "raft" {
			continue
		}
		name := entry.Name()
		if ValidateName(name) != nil {
			continue
		}
		topicPath := filepath.Join(root, name)
		if entry.Type()&os.ModeSymlink != 0 {
			inventory.Problems = append(inventory.Problems, storageProblem(root, topicPath, "topic storage entry is a symbolic link"))
			continue
		}
		if !entry.IsDir() {
			continue
		}
		info, statErr := os.Lstat(topicPath)
		if statErr != nil {
			return StorageInventory{}, fmt.Errorf("inspect topic directory %q: %w", name, statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			inventory.Problems = append(inventory.Problems, storageProblem(root, topicPath, "topic directory is a symbolic link"))
			continue
		}

		topicInventory, candidate, problems, inspectErr := inspectPersistedTopic(root, name, topicPath)
		if inspectErr != nil {
			return StorageInventory{}, inspectErr
		}
		inventory.Problems = append(inventory.Problems, problems...)
		if candidate {
			inventory.Topics = append(inventory.Topics, topicInventory)
		}
	}
	sort.Slice(inventory.Topics, func(i, j int) bool { return inventory.Topics[i].Name < inventory.Topics[j].Name })
	sort.Slice(inventory.Problems, func(i, j int) bool {
		if inventory.Problems[i].Path == inventory.Problems[j].Path {
			return inventory.Problems[i].Message < inventory.Problems[j].Message
		}
		return inventory.Problems[i].Path < inventory.Problems[j].Path
	})
	return inventory, nil
}

func inspectPersistedTopic(root, name, topicPath string) (PersistedTopic, bool, []StorageProblem, error) {
	entries, err := os.ReadDir(topicPath)
	if err != nil {
		return PersistedTopic{}, false, nil, fmt.Errorf("read topic directory %q: %w", name, err)
	}
	segments := make(map[int][]PersistedSegment)
	problems := make([]StorageProblem, 0)
	candidate := false
	for _, entry := range entries {
		fileName := entry.Name()
		matches := persistedSegmentName.FindStringSubmatch(fileName)
		if matches == nil {
			if strings.HasPrefix(fileName, "partition_") && strings.HasSuffix(fileName, ".log") {
				candidate = true
				problems = append(problems, storageProblem(root, filepath.Join(topicPath, fileName), "non-canonical partition log name"))
			}
			continue
		}
		candidate = true
		path := filepath.Join(topicPath, fileName)
		info, statErr := os.Lstat(path)
		if statErr != nil {
			return PersistedTopic{}, false, nil, fmt.Errorf("stat persisted segment %q: %w", path, statErr)
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			problems = append(problems, storageProblem(root, path, "partition log is not a regular file"))
			continue
		}
		partition64, partitionErr := strconv.ParseInt(matches[1], 10, 32)
		baseOffset, baseErr := strconv.ParseUint(matches[2], 10, 64)
		if partitionErr != nil || baseErr != nil {
			problems = append(problems, storageProblem(root, path, "partition or segment offset is out of range"))
			continue
		}
		partition := int(partition64)
		segments[partition] = append(segments[partition], PersistedSegment{BaseOffset: baseOffset, Path: relativeStoragePath(root, path), Size: info.Size(), ModifiedUnixNano: info.ModTime().UnixNano()})
	}
	if !candidate {
		return PersistedTopic{}, false, problems, nil
	}

	topic := PersistedTopic{Name: name}
	partitionIDs := make([]int, 0, len(segments))
	for id := range segments {
		partitionIDs = append(partitionIDs, id)
	}
	sort.Ints(partitionIDs)
	for _, id := range partitionIDs {
		partitionSegments := segments[id]
		sort.Slice(partitionSegments, func(i, j int) bool { return partitionSegments[i].BaseOffset < partitionSegments[j].BaseOffset })
		var previousLast uint64
		previousAllowsGaps := false
		havePrevious := false
		for segmentIndex, segment := range partitionSegments {
			active := segmentIndex == len(partitionSegments)-1
			first, last, count, allowsGaps, scanProblems, scanErr := scanPersistedSegment(root, name, id, segment, active)
			if scanErr != nil {
				return PersistedTopic{}, false, nil, scanErr
			}
			problems = append(problems, scanProblems...)
			if count > 0 && havePrevious {
				if first <= previousLast {
					problems = append(problems, StorageProblem{Path: segment.Path, Message: "segment offsets overlap or regress"})
				} else if first != previousLast+1 && !previousAllowsGaps && !allowsGaps {
					problems = append(problems, StorageProblem{Path: segment.Path, Message: "segment offsets are not contiguous"})
				}
			}
			if count > 0 {
				previousLast = last
				previousAllowsGaps = allowsGaps
				havePrevious = true
			} else if havePrevious && allowsGaps {
				previousAllowsGaps = true
			}
		}
		topic.Partitions = append(topic.Partitions, PersistedPartition{ID: id, Segments: partitionSegments})
	}
	return topic, true, problems, nil
}

func scanPersistedSegment(root, topicName string, partition int, segment PersistedSegment, active bool) (uint64, uint64, int, bool, []StorageProblem, error) {
	path := filepath.Join(root, filepath.FromSlash(segment.Path))
	file, err := os.Open(path) // #nosec G304 -- path came from a contained, validated directory entry.
	if err != nil {
		return 0, 0, 0, false, nil, fmt.Errorf("open persisted segment %q: %w", segment.Path, err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	markerAllowsGaps, markerProblem := compactionMarkerStatus(path, segment.Size)
	allowGaps := markerAllowsGaps && !active
	var first, previous uint64
	count := 0
	problems := make([]StorageProblem, 0)
	if markerProblem != "" {
		problems = append(problems, StorageProblem{Path: segment.Path, Message: markerProblem})
	}
	for {
		var lengthBytes [4]byte
		_, readErr := io.ReadFull(reader, lengthBytes[:])
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			problems = append(problems, StorageProblem{Path: segment.Path, Message: "truncated record length"})
			break
		}
		length := binary.BigEndian.Uint32(lengthBytes[:])
		if length == 0 || length > maxPersistedRecordBytes {
			problems = append(problems, StorageProblem{Path: segment.Path, Message: fmt.Sprintf("invalid record length %d", length)})
			break
		}
		payload := make([]byte, int(length))
		if _, readErr = io.ReadFull(reader, payload); readErr != nil {
			problems = append(problems, StorageProblem{Path: segment.Path, Message: "truncated record payload"})
			break
		}
		message, decodeErr := util.DeserializeDiskMessage(payload)
		if decodeErr != nil {
			problems = append(problems, StorageProblem{Path: segment.Path, Message: "invalid record: " + decodeErr.Error()})
			break
		}
		if message.Topic != topicName || int(message.Partition) != partition {
			problems = append(problems, StorageProblem{Path: segment.Path, Message: "record topic or partition does not match its path"})
		}
		if count == 0 {
			first = message.Offset
			if !allowGaps && first != segment.BaseOffset {
				problems = append(problems, StorageProblem{Path: segment.Path, Message: fmt.Sprintf("first offset %d does not match segment base %d", first, segment.BaseOffset)})
			}
		} else if message.Offset <= previous || (!allowGaps && message.Offset != previous+1) {
			problems = append(problems, StorageProblem{Path: segment.Path, Message: "record offsets are not strictly contiguous"})
		}
		previous = message.Offset
		count++
	}
	return first, previous, count, allowGaps, problems, nil
}

func compactionMarkerStatus(logPath string, size int64) (bool, string) {
	markers, _ := filepath.Glob(logPath + ".compacted-*")
	for _, marker := range markers {
		suffix := strings.TrimPrefix(marker, logPath+".compacted-")
		expected, err := strconv.ParseInt(suffix, 10, 64)
		if err != nil || expected != size {
			continue
		}
		info, err := os.Lstat(marker)
		if err != nil {
			return false, "inspect current compaction marker: " + err.Error()
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return false, "current compaction marker is not a regular file"
		}
		if info.Size() != int64(len(persistedCompactionMarkerVersion)) {
			return false, "current compaction marker has invalid content"
		}
		data, err := os.ReadFile(marker) // #nosec G304 -- marker is adjacent to a contained validated log.
		if err != nil {
			return false, "read current compaction marker: " + err.Error()
		}
		if string(data) != persistedCompactionMarkerVersion {
			return false, "current compaction marker has invalid content"
		}
		return true, ""
	}
	return false, ""
}

// CreateStandaloneManifest publishes explicit definitions without replacing an existing manifest.
func CreateStandaloneManifest(logDir string, definitions []Definition, dryRun bool) (ManifestMigrationResult, error) {
	inventory, err := InspectStandaloneStorage(logDir)
	if err != nil {
		return ManifestMigrationResult{}, err
	}
	result := ManifestMigrationResult{Inventory: inventory}
	normalized, data, err := normalizeAndMarshalManifest(definitions)
	if err != nil {
		return result, err
	}
	root, err := safeStorageRoot(logDir)
	if err != nil {
		return result, err
	}
	manifestPath := filepath.Join(root, TopicMetadataFileName)
	if inventory.ManifestPresent {
		existing, readErr := readStandaloneManifest(manifestPath)
		if readErr != nil {
			return result, readErr
		}
		if reflect.DeepEqual(existing, normalized) {
			if len(inventory.Problems) > 0 {
				return result, fmt.Errorf("persisted topic storage has %d validation problem(s)", len(inventory.Problems))
			}
			if err := definitionsMatchInventory(existing, inventory.Topics); err != nil {
				return result, fmt.Errorf("existing topic metadata is not aligned with storage: %w", err)
			}
			return result, nil
		}
		return result, fmt.Errorf("topic metadata manifest already exists with different definitions")
	}
	if len(inventory.Problems) > 0 {
		return result, fmt.Errorf("persisted topic storage has %d validation problem(s)", len(inventory.Problems))
	}
	if err := definitionsMatchInventory(normalized, inventory.Topics); err != nil {
		return result, err
	}
	if dryRun {
		return result, nil
	}

	second, err := InspectStandaloneStorage(root)
	if err != nil {
		return result, err
	}
	if !reflect.DeepEqual(inventory, second) {
		return result, fmt.Errorf("persisted topic storage changed during validation")
	}
	committed, err := installManifestExclusive(root, manifestPath, data)
	result.Committed = committed
	result.Changed = committed
	return result, err
}

func definitionsMatchInventory(definitions []Definition, topics []PersistedTopic) error {
	if len(definitions) != len(topics) {
		return fmt.Errorf("explicit definitions cover %d topics but storage contains %d", len(definitions), len(topics))
	}
	byName := make(map[string]PersistedTopic, len(topics))
	for _, topic := range topics {
		byName[topic.Name] = topic
	}
	for _, definition := range definitions {
		persisted, ok := byName[definition.Name]
		if !ok {
			return fmt.Errorf("definition %q has no persisted topic storage", definition.Name)
		}
		if len(persisted.Partitions) != definition.Partitions {
			return fmt.Errorf("topic %q definition has %d partitions but storage has %d", definition.Name, definition.Partitions, len(persisted.Partitions))
		}
		for expected, partition := range persisted.Partitions {
			if partition.ID != expected {
				return fmt.Errorf("topic %q persisted partition IDs are not contiguous from zero", definition.Name)
			}
		}
	}
	return nil
}

func normalizeAndMarshalManifest(definitions []Definition) ([]Definition, []byte, error) {
	normalized := make([]Definition, 0, len(definitions))
	seen := make(map[string]struct{}, len(definitions))
	for _, raw := range definitions {
		definition, err := raw.Normalize()
		if err != nil {
			return nil, nil, fmt.Errorf("invalid topic metadata for %q: %w", raw.Name, err)
		}
		if _, exists := seen[definition.Name]; exists {
			return nil, nil, fmt.Errorf("duplicate topic metadata for %q", definition.Name)
		}
		seen[definition.Name] = struct{}{}
		if err := validateCleanupPolicyForTopic(definition.Policy, nil, definition.EventSourcing); err != nil {
			return nil, nil, fmt.Errorf("invalid topic metadata for %q: %w", definition.Name, err)
		}
		normalized = append(normalized, definition)
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].Name < normalized[j].Name })
	data, err := json.MarshalIndent(topicMetadataManifest{Version: topicMetadataFormatVersion, Topics: normalized}, "", "  ")
	if err != nil {
		return nil, nil, fmt.Errorf("marshal topic metadata: %w", err)
	}
	data = append(data, '\n')
	if len(data) > maxTopicMetadataBytes {
		return nil, nil, fmt.Errorf("topic metadata size %d exceeds limit %d", len(data), maxTopicMetadataBytes)
	}
	return normalized, data, nil
}

func installManifestExclusive(root, path string, data []byte) (committed bool, err error) {
	tmp, err := os.CreateTemp(root, ".topic-metadata-migration-*")
	if err != nil {
		return false, fmt.Errorf("create topic metadata temporary file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return false, fmt.Errorf("secure topic metadata temporary file: %w", err)
	}
	if err := writeMetadataFile(tmp, data); err != nil {
		return false, err
	}
	if err := tmp.Close(); err != nil {
		return false, fmt.Errorf("close topic metadata temporary file: %w", err)
	}
	if err := installCheckpointFileExclusive(tmpPath, path); err != nil {
		return false, fmt.Errorf("publish topic metadata without overwrite: %w", err)
	}
	committed = true
	if err := syncTopicMetadataDirectoryFn(root); err != nil {
		return true, fmt.Errorf("sync topic metadata directory: %w", err)
	}
	return true, nil
}

// ArchiveOrphanTopic atomically moves one manifest-omitted topic directory.
func ArchiveOrphanTopic(logDir, archiveDir, topicName string, dryRun bool) (ArchiveResult, error) {
	if err := ValidateName(topicName); err != nil {
		return ArchiveResult{}, err
	}
	if strings.TrimSpace(archiveDir) == "" {
		return ArchiveResult{}, fmt.Errorf("archive directory is empty")
	}
	inventory, err := InspectStandaloneStorage(logDir)
	if err != nil {
		return ArchiveResult{}, err
	}
	for _, declared := range inventory.ManifestTopics {
		if declared == topicName {
			return ArchiveResult{}, fmt.Errorf("topic %q is declared in the manifest", topicName)
		}
	}
	var persisted *PersistedTopic
	for i := range inventory.Topics {
		if inventory.Topics[i].Name == topicName {
			persisted = &inventory.Topics[i]
			break
		}
	}
	if persisted == nil {
		return ArchiveResult{}, fmt.Errorf("topic %q is not a persisted orphan candidate", topicName)
	}
	root, err := safeStorageRoot(logDir)
	if err != nil {
		return ArchiveResult{}, err
	}
	archiveRoot, err := filepath.Abs(filepath.Clean(archiveDir))
	if err != nil {
		return ArchiveResult{}, fmt.Errorf("resolve archive directory: %w", err)
	}
	if err := validateArchiveRoot(archiveRoot, true); err != nil {
		return ArchiveResult{}, err
	}
	if pathWithin(root, archiveRoot) {
		return ArchiveResult{}, fmt.Errorf("archive directory must be outside the log directory")
	}
	insideLogRoot, err := archiveRootWithinLogRoot(root, archiveRoot)
	if err != nil {
		return ArchiveResult{}, err
	}
	if insideLogRoot {
		return ArchiveResult{}, fmt.Errorf("archive directory resolves inside the log directory")
	}
	source := filepath.Join(root, topicName)
	destination := filepath.Join(archiveRoot, topicName)
	result := ArchiveResult{Source: source, Destination: destination}
	for _, problem := range inventory.Problems {
		if problem.Path == topicName || strings.HasPrefix(problem.Path, topicName+"/") {
			result.Problems = append(result.Problems, problem)
		}
	}
	if _, err := os.Lstat(destination); err == nil {
		return result, fmt.Errorf("archive destination already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, fmt.Errorf("inspect archive destination: %w", err)
	}
	if dryRun {
		return result, nil
	}
	if err := os.MkdirAll(archiveRoot, 0o700); err != nil {
		return result, fmt.Errorf("create archive directory: %w", err)
	}
	if err := validateArchiveRoot(archiveRoot, false); err != nil {
		return result, err
	}
	insideLogRoot, err = archiveRootWithinLogRoot(root, archiveRoot)
	if err != nil {
		return result, err
	}
	if insideLogRoot {
		return result, fmt.Errorf("archive directory resolves inside the log directory")
	}
	second, err := InspectStandaloneStorage(root)
	if err != nil {
		return result, err
	}
	if !reflect.DeepEqual(inventory, second) {
		return result, fmt.Errorf("persisted topic storage changed during validation")
	}
	if err := archiveTopicDirectoryExclusive(source, destination); err != nil {
		return result, fmt.Errorf("atomically archive orphan topic: %w", err)
	}
	result.Changed = true
	result.Committed = true
	sourceSyncErr := syncTopicMetadataDirectoryFn(root)
	archiveSyncErr := syncTopicMetadataDirectoryFn(archiveRoot)
	if err := errors.Join(sourceSyncErr, archiveSyncErr); err != nil {
		return result, fmt.Errorf("sync archive directory move: %w", err)
	}
	return result, nil
}

func safeStorageRoot(logDir string) (string, error) {
	if strings.TrimSpace(logDir) == "" {
		return "", fmt.Errorf("log directory is empty")
	}
	root, err := filepath.Abs(filepath.Clean(logDir))
	if err != nil {
		return "", fmt.Errorf("resolve log directory: %w", err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return "", fmt.Errorf("inspect log directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("log directory must be a real directory")
	}
	return root, nil
}

func pathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator))
}

func relativeStoragePath(root, path string) string {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return path
	}
	return filepath.ToSlash(relative)
}

func storageProblem(root, path, message string) StorageProblem {
	return StorageProblem{Path: relativeStoragePath(root, path), Message: message}
}
