package topic

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestInspectStandaloneStorageReadsLogsWithoutOpeningHandlers(t *testing.T) {
	root := t.TempDir()
	writePersistedTestSegment(t, root, "orders", 0, 0, []types.DiskMessage{
		{Topic: "orders", Partition: 0, Offset: 0, Payload: "first"},
		{Topic: "orders", Partition: 0, Offset: 1, Payload: "second"},
	})
	require.NoError(t, os.Mkdir(filepath.Join(root, "raft"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(root, "__transaction_state.journal"), []byte("state"), 0o600))

	inventory, err := InspectStandaloneStorage(root)
	require.NoError(t, err)
	require.False(t, inventory.ManifestPresent)
	require.Len(t, inventory.Topics, 1)
	require.Equal(t, "orders", inventory.Topics[0].Name)
	require.Equal(t, 0, inventory.Topics[0].Partitions[0].ID)
	require.Empty(t, inventory.Problems)
	require.NoFileExists(t, filepath.Join(root, "orders", "partition_0_segment_00000000000000000000.index"))
	require.NoFileExists(t, filepath.Join(root, "orders", "partition_0.hwm"))
}

func TestInspectStandaloneStorageReportsCorruptAndMismatchedLogs(t *testing.T) {
	root := t.TempDir()
	writePersistedTestSegment(t, root, "orders", 0, 0, []types.DiskMessage{
		{Topic: "wrong", Partition: 0, Offset: 0, Payload: "bad"},
	})
	malformed := filepath.Join(root, "orders", "partition_01_segment_0.log")
	require.NoError(t, os.WriteFile(malformed, []byte{0, 0}, 0o600))

	inventory, err := InspectStandaloneStorage(root)
	require.NoError(t, err)
	require.Len(t, inventory.Topics, 1)
	require.Len(t, inventory.Problems, 2)
	combined := inventory.Problems[0].Message + inventory.Problems[1].Message
	require.Contains(t, combined, "non-canonical")
	require.Contains(t, combined, "does not match")
}

func TestInspectStandaloneStorageAcceptsOnlyCurrentConsumerMetadata(t *testing.T) {
	root := t.TempDir()
	current, err := json.Marshal(coordinator.ConsumerMetadataRecord{
		Version: coordinator.ConsumerMetadataRecordVersion,
		Type:    coordinator.ConsumerMetadataRecordRegistration,
		Group:   "workers", Topic: "orders", PartitionCount: 1, Epoch: 1,
		Timestamp: time.Unix(1, 0).UTC(),
	})
	require.NoError(t, err)
	writePersistedTestSegment(t, root, config.ConsumerOffsetsTopicName, 0, 0, []types.DiskMessage{{
		Topic: config.ConsumerOffsetsTopicName, Partition: 0, Offset: 0, Payload: string(current),
	}})

	inventory, err := InspectStandaloneStorage(root)
	require.NoError(t, err)
	require.Empty(t, inventory.Problems)
	require.Len(t, inventory.ConsumerMetadataRecords, 1)
	require.Equal(t, coordinator.ConsumerMetadataRecordVersion, inventory.ConsumerMetadataRecords[0].Version)

	writePersistedTestSegment(t, root, config.ConsumerOffsetsTopicName, 1, 0, []types.DiskMessage{{
		Topic: config.ConsumerOffsetsTopicName, Partition: 1, Offset: 0,
		Payload: `{"group":"legacy","topic":"orders","partition":0,"offset":3}`,
	}})
	inventory, err = InspectStandaloneStorage(root)
	require.NoError(t, err)
	require.NotEmpty(t, inventory.Problems)
	require.Contains(t, inventory.Problems[0].Message, "clean bootstrap required")
}

func TestArchiveOrphanTopicMovesExactlyOneDirectory(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "logs")
	require.NoError(t, os.Mkdir(root, 0o750))
	writePersistedTestSegment(t, root, "declared", 0, 0, nil)
	writePersistedTestSegment(t, root, "orphan", 0, 0, []types.DiskMessage{{Topic: "orphan", Partition: 0, Offset: 0, Payload: "keep"}})
	store := &topicMetadataStore{path: filepath.Join(root, TopicMetadataFileName)}
	require.NoError(t, store.Save([]Definition{{Name: "declared", Partitions: 1, Policy: DefaultPolicy()}}))

	archive := filepath.Join(parent, "archive")
	dryRun, err := ArchiveOrphanTopic(root, archive, "orphan", true)
	require.NoError(t, err)
	require.False(t, dryRun.Changed)
	require.DirExists(t, filepath.Join(root, "orphan"))
	require.NoDirExists(t, archive)

	result, err := ArchiveOrphanTopic(root, archive, "orphan", false)
	require.NoError(t, err)
	require.True(t, result.Committed)
	require.NoDirExists(t, filepath.Join(root, "orphan"))
	require.FileExists(t, filepath.Join(archive, "orphan", "partition_0_segment_00000000000000000000.log"))
	require.DirExists(t, filepath.Join(root, "declared"))

	_, err = ArchiveOrphanTopic(root, archive, "declared", false)
	require.ErrorContains(t, err, "declared in the manifest")
}

func TestArchiveOrphanTopicRejectsArchiveInsideLogRoot(t *testing.T) {
	root := t.TempDir()
	writePersistedTestSegment(t, root, "orphan", 0, 0, nil)
	_, err := ArchiveOrphanTopic(root, filepath.Join(root, "archive"), "orphan", false)
	require.ErrorContains(t, err, "outside the log directory")
	require.DirExists(t, filepath.Join(root, "orphan"))
}

func writePersistedTestSegment(t *testing.T, root, topicName string, partition int, base uint64, messages []types.DiskMessage) {
	t.Helper()
	directory := filepath.Join(root, topicName)
	require.NoError(t, os.MkdirAll(directory, 0o750))
	path := filepath.Join(directory, "partition_"+strconv.Itoa(partition)+"_segment_"+fmt.Sprintf("%020d", base)+".log")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	require.NoError(t, err)
	for _, message := range messages {
		payload, serializeErr := util.SerializeDiskMessage(message)
		require.NoError(t, serializeErr)
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(payload)))
		_, err = file.Write(length[:])
		require.NoError(t, err)
		_, err = file.Write(payload)
		require.NoError(t, err)
	}
	require.NoError(t, file.Close())
}
