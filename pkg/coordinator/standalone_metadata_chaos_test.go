package coordinator_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

const (
	abruptChildEnv      = "CURSUS_STANDALONE_METADATA_ABRUPT_CHILD"
	abruptLogDirEnv     = "CURSUS_STANDALONE_METADATA_LOG_DIR"
	abruptReadyEnv      = "CURSUS_STANDALONE_METADATA_READY_FILE"
	wargameBootstrapEnv = "WARGAME_BROKER_ALLOW_NEW_CONSUMER_GROUP_BOOTSTRAP"
)

func TestStandaloneAckedOffsetSurvivesAbruptProcessExit(t *testing.T) {
	if os.Getenv(abruptChildEnv) == "1" {
		runAbruptMetadataChild(t)
		select {}
	}

	root := t.TempDir()
	readyPath := filepath.Join(root, "commit-acked")
	command := exec.Command(os.Args[0], "-test.run=^TestStandaloneAckedOffsetSurvivesAbruptProcessExit$")
	command.Env = append(os.Environ(),
		abruptChildEnv+"=1",
		abruptLogDirEnv+"="+root,
		abruptReadyEnv+"="+readyPath,
		wargameBootstrapEnv+"=0",
	)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	require.NoError(t, command.Start())

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatalf("child did not acknowledge the offset: %s", output.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NoError(t, command.Process.Kill(), output.String())
	require.Error(t, command.Wait(), "the child must exit without graceful handler shutdown")

	t.Setenv(wargameBootstrapEnv, "0")
	cfg := config.DefaultConfig()
	cfg.LogDir = root
	dm, _, recovered := startStandaloneCoordinator(t, cfg, true)
	t.Cleanup(dm.CloseAllHandlers)
	t.Cleanup(recovered.Stop)
	status, err := recovered.GetGroupStatus("workers")
	require.NoError(t, err)
	require.Equal(t, "events", status.TopicName)
	offset, found := recovered.GetOffset("workers", "events", 0)
	require.True(t, found)
	require.Equal(t, uint64(37), offset)
	require.ErrorContains(t, recovered.CommitOffset("workers", "events", 0, 36), "offset regression")
}

func runAbruptMetadataChild(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = os.Getenv(abruptLogDirEnv)
	dm, tm, cd := startStandaloneCoordinator(t, cfg, false)
	_ = dm
	require.NoError(t, tm.CreateTopic("events", 2, false, false))
	require.NoError(t, cd.RegisterGroup("events", "workers", 2))
	require.NoError(t, cd.CommitOffset("workers", "events", 0, 37))
	require.ErrorContains(t, cd.CommitOffset("workers", "events", 0, 36), "offset regression")
	require.NoError(t, os.WriteFile(os.Getenv(abruptReadyEnv), []byte("acked"), 0o600))
}

func TestStandaloneConsumerMetadataCompactionKeepsLatestStateAcrossRestarts(t *testing.T) {
	t.Setenv(wargameBootstrapEnv, "0")
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.SegmentSize = 512
	cfg.MinCleanableDirtyRatio = 0.01

	dm, tm, cd := startStandaloneCoordinator(t, cfg, false)
	require.NoError(t, tm.CreateTopic("events", 2, false, false))
	require.NoError(t, cd.RegisterGroup("events", "workers", 2))
	for next := uint64(1); next <= 12; next++ {
		require.NoError(t, cd.CommitOffset("workers", "events", 0, next))
	}
	firstCompaction := compactWorkerMetadata(t, dm, tm, 12, "first")
	require.Greater(t, firstCompaction.RecordsRemoved, 0)
	cd.Stop()
	dm.CloseAllHandlers()

	secondDM, secondTM, second := startStandaloneCoordinator(t, cfg, true)
	offset, found := second.GetOffset("workers", "events", 0)
	require.True(t, found)
	require.Equal(t, uint64(12), offset)
	require.NoError(t, second.RegisterGroup("events", "workers", 2), "registration retry remains idempotent after compaction")
	for next := uint64(13); next <= 20; next++ {
		require.NoError(t, second.CommitOffset("workers", "events", 0, next))
	}
	secondCompaction := compactWorkerMetadata(t, secondDM, secondTM, 20, "second")
	require.Greater(t, secondCompaction.RecordsRemoved, 0)
	second.Stop()
	secondDM.CloseAllHandlers()

	finalDM, _, final := startStandaloneCoordinator(t, cfg, true)
	t.Cleanup(finalDM.CloseAllHandlers)
	t.Cleanup(final.Stop)
	status, err := final.GetGroupStatus("workers")
	require.NoError(t, err)
	require.Equal(t, 2, status.PartitionCount)
	offset, found = final.GetOffset("workers", "events", 0)
	require.True(t, found)
	require.Equal(t, uint64(20), offset)
	require.ErrorContains(t, final.CommitOffset("workers", "events", 0, 19), "offset regression")
}

func compactWorkerMetadata(t *testing.T, dm *disk.DiskManager, tm *topic.TopicManager, latest uint64, cycle string) disk.CompactionResult {
	t.Helper()
	internal := tm.GetTopic(config.ConsumerOffsetsTopicName)
	require.NotNil(t, internal)
	key := consumerOffsetRecordKey("workers", "events")
	partitionID := internal.GetPartitionForMessage(types.Message{Key: key})
	storage, err := dm.GetHandler(config.ConsumerOffsetsTopicName, partitionID)
	require.NoError(t, err)
	handler, ok := storage.(*disk.DiskHandler)
	require.True(t, ok)
	payload, err := json.Marshal(coordinator.OffsetCommitMessage{
		Group: "workers", Topic: "events", Partition: 0, Offset: latest,
	})
	require.NoError(t, err)
	for index := 0; index < 8; index++ {
		_, err = handler.AppendMessageSync(config.ConsumerOffsetsTopicName, partitionID, &types.Message{
			Key: fmt.Sprintf("%s-filler-%d", cycle, index), Payload: string(payload),
		})
		require.NoError(t, err)
	}
	result, err := handler.EnforceCompaction()
	require.NoError(t, err)
	return result
}

func consumerOffsetRecordKey(groupName, topicName string) string {
	digest := sha256.Sum256([]byte(groupName + "\x00" + topicName))
	return "cursus.consumer.offset.v1." + hex.EncodeToString(digest[:])
}
