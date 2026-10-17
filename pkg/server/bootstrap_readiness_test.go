package server

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/stretchr/testify/require"
)

func TestConsumerMetadataHWMReadyRejectsBootstrapPending(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes = 0
	cfg.DiskMinFreePercent = 0
	manager := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, manager, nil)
	defer manager.CloseAllHandlers()
	defer tm.Stop()
	require.NoError(t, tm.CreateTopic(config.ConsumerOffsetsTopicName, 1, false, false))
	require.True(t, consumerMetadataHWMReady(tm))
	partition, err := tm.GetTopic(config.ConsumerOffsetsTopicName).GetPartition(0)
	require.NoError(t, err)
	session, err := partition.BeginBootstrapSession()
	require.NoError(t, err)
	require.False(t, consumerMetadataHWMReady(tm))
	session.Close()
	require.True(t, consumerMetadataHWMReady(tm))
}
