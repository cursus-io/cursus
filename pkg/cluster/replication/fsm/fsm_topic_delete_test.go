package fsm

import (
	"encoding/json"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestBrokerFSMDeleteMissingStateDoesNotDeleteLocalTopic(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()

	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	manager := topic.NewTopicManager(cfg, dm, nil)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))

	f := NewBrokerFSM(manager, nil)
	payload, err := json.Marshal(map[string]string{"topic": "orders"})
	require.NoError(t, err)

	result := f.Apply(&raft.Log{Data: []byte("TOPIC_DELETE:" + string(payload)), Index: 1})
	applyErr, ok := result.(error)
	require.True(t, ok)
	require.ErrorIs(t, applyErr, topic.ErrTopicNotFound)
	require.NotNil(t, manager.GetTopic("orders"))
}
