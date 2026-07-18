package topic

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
)

func TestDeleteTopicRecreateStartsWithCleanStorage(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	tm := NewTopicManager(cfg, dm, nil)
	defer dm.CloseAllHandlers()

	const topicName = "recreate-topic"
	if err := tm.CreateTopic(topicName, 1, true, false); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	stale := tm.GetTopic(topicName)
	if err := tm.PublishWithAck(topicName, &types.Message{Payload: "before-delete"}); err != nil {
		t.Fatalf("publish before delete: %v", err)
	}

	deleted, err := tm.DeleteTopic(topicName)
	if err != nil {
		t.Fatalf("delete topic: %v", err)
	}
	if !deleted {
		t.Fatal("expected topic to be deleted")
	}
	if _, err := os.Stat(filepath.Join(cfg.LogDir, topicName)); !os.IsNotExist(err) {
		t.Fatalf("topic log directory still exists, stat error: %v", err)
	}
	if err := stale.PublishSync(types.Message{Payload: "stale"}); err == nil {
		t.Fatal("stale topic accepted a publish after deletion")
	}

	if err := tm.CreateTopic(topicName, 1, true, false); err != nil {
		t.Fatalf("recreate topic: %v", err)
	}
	if recreated := tm.GetTopic(topicName); recreated == stale {
		t.Fatal("recreated topic reused the closed topic instance")
	}
	if got := tm.GetLastOffset(topicName, 0); got != 0 {
		t.Fatalf("recreated topic retained old offset: got %d, want 0", got)
	}
	if err := tm.PublishWithAck(topicName, &types.Message{Payload: "after-recreate"}); err != nil {
		t.Fatalf("publish after recreate: %v", err)
	}
	if got := tm.GetLastOffset(topicName, 0); got != 1 {
		t.Fatalf("recreated topic did not start clean: got tail %d, want 1", got)
	}
}

type failingTopicLifecycleProvider struct {
	handler   types.StorageHandler
	deleteErr error
}

func (p *failingTopicLifecycleProvider) GetHandler(string, int) (types.StorageHandler, error) {
	return p.handler, nil
}

func (p *failingTopicLifecycleProvider) DeleteTopic(string) error {
	return p.deleteErr
}

func TestDeleteTopicStorageFailureIsReturnedAndRetryable(t *testing.T) {
	handler := new(MockStorageHandler)
	handler.On("GetLatestOffset").Return(uint64(0))
	provider := &failingTopicLifecycleProvider{
		handler:   handler,
		deleteErr: errors.New("storage unavailable"),
	}
	tm := NewTopicManager(config.DefaultConfig(), provider, nil)
	if err := tm.CreateTopic("orders", 1, false, false); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	deleted, err := tm.DeleteTopic("orders")
	if err == nil {
		t.Fatal("expected storage deletion error")
	}
	if deleted {
		t.Fatal("topic reported deleted despite storage failure")
	}
	current := tm.GetTopic("orders")
	if current == nil || !current.IsClosed() {
		t.Fatal("failed deletion must retain a closed topic for retry")
	}
	if err := tm.CreateTopic("orders", 1, false, false); err == nil {
		t.Fatal("recreate succeeded while deletion was incomplete")
	}

	provider.deleteErr = nil
	deleted, err = tm.DeleteTopic("orders")
	if err != nil {
		t.Fatalf("retry delete: %v", err)
	}
	if !deleted || tm.GetTopic("orders") != nil {
		t.Fatal("retry did not finish topic deletion")
	}
	handler.AssertExpectations(t)
}
