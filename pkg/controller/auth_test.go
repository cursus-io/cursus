package controller

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func TestSASLACLGuardsPublishAndConsume(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnableSASL = true
	cfg.SASLUsers = []config.SASLUser{{Principal: "alice", Token: "secret"}}

	storage := &authTestStorage{messages: []types.Message{{Offset: 0, Payload: "ready"}}}
	tm := topic.NewTopicManager(cfg, &authStorageProvider{storage: storage}, nil)
	err := tm.CreateTopicWithPolicy("acl-topic", 1, false, false, topic.Policy{
		AuthPolicy: topic.AuthPolicyACL,
		ReadACL:    []string{"alice"},
		WriteACL:   []string{"alice"},
	})
	if err != nil {
		t.Fatalf("CreateTopicWithPolicy failed: %v", err)
	}

	coord := coordinator.NewCoordinator(context.Background(), cfg, &dummyPublisher{})
	if err := coord.RegisterGroup("acl-topic", "acl-group", 1); err != nil {
		t.Fatalf("RegisterGroup failed: %v", err)
	}
	ch := NewCommandHandler(tm, cfg, coord, nil, nil)
	ctx := NewClientContext("acl-group", 0)

	unauth := ch.HandleCommand("PUBLISH topic=acl-topic producerId=p1 message=nope", NewClientContext("", 0))
	if !strings.Contains(unauth, "authentication_required") {
		t.Fatalf("expected unauthenticated publish to be denied, got %q", unauth)
	}

	auth := ch.HandleCommand("AUTH principal=alice token=secret", ctx)
	if !strings.HasPrefix(auth, "OK") {
		t.Fatalf("expected AUTH success, got %q", auth)
	}

	published := ch.HandleCommand("PUBLISH topic=acl-topic producerId=p1 message=ok", ctx)
	if !strings.HasPrefix(published, "OK") && !strings.HasPrefix(published, "{") {
		t.Fatalf("expected authenticated publish success, got %q", published)
	}

	server, client := net.Pipe()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close consume server pipe: %v", err)
		}
	})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close consume client pipe: %v", err)
		}
	})

	errCh := make(chan error, 1)
	go func() {
		_, err := ch.HandleConsumeCommand(server, "CONSUME topic=acl-topic partition=0 offset=0 group=acl-group member=alice batch=1", ctx)
		errCh <- err
	}()

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	data, err := util.ReadWithLength(client)
	if err != nil {
		t.Fatalf("ReadWithLength failed: %v", err)
	}
	batch, err := util.DecodeBatchMessages(data)
	if err != nil {
		t.Fatalf("DecodeBatchMessages failed: %v", err)
	}
	if len(batch.Messages) != 1 || batch.Messages[0].Payload != "ready" {
		t.Fatalf("unexpected consume batch: %+v", batch.Messages)
	}
	_ = client.Close()
	if err := <-errCh; err != nil && !strings.Contains(err.Error(), "closed pipe") && !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("consume failed: %v", err)
	}
}

func TestListOffsetsRequiresReadACL(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnableSASL = true
	cfg.SASLUsers = []config.SASLUser{{Principal: "alice", Token: "secret"}}

	storage := &authTestStorage{}
	tm := topic.NewTopicManager(cfg, &authStorageProvider{storage: storage}, nil)
	if err := tm.CreateTopicWithPolicy("offset-acl-topic", 1, false, false, topic.Policy{
		AuthPolicy: topic.AuthPolicyACL,
		ReadACL:    []string{"alice"},
		WriteACL:   []string{"alice"},
	}); err != nil {
		t.Fatalf("CreateTopicWithPolicy failed: %v", err)
	}

	ch := NewCommandHandler(tm, cfg, nil, nil, nil)
	unauth := ch.HandleCommand("LIST_OFFSETS topic=offset-acl-topic", NewClientContext("", 0))
	if !strings.Contains(unauth, "authentication_required") {
		t.Fatalf("expected unauthenticated LIST_OFFSETS to be denied, got %q", unauth)
	}

	auth := ch.HandleCommand("LIST_OFFSETS topic=offset-acl-topic principal=alice auth_token=secret", NewClientContext("", 0))
	if !strings.HasPrefix(auth, "OK") {
		t.Fatalf("expected authenticated LIST_OFFSETS success, got %q", auth)
	}
}

func TestPublicPublishCannotUseInternalTxnFlag(t *testing.T) {
	cfg := config.DefaultConfig()
	storage := &authTestStorage{}
	tm := topic.NewTopicManager(cfg, &authStorageProvider{storage: storage}, nil)
	if err := tm.CreateTopicWithPolicy("txn-internal-topic", 1, false, false, topic.DefaultPolicy()); err != nil {
		t.Fatalf("CreateTopicWithPolicy failed: %v", err)
	}

	ch := NewCommandHandler(tm, cfg, nil, nil, nil)
	resp := ch.HandleCommand("PUBLISH topic=txn-internal-topic producerId=p1 internal_txn_publish=true message=blocked", NewClientContext("", 0))
	if !strings.Contains(resp, "internal_txn_publish_forbidden") {
		t.Fatalf("expected public internal transaction publish to be rejected, got %q", resp)
	}
}
func TestCommitOffsetRequiresMemberGeneration(t *testing.T) {
	cfg := config.DefaultConfig()
	coord := coordinator.NewCoordinator(context.Background(), cfg, &dummyPublisher{})
	ch := NewCommandHandler(nil, cfg, coord, nil, nil)

	resp := ch.HandleCommand("COMMIT_OFFSET topic=t1 group=g1 partition=0 offset=1 validate_only=true ownership_only=true", NewClientContext("", 0))
	if !strings.Contains(resp, "missing_member") {
		t.Fatalf("expected missing member, got %q", resp)
	}

	resp = ch.HandleCommand("COMMIT_OFFSET topic=t1 group=g1 partition=0 offset=1 member=m1 validate_only=true", NewClientContext("", 0))
	if !strings.Contains(resp, "missing_generation") {
		t.Fatalf("expected missing generation, got %q", resp)
	}
}

func TestPublishRejectsMalformedControlBatchMetadata(t *testing.T) {
	cfg := config.DefaultConfig()
	storage := &authTestStorage{}
	tm := topic.NewTopicManager(cfg, &authStorageProvider{storage: storage}, nil)
	if err := tm.CreateTopicWithPolicy("txn-control-topic", 1, false, false, topic.DefaultPolicy()); err != nil {
		t.Fatalf("CreateTopicWithPolicy failed: %v", err)
	}

	ch := NewCommandHandler(tm, cfg, nil, nil, nil)
	resp := ch.HandleCommand("PUBLISH topic=txn-control-topic producerId=p1 partition=0 seqNum=1 epoch=0 internal_txn_publish=true control_batch_version=oops message=blocked", NewInternalClientContext("", 0))
	if !strings.Contains(resp, "invalid_control_batch_version") {
		t.Fatalf("expected invalid control batch version, got %q", resp)
	}
}
func TestParseControlBatchBytesUsesStableErrorCode(t *testing.T) {
	_, resp := parseControlBatchBytes("%%%", "key")
	if !strings.Contains(resp, "ERROR: invalid_control_batch_bytes field=key") {
		t.Fatalf("expected stable control batch bytes error, got %q", resp)
	}
}
func TestTransactionMarkerRequiresControlBatchMetadata(t *testing.T) {
	tx := &transaction.Transaction{
		ID:       "txn-1",
		Producer: "producer-1",
		Epoch:    4,
		State:    transaction.StateCommitting,
		Messages: []transaction.MessageOperation{{
			Topic:     "txn-topic",
			Partition: 0,
			Message: types.Message{
				ProducerID:      "producer-1",
				SeqNum:          1,
				Epoch:           4,
				TransactionalID: "txn-1",
				Payload:         "value",
			},
		}},
	}
	ch := NewCommandHandler(nil, config.DefaultConfig(), nil, nil, nil)
	msg := types.Message{
		ProducerID:        transactionMarkerProducerID(tx, types.TransactionMarkerCommit),
		SeqNum:            1,
		Epoch:             tx.Epoch,
		TransactionalID:   tx.ID,
		TransactionState:  types.TransactionStateCommitted,
		TransactionMarker: types.TransactionMarkerCommit,
	}

	errResp := ch.validateTransactionMarkerPublish(tx, "txn-topic", 0, &msg)
	if !strings.Contains(errResp, "invalid_transaction_control_batch") {
		t.Fatalf("expected invalid control batch, got %q", errResp)
	}

	msg.ControlBatchType = types.ControlBatchTransaction
	msg.ControlBatchVersion = types.ControlBatchVersionCursusV2
	msg.ControlBatchCoordinatorEpoch = tx.Epoch
	key, value, err := transactionMarkerControlBytes(types.TransactionMarkerCommit, tx.Epoch)
	if err != nil {
		t.Fatalf("transactionMarkerControlBytes failed: %v", err)
	}
	msg.ControlBatchKey = key
	msg.ControlBatchValue = value
	if resp := ch.validateTransactionMarkerPublish(tx, "txn-topic", 0, &msg); resp != "" {
		t.Fatalf("expected valid transaction marker, got %q", resp)
	}
}

type authStorageProvider struct {
	storage types.StorageHandler
}

func (p *authStorageProvider) GetHandler(string, int) (types.StorageHandler, error) {
	return p.storage, nil
}

type authTestStorage struct {
	messages []types.Message
}

func (s *authTestStorage) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	var out []types.Message
	for _, msg := range s.messages {
		if msg.Offset >= offset {
			out = append(out, msg)
			if len(out) == max {
				break
			}
		}
	}
	return out, nil
}

func (s *authTestStorage) GetFirstOffset() uint64 {
	if len(s.messages) == 0 {
		return 0
	}
	return s.messages[0].Offset
}

func (s *authTestStorage) tailOffset() uint64 {
	if len(s.messages) == 0 {
		return 0
	}
	return s.messages[len(s.messages)-1].Offset + 1
}

func (s *authTestStorage) GetAbsoluteOffset() uint64 { return s.tailOffset() }
func (s *authTestStorage) GetFlushedOffset() uint64  { return s.tailOffset() }
func (s *authTestStorage) GetLatestOffset() uint64   { return s.tailOffset() }
func (s *authTestStorage) GetSegmentPath(uint64) string {
	return ""
}
func (s *authTestStorage) AppendMessage(string, int, *types.Message) (uint64, error) {
	return 0, nil
}
func (s *authTestStorage) AppendMessageSync(string, int, *types.Message) (uint64, error) {
	return 0, nil
}
func (s *authTestStorage) AppendMessageWithOffset(string, int, *types.Message) error {
	return nil
}
func (s *authTestStorage) WriteBatch([]types.DiskMessage) error { return nil }
func (s *authTestStorage) TruncateTo(uint64) error              { return nil }
func (s *authTestStorage) Flush()                               {}
func (s *authTestStorage) Close() error                         { return nil }
