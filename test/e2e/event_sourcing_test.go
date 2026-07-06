package e2e

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	sdk "github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/cursus/util"
)

type readStreamEnvelope struct {
	Status    string `json:"status"`
	Topic     string `json:"topic"`
	Key       string `json:"key"`
	Partition int    `json:"partition"`
	Count     int    `json:"count"`
	Snapshot  *struct {
		Version uint64 `json:"version"`
		Payload string `json:"payload"`
	} `json:"snapshot,omitempty"`
}

func (bc *BrokerClient) CreateEventSourcingTopic(topic string, partitions int) error {
	cmd := fmt.Sprintf("CREATE topic=%s partitions=%d event_sourcing=true", topic, partitions)
	resp, err := bc.SendCommand("admin", cmd, 5*time.Second)
	if err != nil {
		return err
	}
	if strings.HasPrefix(resp, "ERROR:") && !strings.Contains(resp, "already_exists") && !strings.Contains(resp, "topic_exists") {
		return fmt.Errorf("broker error: %s", resp)
	}
	return nil
}

func (bc *BrokerClient) AppendStream(topic, key string, expectedNextVersion uint64, eventType string, schemaVersion uint32, payload string) (string, error) {
	cmd := fmt.Sprintf("APPEND_STREAM topic=%s key=%s version=%d event_type=%s schema_version=%d message=%s", topic, key, expectedNextVersion, eventType, schemaVersion, payload)
	resp, err := bc.SendCommand(topic, cmd, 5*time.Second)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(resp, "ERROR:") {
		return "", fmt.Errorf("broker error: %s", resp)
	}
	return resp, nil
}

func (bc *BrokerClient) StreamVersion(topic, key string) (uint64, error) {
	cmd := fmt.Sprintf("STREAM_VERSION topic=%s key=%s", topic, key)
	resp, err := bc.SendCommand(topic, cmd, 5*time.Second)
	if err != nil {
		return 0, err
	}
	if strings.HasPrefix(resp, "ERROR:") {
		return 0, fmt.Errorf("broker error: %s", resp)
	}
	if strings.HasPrefix(resp, "OK") {
		for _, part := range strings.Fields(resp) {
			if strings.HasPrefix(part, "version=") {
				var version uint64
				if n, scanErr := fmt.Sscanf(part, "version=%d", &version); scanErr != nil || n != 1 {
					return 0, fmt.Errorf("invalid version response: %s", resp)
				}
				return version, nil
			}
		}
		return 0, fmt.Errorf("missing version in response: %s", resp)
	}
	var version uint64
	if n, scanErr := fmt.Sscanf(resp, "%d", &version); scanErr != nil || n != 1 {
		return 0, fmt.Errorf("unexpected version response: %s", resp)
	}
	return version, nil
}

func (bc *BrokerClient) ReadStream(topic, key string, fromVersion uint64) (*readStreamEnvelope, []types.Message, error) {
	addr, err := bc.getPrimaryAddr()
	if err != nil {
		return nil, nil, err
	}
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = conn.Close() }()

	cmd := fmt.Sprintf("READ_STREAM topic=%s key=%s from_version=%d", topic, key, fromVersion)
	if err := util.WriteWithLength(conn, util.EncodeMessage(topic, cmd)); err != nil {
		return nil, nil, fmt.Errorf("send read stream: %w", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return nil, nil, err
	}
	envelopeBytes, err := util.ReadWithLength(conn)
	if err != nil {
		return nil, nil, fmt.Errorf("read envelope: %w", err)
	}
	var envelope readStreamEnvelope
	if err := json.Unmarshal(envelopeBytes, &envelope); err != nil {
		return nil, nil, fmt.Errorf("decode envelope %q: %w", string(envelopeBytes), err)
	}
	if envelope.Status == "ERROR" {
		return &envelope, nil, fmt.Errorf("broker error: %s", string(envelopeBytes))
	}
	batchBytes, err := util.ReadWithLength(conn)
	if err != nil {
		return &envelope, nil, fmt.Errorf("read batch: %w", err)
	}
	batch, err := util.DecodeBatchMessages(batchBytes)
	if err != nil {
		return &envelope, nil, fmt.Errorf("decode batch: %w", err)
	}
	return &envelope, batch.Messages, nil
}

func TestEventSourcingTCPRoundTrip(t *testing.T) {
	ctx := GivenStandalone(t).WithTopic("event-sourcing-e2e").WithPartitions(2)
	defer ctx.Cleanup()

	client := ctx.GetClient()
	if err := client.CreateEventSourcingTopic(ctx.topic, ctx.partitions); err != nil {
		t.Fatalf("create event-sourcing topic: %v", err)
	}

	key := "order-e2e-1"
	for version := uint64(1); version <= 3; version++ {
		payload := fmt.Sprintf(`{"version":%d}`, version)
		resp, err := client.AppendStream(ctx.topic, key, version, "OrderEvent", 1, payload)
		if err != nil {
			t.Fatalf("append stream version %d: %v", version, err)
		}
		if !strings.HasPrefix(resp, "OK version=") || !strings.Contains(resp, "offset=") || !strings.Contains(resp, "partition=") {
			t.Fatalf("unexpected append response: %s", resp)
		}
	}

	version, err := client.StreamVersion(ctx.topic, key)
	if err != nil {
		t.Fatalf("stream version: %v", err)
	}
	if version != 3 {
		t.Fatalf("stream version = %d, want 3", version)
	}

	envelope, messages, err := client.ReadStream(ctx.topic, key, 1)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if envelope.Status != "OK" || envelope.Topic != ctx.topic || envelope.Key != key || envelope.Count != 3 {
		t.Fatalf("unexpected envelope: %+v", envelope)
	}
	if len(messages) != 3 {
		t.Fatalf("read %d messages, want 3", len(messages))
	}
	for i, msg := range messages {
		wantVersion := uint64(i + 1)
		if msg.Key != key || msg.EventType != "OrderEvent" || msg.SchemaVersion != 1 || msg.AggregateVersion != wantVersion {
			t.Fatalf("message[%d] metadata = key:%q type:%q schema:%d version:%d", i, msg.Key, msg.EventType, msg.SchemaVersion, msg.AggregateVersion)
		}
	}

	_, err = client.AppendStream(ctx.topic, key, 2, "OrderEvent", 1, `{"duplicate":true}`)
	if err == nil || !strings.Contains(err.Error(), "version_conflict") {
		t.Fatalf("lower/stale append error = %v, want version_conflict", err)
	}
}

func TestGoSDKEventStoreTCPRoundTrip(t *testing.T) {
	ctx := GivenStandalone(t).WithTopic("event-sourcing-sdk-e2e").WithPartitions(2)
	defer ctx.Cleanup()

	store := sdk.NewEventStore(defaultBrokerAddrs[0], ctx.topic, "sdk-e2e-producer")
	defer func() { _ = store.Close() }()

	if err := store.CreateTopic(ctx.partitions); err != nil {
		t.Fatalf("sdk create topic: %v", err)
	}

	key := "sdk-order-1"
	created, err := store.Append(key, 0, &sdk.Event{Type: "OrderCreated", Payload: `{"id":1}`})
	if err != nil {
		t.Fatalf("sdk append create: %v", err)
	}
	if created.Version != 1 {
		t.Fatalf("created version = %d, want 1", created.Version)
	}

	paid, err := store.Append(key, 1, &sdk.Event{Type: "OrderPaid", SchemaVersion: 2, Payload: `{"amount":42}`, Metadata: `{"trace":"sdk-e2e"}`})
	if err != nil {
		t.Fatalf("sdk append paid: %v", err)
	}
	if paid.Version != 2 {
		t.Fatalf("paid version = %d, want 2", paid.Version)
	}

	if _, err := store.Append(key, 1, &sdk.Event{Type: "Duplicate", Payload: `{}`}); err == nil || !strings.Contains(err.Error(), "version_conflict") {
		t.Fatalf("stale sdk append error = %v, want version_conflict", err)
	}

	version, err := store.StreamVersion(key)
	if err != nil {
		t.Fatalf("sdk stream version: %v", err)
	}
	if version != 2 {
		t.Fatalf("sdk stream version = %d, want 2", version)
	}

	stream, err := store.ReadStreamFrom(key, 1)
	if err != nil {
		t.Fatalf("sdk read stream: %v", err)
	}
	if len(stream.Events) != 2 {
		t.Fatalf("sdk read events = %d, want 2", len(stream.Events))
	}
	if stream.Events[1].Type != "OrderPaid" || stream.Events[1].SchemaVersion != 2 || stream.Events[1].Metadata != `{"trace":"sdk-e2e"}` {
		t.Fatalf("sdk event metadata not preserved: %+v", stream.Events[1])
	}

	if err := store.SaveSnapshot(key, 2, `{"status":"paid"}`); err != nil {
		t.Fatalf("sdk save snapshot: %v", err)
	}
	snapshot, err := store.ReadSnapshot(key)
	if err != nil {
		t.Fatalf("sdk read snapshot: %v", err)
	}
	if snapshot == nil || snapshot.Version != 2 || snapshot.Payload != `{"status":"paid"}` {
		t.Fatalf("sdk snapshot = %+v", snapshot)
	}

	shipped, err := store.Append(key, 2, &sdk.Event{Type: "OrderShipped", Payload: `{"carrier":"fast"}`})
	if err != nil {
		t.Fatalf("sdk append shipped: %v", err)
	}
	if shipped.Version != 3 {
		t.Fatalf("shipped version = %d, want 3", shipped.Version)
	}

	withSnapshot, err := store.ReadStream(key)
	if err != nil {
		t.Fatalf("sdk read stream with snapshot: %v", err)
	}
	if withSnapshot.Snapshot == nil || withSnapshot.Snapshot.Version != 2 {
		t.Fatalf("sdk stream snapshot = %+v", withSnapshot.Snapshot)
	}
	if len(withSnapshot.Events) != 1 || withSnapshot.Events[0].Version != 3 || withSnapshot.Events[0].Type != "OrderShipped" {
		t.Fatalf("sdk stream after snapshot = %+v", withSnapshot.Events)
	}
}

func BenchmarkEventSourcingTCPRoundTrip(b *testing.B) {
	initEnvironment(b)

	topic := fmt.Sprintf("event-sourcing-bench-%d", time.Now().UnixNano())
	client := NewBrokerClient(defaultBrokerAddrs)
	b.Cleanup(func() {
		_ = client.DeleteTopic(topic)
		client.Close()
	})

	if err := client.CreateEventSourcingTopic(topic, 4); err != nil {
		b.Fatalf("create event-sourcing topic: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-order-%d", i)
		if _, err := client.AppendStream(topic, key, 1, "BenchCreated", 1, `{"ok":true}`); err != nil {
			b.Fatalf("append stream: %v", err)
		}
		if _, _, err := client.ReadStream(topic, key, 1); err != nil {
			b.Fatalf("read stream: %v", err)
		}
	}
}
