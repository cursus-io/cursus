package util_test

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func TestEncodeDecodeMessage(t *testing.T) {
	topic := "default"
	payload := "hello world"

	data := util.EncodeMessage(topic, payload)

	// 2(topicLen) + len(topic) + len(payload)
	expectedLen := 2 + len(topic) + len(payload)
	if len(data) != expectedLen {
		t.Errorf("Unexpected encoded length: got %d, want %d", len(data), expectedLen)
	}

	decodedTopic, decodedPayload, err := util.DecodeMessage(data)
	if err != nil {
		t.Fatalf("DecodeMessage failed unexpectedly: %v", err)
	}
	if decodedTopic != topic {
		t.Errorf("Expected topic %s, got %s", topic, decodedTopic)
	}
	if decodedPayload != payload {
		t.Errorf("Expected payload %s, got %s", payload, decodedPayload)
	}
}

func TestDecodeMessageInvalidData(t *testing.T) {
	t.Run("EmptyData", func(t *testing.T) {
		empty := []byte{}
		_, _, err := util.DecodeMessage(empty)
		if err == nil {
			t.Error("Expected error for empty data, but got nil")
		}
	})

	t.Run("ShortData", func(t *testing.T) {
		short := []byte{0x00}
		_, _, err := util.DecodeMessage(short)
		if err == nil {
			t.Error("Expected error for short data, but got nil")
		}
	})

	t.Run("InvalidTopicLength", func(t *testing.T) {
		data := []byte{0x00, 0x05, 'a'}
		_, _, err := util.DecodeMessage(data)
		if err == nil {
			t.Error("Expected error for invalid topic length, but got nil")
		}
		expectedErr := "invalid topic length"
		if err != nil && err.Error() != expectedErr {
			t.Errorf("Expected error message '%s', got '%v'", expectedErr, err)
		}
	})
}

func TestBatchMessagesRoundTrip(t *testing.T) {
	testTopic := "batch-test-topic"
	testPartition := 1
	acksValues := []string{"0", "1", "-1", "all"}

	for _, acks := range acksValues {
		t.Run("Acks_"+acks, func(t *testing.T) {
			msgs := []types.Message{
				{Offset: 100, SeqNum: 1, ProducerID: "p1", Key: "k1", Payload: "msg1", Epoch: 1},
				{Offset: 101, SeqNum: 2, ProducerID: "p2", Key: "k2", Payload: "msg2", Epoch: 2},
			}

			data, err := util.EncodeBatchMessages(testTopic, testPartition, acks, msgs)
			if err != nil {
				t.Fatalf("EncodeBatchMessages failed: %v", err)
			}

			batch, err := util.DecodeBatchMessages(data)
			if err != nil {
				t.Fatalf("DecodeBatchMessages failed: %v", err)
			}

			if batch.Topic != testTopic {
				t.Errorf("Topic mismatch: got %s, want %s", batch.Topic, testTopic)
			}
			if batch.Partition != testPartition {
				t.Errorf("Partition mismatch: got %d, want %d", batch.Partition, testPartition)
			}
			if batch.Acks != acks {
				t.Errorf("Acks mismatch: got %s, want %s", batch.Acks, acks)
			}
			if len(batch.Messages) != len(msgs) {
				t.Errorf("Message count mismatch: got %d, want %d", len(batch.Messages), len(msgs))
			}

			for i := range msgs {
				want := msgs[i]
				got := batch.Messages[i]
				if got.Offset != want.Offset {
					t.Errorf("[%d] Offset mismatch: got %d, want %d", i, got.Offset, want.Offset)
				}
				if got.SeqNum != want.SeqNum {
					t.Errorf("[%d] SeqNum mismatch: got %d, want %d", i, got.SeqNum, want.SeqNum)
				}
				if got.ProducerID != want.ProducerID {
					t.Errorf("[%d] ProducerID mismatch: got %s, want %s", i, got.ProducerID, want.ProducerID)
				}
				if got.Key != want.Key {
					t.Errorf("[%d] Key mismatch: got %s, want %s", i, got.Key, want.Key)
				}
				if got.Payload != want.Payload {
					t.Errorf("[%d] Payload mismatch: got %s, want %s", i, got.Payload, want.Payload)
				}
				if got.Epoch != want.Epoch {
					t.Errorf("[%d] Epoch mismatch: got %d, want %d", i, got.Epoch, want.Epoch)
				}
			}
		})
	}
}

func TestDecodeBatchMessagesInvalidData(t *testing.T) {
	t.Run("TruncatedBuffer", func(t *testing.T) {
		msgs := []types.Message{{Payload: "test-data"}}
		data, _ := util.EncodeBatchMessages("topic", 0, "1", msgs)

		truncated := data[:len(data)-5]
		_, err := util.DecodeBatchMessages(truncated)
		if err == nil {
			t.Error("Expected error for truncated batch data, but got nil")
		}
	})

	t.Run("InvalidMessageCount", func(t *testing.T) {
		data, _ := util.EncodeBatchMessages("topic", 0, "1", []types.Message{{Payload: "msg"}})
		corrupted := data[:10]

		_, err := util.DecodeBatchMessages(corrupted[:10])
		if err == nil {
			t.Error("Expected error for corrupted batch data (short length), but got nil")
		} else {
			util.Debug("Caught expected error for corrupted batch: %v", err)
		}
	})
}

func TestBatchMessagesEdgeCases(t *testing.T) {
	t.Run("EmptyBatch", func(t *testing.T) {
		data, err := util.EncodeBatchMessages("topic", 0, "1", []types.Message{})
		if err != nil {
			t.Fatalf("Encoding empty batch failed: %v", err)
		}
		batch, err := util.DecodeBatchMessages(data)
		if err != nil {
			t.Fatalf("Decoding empty batch failed: %v", err)
		}
		if len(batch.Messages) != 0 {
			t.Errorf("Expected 0 messages, got %d", len(batch.Messages))
		}
	})

	t.Run("LargePayload", func(t *testing.T) {
		largeData := make([]byte, 1024*10)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}
		largePayload := string(largeData)
		msgs := []types.Message{{Payload: largePayload}}
		data, err := util.EncodeBatchMessages("topic", 0, "1", msgs)
		if err != nil {
			t.Fatalf("Encoding large payload failed: %v", err)
		}
		batch, err := util.DecodeBatchMessages(data)
		if err != nil {
			t.Fatalf("Decoding failed for large payload: %v", err)
		}
		if len(batch.Messages) != 1 {
			t.Fatalf("Message count mismatch: got %d, want 1", len(batch.Messages))
		}
		if batch.Messages[0].Payload != largePayload {
			t.Errorf("Large payload content integrity check failed: got length %d, want %d", len(batch.Messages[0].Payload), len(largePayload))
		}
	})
}
