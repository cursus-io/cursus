package wire

import "testing"

func FuzzBatchRecordOptionals(f *testing.F) {
	seed, err := EncodeBatch(Batch{
		Topic: "orders", Partition: 2, Acks: "all", IsIdempotent: true,
		Messages: []Message{{
			Offset: 7, Timestamp: 1234, Key: "order-7", Payload: "created",
			EventType: "OrderCreated", SchemaVersion: 3, AggregateVersion: 9,
			Metadata: `{"trace":"abc"}`, TransactionalID: "txn-7",
			TransactionState: "committed", TransactionMarker: "commit",
			ControlBatchType: "transaction", ControlBatchVersion: 1,
			ControlBatchCoordinatorEpoch: 4, ControlBatchKey: []byte("key"),
			ControlBatchValue: []byte("value"),
		}},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{0x43, 0x42, 0x32})
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := DecodeBatch(data)
		if err != nil {
			return
		}
		reencoded, err := EncodeBatch(*decoded)
		if err != nil {
			t.Fatalf("decoded batch could not be encoded: %v", err)
		}
		roundTrip, err := DecodeBatch(reencoded)
		if err != nil {
			t.Fatalf("re-encoded batch could not be decoded: %v", err)
		}
		if len(roundTrip.Messages) != len(decoded.Messages) {
			t.Fatalf("message count changed: %d -> %d", len(decoded.Messages), len(roundTrip.Messages))
		}
	})
}

func FuzzDecodeCommandPayload(f *testing.F) {
	seed, err := EncodeCommandPayload(CommandPayload{Fields: map[string]string{
		"topic": "orders", "partition": "2", "message": "created",
	}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{0x43, 0x52, 0x51, 0x32})
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := DecodeCommandPayload(data)
		if err != nil {
			return
		}
		reencoded, err := EncodeCommandPayload(decoded)
		if err != nil {
			t.Fatalf("decoded payload could not be encoded: %v", err)
		}
		if len(reencoded) > MaxFramePayload {
			t.Fatalf("encoded payload exceeded limit: %d", len(reencoded))
		}
	})
}

func FuzzDecompressDeclaredLength(f *testing.F) {
	plain := []byte("wire-v2-compression-seed")
	for _, compression := range []Compression{CompressionNone, CompressionGZIP, CompressionSnappy, CompressionLZ4} {
		encoded, err := Compress(plain, compression)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(byte(compression), uint32(len(plain)), encoded)
	}
	f.Fuzz(func(t *testing.T, algorithm byte, decodedSize uint32, encoded []byte) {
		decoded, err := Decompress(encoded, Compression(algorithm), decodedSize)
		if err != nil {
			return
		}
		if len(decoded) != int(decodedSize) {
			t.Fatalf("decoded length=%d declared=%d", len(decoded), decodedSize)
		}
		if len(decoded) > MaxFramePayload {
			t.Fatalf("decoded payload exceeded limit: %d", len(decoded))
		}
	})
}
