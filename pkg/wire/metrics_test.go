package wire

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestRuntimeMetricsTrackBoundedDecodeFailures(t *testing.T) {
	codec, err := NewCodec(CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	before := RuntimeMetrics()

	if _, err := codec.Decode([]byte("short")); err == nil {
		t.Fatal("expected short frame rejection")
	}
	after := RuntimeMetrics()
	if got := after.ProtocolFailures[ProtocolFailureInvalidFrame] - before.ProtocolFailures[ProtocolFailureInvalidFrame]; got != 1 {
		t.Fatalf("invalid frame metric delta = %d, want 1", got)
	}
}

func TestRuntimeMetricsTrackDecompressionRejection(t *testing.T) {
	codec, err := NewCodec(CompressionGZIP)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.Encode(Frame{
		Kind: KindRequest, Command: CommandPublish, RequestID: 1, Payload: []byte("payload"),
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded[HeaderSize] ^= 0xff
	binary.BigEndian.PutUint32(encoded[28:32], crc32.Checksum(encoded[HeaderSize:], crc32cTable))
	before := RuntimeMetrics()

	if _, err := codec.Decode(encoded); err == nil {
		t.Fatal("expected corrupted gzip rejection")
	}
	after := RuntimeMetrics()
	if got := after.ProtocolFailures[ProtocolFailureDecompression] - before.ProtocolFailures[ProtocolFailureDecompression]; got != 1 {
		t.Fatalf("protocol decompression metric delta = %d, want 1", got)
	}
	if got := after.DecompressionRejections[DecompressionRejectionInvalidPayload] - before.DecompressionRejections[DecompressionRejectionInvalidPayload]; got != 1 {
		t.Fatalf("decompression rejection metric delta = %d, want 1", got)
	}
}
