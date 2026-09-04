package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cursus-io/cursus/pkg/wire"
)

type vector struct {
	Name string `json:"name"`
	Hex  string `json:"hex"`
}

type fixture struct {
	SchemaVersion  int               `json:"schema_version"`
	WireVersion    uint16            `json:"wire_version"`
	HeaderSize     int               `json:"header_size"`
	MaxPayload     int               `json:"max_payload"`
	Capabilities   []string          `json:"capabilities"`
	CommandIDs     map[string]uint16 `json:"command_ids"`
	CompressionIDs map[string]uint8  `json:"compression_ids"`
	Vectors        []vector          `json:"vectors"`
}

func mustHex(payload []byte, err error) string {
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(payload)
}

func main() {
	out := flag.String("out", "", "write the canonical fixture to this path; stdout when empty")
	flag.Parse()
	data, err := buildFixture()
	if err != nil {
		panic(err)
	}
	if *out == "" {
		_, _ = os.Stdout.Write(data)
		return
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o750); err != nil {
		panic(err)
	}
	if err := os.WriteFile(*out, data, 0o600); err != nil {
		panic(err)
	}
}

func buildFixture() ([]byte, error) {

	commandIDs := make(map[string]uint16)
	for _, command := range wire.Commands() {
		commandIDs[command.String()] = uint16(command)
	}

	negotiation := mustHex(wire.EncodeNegotiationRequest(wire.NegotiationRequest{
		MinimumVersion: wire.ProtocolVersion,
		MaximumVersion: wire.ProtocolVersion,
		Compressions: []wire.Compression{
			wire.CompressionGZIP,
			wire.CompressionSnappy,
			wire.CompressionLZ4,
			wire.CompressionNone,
		},
	}))
	negotiationResponse := mustHex(wire.EncodeNegotiationResponse(wire.NegotiationResponse{
		Version: wire.ProtocolVersion, Compression: wire.CompressionLZ4,
	}))

	codec, err := wire.NewCodec(wire.CompressionNone)
	if err != nil {
		panic(err)
	}
	frame := mustHex(codec.Encode(wire.Frame{
		Kind: wire.KindRequest, Command: wire.CommandPublish, RequestID: 42, Payload: []byte("hello"),
	}))
	compressionFrames := make(map[wire.Compression]string)
	for _, compression := range []wire.Compression{
		wire.CompressionGZIP, wire.CompressionSnappy, wire.CompressionLZ4,
	} {
		codec, codecErr := wire.NewCodec(compression)
		if codecErr != nil {
			return nil, fmt.Errorf("build %s codec: %w", compression, codecErr)
		}
		compressionFrames[compression] = mustHex(codec.Encode(wire.Frame{
			Kind: wire.KindRequest, Command: wire.CommandPublish, RequestID: 77,
			Payload: []byte("cross-language-compression-cross-language-compression-cross-language-compression"),
		}))
	}

	command, commandPayload, err := wire.ParseCommandText(
		`APPEND_STREAM topic=events key=aggregate-7 expectedVersion=3 eventType=Updated schemaVersion=2 metadata={"trace":"a b"} message={"value":"x y"}`,
	)
	if err != nil || command != wire.CommandAppendStream {
		return nil, fmt.Errorf("build command fixture: command=%s error=%w", command, err)
	}
	encodedCommand := mustHex(wire.EncodeCommandPayload(commandPayload))

	encodedBatch := mustHex(wire.EncodeBatch(wire.Batch{
		Topic: "events", Partition: 2, Acks: "all", IsIdempotent: true,
		Messages: []wire.Message{{
			Offset: 7, Payload: "  opaque\x00한글\tpayload  ", Timestamp: -123,
			ProducerID: "producer-1", SeqNum: 9, Epoch: -2, Key: "aggregate-7",
			EventType: "Updated", SchemaVersion: 2, AggregateVersion: 3,
			Metadata: `{"trace":"a b"}`, TransactionalID: "txn-1",
			TransactionState:             wire.TransactionStateAborted,
			TransactionMarker:            wire.TransactionMarkerAbort,
			ControlBatchType:             wire.ControlBatchTransaction,
			ControlBatchVersion:          wire.ControlBatchVersionCursusV2,
			ControlBatchCoordinatorEpoch: 11,
			ControlBatchKey:              []byte{0x00, 0x01, 0xff},
			ControlBatchValue:            []byte("control-value"),
		}},
	}))

	encodedError := mustHex(wire.EncodeError(wire.ErrorPayload{
		Code: "replication_unavailable", Class: wire.ErrorClassAvailability, Retryable: true,
		Message: "replication quorum unavailable",
		Fields:  map[string]string{"offset": "7", "reason": "replica timeout"},
	}))

	data, err := json.MarshalIndent(fixture{
		SchemaVersion: 1,
		WireVersion:   wire.ProtocolVersion,
		HeaderSize:    wire.HeaderSize,
		MaxPayload:    wire.MaxFramePayload,
		Capabilities: []string{
			"wire_v2", "compression_none", "compression_gzip", "compression_snappy",
			"compression_lz4", "transport_tls", "authentication", "typed_errors",
			"request_correlation", "producer_acks", "producer_batching",
			"producer_idempotence", "safe_retry", "consumer_polling", "consumer_streaming",
			"consumer_groups", "offset_reset", "isolation_levels", "event_store", "snapshots",
			"transactions", "transactional_offsets", "transaction_status", "admin_client",
			"event_envelope", "aggregate_repository", "saga", "client_metrics",
			"error_classification",
		},
		CommandIDs: commandIDs,
		CompressionIDs: map[string]uint8{
			"none":   uint8(wire.CompressionNone),
			"gzip":   uint8(wire.CompressionGZIP),
			"snappy": uint8(wire.CompressionSnappy),
			"lz4":    uint8(wire.CompressionLZ4),
		},
		Vectors: []vector{
			{Name: "negotiation_request_all_compressions", Hex: negotiation},
			{Name: "negotiation_response_lz4", Hex: negotiationResponse},
			{Name: "uncompressed_publish_frame", Hex: frame},
			{Name: "gzip_publish_frame", Hex: compressionFrames[wire.CompressionGZIP]},
			{Name: "snappy_publish_frame", Hex: compressionFrames[wire.CompressionSnappy]},
			{Name: "lz4_publish_frame", Hex: compressionFrames[wire.CompressionLZ4]},
			{Name: "append_stream_command_payload", Hex: encodedCommand},
			{Name: "full_record_batch", Hex: encodedBatch},
			{Name: "structured_availability_error", Hex: encodedError},
		},
	}, "", "  ")
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')
	return data, nil
}
