package wire

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"

	xerialsnappy "github.com/eapache/go-xerial-snappy"
	mastersnappy "github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

var xerialSnappyHeader = []byte{130, 83, 78, 65, 80, 80, 89, 0}

const xerialSnappyFrameHeaderSize = 16

func Compress(payload []byte, algorithm Compression) ([]byte, error) {
	if len(payload) > MaxFramePayload {
		return nil, fmt.Errorf("decoded payload size %d exceeds maximum %d", len(payload), MaxFramePayload)
	}
	var encoded []byte
	switch algorithm {
	case CompressionNone:
		encoded = payload
	case CompressionGZIP:
		var buffer bytes.Buffer
		writer := gzip.NewWriter(&buffer)
		if _, err := writer.Write(payload); err != nil {
			_ = writer.Close()
			return nil, fmt.Errorf("gzip payload: %w", err)
		}
		if err := writer.Close(); err != nil {
			return nil, fmt.Errorf("close gzip payload: %w", err)
		}
		encoded = buffer.Bytes()
	case CompressionSnappy:
		encoded = xerialsnappy.Encode(payload)
	case CompressionLZ4:
		var buffer bytes.Buffer
		writer := lz4.NewWriter(&buffer)
		if _, err := writer.Write(payload); err != nil {
			_ = writer.Close()
			return nil, fmt.Errorf("lz4 payload: %w", err)
		}
		if err := writer.Close(); err != nil {
			return nil, fmt.Errorf("close lz4 payload: %w", err)
		}
		encoded = buffer.Bytes()
	default:
		return nil, fmt.Errorf("unsupported compression %q", algorithm.String())
	}
	if len(encoded) > MaxFramePayload {
		return nil, fmt.Errorf("encoded payload size %d exceeds maximum %d", len(encoded), MaxFramePayload)
	}
	return encoded, nil
}

func Decompress(payload []byte, algorithm Compression, decodedSize uint32) ([]byte, error) {
	if len(payload) > MaxFramePayload {
		return nil, fmt.Errorf("encoded payload size %d exceeds maximum %d", len(payload), MaxFramePayload)
	}
	if decodedSize > MaxFramePayload {
		return nil, fmt.Errorf("decoded payload size %d exceeds maximum %d", decodedSize, MaxFramePayload)
	}
	if algorithm == CompressionNone {
		// #nosec G115 -- payload length is bounded by MaxFramePayload above.
		if uint32(len(payload)) != decodedSize {
			return nil, fmt.Errorf("uncompressed payload length mismatch: encoded=%d decoded=%d", len(payload), decodedSize)
		}
		return payload, nil
	}

	var (
		decoded []byte
		err     error
	)
	switch algorithm {
	case CompressionGZIP:
		reader, openErr := gzip.NewReader(bytes.NewReader(payload))
		if openErr != nil {
			return nil, fmt.Errorf("open gzip payload: %w", openErr)
		}
		decoded, err = readBounded(reader, decodedSize)
		closeErr := reader.Close()
		if err == nil && closeErr != nil {
			err = fmt.Errorf("close gzip payload: %w", closeErr)
		}
	case CompressionSnappy:
		if err := validateSnappySize(payload, decodedSize); err != nil {
			return nil, err
		}
		decoded, err = xerialsnappy.Decode(payload)
	case CompressionLZ4:
		decoded, err = readBounded(lz4.NewReader(bytes.NewReader(payload)), decodedSize)
	default:
		return nil, fmt.Errorf("unsupported compression %q", algorithm.String())
	}
	if err != nil {
		return nil, err
	}
	if len(decoded) != int(decodedSize) {
		return nil, fmt.Errorf("decoded payload length mismatch: got=%d expected=%d", len(decoded), decodedSize)
	}
	return decoded, nil
}

// DecompressBounded supports transitional call sites that do not yet carry a
// Wire v2 decoded-length header. It still enforces the same 64 MiB allocation
// boundary. New network paths should call Codec.ReadFrame instead.
func DecompressBounded(payload []byte, algorithm Compression) ([]byte, error) {
	if len(payload) > MaxFramePayload {
		return nil, fmt.Errorf("encoded payload size %d exceeds maximum %d", len(payload), MaxFramePayload)
	}
	switch algorithm {
	case CompressionNone:
		return payload, nil
	case CompressionGZIP:
		reader, err := gzip.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, fmt.Errorf("open gzip payload: %w", err)
		}
		decoded, readErr := readUnknownBounded(reader)
		closeErr := reader.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close gzip payload: %w", closeErr)
		}
		return decoded, nil
	case CompressionSnappy:
		decodedSize, err := snappyDecodedSize(payload)
		if err != nil {
			return nil, err
		}
		if decodedSize > MaxFramePayload {
			return nil, fmt.Errorf("decoded payload size %d exceeds maximum %d", decodedSize, MaxFramePayload)
		}
		// #nosec G115 -- decodedSize is non-negative and bounded by MaxFramePayload above.
		return Decompress(payload, algorithm, uint32(decodedSize))
	case CompressionLZ4:
		return readUnknownBounded(lz4.NewReader(bytes.NewReader(payload)))
	default:
		return nil, fmt.Errorf("unsupported compression %q", algorithm.String())
	}
}

func readBounded(reader io.Reader, expected uint32) ([]byte, error) {
	limit := int64(expected) + 1
	if limit > int64(MaxFramePayload)+1 {
		limit = int64(MaxFramePayload) + 1
	}
	decoded, err := io.ReadAll(io.LimitReader(reader, limit))
	if err != nil {
		return nil, fmt.Errorf("decompress payload: %w", err)
	}
	if len(decoded) > int(expected) {
		return nil, fmt.Errorf("decoded payload exceeds declared length %d", expected)
	}
	return decoded, nil
}

func readUnknownBounded(reader io.Reader) ([]byte, error) {
	decoded, err := io.ReadAll(io.LimitReader(reader, int64(MaxFramePayload)+1))
	if err != nil {
		return nil, fmt.Errorf("decompress payload: %w", err)
	}
	if len(decoded) > MaxFramePayload {
		return nil, fmt.Errorf("decoded payload size %d exceeds maximum %d", len(decoded), MaxFramePayload)
	}
	return decoded, nil
}

func validateSnappySize(payload []byte, expected uint32) error {
	decoded, err := snappyDecodedSize(payload)
	if err != nil {
		return err
	}
	if decoded != int(expected) {
		return fmt.Errorf("snappy decoded length mismatch: got=%d expected=%d", decoded, expected)
	}
	return nil
}

func snappyDecodedSize(payload []byte) (int, error) {
	if len(payload) < len(xerialSnappyHeader) || !bytes.Equal(payload[:len(xerialSnappyHeader)], xerialSnappyHeader) {
		decoded, err := mastersnappy.DecodedLen(payload)
		if err != nil {
			return 0, fmt.Errorf("inspect snappy payload: %w", err)
		}
		return decoded, nil
	}
	if len(payload) < xerialSnappyFrameHeaderSize {
		return 0, fmt.Errorf("malformed xerial snappy header")
	}
	total := 0
	for position := xerialSnappyFrameHeaderSize; position < len(payload); {
		if len(payload)-position < 4 {
			return 0, fmt.Errorf("malformed xerial snappy block size")
		}
		blockSize := int(binary.BigEndian.Uint32(payload[position : position+4]))
		position += 4
		if blockSize > len(payload)-position {
			return 0, fmt.Errorf("malformed xerial snappy block")
		}
		end := position + blockSize
		decoded, err := mastersnappy.DecodedLen(payload[position:end])
		if err != nil {
			return 0, fmt.Errorf("inspect xerial snappy block: %w", err)
		}
		if decoded > MaxFramePayload-total {
			return 0, fmt.Errorf("decoded payload size exceeds maximum %d", MaxFramePayload)
		}
		total += decoded
		position = end
	}
	return total, nil
}
