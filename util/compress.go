package util

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"

	snappy "github.com/eapache/go-xerial-snappy"
	mastersnappy "github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

var xerialSnappyHeader = []byte{130, 83, 78, 65, 80, 80, 89, 0}

const xerialSnappyFrameHeaderSize = 16

// CompressMessage compresses a message if enabled
func CompressMessage(data []byte, compressionType string) ([]byte, error) {
	switch compressionType {
	case "gzip":
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		if _, err := gw.Write(data); err != nil {
			return nil, err
		}
		if err := gw.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil

	case "snappy":
		return snappy.Encode(data), nil

	case "lz4":
		var buf bytes.Buffer
		zw := lz4.NewWriter(&buf)
		if _, err := zw.Write(data); err != nil {
			return nil, err
		}
		if err := zw.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil

	case "none", "":
		return data, nil

	default:
		return nil, fmt.Errorf("unsupported compression type: %s", compressionType)
	}
}

// DecompressMessage decompresses a message if enabled.
func DecompressMessage(data []byte, compressionType string) ([]byte, error) {
	if len(data) > MaxMessageSize {
		return nil, fmt.Errorf("compressed message size %d exceeds maximum %d", len(data), MaxMessageSize)
	}

	switch compressionType {
	case "gzip":
		gr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer func() {
			if err := gr.Close(); err != nil {
				Error("failed to close gr: %v", err)
			}
		}()
		return readAllDecompressed(gr)

	case "snappy":
		if err := validateSnappyDecodedSize(data); err != nil {
			return nil, err
		}
		return snappy.Decode(data)

	case "lz4":
		reader := lz4.NewReader(bytes.NewReader(data))
		return readAllDecompressed(reader)

	case "none", "":
		return data, nil

	default:
		return nil, fmt.Errorf("unsupported compression type: %s", compressionType)
	}
}

func readAllDecompressed(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, int64(MaxMessageSize)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxMessageSize {
		return nil, decompressedSizeError(len(data))
	}
	return data, nil
}

func validateSnappyDecodedSize(data []byte) error {
	if len(data) < len(xerialSnappyHeader) || !bytes.Equal(data[:len(xerialSnappyHeader)], xerialSnappyHeader) {
		decodedSize, err := mastersnappy.DecodedLen(data)
		if err != nil {
			return err
		}
		if decodedSize > MaxMessageSize {
			return decompressedSizeError(decodedSize)
		}
		return nil
	}

	if len(data) < xerialSnappyFrameHeaderSize {
		return fmt.Errorf("malformed xerial snappy framing: header is %d bytes", len(data))
	}

	totalDecoded := 0
	for position := xerialSnappyFrameHeaderSize; position < len(data); {
		if len(data)-position < 4 {
			return fmt.Errorf("malformed xerial snappy framing: truncated block size")
		}

		compressedSize := int(binary.BigEndian.Uint32(data[position : position+4]))
		position += 4
		if compressedSize > len(data)-position {
			return fmt.Errorf("malformed xerial snappy framing: truncated block")
		}

		nextPosition := position + compressedSize
		decodedSize, err := mastersnappy.DecodedLen(data[position:nextPosition])
		if err != nil {
			return err
		}
		if decodedSize > MaxMessageSize-totalDecoded {
			return decompressedSizeError(totalDecoded + decodedSize)
		}

		totalDecoded += decodedSize
		position = nextPosition
	}

	return nil
}

func decompressedSizeError(size int) error {
	return fmt.Errorf("decompressed message size %d exceeds maximum %d", size, MaxMessageSize)
}
