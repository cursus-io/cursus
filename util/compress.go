package util

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
	snappy "github.com/eapache/go-xerial-snappy"
)

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

// DecompressMessage decompresses a message if enabled
func DecompressMessage(data []byte, compressionType string) ([]byte, error) {
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
		return io.ReadAll(gr)

	case "snappy":
		return snappy.Decode(data)

	case "lz4":
		reader := lz4.NewReader(bytes.NewReader(data))
		return io.ReadAll(reader)

	case "none", "":
		return data, nil

	default:
		return nil, fmt.Errorf("unsupported compression type: %s", compressionType)
	}
}
