package util_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/cursus-io/cursus/util"
)

// TestCompressMessage_AllTypes tests all supported compression types
func TestCompressMessage_AllTypes(t *testing.T) {
	testData := []byte("Hello, World! This is a test string for compression.")

	tests := []struct {
		name            string
		compressionType string
		expectError     bool
	}{
		{"gzip", "gzip", false},
		{"snappy", "snappy", false},
		{"lz4", "lz4", false},
		{"none", "none", false},
		{"empty", "", false},
		{"unsupported", "unknown", true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			result, err := util.CompressMessage(testData, tt.compressionType)

			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error for compression type %s", tt.compressionType)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error for compression type %s: %v", tt.compressionType, err)
			}

			if tt.compressionType == "none" || tt.compressionType == "" {
				if !bytes.Equal(result, testData) {
					t.Fatalf("expected original data for type %s", tt.compressionType)
				}
			} else {
				if result == nil {
					t.Fatalf("expected non-nil compressed result for type %s", tt.compressionType)
				}
			}
		})
	}
}

// TestDecompressMessage_AllTypes tests decompression for all supported types
func TestDecompressMessage_AllTypes(t *testing.T) {
	testData := []byte("Hello, World! This is a test string for compression.")

	compressedData := make(map[string][]byte)
	for _, ct := range []string{"gzip", "snappy", "lz4", "none"} {
		data, err := util.CompressMessage(testData, ct)
		if err != nil {
			t.Fatalf("failed to compress with %s: %v", ct, err)
		}
		compressedData[ct] = data
	}

	tests := []struct {
		name            string
		compressionType string
		expectError     bool
	}{
		{"gzip", "gzip", false},
		{"snappy", "snappy", false},
		{"lz4", "lz4", false},
		{"none", "none", false},
		{"empty", "", false},
		{"unsupported", "unknown", true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var input []byte
			if tt.compressionType == "" {
				input = testData
			} else if v, ok := compressedData[tt.compressionType]; ok {
				input = v
			} else {
				input = []byte("invalid")
			}

			result, err := util.DecompressMessage(input, tt.compressionType)

			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error for compression type %s", tt.compressionType)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error for compression type %s: %v", tt.compressionType, err)
			}

			if !bytes.Equal(result, testData) {
				t.Fatalf(
					"decompressed data mismatch for type %s\nexpected=%q\ngot=%q",
					tt.compressionType,
					string(testData),
					string(result),
				)
			}
		})
	}
}

// TestCompressDecompressRoundtrip verifies roundtrip compression/decompression
func TestCompressDecompressRoundtrip(t *testing.T) {
	testCases := [][]byte{
		[]byte("a"),
		[]byte("Hello, World!"),
		make([]byte, 1000),
		make([]byte, 10000),
	}

	for _, tc := range testCases {
		tc := tc
		for _, ct := range []string{"gzip", "snappy", "lz4", "none"} {
			ct := ct

			if ct == "snappy" && len(tc) <= 1 {
				continue
			}

			t.Run(fmt.Sprintf("%s_%dB", ct, len(tc)), func(t *testing.T) {
				compressed, err := util.CompressMessage(tc, ct)
				if err != nil {
					t.Fatalf("compression failed: %v", err)
				}

				decompressed, err := util.DecompressMessage(compressed, ct)
				if err != nil {
					t.Fatalf("decompression failed: %v", err)
				}

				if !bytes.Equal(decompressed, tc) {
					t.Fatalf("roundtrip failed: original=%d decompressed=%d", len(tc), len(decompressed))
				}
			})
		}
	}
}

// TestCompressMessage_EdgeCases tests edge cases for compression
func TestCompressMessage_EdgeCases(t *testing.T) {
	tests := []struct {
		name            string
		data            []byte
		compressionType string
	}{
		{"nil_gzip", nil, "gzip"},
		{"nil_snappy", nil, "snappy"},
		{"nil_lz4", nil, "lz4"},
		{"nil_none", nil, "none"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			result, err := util.CompressMessage(tt.data, tt.compressionType)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.compressionType == "none" {
				if !bytes.Equal(result, tt.data) {
					t.Fatalf("expected passthrough behavior for none compression")
				}
			} else {
				if result == nil {
					t.Fatalf("expected non-nil result for compression type %s", tt.compressionType)
				}
			}
		})
	}
}

// TestConcurrentCompression tests thread safety of compression functions
func TestConcurrentCompression(t *testing.T) {
	testData := []byte("Hello, concurrent compression")

	var wg sync.WaitGroup
	errCh := make(chan error, 300)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			compType := []string{"gzip", "snappy", "lz4", "none"}[id%4]

			c, err := util.CompressMessage(testData, compType)
			if err != nil {
				errCh <- fmt.Errorf("compress failed (id=%d type=%s): %v", id, compType, err)
				return
			}

			d, err := util.DecompressMessage(c, compType)
			if err != nil {
				errCh <- fmt.Errorf("decompress failed (id=%d type=%s): %v", id, compType, err)
				return
			}

			if !bytes.Equal(d, testData) {
				errCh <- fmt.Errorf("data mismatch (id=%d type=%s)", id, compType)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
}
