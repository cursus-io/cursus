package util

import "github.com/cursus-io/cursus/pkg/wire"

func CompressMessage(data []byte, compressionType string) ([]byte, error) {
	compression, err := wire.ParseCompression(compressionType)
	if err != nil {
		return nil, err
	}
	return wire.Compress(data, compression)
}

func DecompressMessage(data []byte, compressionType string) ([]byte, error) {
	compression, err := wire.ParseCompression(compressionType)
	if err != nil {
		return nil, err
	}
	return wire.DecompressBounded(data, compression)
}
