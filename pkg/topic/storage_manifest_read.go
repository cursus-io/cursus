package topic

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

func readStandaloneManifestIfPresent(path string) ([]Definition, error) {
	definitions, err := readStandaloneManifest(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return definitions, err
}

func readStandaloneManifest(path string) ([]Definition, error) {
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !pathInfo.Mode().IsRegular() || pathInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("topic metadata manifest must be a regular file")
	}
	file, err := os.Open(path) // #nosec G304 -- caller supplies the broker-owned manifest path.
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat topic metadata: %w", err)
	}
	if info.Size() > maxTopicMetadataBytes {
		return nil, fmt.Errorf("topic metadata size %d exceeds limit %d", info.Size(), maxTopicMetadataBytes)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxTopicMetadataBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read topic metadata: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest topicMetadataManifest
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode topic metadata: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode topic metadata trailing content")
	}
	if manifest.Version != topicMetadataFormatVersion {
		return nil, fmt.Errorf("unsupported topic metadata version %d", manifest.Version)
	}
	definitions, _, err := normalizeAndMarshalManifest(manifest.Topics)
	if err != nil {
		return nil, err
	}
	sort.Slice(definitions, func(i, j int) bool { return definitions[i].Name < definitions[j].Name })
	return definitions, nil
}
