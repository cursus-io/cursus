package topic

import "fmt"

// ReadTopicDefinitionsFile reads a strict manifest-shaped operator definition file.
func ReadTopicDefinitionsFile(path string) ([]Definition, error) {
	definitions, err := readStandaloneManifest(path)
	if err != nil {
		return nil, fmt.Errorf("read explicit topic definitions: %w", err)
	}
	return definitions, nil
}
