package topic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
)

// loadBootstrapCommittedHWM reads only the committed checkpoint as a floor for
// destructive install. Legacy values are conservative floors, not readiness
// evidence. Normal checkpoint recovery retains its compatibility decoder.
func loadBootstrapCommittedHWM(path string) (uint64, error) {
	// #nosec G304 -- checkpoint path is derived from the broker-owned partition log directory.
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read bootstrap committed checkpoint: %w", err)
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		hwm, err := strconv.ParseUint(string(data), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid bootstrap committed checkpoint: %w", err)
		}
		return hwm, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if _, err = decoder.Token(); err != nil {
		return 0, fmt.Errorf("invalid bootstrap committed checkpoint: %w", err)
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return 0, fmt.Errorf("invalid bootstrap committed checkpoint: %w", err)
		}
		key, ok := token.(string)
		if !ok {
			return 0, fmt.Errorf("invalid bootstrap committed checkpoint field")
		}
		if key != "version" && key != "hwm" {
			return 0, fmt.Errorf("unknown bootstrap committed checkpoint field %q", key)
		}
		if _, exists := fields[key]; exists {
			return 0, fmt.Errorf("duplicate bootstrap committed checkpoint field %q", key)
		}
		var value json.RawMessage
		if err = decoder.Decode(&value); err != nil {
			return 0, fmt.Errorf("invalid bootstrap committed checkpoint: %w", err)
		}
		fields[key] = value
	}
	if _, err = decoder.Token(); err != nil {
		return 0, fmt.Errorf("invalid bootstrap committed checkpoint: %w", err)
	}
	if _, err = decoder.Token(); err != io.EOF {
		return 0, fmt.Errorf("bootstrap committed checkpoint contains trailing data")
	}
	var version *int
	var hwm *uint64
	if err = json.Unmarshal(fields["version"], &version); err != nil || version == nil || *version != hwmCheckpointVersion {
		return 0, fmt.Errorf("invalid bootstrap committed checkpoint version")
	}
	if err = json.Unmarshal(fields["hwm"], &hwm); err != nil || hwm == nil {
		return 0, fmt.Errorf("invalid bootstrap committed checkpoint HWM")
	}
	return *hwm, nil
}
