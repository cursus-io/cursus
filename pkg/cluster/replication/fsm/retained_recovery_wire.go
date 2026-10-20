package fsm

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cursus-io/cursus/pkg/disk"
)

func validRetainedPrefix(p *disk.RetainedPrefix) bool {
	if p == nil || p.Version != 1 || p.Start > p.End || len(p.Digest) != 64 {
		return false
	}
	_, err := hex.DecodeString(p.Digest)
	return err == nil
}

// Zero is a meaningful bound; omitted and null fields must not manufacture an
// empty-prefix challenge. Older modes reject even explicitly null new fields.
func validateRetainedJSON(data []byte, mode int, fields ...string) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return err
	}
	for _, name := range fields {
		for key := range object {
			if key != name && strings.EqualFold(key, name) {
				return fmt.Errorf("recovery field must use exact name %s", name)
			}
		}
		raw, present := object[name]
		if !present {
			continue
		}
		if mode != BootstrapRecoveryProtocolVersion {
			return fmt.Errorf("unexpected %s for recovery mode %d", name, mode)
		}
		var required struct {
			Version *uint32 `json:"version"`
			Start   *uint64 `json:"start"`
			End     *uint64 `json:"end"`
			Digest  *string `json:"digest"`
		}
		if err := json.Unmarshal(raw, &required); err != nil {
			return err
		}
		if required.Version == nil || required.Start == nil || required.End == nil || required.Digest == nil {
			return fmt.Errorf("%s requires explicit version, start, end and digest", name)
		}
	}
	return nil
}

func (r *ReplicaCatchupRequest) UnmarshalJSON(data []byte) error {
	type plain ReplicaCatchupRequest
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if err := validateRetainedJSON(data, decoded.RecoveryProtocol, "retained_prefix", "matched_retained_prefix"); err != nil {
		return err
	}
	if decoded.RecoveryProtocol == BootstrapRecoveryProtocolVersion && decoded.RetainedPrefix == nil {
		return fmt.Errorf("retained recovery request requires a challenge")
	}
	*r = ReplicaCatchupRequest(decoded)
	return nil
}

func (b *ReplicaCatchupBatch) UnmarshalJSON(data []byte) error {
	type plain ReplicaCatchupBatch
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if err := validateRetainedJSON(data, decoded.RecoveryProtocol, "retained_prefix", "matched_retained_prefix"); err != nil {
		return err
	}
	if decoded.RecoveryProtocol == BootstrapRecoveryProtocolVersion && decoded.RetainedPrefix == nil {
		return fmt.Errorf("retained recovery response requires a challenge")
	}
	*b = ReplicaCatchupBatch(decoded)
	return nil
}
