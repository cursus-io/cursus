package protocol

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// CooperativePartition identifies a physical topic lifecycle, not merely its
// reusable name. The caller must supply the authoritative lifecycle epoch.
type CooperativePartition struct {
	Topic          string `json:"topic"`
	Partition      int    `json:"partition"`
	LifecycleEpoch uint64 `json:"lifecycle_epoch"`
}

type CooperativeMember struct {
	ID      string `json:"id"`
	Session uint64 `json:"session"`
}

type CooperativeTarget struct {
	Partition CooperativePartition `json:"partition"`
	Member    string               `json:"member"`
}

type CooperativeGrant struct {
	Partition      CooperativePartition `json:"partition"`
	Member         string               `json:"member"`
	Session        uint64               `json:"session"`
	Epoch          uint64               `json:"epoch"`
	RevokeRevision uint64               `json:"revoke_revision,omitempty"`
}

type CooperativeRevocation struct {
	Partition      CooperativePartition `json:"partition"`
	GrantEpoch     uint64               `json:"grant_epoch"`
	RevokeRevision uint64               `json:"revoke_revision"`
}

// CooperativeMembership is published only after the ownership checkpoint is durable.
type CooperativeMembership struct {
	RegistrationEpoch uint64             `json:"registration_epoch"`
	Revision          uint64             `json:"revision"`
	Member            string             `json:"member"`
	Session           uint64             `json:"session"`
	Grants            []CooperativeGrant `json:"grants"`
}

// CooperativeToken binds one operation to its original physical ownership.
type CooperativeToken struct {
	RegistrationEpoch uint64           `json:"registration_epoch"`
	Grant             CooperativeGrant `json:"grant"`
}

func EncodeCooperative(value any) string {
	data, _ := json.Marshal(value)
	return base64.RawURLEncoding.EncodeToString(data)
}
func DecodeCooperative(value string, target any) error {
	if len(value) == 0 || len(value) > 16*1024*1024 {
		return fmt.Errorf("ERROR: invalid_cooperative_payload")
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return fmt.Errorf("ERROR: invalid_cooperative_payload")
	}
	if err = json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("ERROR: invalid_cooperative_payload")
	}
	return nil
}
