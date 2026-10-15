package fsm

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
)

const (
	BootstrapTransferVersion = 1
	BootstrapPurposeMetadata = "metadata"
	BootstrapPurposeReplica  = "replica"
	BootstrapCaptureFull     = "full"
	BootstrapCaptureSuffix   = "suffix"
)

// BootstrapTransferError is a stable classification for the future transport.
// Storage causes remain wrapped; corruption must never become a continuity miss.
type BootstrapTransferError string

func (e BootstrapTransferError) Error() string { return string(e) }

const (
	ErrBootstrapTransferUnavailable     BootstrapTransferError = "bootstrap unavailable"
	ErrBootstrapTransferUnsupported     BootstrapTransferError = "bootstrap unsupported"
	ErrBootstrapTransferStaleFence      BootstrapTransferError = "bootstrap stale fence"
	ErrBootstrapTransferAttemptConflict BootstrapTransferError = "bootstrap attempt conflict"
	ErrBootstrapTransferLimit           BootstrapTransferError = "bootstrap limit"
	ErrBootstrapTransferNotFound        BootstrapTransferError = "bootstrap expired or not found"
	ErrBootstrapContinuityMiss          BootstrapTransferError = "bootstrap continuity miss"
)

type BootstrapProcessIdentity struct {
	BrokerID         string `json:"broker_id"`
	IncarnationID    string `json:"incarnation_id"`
	IncarnationEpoch uint64 `json:"incarnation_epoch"`
}

func (p BootstrapProcessIdentity) valid() bool {
	return p.BrokerID != "" && p.IncarnationID != "" && p.IncarnationEpoch > 0
}

// BootstrapBeginRequest is comparable: the entire value is the attempt's exact
// fingerprint. SuffixStart is zero without a prior certificate; when a certificate
// is supplied, zero is a meaningful suffix boundary and must be sent explicitly.
type BootstrapBeginRequest struct {
	Version            int                      `json:"version"`
	Purpose            string                   `json:"purpose"`
	Requester          BootstrapProcessIdentity `json:"requester"`
	Source             BootstrapProcessIdentity `json:"source"`
	Topic              string                   `json:"topic"`
	Partition          int                      `json:"partition"`
	Leader             string                   `json:"leader"`
	LeaderEpoch        int                      `json:"leader_epoch"`
	LifecycleEpoch     uint64                   `json:"lifecycle_epoch"`
	AttemptID          string                   `json:"attempt_id"`
	FixedEnd           uint64                   `json:"fixed_end"`
	PriorCertificateID string                   `json:"prior_certificate_id,omitempty"`
	SuffixStart        uint64                   `json:"suffix_start"`
}

func (r BootstrapBeginRequest) Validate() error {
	if r.Version != BootstrapTransferVersion || r.Purpose != BootstrapPurposeMetadata {
		return ErrBootstrapTransferUnsupported
	}
	if !r.Requester.valid() || !r.Source.valid() || r.Topic != config.ConsumerOffsetsTopicName || r.Partition < 0 || r.Leader == "" || r.LeaderEpoch < 0 || r.LifecycleEpoch == 0 || !validTransferID(r.AttemptID) {
		return fmt.Errorf("invalid bootstrap begin identity or fence")
	}
	if r.PriorCertificateID == "" {
		if r.SuffixStart != 0 {
			return fmt.Errorf("suffix requires prior certificate")
		}
	} else if !validTransferID(r.PriorCertificateID) || r.SuffixStart > r.FixedEnd {
		return fmt.Errorf("invalid bootstrap suffix")
	}
	return nil
}
func (r BootstrapBeginRequest) diskFence() disk.BootstrapFence {
	return disk.BootstrapFence{Topic: r.Topic, Partition: r.Partition, ReceiverID: r.Requester.BrokerID, SourceID: r.Source.BrokerID, LeaderID: r.Leader, LeaderEpoch: r.LeaderEpoch, LifecycleEpoch: r.LifecycleEpoch, CommittedHWM: r.FixedEnd, RecoveryProtocol: BootstrapRecoveryProtocolVersion}
}

type BootstrapBeginResponse struct {
	Version       int                    `json:"version"`
	Request       BootstrapBeginRequest  `json:"request"`
	CaptureMode   string                 `json:"capture_mode"`
	StartOffset   uint64                 `json:"start_offset"`
	Manifest      disk.BootstrapMetadata `json:"manifest"`
	CertificateID string                 `json:"certificate_id"`
}

func (r BootstrapBeginResponse) ValidateFor(request BootstrapBeginRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if r.Version != BootstrapTransferVersion {
		return ErrBootstrapTransferUnsupported
	}
	if r.Request != request || !validTransferID(r.CertificateID) {
		return fmt.Errorf("bootstrap response identity mismatch")
	}
	m := r.Manifest
	if m.Version != disk.BootstrapVersion || !validTransferID(m.TransferID) || m.Fence != request.diskFence() || m.StartOffset != r.StartOffset || m.EndOffset != request.FixedEnd || m.StartOffset > m.EndOffset {
		return fmt.Errorf("bootstrap manifest fence or range mismatch")
	}
	if r.CaptureMode != BootstrapCaptureFull && r.CaptureMode != BootstrapCaptureSuffix {
		return fmt.Errorf("invalid bootstrap capture mode")
	}
	if r.CaptureMode == BootstrapCaptureSuffix && (request.PriorCertificateID == "" || r.StartOffset != request.SuffixStart) {
		return fmt.Errorf("bootstrap suffix mismatch")
	}
	if len(m.Files) != 2 {
		return fmt.Errorf("bootstrap requires records and empty catalog")
	}
	for i, f := range m.Files {
		id, role := "records", "canonical-log"
		if i == 1 {
			id, role = "catalog", "snapshot-catalog"
		}
		if f.ID != id || f.Role != role || f.Size < 0 || !validHex(f.SHA256, 32) || (i == 1 && f.Size != 0) {
			return fmt.Errorf("invalid metadata bootstrap file")
		}
	}
	return nil
}

type BootstrapChunkRequest struct {
	Version    int                      `json:"version"`
	Requester  BootstrapProcessIdentity `json:"requester"`
	AttemptID  string                   `json:"attempt_id"`
	TransferID string                   `json:"transfer_id"`
	FileID     string                   `json:"file_id"`
	Offset     int64                    `json:"offset"`
	Size       int                      `json:"size"`
}

func (r BootstrapChunkRequest) Validate() error {
	if r.Version != BootstrapTransferVersion {
		return ErrBootstrapTransferUnsupported
	}
	if !r.Requester.valid() || !validTransferID(r.AttemptID) || !validTransferID(r.TransferID) || (r.FileID != "records" && r.FileID != "catalog") || r.Offset < 0 {
		return fmt.Errorf("invalid bootstrap chunk identity")
	}
	if r.Size < 1 || r.Size > disk.BootstrapChunkBytes {
		return ErrBootstrapTransferLimit
	}
	return nil
}

type BootstrapChunkResponse struct {
	Version    int                      `json:"version"`
	Requester  BootstrapProcessIdentity `json:"requester"`
	AttemptID  string                   `json:"attempt_id"`
	TransferID string                   `json:"transfer_id"`
	FileID     string                   `json:"file_id"`
	Offset     int64                    `json:"offset"`
	Data       []byte                   `json:"data"`
}

func (r BootstrapChunkResponse) ValidateFor(request BootstrapChunkRequest, fileSize int64) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if r.Version != request.Version || r.Requester != request.Requester || r.AttemptID != request.AttemptID || r.TransferID != request.TransferID || r.FileID != request.FileID || r.Offset != request.Offset {
		return fmt.Errorf("bootstrap chunk response identity mismatch")
	}
	if fileSize < 0 || request.Offset > fileSize {
		return fmt.Errorf("bootstrap chunk outside file")
	}
	expected := min(int64(request.Size), fileSize-request.Offset)
	if int64(len(r.Data)) != expected {
		return fmt.Errorf("bootstrap chunk length mismatch")
	}
	return nil
}

// Release deliberately carries no leader or lifecycle fence: an old transfer
// still needs cleanup after topology changes. The current owner is mandatory.
type BootstrapReleaseRequest struct {
	Version    int                      `json:"version"`
	Requester  BootstrapProcessIdentity `json:"requester"`
	AttemptID  string                   `json:"attempt_id"`
	TransferID string                   `json:"transfer_id,omitempty"`
}

func (r BootstrapReleaseRequest) Validate() error {
	if r.Version != BootstrapTransferVersion {
		return ErrBootstrapTransferUnsupported
	}
	if !r.Requester.valid() || !validTransferID(r.AttemptID) || (r.TransferID != "" && !validTransferID(r.TransferID)) {
		return fmt.Errorf("invalid bootstrap release identity")
	}
	return nil
}

type BootstrapReleaseResponse struct {
	Version int                     `json:"version"`
	Request BootstrapReleaseRequest `json:"request"`
}

func (r BootstrapReleaseResponse) ValidateFor(request BootstrapReleaseRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if r.Version != BootstrapTransferVersion || r.Request != request {
		return fmt.Errorf("bootstrap release response mismatch")
	}
	return nil
}

// NewBootstrapAttemptID creates a process-independent 128-bit retry identity.
func NewBootstrapAttemptID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}
func validTransferID(id string) bool { return validHex(id, 16) }
func validHex(s string, n int) bool {
	if len(s) != 2*n {
		return false
	}
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == n && hex.EncodeToString(b) == s
}

// Decode presence separately from values so valid numeric zero cannot be
// confused with missing/null. Recursing also covers the disk manifest and fence.
func decodeTransferJSON(data []byte, target any) error {
	if err := decodeStrictJSON(data, target); err != nil {
		return err
	}
	return requireTransferFields(data, reflect.TypeOf(target).Elem())
}
func requireTransferFields(data []byte, kind reflect.Type) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("null bootstrap field")
	}
	if kind.Kind() == reflect.Struct {
		fields, err := transferObjectFields(data)
		if err != nil {
			return err
		}
		for i := 0; i < kind.NumField(); i++ {
			field := kind.Field(i)
			tag := strings.Split(field.Tag.Get("json"), ",")
			if tag[0] == "-" {
				continue
			}
			value, exists := fields[tag[0]]
			optional := len(tag) > 1 && tag[1] == "omitempty"
			if !exists {
				if optional {
					continue
				}
				return fmt.Errorf("missing bootstrap field %s", tag[0])
			}
			if err := requireTransferFields(value, field.Type); err != nil {
				return fmt.Errorf("%s: %w", tag[0], err)
			}
			delete(fields, tag[0])
		}
		if len(fields) != 0 {
			return fmt.Errorf("unknown or noncanonical bootstrap field")
		}
	} else if kind.Kind() == reflect.Slice && kind.Elem().Kind() == reflect.Struct {
		var values []json.RawMessage
		if err := json.Unmarshal(data, &values); err != nil {
			return err
		}
		for _, value := range values {
			if err := requireTransferFields(value, kind.Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}

func transferObjectFields(data []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return nil, fmt.Errorf("bootstrap field requires an object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("invalid bootstrap object key")
		}
		if _, exists := fields[key]; exists {
			return nil, fmt.Errorf("duplicate bootstrap field %s", key)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields[key] = value
	}
	return fields, nil
}
func (r *BootstrapBeginRequest) UnmarshalJSON(data []byte) error {
	type wire BootstrapBeginRequest
	var v wire
	if err := decodeTransferJSON(data, &v); err != nil {
		return err
	}
	candidate := BootstrapBeginRequest(v)
	if err := candidate.Validate(); err != nil {
		return err
	}
	*r = candidate
	return nil
}
func (r *BootstrapBeginResponse) UnmarshalJSON(data []byte) error {
	type wire BootstrapBeginResponse
	var v wire
	if err := decodeTransferJSON(data, &v); err != nil {
		return err
	}
	candidate := BootstrapBeginResponse(v)
	if err := candidate.ValidateFor(candidate.Request); err != nil {
		return err
	}
	*r = candidate
	return nil
}
func (r *BootstrapChunkRequest) UnmarshalJSON(data []byte) error {
	type wire BootstrapChunkRequest
	var v wire
	if err := decodeTransferJSON(data, &v); err != nil {
		return err
	}
	candidate := BootstrapChunkRequest(v)
	if err := candidate.Validate(); err != nil {
		return err
	}
	*r = candidate
	return nil
}
func (r *BootstrapChunkResponse) UnmarshalJSON(data []byte) error {
	type wire BootstrapChunkResponse
	var v wire
	if err := decodeTransferJSON(data, &v); err != nil {
		return err
	}
	candidate := BootstrapChunkResponse(v)
	request := BootstrapChunkRequest{Version: candidate.Version, Requester: candidate.Requester, AttemptID: candidate.AttemptID, TransferID: candidate.TransferID, FileID: candidate.FileID, Offset: candidate.Offset, Size: disk.BootstrapChunkBytes}
	if err := request.Validate(); err != nil {
		return err
	}
	if len(candidate.Data) > disk.BootstrapChunkBytes {
		return ErrBootstrapTransferLimit
	}
	*r = candidate
	return nil
}
func (r *BootstrapReleaseRequest) UnmarshalJSON(data []byte) error {
	type wire BootstrapReleaseRequest
	var v wire
	if err := decodeTransferJSON(data, &v); err != nil {
		return err
	}
	candidate := BootstrapReleaseRequest(v)
	if err := candidate.Validate(); err != nil {
		return err
	}
	*r = candidate
	return nil
}
func (r *BootstrapReleaseResponse) UnmarshalJSON(data []byte) error {
	type wire BootstrapReleaseResponse
	var v wire
	if err := decodeTransferJSON(data, &v); err != nil {
		return err
	}
	candidate := BootstrapReleaseResponse(v)
	if err := candidate.ValidateFor(candidate.Request); err != nil {
		return err
	}
	*r = candidate
	return nil
}

func classifyBootstrapStorageError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, disk.ErrStaleContentGeneration) {
		return errors.Join(ErrBootstrapTransferStaleFence, err)
	}
	var limit *disk.BootstrapLimitError
	if errors.As(err, &limit) {
		return errors.Join(ErrBootstrapTransferLimit, err)
	}
	return err
}
