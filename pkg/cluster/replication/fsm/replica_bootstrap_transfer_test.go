package fsm

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/stretchr/testify/require"
)

func TestBootstrapPurposeSpecificWire(t *testing.T) {
	for _, purpose := range []string{BootstrapPurposeMetadata, BootstrapPurposeReplica} {
		t.Run(purpose, func(t *testing.T) {
			r := metadataBeginRequest()
			r.Purpose = purpose
			data, err := json.Marshal(r)
			require.NoError(t, err)
			selected, forbidden := "fixed_end", []string{"minimum_end"}
			if purpose == BootstrapPurposeReplica {
				selected, forbidden = "minimum_end", []string{"fixed_end", "prior_certificate_id", "suffix_start"}
			}
			require.Contains(t, string(data), `"`+selected+`":0`)
			for _, field := range forbidden {
				require.NotContains(t, string(data), `"`+field+`"`)
			}
			var decoded BootstrapBeginRequest
			require.NoError(t, json.Unmarshal(data, &decoded))
			require.Equal(t, r, decoded)
			response := BootstrapBeginResponse{Version: 1, Request: r, CaptureMode: BootstrapCaptureFull,
				Manifest: disk.BootstrapMetadata{Version: disk.BootstrapVersion, TransferID: strings.Repeat("b", 32), Fence: r.diskFence(), Files: []disk.BootstrapFile{
					{ID: "records", Role: "canonical-log", SHA256: strings.Repeat("c", 64)}, {ID: "catalog", Role: "snapshot-catalog", SHA256: strings.Repeat("c", 64)},
				}}}
			if purpose == BootstrapPurposeMetadata {
				response.CertificateID = strings.Repeat("d", 32)
			}
			encoded, err := json.Marshal(response)
			require.NoError(t, err)
			var decodedResponse BootstrapBeginResponse
			require.NoError(t, json.Unmarshal(encoded, &decodedResponse))
			if purpose == BootstrapPurposeReplica {
				require.NotContains(t, string(encoded), "certificate_id")
				bad := strings.TrimSuffix(string(encoded), "}") + `,"certificate_id":""}`
				require.Error(t, json.Unmarshal([]byte(bad), &decodedResponse))
			}
			for _, mutation := range append(forbidden, "missing", "null", "duplicate", "case", "unknown") {
				t.Run(mutation, func(t *testing.T) {
					bad := string(data)
					switch mutation {
					case "missing":
						bad = strings.Replace(bad, `"`+selected+`":0`, `"removed":0`, 1)
					case "null":
						bad = strings.Replace(bad, `"`+selected+`":0`, `"`+selected+`":null`, 1)
					case "duplicate":
						bad = strings.TrimSuffix(bad, "}") + `,"` + selected + `":0}`
					case "case":
						bad = strings.Replace(bad, `"`+selected+`"`, `"`+strings.ToUpper(selected)+`"`, 1)
					case "prior_certificate_id":
						bad = strings.TrimSuffix(bad, "}") + `,"prior_certificate_id":""}`
					default:
						bad = strings.TrimSuffix(bad, "}") + `,"` + mutation + `":0}`
					}
					require.Error(t, json.Unmarshal([]byte(bad), &decoded))
					// Nested validation must apply the same purpose-specific presence rules.
					nested := strings.Replace(string(encoded), string(data), bad, 1)
					require.Error(t, json.Unmarshal([]byte(nested), &decodedResponse))
				})
			}
		})
	}
}

func TestBootstrapReplicaValueValidation(t *testing.T) {
	r := metadataBeginRequest()
	r.Purpose = BootstrapPurposeReplica
	r.MinimumEnd = 2
	require.NoError(t, r.Validate())
	for _, field := range []string{"fixed", "prior", "suffix"} {
		bad := r
		switch field {
		case "fixed":
			bad.FixedEnd = 1
		case "prior":
			bad.PriorCertificateID = strings.Repeat("b", 32)
		case "suffix":
			bad.SuffixStart = 1
		}
		require.Error(t, bad.Validate())
	}
	r = metadataBeginRequest()
	r.MinimumEnd = 1
	require.Error(t, r.Validate())
}
