package fsm

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestBootstrapTransferStrictResponseManifest(t *testing.T) {
	f, _, _, request := metadataSourceFSM(t)
	ticket, err := f.PrepareMetadataBootstrapSource(context.Background(), request.Source, request, nil)
	require.NoError(t, err)
	export, err := ticket.ExportContext(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, export.Close()) }()
	response := BootstrapBeginResponse{Version: BootstrapTransferVersion, Request: request, CaptureMode: BootstrapCaptureFull, Manifest: export.Manifest(), CertificateID: strings.Repeat("b", 32)}
	require.NoError(t, response.ValidateFor(request))
	data, err := json.Marshal(response)
	require.NoError(t, err)
	var decoded BootstrapBeginResponse
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, response, decoded)
	for _, path := range [][]string{{"manifest"}, {"manifest", "fence"}, {"manifest", "start_offset"}, {"manifest", "end_offset"}, {"manifest", "fence", "partition"}, {"manifest", "fence", "committed_hwm"}, {"manifest", "files"}, {"request", "requester", "incarnation_epoch"}, {"start_offset"}} {
		for _, replacement := range []string{"missing", "null"} {
			t.Run(strings.Join(path, ".")+replacement, func(t *testing.T) {
				var values map[string]any
				require.NoError(t, json.Unmarshal(data, &values))
				current := values
				for _, field := range path[:len(path)-1] {
					current = current[field].(map[string]any)
				}
				field := path[len(path)-1]
				if replacement == "missing" {
					delete(current, field)
				} else {
					current[field] = nil
				}
				broken, err := json.Marshal(values)
				require.NoError(t, err)
				require.Error(t, json.Unmarshal(broken, &decoded))
			})
		}
	}
	for _, change := range []string{"incarnation", "fence", "mode", "start", "catalog", "file hash", "file identity"} {
		t.Run(change, func(t *testing.T) {
			bad := response
			bad.Manifest = export.Manifest()
			switch change {
			case "incarnation":
				bad.Request.Requester.IncarnationEpoch++
			case "fence":
				bad.Manifest.Fence.SourceID = "another"
			case "mode":
				bad.CaptureMode = "unknown"
			case "start":
				bad.StartOffset++
			case "catalog":
				bad.Manifest.Files[1].Size++
			case "file hash":
				bad.Manifest.Files[0].SHA256 = "bad"
			case "file identity":
				bad.Manifest.Files[0].ID = "../records"
			}
			require.Error(t, bad.ValidateFor(request))
		})
	}
}

func TestBootstrapTransferZeroSuffixAndOwnerIdentity(t *testing.T) {
	r := metadataBeginRequest()
	r.PriorCertificateID = strings.Repeat("b", 32)
	data, err := json.Marshal(r)
	require.NoError(t, err)
	var decoded BootstrapBeginRequest
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, r, decoded)
	for _, field := range []string{"broker_id", "incarnation_id", "incarnation_epoch"} {
		var values map[string]any
		require.NoError(t, json.Unmarshal(data, &values))
		delete(values["requester"].(map[string]any), field)
		broken, err := json.Marshal(values)
		require.NoError(t, err)
		require.Error(t, json.Unmarshal(broken, &decoded))
	}
	chunk := BootstrapChunkRequest{Version: BootstrapTransferVersion, Requester: r.Requester, AttemptID: r.AttemptID, TransferID: strings.Repeat("c", 32), FileID: "catalog", Size: 1}
	response := BootstrapChunkResponse{Version: chunk.Version, Requester: chunk.Requester, AttemptID: chunk.AttemptID, TransferID: chunk.TransferID, FileID: chunk.FileID, Data: []byte{}}
	require.NoError(t, response.ValidateFor(chunk, 0))
	data, err = json.Marshal(response)
	require.NoError(t, err)
	var decodedChunk BootstrapChunkResponse
	require.NoError(t, json.Unmarshal(data, &decodedChunk))
	for _, field := range []string{"offset", "data", "requester", "transfer_id"} {
		var values map[string]any
		require.NoError(t, json.Unmarshal(data, &values))
		delete(values, field)
		broken, err := json.Marshal(values)
		require.NoError(t, err)
		require.Error(t, json.Unmarshal(broken, &decodedChunk))
	}
}

func metadataBeginRequest() BootstrapBeginRequest {
	return BootstrapBeginRequest{Version: BootstrapTransferVersion, Purpose: BootstrapPurposeMetadata,
		Requester: BootstrapProcessIdentity{BrokerID: "requester", IncarnationID: "requester-process", IncarnationEpoch: 1},
		Source:    BootstrapProcessIdentity{BrokerID: "source", IncarnationID: "source-process", IncarnationEpoch: 2},
		Topic:     config.ConsumerOffsetsTopicName, Leader: "source", LeaderEpoch: 4, LifecycleEpoch: 1,
		AttemptID: strings.Repeat("a", 32)}
}

func TestBootstrapTransferStrictBegin(t *testing.T) {
	r := metadataBeginRequest()
	data, err := json.Marshal(r)
	require.NoError(t, err)
	var decoded BootstrapBeginRequest
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, r, decoded)
	for _, field := range []string{"partition", "fixed_end", "leader_epoch", "lifecycle_epoch", "source", "requester", "version", "purpose"} {
		for _, replacement := range []string{"missing", "null", `"nonnumeric"`} {
			t.Run(field+replacement, func(t *testing.T) {
				var fields map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(data, &fields))
				if replacement == "missing" {
					delete(fields, field)
				} else {
					fields[field] = json.RawMessage(replacement)
				}
				broken, err := json.Marshal(fields)
				require.NoError(t, err)
				require.Error(t, json.Unmarshal(broken, &decoded))
			})
		}
	}
	require.Error(t, json.Unmarshal(append(data[:len(data)-1], []byte(`,"unknown":1}`)...), &decoded))
	require.Error(t, decodeStrictJSON(append(data, []byte(` {}`)...), &decoded))
	for _, addition := range []string{`,"partition":0}`, `,"Partition":0}`, `,"FIXED_END":0}`} {
		require.Error(t, json.Unmarshal(append(append([]byte(nil), data[:len(data)-1]...), []byte(addition)...), &decoded))
	}
	for _, addition := range []string{`"incarnation_epoch":1,"incarnation_epoch":1`, `"incarnation_epoch":1,"Incarnation_epoch":1`} {
		require.Error(t, json.Unmarshal([]byte(strings.Replace(string(data), `"incarnation_epoch":1`, addition, 1)), &decoded))
	}
	r.Version++
	require.ErrorIs(t, r.Validate(), ErrBootstrapTransferUnsupported)
	r = metadataBeginRequest()
	r.Purpose = "unknown"
	require.ErrorIs(t, r.Validate(), ErrBootstrapTransferUnsupported)
}

func TestBootstrapTransferChunkAndRelease(t *testing.T) {
	r := BootstrapChunkRequest{Version: BootstrapTransferVersion, Requester: metadataBeginRequest().Requester,
		AttemptID: strings.Repeat("a", 32), TransferID: strings.Repeat("b", 32), FileID: "records", Size: 10}
	require.NoError(t, r.Validate())
	response := BootstrapChunkResponse{Version: r.Version, Requester: r.Requester, AttemptID: r.AttemptID, TransferID: r.TransferID, FileID: r.FileID, Offset: r.Offset, Data: []byte("abc")}
	require.NoError(t, response.ValidateFor(r, 3))
	require.Error(t, response.ValidateFor(r, 10))
	response.Offset++
	require.Error(t, response.ValidateFor(r, 3))
	data, err := json.Marshal(r)
	require.NoError(t, err)
	var decoded BootstrapChunkRequest
	require.Error(t, json.Unmarshal([]byte(strings.Replace(string(data), `"offset":0,`, "", 1)), &decoded))
	r.Size = 256<<10 + 1
	require.ErrorIs(t, r.Validate(), ErrBootstrapTransferLimit)
	release := BootstrapReleaseRequest{Version: BootstrapTransferVersion, Requester: r.Requester, AttemptID: r.AttemptID}
	require.NoError(t, release.Validate())
	data, err = json.Marshal(release)
	require.NoError(t, err)
	var roundTrip BootstrapReleaseRequest
	require.NoError(t, json.Unmarshal(data, &roundTrip))
	require.Equal(t, release, roundTrip)
}
