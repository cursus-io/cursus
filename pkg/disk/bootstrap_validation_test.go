package disk

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func bootstrapEncodedRecords(t *testing.T, records ...types.DiskMessage) []byte {
	t.Helper()
	var out []byte
	for _, record := range records {
		body, err := util.SerializeDiskMessage(record)
		require.NoError(t, err)
		out = binary.BigEndian.AppendUint32(out, uint32(len(body)))
		out = append(out, body...)
	}
	return out
}

func bootstrapIncomingManifest(t *testing.T, records []byte) BootstrapMetadata {
	t.Helper()
	id, err := newBootstrapID()
	require.NoError(t, err)
	file := func(id, role string, data []byte) BootstrapFile {
		sum := sha256.Sum256(data)
		return BootstrapFile{ID: id, Role: role, Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
	}
	return BootstrapMetadata{
		Version: BootstrapVersion, TransferID: id, EndOffset: 2, Fence: bootstrapTestFence(1, 2),
		Files: []BootstrapFile{file("records", "canonical-log", records), file("catalog", "snapshot-catalog", nil)},
	}
}

func requireBootstrapOldLog(t *testing.T, d *DiskHandler) {
	t.Helper()
	pending, err := d.PendingBootstrap()
	require.NoError(t, err)
	require.Nil(t, pending)
	require.NoFileExists(t, bootstrapPendingPath(d.BaseName))
	require.Equal(t, uint64(1), d.GetAbsoluteOffset())
	records := bootstrapRawRecords(t, d.GetSegmentPath(0))
	require.Len(t, records, 1)
	require.Equal(t, "keep old log", records[0].Payload)
}

func TestBootstrapRejectsMalformedManifestBeforeStaging(t *testing.T) {
	cases := []struct {
		name string
		edit func(*BootstrapMetadata)
	}{
		{"version", func(m *BootstrapMetadata) { m.Version++ }},
		{"transfer path", func(m *BootstrapMetadata) { m.TransferID = "../other" }},
		{"reversed bounds", func(m *BootstrapMetadata) { m.StartOffset = 3 }},
		{"above committed end", func(m *BootstrapMetadata) { m.EndOffset = 3 }},
		{"foreign topic", func(m *BootstrapMetadata) { m.Fence.Topic = "other" }},
		{"foreign partition", func(m *BootstrapMetadata) { m.Fence.Partition = 10 }},
		{"missing source", func(m *BootstrapMetadata) { m.Fence.SourceID = "" }},
		{"missing receiver", func(m *BootstrapMetadata) { m.Fence.ReceiverID = "" }},
		{"missing leader", func(m *BootstrapMetadata) { m.Fence.LeaderID = "" }},
		{"negative epoch", func(m *BootstrapMetadata) { m.Fence.LeaderEpoch = -1 }},
		{"missing protocol", func(m *BootstrapMetadata) { m.Fence.RecoveryProtocol = 0 }},
		{"missing catalog", func(m *BootstrapMetadata) { m.Files = m.Files[:1] }},
		{"descriptor path", func(m *BootstrapMetadata) { m.Files[0].ID = "../records" }},
		{"descriptor role", func(m *BootstrapMetadata) { m.Files[0].Role = "local-install" }},
		{"negative size", func(m *BootstrapMetadata) { m.Files[0].Size = -1 }},
		{"short checksum", func(m *BootstrapMetadata) { m.Files[0].SHA256 = "ab" }},
		{"uppercase checksum", func(m *BootstrapMetadata) { m.Files[0].SHA256 = strings.ToUpper(m.Files[0].SHA256) }},
		{"oversized records", func(m *BootstrapMetadata) { m.Files[0].Size = bootstrapStructuralMaxBytes + 1 }},
		{"oversized catalog", func(m *BootstrapMetadata) { m.Files[1].Size = BootstrapCatalogBytes + 1 }},
		{"combined size overflow", func(m *BootstrapMetadata) { m.Files[0].Size = bootstrapStructuralMaxBytes; m.Files[1].Size = 1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, _ := bootstrapTestHandler(t, 1)
			_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "keep old log"})
			require.NoError(t, err)
			before, err := os.ReadDir(filepath.Dir(d.BaseName))
			require.NoError(t, err)
			m := bootstrapIncomingManifest(t, nil)
			tc.edit(&m)
			r, err := d.StageBootstrap(m)
			require.Error(t, err)
			require.Nil(t, r)
			after, err := os.ReadDir(filepath.Dir(d.BaseName))
			require.NoError(t, err)
			require.Equal(t, before, after, "invalid metadata must not create a staging directory")
			requireBootstrapOldLog(t, d)
		})
	}
}

func TestBootstrapRejectsChecksumValidInvalidRecordsBeforeIntent(t *testing.T) {
	zero := types.DiskMessage{Topic: "orders", Partition: 1, Offset: 0, Payload: "new"}
	one := zero
	one.Offset = 1
	wrongTopic, wrongPartition, outside := zero, zero, zero
	wrongTopic.Topic = "other"
	wrongPartition.Partition = 10
	outside.Offset = 2
	valid := bootstrapEncodedRecords(t, zero, one)
	cases := []struct {
		name string
		data []byte
	}{
		{"foreign topic", bootstrapEncodedRecords(t, wrongTopic, one)},
		{"foreign partition", bootstrapEncodedRecords(t, wrongPartition, one)},
		{"outside logical end", bootstrapEncodedRecords(t, zero, outside)},
		{"duplicate offset", bootstrapEncodedRecords(t, zero, zero)},
		{"descending offset", bootstrapEncodedRecords(t, one, zero)},
		{"missing first offset", bootstrapEncodedRecords(t, one)},
		{"missing last offset", bootstrapEncodedRecords(t, zero)},
		{"partial header", append(append([]byte(nil), valid...), 0, 1)},
		{"partial body", valid[:len(valid)-1]},
		{"zero frame length", []byte{0, 0, 0, 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, _ := bootstrapTestHandler(t, 1)
			_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "keep old log"})
			require.NoError(t, err)
			// The descriptor deliberately matches the malformed bytes. A checksum
			// alone cannot establish logical identity or valid record framing.
			r, err := d.StageBootstrap(bootstrapIncomingManifest(t, tc.data))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, r.Abort()) })
			require.NoError(t, r.WriteChunk("records", 0, tc.data))
			for retry := 0; retry < 2; retry++ {
				require.Error(t, r.Install())
				requireBootstrapOldLog(t, d)
			}
		})
	}
}

func TestBootstrapRejectsDamagedDownloadBeforeIntent(t *testing.T) {
	for _, fileID := range []string{"records", "catalog"} {
		for _, damage := range []string{"missing", "truncated", "changed"} {
			t.Run(fileID+"/"+damage, func(t *testing.T) {
				source, _ := bootstrapTestHandler(t, 1)
				_, err := source.AppendMessageSync("orders", 1, &types.Message{Payload: "new"})
				require.NoError(t, err)
				e := bootstrapTestExport(t, source, 0, 1)
				d, _ := bootstrapTestHandler(t, 1)
				_, err = d.AppendMessageSync("orders", 1, &types.Message{Payload: "keep old log"})
				require.NoError(t, err)
				r := bootstrapTestReceive(t, d, e)
				path := filepath.Join(r.transfer.dir, fileID)
				switch damage {
				case "missing":
					require.NoError(t, os.Remove(path))
				case "truncated":
					require.NoError(t, os.Truncate(path, 0))
				case "changed":
					data, err := os.ReadFile(path)
					require.NoError(t, err)
					data[len(data)-1] ^= 1
					require.NoError(t, os.WriteFile(path, data, 0o600))
				}
				require.Error(t, r.Install())
				requireBootstrapOldLog(t, d)
			})
		}
	}
}

func TestBootstrapRejectsInvalidPersistentInventoryBeforeReplay(t *testing.T) {
	cases := []struct {
		name string
		edit func(*bootstrapTransaction)
	}{
		{"staging path", func(tx *bootstrapTransaction) { tx.StageID = "../neighbor" }},
		{"missing replacement", func(tx *bootstrapTransaction) { tx.Files = tx.Files[:1] }},
		{"duplicate replacement", func(tx *bootstrapTransaction) { tx.Files[1] = tx.Files[0] }},
		{"replacement role", func(tx *bootstrapTransaction) { tx.Files[0].Role = "canonical-log" }},
		{"replacement checksum", func(tx *bootstrapTransaction) { tx.Files[0].SHA256 = "bad" }},
		{"records descriptor conflict", func(tx *bootstrapTransaction) { tx.Files[0].Size++ }},
		{"neighbor deletion", func(tx *bootstrapTransaction) { tx.Delete = append(tx.Delete, "partition_10.hwm") }},
		{"parent deletion", func(tx *bootstrapTransaction) { tx.Delete = append(tx.Delete, "../other") }},
		{"duplicate deletion", func(tx *bootstrapTransaction) { tx.Delete = append(tx.Delete, tx.Delete[0]) }},
		{"replacement deletion", func(tx *bootstrapTransaction) { tx.Delete = append(tx.Delete, tx.Files[0].Destination) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source, _ := bootstrapTestHandler(t, 1)
			e := bootstrapTestExport(t, source, 0, 0)
			d, cfg := bootstrapTestHandler(t, 1)
			r := bootstrapTestReceive(t, d, e)
			require.NoError(t, r.Install())
			require.NoError(t, d.Close())
			var tx bootstrapTransaction
			require.NoError(t, readBootstrapEnvelope(bootstrapPendingPath(d.BaseName), &tx))
			// Put a valid deletion before the malformed entry. Validation must
			// finish before replay starts deleting even partition-owned files.
			sentinel := d.BaseName + ".producers"
			neighbor := filepath.Join(filepath.Dir(d.BaseName), "partition_10.hwm")
			require.NoError(t, os.WriteFile(sentinel, []byte("preserve owned file"), 0o600))
			require.NoError(t, os.WriteFile(neighbor, []byte("preserve neighbor"), 0o600))
			tx.Delete = []string{filepath.Base(sentinel)}
			tc.edit(&tx)
			// Recompute the envelope checksum so semantic validation is exercised.
			require.NoError(t, writeBootstrapEnvelope(bootstrapPendingPath(d.BaseName), tx))
			retry, err := d.StageBootstrap(e.Manifest())
			require.Error(t, err)
			require.Nil(t, retry)
			reopened, err := NewDiskHandler(cfg, "orders", 1)
			require.Error(t, err)
			require.Nil(t, reopened)
			owned, err := os.ReadFile(sentinel)
			require.NoError(t, err)
			require.Equal(t, "preserve owned file", string(owned))
			other, err := os.ReadFile(neighbor)
			require.NoError(t, err)
			require.Equal(t, "preserve neighbor", string(other))
			require.FileExists(t, bootstrapPendingPath(d.BaseName))
		})
	}
}
