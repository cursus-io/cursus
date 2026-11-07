package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

const storageRecoveryRecords = 32

func storageRecoveryPayload(sequence int) string {
	return fmt.Sprintf("ack-recovery-%04d-%s", sequence, strings.Repeat("x", 256))
}

func publishStorageRecoveryRecord(t *testing.T, port, sequence int) types.AckResponse {
	t.Helper()
	response := brokerCommand(t, port, fmt.Sprintf("PUBLISH topic=events partition=0 producerId=power-producer epoch=1 seqNum=%d isIdempotent=true acks=all message=%s", sequence, storageRecoveryPayload(sequence)))
	var ack types.AckResponse
	require.NoError(t, json.Unmarshal([]byte(response), &ack), response)
	require.Equal(t, "OK", ack.Status, response)
	require.Equal(t, uint64(sequence-1), ack.LastOffset, response)
	return ack
}

func seedStorageRecoveryRecords(t *testing.T, port int) {
	t.Helper()
	require.Contains(t, brokerCommand(t, port, "CREATE topic=events partitions=1 idempotent=true min_in_sync_replicas=1"), "OK")
	for sequence := 1; sequence <= storageRecoveryRecords; sequence++ {
		publishStorageRecoveryRecord(t, port, sequence)
	}
	for _, group := range []string{"workers", "empty-workers"} {
		require.Contains(t, brokerCommand(t, port, "REGISTER_GROUP topic=events group="+group), "OK")
	}
	joined := joinResponsePattern.FindStringSubmatch(brokerCommand(t, port, "JOIN_GROUP topic=events group=workers member=power-seed"))
	require.Len(t, joined, 3)
	// This is the final durable ACK before the external guest power cut. Do not
	// sync a proof file, leave the group, or gracefully stop the broker here.
	require.Equal(t, "OK", brokerCommand(t, port, fmt.Sprintf("COMMIT_OFFSET topic=events partition=0 group=workers offset=13 generation=%s member=%s", joined[1], joined[2])))
}

func verifyStorageRecoveryRecords(t *testing.T, port int) {
	t.Helper()
	for group, offset := range map[string]int{"workers": 13, "empty-workers": 0} {
		require.Equal(t, fmt.Sprintf("OK offset=%d", offset), brokerCommand(t, port, "FETCH_OFFSET topic=events partition=0 group="+group))
	}
	require.Contains(t, brokerCommand(t, port, "LIST_GROUPS"), "empty-workers")
	// Retrying the acknowledged identity must not append a second record.
	publishStorageRecoveryRecord(t, port, storageRecoveryRecords)
	require.Contains(t, brokerCommand(t, port, "LIST_OFFSETS topic=events partition=0"), "latest=32:leo=32:hwm=32")
	client := e2e.NewBrokerClient([]string{fmt.Sprintf("127.0.0.1:%d", port)})
	defer client.Close()
	generation, member, err := client.JoinGroup("events", "recovery-readback")
	require.NoError(t, err)
	_, err = client.SyncGroup("events", "recovery-readback", generation, member)
	require.NoError(t, err)
	var next uint64
	for next < storageRecoveryRecords {
		payloads, offsets, err := client.ConsumeMessagesAtOffsetWithOffsets("events", 0, next, "recovery-readback", member, generation, 10*time.Second)
		require.NoError(t, err)
		require.NotEmpty(t, payloads)
		require.Len(t, offsets, len(payloads))
		for i, payload := range payloads {
			require.Less(t, next, uint64(storageRecoveryRecords))
			require.Equal(t, next, offsets[i])
			require.Equal(t, storageRecoveryPayload(int(next)+1), payload)
			next++
		}
	}
	publishStorageRecoveryRecord(t, port, storageRecoveryRecords+1)
	require.Contains(t, brokerCommand(t, port, "LIST_OFFSETS topic=events partition=0"), "latest=33:leo=33:hwm=33")
	t.Logf("verified records=%d payload_bytes=%d committed_offset=13 new_end=33", storageRecoveryRecords, storageRecoveryRecords*len(storageRecoveryPayload(1)))
}

func TestStandaloneBrokerAcknowledgedPrefixPowerRecovery(t *testing.T) {
	if phase := os.Getenv("CURSUS_POWER_PHASE"); phase != "" && phase != "acknowledged_prefix" {
		t.Skip("another guest power boundary selected")
	}
	t.Setenv("CURSUS_E2E_FAULTS", "1")
	root := t.TempDir()
	if selected := os.Getenv("CURSUS_POWER_ROOT"); selected != "" {
		root = selected
	}
	if os.Getenv("CURSUS_POWER_STAGE") != "verify" {
		port, health := reserveBrokerTestPorts(t)
		child, output := startBrokerRestartChild(t, root, port, health)
		waitForBrokerReady(t, health, output)
		seedStorageRecoveryRecords(t, port)
		if os.Getenv("CURSUS_POWER_STAGE") == "seed" {
			fmt.Println("POWER_CUT_READY acknowledged_prefix")
			time.Sleep(time.Hour)
			t.Fatal("external guest power-cut harness did not stop the VM")
		}
		require.NoError(t, child.Process.Kill())
		require.Error(t, child.Wait())
	}
	began := time.Now()
	port, health := reserveBrokerTestPorts(t)
	child, output := startBrokerRestartChild(t, root, port, health)
	waitForBrokerReady(t, health, output)
	verifyStorageRecoveryRecords(t, port)
	t.Logf("acknowledged prefix recovery and readback elapsed=%s", time.Since(began))
	require.NoError(t, child.Process.Kill())
	require.Error(t, child.Wait())
}

type recoveryFileDigest struct {
	Bytes  int64
	SHA256 string
}

func recoveryDirectoryDigests(t *testing.T, root string) map[string]recoveryFileDigest {
	t.Helper()
	result := make(map[string]recoveryFileDigest)
	require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unexpected nonregular backup file: %s", path)
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		digest := sha256.New()
		count, err := io.Copy(digest, file)
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		result[relative] = recoveryFileDigest{count, hex.EncodeToString(digest.Sum(nil))}
		return nil
	}))
	return result
}

func TestStandaloneBrokerBackupCorruptionRestore(t *testing.T) {
	t.Setenv("CURSUS_E2E_FAULTS", "1")
	root, backup := t.TempDir(), t.TempDir()
	port, health := reserveBrokerTestPorts(t)
	child, output := startBrokerRestartChild(t, root, port, health)
	waitForBrokerReady(t, health, output)
	seedStorageRecoveryRecords(t, port)
	require.NoError(t, child.Process.Kill())
	require.Error(t, child.Wait())
	source := recoveryDirectoryDigests(t, root)
	require.NoError(t, os.CopyFS(backup, os.DirFS(root)))
	require.Equal(t, source, recoveryDirectoryDigests(t, backup))
	_, err := topic.ValidateStandaloneBackup(backup)
	require.NoError(t, err)
	require.Equal(t, source, recoveryDirectoryDigests(t, backup), "validation must not modify the backup")
	var copiedBytes int64
	for _, digest := range source {
		copiedBytes += digest.Bytes
	}
	t.Logf("backup_generation files=%d copied_bytes=%d digest_read_bytes=%d", len(source), copiedBytes, 3*copiedBytes)
	for _, scenario := range []string{"corrupt-record", "corrupt-derived-index", "validated-restore"} {
		t.Run(scenario, func(t *testing.T) {
			clone := t.TempDir()
			require.NoError(t, os.CopyFS(clone, os.DirFS(backup)))
			require.Equal(t, source, recoveryDirectoryDigests(t, clone))
			var damaged string
			var corrupted []byte
			switch scenario {
			case "corrupt-record":
				logs, err := filepath.Glob(filepath.Join(clone, "events", "*.log"))
				require.NoError(t, err)
				require.Len(t, logs, 1)
				damaged = logs[0]
				corrupted, err = os.ReadFile(damaged)
				require.NoError(t, err)
				position := bytes.Index(corrupted, []byte(storageRecoveryPayload(16)))
				require.GreaterOrEqual(t, position, 0)
				corrupted[position] ^= 1 // Complete frame with an invalid checksum.
				require.NoError(t, os.WriteFile(damaged, corrupted, 0o600))
				_, err = topic.ValidateStandaloneBackup(clone)
				require.Error(t, err)
			case "corrupt-derived-index":
				indexes, err := filepath.Glob(filepath.Join(clone, "events", "*.index"))
				require.NoError(t, err)
				require.NotEmpty(t, indexes)
				for _, index := range indexes {
					require.NoError(t, os.WriteFile(index, []byte{0xff, 0x01, 0x02}, 0o600))
				}
			}
			began := time.Now()
			port, health := reserveBrokerTestPorts(t)
			child, output := startBrokerRestartChild(t, clone, port, health)
			if scenario == "corrupt-record" {
				// Corrupt startup intentionally leaves the diagnostics listener
				// alive, while keeping readiness and the client listener closed.
				healthClient := &http.Client{Timeout: time.Second}
				require.Eventually(t, func() bool {
					response, err := healthClient.Get(fmt.Sprintf("http://127.0.0.1:%d/ready", health)) // #nosec G107 -- reserved loopback child port.
					if err != nil {
						return false
					}
					body, readErr := io.ReadAll(io.LimitReader(response.Body, 16<<10))
					closeErr := response.Body.Close()
					return readErr == nil && closeErr == nil && response.StatusCode == http.StatusServiceUnavailable && strings.Contains(strings.ToLower(string(body)), "checksum")
				}, 20*time.Second, 100*time.Millisecond)
				connection, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
				if connection != nil {
					require.NoError(t, connection.Close())
				}
				require.Error(t, err, "corrupt broker must not accept clients")
				require.NoError(t, child.Process.Kill())
				require.Error(t, child.Wait())
				require.Contains(t, strings.ToLower(output.String()), "checksum")
				after, err := os.ReadFile(damaged)
				require.NoError(t, err)
				require.Equal(t, corrupted, after, "preserve the corrupt authoritative log")
			} else {
				waitForBrokerReady(t, health, output)
				verifyStorageRecoveryRecords(t, port)
				require.NoError(t, child.Process.Kill())
				require.Error(t, child.Wait())
			}
			t.Logf("recovery scenario=%s clone_bytes=%d elapsed=%s", scenario, copiedBytes, time.Since(began))
			require.Equal(t, source, recoveryDirectoryDigests(t, backup), "rehearsal must preserve its backup generation")
		})
	}
}
