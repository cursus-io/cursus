//go:build e2e_faults

package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestStandaloneBrokerTruncateJournalCrashRecovery(t *testing.T) {
	t.Setenv("CURSUS_E2E_FAULTS", "1")
	for _, phase := range []string{"journal_synced", "journal_renamed", "journal_directory_synced", "journal_manifest_written", "journal_manifest_synced", "journal_manifest_directory_synced"} {
		if selected := os.Getenv("CURSUS_POWER_PHASE"); selected != "" && selected != phase {
			continue
		}
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			if selected := os.Getenv("CURSUS_POWER_ROOT"); selected != "" {
				root = selected
				require.NoError(t, os.MkdirAll(root, 0o700))
			}
			journalPath := filepath.Join(root, "__transaction_state.journal")
			var proof struct{ Instance, Epoch, Producer string }
			if os.Getenv("CURSUS_POWER_STAGE") == "verify" {
				data, err := os.ReadFile(filepath.Join(root, "power-proof.json"))
				require.NoError(t, err)
				require.NoError(t, json.Unmarshal(data, &proof))
				_ = os.Remove(filepath.Join(root, ".truncate-fault-"+phase)) // disarm only the test marker after VM reboot
			} else {
				port, health := reserveBrokerTestPorts(t)
				first, output := startBrokerRestartChild(t, root, port, health)
				waitForBrokerReady(t, health, output)
				created := brokerCommand(t, port, "CREATE topic=events partitions=1 idempotent=true event_sourcing=true min_in_sync_replicas=1")
				id := regexp.MustCompile(`instance_id=([^ ]+)`).FindStringSubmatch(created)
				require.Len(t, id, 2, created)
				require.Contains(t, brokerCommand(t, port, "CREATE topic=audit partitions=1"), "OK")
				initialized := brokerCommand(t, port, "INIT_PRODUCER_ID transactional_id=terminal")
				producer := regexp.MustCompile(`producerId=([^ ]+)`).FindStringSubmatch(initialized)
				epoch := regexp.MustCompile(`epoch=([0-9]+)`).FindStringSubmatch(initialized)
				require.Len(t, producer, 2, initialized)
				require.Len(t, epoch, 2, initialized)
				txArgs := " transactional_id=terminal producerId=" + producer[1] + " epoch=" + epoch[1]
				require.Contains(t, brokerCommand(t, port, "BEGIN_TXN"+txArgs), "state=open")
				for _, name := range []string{"events", "audit"} {
					require.Contains(t, brokerCommand(t, port, "TXN_PUBLISH"+txArgs+" topic="+name+" partition=0 seqNum=1 message=old-transaction"), "appended=true")
				}
				require.Contains(t, brokerCommand(t, port, "END_TXN"+txArgs+" result=commit"), "state=committed")
				require.Contains(t, brokerCommand(t, port, "APPEND_STREAM topic=events key=aggregate version=1 message=old-event"), "OK")
				require.Contains(t, brokerCommand(t, port, `SAVE_SNAPSHOT topic=events key=aggregate version=1 message={"old":true}`), "OK")
				require.Contains(t, brokerCommand(t, port, "PUBLISH topic=events partition=0 producerId=sequence-producer epoch=1 seqNum=1 isIdempotent=true acks=all message=old-sequence"), `"status":"OK"`)
				require.Contains(t, brokerCommand(t, port, "REGISTER_GROUP topic=events group=workers"), "OK")
				joined := joinResponsePattern.FindStringSubmatch(brokerCommand(t, port, "JOIN_GROUP topic=events group=workers member=crash"))
				require.Len(t, joined, 3)
				require.Equal(t, "OK", brokerCommand(t, port, fmt.Sprintf("COMMIT_OFFSET topic=events partition=0 group=workers offset=17 generation=%s member=%s", joined[1], joined[2])))
				require.Contains(t, brokerCommand(t, port, fmt.Sprintf("LEAVE_GROUP topic=events group=workers member=%s generation=%s", joined[2], joined[1])), "OK")
				proof.Instance, proof.Epoch, proof.Producer = id[1], epoch[1], producer[1]
				if os.Getenv("CURSUS_POWER_STAGE") == "seed" {
					data, err := json.Marshal(proof)
					require.NoError(t, err)
					file, err := os.Create(filepath.Join(root, "power-proof.json"))
					require.NoError(t, err)
					_, err = file.Write(data)
					require.NoError(t, err)
					require.NoError(t, file.Sync())
					require.NoError(t, file.Close())
					dir, err := os.Open(root)
					require.NoError(t, err)
					require.NoError(t, dir.Sync())
					require.NoError(t, dir.Close())
				}
				require.NoError(t, os.WriteFile(filepath.Join(root, ".truncate-fault-"+phase), nil, 0o600))
				command := "TRUNCATE_INSTANCE topic=events expected_revision=1 expected_instance=" + id[1]
				require.Error(t, sendTruncateCrashCommand(port, command), "the broker must die before replying")
				require.Error(t, first.Wait())
				require.Equal(t, 86, first.ProcessState.ExitCode(), output.String())
				if os.Getenv("CURSUS_POWER_STAGE") == "seed" {
					fmt.Println("POWER_CUT_READY " + phase)
					// The external VM harness kills QEMU here without guest shutdown,
					// fsync, unmount or broker recovery. Do not clean up this disk.
					time.Sleep(time.Hour)
					t.Fatal("VM power-cut harness did not stop the guest")
				}
			}

			command := "TRUNCATE_INSTANCE topic=events expected_revision=1 expected_instance=" + proof.Instance
			port, health := reserveBrokerTestPorts(t)
			second, output := startBrokerRestartChild(t, root, port, health)
			waitForBrokerReady(t, health, output)
			require.Contains(t, brokerCommand(t, port, command), "replayed=true")
			require.Contains(t, brokerCommand(t, port, "END_TXN transactional_id=terminal producerId="+proof.Producer+" epoch="+proof.Epoch+" result=commit"), "state=committed")
			require.Contains(t, brokerCommand(t, port, "LIST_OFFSETS topic=events partition=0"), "earliest=0:latest=0:leo=0:hwm=0")
			listed := brokerCommand(t, port, "LIST_GROUPS")
			require.Contains(t, listed, "OK")
			require.NotContains(t, listed, "workers")
			require.Contains(t, brokerCommand(t, port, "STREAM_VERSION topic=events key=aggregate"), "version=0")
			require.Equal(t, "OK snapshot=null", brokerCommand(t, port, "READ_SNAPSHOT topic=events key=aggregate"))
			require.Contains(t, brokerCommand(t, port, "APPEND_STREAM topic=events key=aggregate version=1 message=fresh-event"), "OK")
			require.Contains(t, brokerCommand(t, port, "PUBLISH topic=events partition=0 producerId=sequence-producer epoch=1 seqNum=1 isIdempotent=true acks=all message=fresh-sequence"), `"status":"OK"`)
			require.Contains(t, brokerCommand(t, port, command), "replayed=true")
			require.Contains(t, brokerCommand(t, port, "LIST_OFFSETS topic=events partition=0"), "earliest=0:latest=2:leo=2:hwm=2")
			require.NoError(t, second.Process.Kill())
			require.Error(t, second.Wait())
			journal, err := transaction.OpenJournal(journalPath)
			require.NoError(t, err)
			state, err := journal.Load()
			require.NoError(t, err)
			require.NotNil(t, state["terminal"])
			encoded, err := json.Marshal(state["terminal"])
			require.NoError(t, err)
			require.NotContains(t, string(encoded), `"events"`)
			require.NotContains(t, string(encoded), "events:0")
			require.Contains(t, string(encoded), `"audit"`, "unrelated terminal references must survive pruning")
			initialEpoch, err := strconv.ParseUint(proof.Epoch, 10, 64)
			require.NoError(t, err)
			require.Greater(t, journal.NextProducerEpoch(), initialEpoch)
		})
	}
}

func sendTruncateCrashCommand(port int, command string) error {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 3*time.Second)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	id, args, err := wire.ParseCommandText(command)
	if err != nil {
		return err
	}
	payload, err := wire.EncodeCommandPayload(args)
	if err != nil {
		return err
	}
	framed, err := wire.ClientHandshake(conn, []wire.Compression{wire.CompressionNone})
	if err != nil {
		return err
	}
	if err := framed.WriteFrame(wire.Frame{Kind: wire.KindRequest, Command: id, RequestID: 1, Payload: payload}); err != nil {
		return err
	}
	_, err = framed.ReadFrame()
	return err
}
