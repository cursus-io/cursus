package server

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/sdk"
	"github.com/stretchr/testify/require"
)

// A reconnected reader has no server-side cursor. Its original grant and
// explicit fetch position must survive an overlapping durable commit.
func TestCooperativeWireReconnectDuringCommit(t *testing.T) {
	for _, command := range []string{"CONSUME", "STREAM"} {
		t.Run(command, func(t *testing.T) {
			cfg, cd, tm := cooperativeWireFixture(t, true)
			p, err := tm.GetTopic(cfg.Topic).GetPartition(0)
			require.NoError(t, err)
			records := make([]types.Message, 24)
			for i := range records {
				records[i].Payload = "reconnect-payload"
			}
			require.NoError(t, p.EnqueueBatchSync(records))
			require.NoError(t, cd.RegisterGroup(cfg.Topic, cfg.GroupID, 6))
			parts := make([]coordinator.CooperativePartition, 6)
			for i := range parts {
				parts[i] = coordinator.CooperativePartition{Topic: cfg.Topic, Partition: i, LifecycleEpoch: tm.GetTopic(cfg.Topic).Definition().LifecycleEpoch}
			}
			membership, err := cd.ReconcileCooperative(cfg.GroupID, "reader", 0, 0, parts, nil, false)
			require.NoError(t, err)
			token := protocol.CooperativeToken{RegistrationEpoch: membership.RegistrationEpoch, Grant: membership.Grants[0]}
			require.NoError(t, cd.CommitCooperative(cfg.GroupID, token.RegistrationEpoch, token.Grant, 8))
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			cd.SetOffsetRecordWriter(func(record coordinator.ConsumerMetadataRecord) error {
				if record.Type == coordinator.ConsumerMetadataRecordOffsetSnapshot {
					close(entered)
					<-release
				}
				return nil
			})
			committed := make(chan error, 1)
			go func() { committed <- cd.CommitCooperative(cfg.GroupID, token.RegistrationEpoch, token.Grant, 16) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("commit did not reach durable writer")
			}
			raw, err := net.DialTimeout("tcp", cfg.CoordinatorAddr, time.Second)
			require.NoError(t, err)
			t.Cleanup(func() { _ = raw.Close() })
			require.NoError(t, raw.SetDeadline(time.Now().Add(5*time.Second)))
			client := newWireTestClient(t, raw)
			id := wire.CommandConsume
			if command == "STREAM" {
				id = wire.CommandStream
			}
			writeWireCommand(t, client, id, 1, fmt.Sprintf("%s topic=%s partition=0 offset=16 group=%s member=reader ownership=%s batch=8", command, cfg.Topic, cfg.GroupID, protocol.EncodeCooperative(token)))
			type response struct {
				frame wire.Frame
				err   error
			}
			responses := make(chan response, 1)
			go func() { frame, err := client.ReadFrame(); responses <- response{frame, err} }()
			var got response
			early := false
			select {
			case got = <-responses:
				early = true
			case <-time.After(250 * time.Millisecond):
			}
			select {
			case <-committed:
				t.Fatal("commit acknowledged before durable writer completed")
			default:
			}
			unblock()
			require.NoError(t, <-committed)
			if !early {
				got = <-responses
			}
			require.NoError(t, got.err)
			messages, _, _, err := sdk.DecodeBatchMessages(got.frame.Payload)
			require.NoError(t, err)
			require.Len(t, messages, 8)
			t.Logf("fetch_before_commit_ack=%t requested=16 returned=%d replay_records=%d replay_payload_bytes=%d", early, messages[0].Offset, 16-min(uint64(16), messages[0].Offset), (16-min(uint64(16), messages[0].Offset))*uint64(len(records[0].Payload)))
			require.Equal(t, uint64(16), messages[0].Offset, "same-grant reconnect must not rewind to the previous committed offset")
		})
	}
}
