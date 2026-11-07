package e2e_cluster

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

func TestTransactionDecisionsSurviveCoordinatorBoundaries(t *testing.T) {
	if os.Getenv("RUN_E2E_RECOVERY_BOUNDARIES") != "1" {
		t.Skip("set RUN_E2E_RECOVERY_BOUNDARIES=1 for deterministic recovery faults")
	}
	t.Setenv("TRANSACTION_TIMEOUT_MS", "120000")
	ctx := newAckFaultClusterMode(t, "txn-boundary-input", true)
	for _, decision := range []string{"commit", "abort"} {
		for _, boundary := range []string{"prepare", "markers", "decision"} {
			if !t.Run(decision+"-"+boundary, func(t *testing.T) {
				journal := newAckFaultJournal(t)
				id := "txn-" + decision + "-" + boundary
				group := id + "-workers"
				outputs := []string{id + "-a", id + "-b"}
				client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
				defer client.Close()
				for _, output := range outputs {
					require.NoError(t, client.CreateTopic(output, 1, false))
					waitAckFaultState(t, output, []int{1, 2, 3}, "", 3)
				}
				reader, generation, member := joinRecoveryGroup(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), group)
				defer reader.Close()
				// The same input record can be processed by independent groups.
				require.NoError(t, client.PublishIdempotent(ctx.GetTopic(), id, 1, 0, id, "all", true))
				producer, err := client.InitTransactionProducer(id)
				require.NoError(t, err)
				require.NoError(t, client.BeginTransaction(id, producer))
				for i, output := range outputs {
					require.NoError(t, client.TransactionalPublish(id, output, 0, producer, uint64(i+1), output+"-payload"))
				}
				require.NoError(t, client.SendOffsetsToTransaction(id, ctx.GetTopic(), group, member, generation, producer, map[int]uint64{0: 1}))
				owner, err := client.FindTransactionCoordinator(id)
				require.NoError(t, err)
				node := coordinatorNode(t, owner)
				path := armRecoveryBoundary(t, node, "transaction-"+boundary+"-"+decision, id)
				type reply struct {
					response []byte
					sent     bool
					err      error
				}
				done := make(chan reply, 1)
				go func() {
					data, sent, err := ackFaultRoundTrip(ctx.GetBrokerAddrs()[node-1], fmt.Sprintf("END_TXN transactional_id=%s producerId=%s epoch=%d result=%s", id, producer.ProducerID, producer.Epoch, decision))
					done <- reply{data, sent, err}
				}()
				waitRecoveryBoundary(t, node, path, journal)
				captureAckFaultMetrics(t, journal, "before-coordinator-crash")
				ackFaultDocker(t, "kill", "--signal=KILL", ackFaultContainer(node))
				select {
				case result := <-done:
					require.True(t, result.sent)
					require.Error(t, result.err, "END_TXN must not have received a successful reply")
					journal.record("end_transaction_unknown", fmt.Sprint(result.err))
				case <-time.After(20 * time.Second):
					t.Fatal("END_TXN transport did not finish")
				}
				survivors := ackFaultOtherNodes(node)
				recovered := e2e.NewBrokerClient(clusterBrokerAddrsForNodes(survivors))
				defer recovered.Close()
				want := "committed"
				if decision == "abort" {
					want = "aborted"
				}
				require.NoError(t, eventually(t, "durable transaction decision recovered", 90*time.Second, func() (bool, string, error) {
					status, err := recovered.GetTransactionStatus(id)
					return err == nil && status.State == want, fmt.Sprintf("%+v %v", status, err), nil
				}))
				require.NoError(t, eventually(t, "same decision retry completes after coordinator recovery", 60*time.Second, func() (bool, string, error) {
					err := recovered.EndTransaction(id, producer, decision)
					journal.record("end_transaction_retry", fmt.Sprint(err))
					return err == nil, fmt.Sprint(err), nil
				}))
				opposite := "abort"
				if decision == "abort" {
					opposite = "commit"
				}
				require.Error(t, recovered.EndTransaction(id, producer, opposite), "a durable decision cannot be reversed")
				verify := func(addrs []string) {
					admin, err := sdk.NewAdminClient(&sdk.AdminConfig{BrokerAddrs: addrs, RequestTimeoutMS: 3000, MaxRetries: 5, RetryBackoffMS: 100})
					require.NoError(t, err)
					for _, output := range outputs {
						requestCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
						page, err := admin.BrowseMessages(requestCtx, sdk.BrowseRequest{Topic: output, Partition: 0, MaxRecords: 32, MaxBytes: 65536})
						cancel()
						require.NoError(t, err)
						if decision == "commit" {
							require.Len(t, page.Messages, 1)
							require.Equal(t, output+"-payload", page.Messages[0].Payload)
						} else {
							require.Empty(t, page.Messages, "aborted output leaked into committed view")
						}
						journal.record("committed_view", page)
					}
					wantOffset := uint64(1)
					if decision == "abort" {
						wantOffset = 0
					}
					require.NoError(t, eventually(t, "input offset matches decision", 30*time.Second, func() (bool, string, error) {
						offset, err := recovered.FetchCommittedOffset(ctx.GetTopic(), 0, group)
						return err == nil && offset == wantOffset, fmt.Sprintf("offset=%d err=%v", offset, err), nil
					}))
					journal.record("verified_decision", map[string]any{"state": want, "input_offset": wantOffset})
				}
				verify(clusterBrokerAddrsForNodes(survivors))
				ackFaultDocker(t, "start", ackFaultContainer(node))
				for _, output := range outputs {
					waitAckFaultState(t, output, []int{1, 2, 3}, "", 3)
				}
				verify([]string{ctx.GetBrokerAddrs()[node-1]})
				captureAckFaultMetrics(t, journal, "after-coordinator-rejoin")
			}) {
				return
			}
		}
	}
}
