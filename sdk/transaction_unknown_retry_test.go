package sdk

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestTransactionalProducerRetriesUnknownCompletion exercises the public wrapper
// across a lost response and a coordinator redirect, preserving one decision.
func TestTransactionalProducerRetriesUnknownCompletion(t *testing.T) {
	for _, decision := range []string{"commit", "abort"} {
		t.Run(decision, func(t *testing.T) {
			original := transactionRetryListener(t)
			replacement := transactionRetryListener(t)
			_, port, err := net.SplitHostPort(replacement.Addr().String())
			require.NoError(t, err)
			state := "committed"
			if decision == "abort" {
				state = "aborted"
			}
			commands := make(chan string, 7)
			results := make(chan error, 2)
			go func() {
				results <- serveTransactionRetryScript(original, []string{
					"OK transactional_id=tx-unknown producerId=p-session epoch=7",
					"OK transactional_id=tx-unknown state=open",
					"", // Request received; close without revealing its outcome.
					"ERROR: NOT_COORDINATOR host=127.0.0.1 port=" + port,
				}, commands)
			}()
			go func() {
				results <- serveTransactionRetryScript(replacement, []string{
					"OK transactional_id=tx-unknown state=" + state + " messages=0 offsets=0",
					"OK transactional_id=tx-unknown producerId=p-session epoch=8",
					"OK transactional_id=tx-unknown state=open",
				}, commands)
			}()
			cfg := NewDefaultConsumerConfig()
			cfg.BrokerAddrs = []string{original.Addr().String()}
			client, err := NewConsumerClient(cfg)
			require.NoError(t, err)
			producer, err := client.NewTransactionalProducer("tx-unknown")
			require.NoError(t, err)
			require.NoError(t, producer.Begin())
			session := producer.Session()
			complete := producer.Commit
			if decision == "abort" {
				complete = producer.Abort
			}
			require.ErrorContains(t, complete(), "read transaction response")
			require.Equal(t, session, producer.Session(), "unknown completion must not replace producer authority")
			observed := readTransactionRetryCommands(t, commands, 3)
			require.False(t, producer.needsReinitialization)
			require.NoError(t, complete(), "explicit retry must follow the coordinator move")
			require.Equal(t, session, producer.Session())
			require.NoError(t, producer.Begin())
			require.Equal(t, int64(8), producer.Session().Epoch)
			observed = append(observed, readTransactionRetryCommands(t, commands, 4)...)
			want := fmt.Sprintf("END_TXN transactional_id=tx-unknown producerId=p-session epoch=7 result=%s", decision)
			require.Equal(t, []string{want, want, want}, observed[2:5], "response loss and redirect must preserve the exact decision and identity")
			require.Equal(t, "INIT_PRODUCER_ID transactional_id=tx-unknown", observed[5])
			require.Contains(t, observed[6], "epoch=8")
			for i := 0; i < 2; i++ {
				select {
				case err := <-results:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("transaction script did not finish")
				}
			}
		})
	}
}

// TestTransactionalProducerDoesNotRenewFencedSession requires caller-visible
// fencing; the wrapper must not silently acquire a new epoch after END_TXN.
func TestTransactionalProducerDoesNotRenewFencedSession(t *testing.T) {
	listener := transactionRetryListener(t)
	commands := make(chan string, 3)
	result := make(chan error, 1)
	go func() {
		result <- serveTransactionRetryScript(listener, []string{
			"OK transactional_id=tx-fenced producerId=p-session epoch=7",
			"OK transactional_id=tx-fenced state=open",
			"ERROR: producer_fenced class=fencing retryable=false current_epoch=8 requested_epoch=7",
		}, commands)
	}()
	cfg := NewDefaultConsumerConfig()
	cfg.BrokerAddrs = []string{listener.Addr().String()}
	client, err := NewConsumerClient(cfg)
	require.NoError(t, err)
	producer, err := client.NewTransactionalProducer("tx-fenced")
	require.NoError(t, err)
	require.NoError(t, producer.Begin())
	err = producer.Commit()
	var brokerErr *BrokerError
	require.ErrorAs(t, err, &brokerErr)
	require.Equal(t, "producer_fenced", brokerErr.Code)
	require.False(t, brokerErr.Retryable)
	require.Equal(t, int64(7), producer.Session().Epoch)
	require.False(t, producer.needsReinitialization)
	readTransactionRetryCommands(t, commands, 3)
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("fencing script did not finish")
	}
}

func transactionRetryListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	require.NoError(t, listener.(*net.TCPListener).SetDeadline(time.Now().Add(10*time.Second)))
	return listener
}

// serveTransactionRetryScript reports errors to the test goroutine. An empty
// response consumes a real Wire request and closes its transport without reply.
func serveTransactionRetryScript(listener net.Listener, responses []string, commands chan<- string) error {
	for _, response := range responses {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			_ = conn.Close()
			return err
		}
		connection, request, command, err := acceptWireTestRequest(conn)
		if err == nil {
			commands <- command
			if response != "" {
				err = writeWireTestResponse(connection, request, response)
			}
		}
		_ = conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func readTransactionRetryCommands(t *testing.T, commands <-chan string, count int) []string {
	t.Helper()
	var result []string
	for len(result) < count {
		select {
		case command := <-commands:
			result = append(result, command)
		case <-time.After(5 * time.Second):
			t.Fatalf("transaction commands incomplete: %v", result)
		}
	}
	return result
}
