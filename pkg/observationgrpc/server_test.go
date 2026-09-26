package observationgrpc

import (
	"context"
	"net"
	"testing"
	"time"

	observationv1 "github.com/cursus-io/cursus/api/gen/go/cursus/observation/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestStartRejectsInvalidArguments(t *testing.T) {
	backend := &testBackend{}
	var nilContext context.Context
	if _, err := startForTest(nilContext, "127.0.0.1:0", backend); err == nil {
		t.Fatal("expected nil context to be rejected")
	}
	if _, err := Start(context.Background(), "127.0.0.1:0", nil); err == nil {
		t.Fatal("expected nil backend to be rejected")
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	if _, err := Start(context.Background(), listener.Addr().String(), backend); err == nil {
		t.Fatal("expected occupied address to fail")
	}
}

func startForTest(ctx context.Context, address string, backend Backend) (func(), error) {
	return Start(ctx, address, backend)
}

func TestStartServesAndShutsDownObservationService(t *testing.T) {
	address := unusedTCPAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	shutdown, err := Start(ctx, address, &testBackend{})
	require.NoError(t, err)
	defer shutdown()

	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer connection.Close()
	client := observationv1.NewObservationServiceClient(connection)

	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), time.Second)
	defer rpcCancel()
	response, err := client.ListTopics(rpcCtx, &observationv1.ListTopicsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"orders"}, response.Topics)

	// Cancellation owns the server lifecycle, and an explicit shutdown remains safe.
	cancel()
	shutdown()
	shutdown()
}

func unusedTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}
