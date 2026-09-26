package observationgrpc

import (
	"context"
	"fmt"
	"net"
	"sync"

	observationv1 "github.com/cursus-io/cursus/api/gen/go/cursus/observation/v1"
	"google.golang.org/grpc"
)

// Start serves ObservationService until ctx is cancelled. The caller owns
// transport authentication; this helper only owns the listener lifecycle and
// conservative protobuf message limits.
func Start(ctx context.Context, address string, backend Backend) (func(), error) {
	if ctx == nil {
		return nil, fmt.Errorf("observation server context is nil")
	}
	service, err := NewService(backend)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen for observation gRPC: %w", err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(maxBytes), grpc.MaxSendMsgSize(maxBytes))
	observationv1.RegisterObservationServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	var once sync.Once
	shutdown := func() {
		once.Do(func() {
			server.Stop()
			_ = listener.Close()
		})
	}
	go func() {
		<-ctx.Done()
		shutdown()
	}()
	return shutdown, nil
}
