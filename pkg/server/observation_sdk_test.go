package server

import (
	"context"
	"net"
	"sync"
	"testing"

	observationv1 "github.com/cursus-io/cursus/api/gen/go/cursus/observation/v1"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/observationgrpc"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/sdk"
	"github.com/stretchr/testify/require"
)

func TestObservationSDKUsesRealWireV2Server(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = false
	cfg.MinInSyncReplicas = 1
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	ch := controller.NewCommandHandler(tm, cfg, nil, nil, nil)
	require.NoError(t, tm.CreateTopic("observation", 1, false, true))
	require.Contains(t, ch.HandleCommand("APPEND_STREAM topic=observation key=order version=1 message=committed-event", controller.NewClientContext("", 0)), "OK ")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	acceptDone := make(chan struct{})
	var workers sync.WaitGroup
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				handleConn(ctx, conn, ch)
			}()
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		<-acceptDone
		workers.Wait()
		_ = ch.Close()
		tm.Stop()
		dm.CloseAllHandlers()
	})
	client, err := sdk.NewAdminClient(&sdk.AdminConfig{BrokerAddrs: []string{listener.Addr().String()}, RequestTimeoutMS: 1000})
	require.NoError(t, err)
	capabilities, err := client.Capabilities(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, capabilities.Version)
	require.Contains(t, capabilities.Enabled, "browse_messages_v1")
	require.Contains(t, capabilities.Enabled, "stream_history_v1")
	browse, err := client.BrowseMessages(ctx, sdk.BrowseRequest{Topic: "observation", MaxRecords: 10, MaxBytes: 1024})
	require.NoError(t, err)
	require.Len(t, browse.Messages, 1)
	require.Equal(t, "committed-event", browse.Messages[0].Payload)
	history, err := client.ReadStreamHistory(ctx, sdk.HistoryRequest{Topic: "observation", Key: "order", FromVersion: 1, MaxRecords: 10, MaxBytes: 1024})
	require.NoError(t, err)
	require.Equal(t, sdk.HistoryComplete, history.Completeness)
	require.Len(t, history.Events, 1)
	require.Equal(t, "committed-event", history.Events[0].Payload)
	service, err := observationgrpc.NewService(client)
	require.NoError(t, err)
	response, err := service.GetCapabilities(ctx, &observationv1.GetCapabilitiesRequest{})
	require.NoError(t, err)
	require.Equal(t, int32(2), response.ProtocolVersion)
	require.Equal(t, capabilities.Enabled, response.Features)
}
