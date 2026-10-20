package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHandlerCloseJoinsMonitorInitialization(t *testing.T) {
	ch := &CommandHandler{}
	ctx, finish, err := ch.beginMonitor(context.Background(), "consumer-recovery")
	require.NoError(t, err)
	defer finish()
	_, _, err = ch.beginMonitor(context.Background(), "consumer-recovery")
	require.Error(t, err)
	closed := make(chan error, 1)
	go func() { closed <- ch.Close() }()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("monitor initialization was not canceled")
	}
	select {
	case <-closed:
		t.Fatal("handler closed before monitor initialization released its dependencies")
	default:
	}
	finish()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("monitor initialization was not joined")
	}
	_, _, err = ch.beginMonitor(context.Background(), "transaction-timeout")
	require.Error(t, err)
	require.NoError(t, ch.Close())
}
