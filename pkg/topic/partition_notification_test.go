package topic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPartitionMessageNotificationBroadcastsGeneration(t *testing.T) {
	partition := &Partition{messageNotifyCh: make(chan struct{})}
	firstGeneration, first := partition.MessageNotification()
	secondGeneration, second := partition.MessageNotification()
	require.Equal(t, firstGeneration, secondGeneration)

	partition.NotifyNewMessage()
	for _, notification := range []<-chan struct{}{first, second} {
		select {
		case <-notification:
		case <-time.After(time.Second):
			t.Fatal("partition notification did not wake every waiter")
		}
	}

	nextGeneration, next := partition.MessageNotification()
	require.Equal(t, firstGeneration+1, nextGeneration)
	select {
	case <-next:
		t.Fatal("next generation notification was already closed")
	default:
	}
}
