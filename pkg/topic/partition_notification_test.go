package topic

import (
	"fmt"
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

func BenchmarkPartitionMessageNotificationFanout(b *testing.B) {
	for _, streams := range []int{1, 64, 1024} {
		b.Run(fmt.Sprintf("streams-%d", streams), func(b *testing.B) {
			partition := &Partition{messageNotifyCh: make(chan struct{})}
			waiters := make([]<-chan struct{}, streams)
			b.ResetTimer()
			for range b.N {
				for index := range waiters {
					_, waiters[index] = partition.MessageNotification()
				}
				partition.NotifyNewMessage()
				for _, waiter := range waiters {
					select {
					case <-waiter:
					default:
						b.Fatal("notification did not broadcast to every stream")
					}
				}
			}
		})
	}
}
