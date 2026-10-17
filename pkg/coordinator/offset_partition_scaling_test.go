package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

type scalingTopicHandler struct {
	DummyPublisher
	created chan int
}

func (h *scalingTopicHandler) CreateTopic(_ string, count int, _, _ bool) error {
	h.created <- count
	return nil
}

func TestOffsetPartitionScalingUsesDurableTopologyInDistributedMode(t *testing.T) {
	for _, distributed := range []bool{true, false} {
		t.Run(fmt.Sprint(distributed), func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution = distributed
			handler := &scalingTopicHandler{created: make(chan int, 8)}
			c := NewCoordinator(context.Background(), cfg, handler)
			t.Cleanup(c.Stop)
			for len(handler.created) > 0 {
				<-handler.created
			}
			for i := 0; i < 50; i++ {
				c.groups[fmt.Sprint(i)] = &GroupMetadata{}
			}
			c.updateOffsetPartitionCount()
			if distributed {
				require.Equal(t, 4, c.offsetTopicPartitionCount)
				require.Empty(t, handler.created)
			} else {
				require.Equal(t, 5, c.offsetTopicPartitionCount)
				select {
				case count := <-handler.created:
					require.Equal(t, 5, count)
				case <-time.After(time.Second):
					t.Fatal("standalone scaling did not create topic")
				}
			}
		})
	}
}
