package coordinator

import (
	"fmt"
	"time"

	"github.com/cursus-io/cursus/util"
)

// monitorHeartbeats checks consumer heartbeat intervals and triggers rebalancing when timeouts occur.
func (c *Coordinator) monitorHeartbeats() {
	checkInterval := time.Duration(c.cfg.ConsumerHeartbeatCheckMS) * time.Millisecond
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if c.cfg.EnabledDistribution && c.leaderChecker != nil {
				if !c.leaderChecker() {
					continue
				}
			}
			c.checkAllGroupsTimeout()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Coordinator) checkAllGroupsTimeout() {
	c.mu.RLock()
	groupNames := make([]string, 0, len(c.groups))
	for name := range c.groups {
		groupNames = append(groupNames, name)
	}
	c.mu.RUnlock()

	timeout := time.Duration(c.cfg.ConsumerSessionTimeoutMS) * time.Millisecond
	for _, name := range groupNames {
		c.checkSingleGroupTimeout(name, timeout)
	}
}

func (c *Coordinator) checkSingleGroupTimeout(groupName string, timeout time.Duration) {
	c.mu.Lock()
	group, exists := c.groups[groupName]
	if !exists || len(group.Members) == 0 {
		c.mu.Unlock()
		return
	}

	var timedOutMembers []string
	now := time.Now()

	for id, member := range group.Members {
		if now.Sub(member.LastHeartbeat) > timeout {
			timedOutMembers = append(timedOutMembers, id)
			delete(group.Members, id)
		}
	}

	if len(timedOutMembers) > 0 {
		group.Generation++
		c.rebalanceRange(groupName)
	}
	c.mu.Unlock()

	if len(timedOutMembers) > 0 {
		util.Warn("⚠️ Group '%s': %d members timed out. Triggered rebalance.", groupName, len(timedOutMembers))
	}
}

// RecordHeartbeat updates the consumer's last heartbeat timestamp.
func (c *Coordinator) RecordHeartbeat(groupName, consumerID string) error {
	c.mu.Lock()
	group := c.groups[groupName]
	if group == nil {
		c.mu.Unlock()
		return fmt.Errorf("group '%s' not found", groupName)
	}

	member := group.Members[consumerID]
	if member == nil {
		c.mu.Unlock()
		return fmt.Errorf("consumer '%s' not found", consumerID)
	}

	member.LastHeartbeat = time.Now()
	c.mu.Unlock()

	util.Info("💓 Heartbeat from %s/%s", groupName, consumerID)
	return nil
}
