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
	now := time.Now()
	if !c.ownsGroupSession(groupName, now, timeout) {
		return
	}

	c.mu.Lock()
	group, exists := c.groups[groupName]
	if !exists || len(group.Members) == 0 {
		c.mu.Unlock()
		return
	}

	var timedOutMembers []string
	for id, member := range group.Members {
		if now.Sub(member.LastHeartbeat) > timeout {
			timedOutMembers = append(timedOutMembers, id)
		}
	}
	generation := group.Generation
	handler := c.expirationHandler
	distributed := c.cfg.EnabledDistribution
	c.mu.Unlock()

	if len(timedOutMembers) == 0 {
		return
	}

	if distributed {
		if handler == nil {
			util.Warn("Group '%s': session expiration deferred because no cluster handler is configured", groupName)
			return
		}
		if err := handler(groupName, generation, timedOutMembers); err != nil {
			util.Warn("Group '%s': failed to replicate session expiration: %v", groupName, err)
			return
		}
	} else if err := c.ExpireConsumers(groupName, generation, timedOutMembers); err != nil {
		util.Warn("Group '%s': failed to expire members: %v", groupName, err)
		return
	}

	util.Warn("Group '%s': %d members timed out; generation advanced", groupName, len(timedOutMembers))
}

func (c *Coordinator) ownsGroupSession(groupName string, now time.Time, timeout time.Duration) bool {
	if !c.cfg.EnabledDistribution {
		return true
	}

	c.mu.RLock()
	checker := c.groupOwnerChecker
	c.mu.RUnlock()
	if checker == nil || !checker(groupName) {
		c.mu.Lock()
		delete(c.ownershipSince, groupName)
		c.mu.Unlock()
		return false
	}

	c.mu.Lock()
	since, observed := c.ownershipSince[groupName]
	if !observed {
		c.ownershipSince[groupName] = now
		c.mu.Unlock()
		return false
	}
	c.mu.Unlock()

	// A new group coordinator waits one full session timeout before expiring
	// members, giving healthy clients time to redirect and heartbeat.
	return now.Sub(since) >= timeout
}

// RecordHeartbeat updates the consumer's last heartbeat timestamp.
func (c *Coordinator) RecordHeartbeat(groupName, consumerID string) error {
	return c.RecordHeartbeatForGeneration(groupName, consumerID, -1)
}

// RecordHeartbeatForGeneration atomically fences and refreshes a member session.
func (c *Coordinator) RecordHeartbeatForGeneration(groupName, consumerID string, generation int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if errResp := c.validateMemberGenerationLocked(groupName, consumerID, generation); errResp != "" {
		return fmt.Errorf("%s", errResp)
	}

	member := c.groups[groupName].Members[consumerID]
	member.LastHeartbeat = time.Now()
	util.Debug("Heartbeat from %s/%s generation=%d", groupName, consumerID, generation)
	return nil
}
