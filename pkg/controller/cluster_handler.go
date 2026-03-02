package controller

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/cursus-io/cursus/util"
	"github.com/google/uuid"
)

const DefaultFSMApplyTimeout = 5 * time.Second

func (ch *CommandHandler) ProcessCommand(cmd string) string {
	ctx := NewClientContext("default-group", 0)
	return ch.HandleCommand(cmd, ctx)
}

func (ch *CommandHandler) isAuthorizedForPartition(topic string, partition int) bool {
	if ch.Cluster == nil {
		return true
	}
	return ch.Cluster.IsAuthorized(topic, partition)
}

// todo. (issues #27) isLeaderAndForward checks if the current node is the cluster leader
func (ch *CommandHandler) isLeaderAndForward(cmd string) (string, bool, error) {
	if !ch.Config.EnabledDistribution || ch.Cluster == nil || ch.Cluster.RaftManager == nil {
		return "", false, nil
	}

	const (
		maxRetries = 5
		retryDelay = 500 * time.Millisecond
	)

	for i := 0; i < maxRetries; i++ {
		if ch.Cluster.RaftManager.GetLeaderAddress() != "" {
			break
		}
		if i == maxRetries-1 {
			return "ERROR: no Raft leader elected after retries", true, fmt.Errorf("no leader elected")
		}
		util.Debug("Waiting for Raft leader to be elected... (attempt %d/%d)", i+1, maxRetries)
		time.Sleep(retryDelay)
	}

	if !ch.Cluster.RaftManager.IsLeader() {
		if ch.Cluster.Router == nil {
			return "ERROR: not the leader, and router is nil", true, nil
		}

		encodedCmd := util.EncodeMessage("", cmd)
		var lastErr error
		for i := 0; i < maxRetries; i++ {
			resp, err := ch.Cluster.Router.ForwardToLeader(string(encodedCmd))
			if err == nil {
				return resp, true, nil
			}
			lastErr = err
			util.Debug("Retrying forward to leader (Target Leader %s)... attempt %d: %v", ch.Cluster.RaftManager.GetLeaderAddress(), i+1, err)
			time.Sleep(retryDelay)
		}
		leaderAddr := ch.Cluster.RaftManager.GetLeaderAddress()
		return fmt.Sprintf("ERROR: failed to forward command to leader (Leader: %s, Error: %v)", leaderAddr, lastErr), true, nil
	}
	return "", false, nil
}

func (ch *CommandHandler) applyAndWait(cmdType string, payload map[string]interface{}) (interface{}, error) {
	if ch.Cluster == nil {
		return nil, fmt.Errorf("cluster controller is not initialized")
	}
	if ch.Cluster.RaftManager == nil {
		return nil, fmt.Errorf("raft manager not available")
	}
	fsm := ch.Cluster.RaftManager.GetFSM()
	if fsm == nil {
		return nil, fmt.Errorf("fsm not available")
	}

	reqID := uuid.New().String()
	payload["req_id"] = reqID

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	respChan := fsm.RegisterNotifier(reqID)
	defer fsm.UnregisterNotifier(reqID)

	err = ch.Cluster.RaftManager.ApplyCommand(cmdType, data)
	if err != nil {
		return nil, fmt.Errorf("raft apply failed: %w", err)
	}

	select {
	case res := <-respChan:
		if err, ok := res.(error); ok && err != nil {
			return nil, err
		}
		return res, nil
	case <-time.After(DefaultFSMApplyTimeout):
		return nil, fmt.Errorf("timeout waiting for FSM")
	}
}
