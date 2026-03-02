package controller

import (
	"context"

	"github.com/cursus-io/cursus/util"
)

type ControllerElection struct {
	rm     RaftManager
	ctx    context.Context
	cancel context.CancelFunc
}

func NewControllerElection(rm RaftManager) *ControllerElection {
	ctx, cancel := context.WithCancel(context.Background())
	return &ControllerElection{
		rm:     rm,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (ce *ControllerElection) Start() {
	go ce.monitorLeadership()
}

func (ce *ControllerElection) Stop() {
	ce.cancel()
}

func (ce *ControllerElection) monitorLeadership() {
	notifyCh := ce.rm.LeaderCh()

	for {
		select {
		case <-ce.ctx.Done():
			util.Info("Leadership monitor stopping")
			return
		case isLeader := <-notifyCh:
			if isLeader {
				util.Info("This node is now the Cluster Leader")
			} else {
				util.Info("This node stepped down from Leadership")
			}
		}
	}
}
