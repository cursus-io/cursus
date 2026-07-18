package controller

import "testing"

func TestRuntimeSnapshotReportsLeadershipWithoutFSM(t *testing.T) {
	manager := &ComprehensiveMockRaftManager{isLeader: true}
	manager.On("GetLeaderAddress").Return("broker-1:9001").Once()
	controller := &ClusterController{RaftManager: manager, brokerID: "broker-1"}

	snapshot := controller.RuntimeSnapshot()
	if !snapshot.Enabled || !snapshot.HasLeader || !snapshot.IsLeader {
		t.Fatalf("unexpected cluster snapshot: %+v", snapshot)
	}
	if snapshot.BrokerID != "broker-1" || snapshot.BrokerCount != 0 {
		t.Fatalf("unexpected cluster identity snapshot: %+v", snapshot)
	}
}

func TestRuntimeSnapshotHandlesDisabledController(t *testing.T) {
	var controller *ClusterController
	if snapshot := controller.RuntimeSnapshot(); snapshot.Enabled {
		t.Fatalf("nil controller reported enabled: %+v", snapshot)
	}
}
