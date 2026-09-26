package sdk

import "testing"

func TestParseClusterStatus(t *testing.T) {
	value, err := parseClusterStatus(`OK cluster={"raft_leader":"broker-1","raft_state":"leader","broker_count":2,"active_brokers":2,"inactive_brokers":0,"partition_count":1,"leaderless_partitions":0,"under_replicated_partitions":0,"brokers":[{"id":"broker-1","status":"active","addr":"10.0.0.1:7000"},{"id":"broker-2","status":"active","addr":"10.0.0.2:7000"}],"partitions":[{"key":"orders-0","topic":"orders","partition":0,"leader":"broker-1","replicas":["broker-1","broker-2"],"isr":["broker-1","broker-2"],"leader_available":true,"under_replicated":false}]}`)
	if err != nil {
		t.Fatalf("parse cluster status: %v", err)
	}
	if value.RaftLeader != "broker-1" || len(value.Brokers) != 2 || value.Partitions[0].Leader != "broker-1" {
		t.Fatalf("unexpected cluster status: %+v", value)
	}
}

func TestParseClusterStatusRejectsInconsistentCounts(t *testing.T) {
	if _, err := parseClusterStatus(`OK cluster={"broker_count":1,"active_brokers":1,"inactive_brokers":0,"partition_count":0,"leaderless_partitions":0,"under_replicated_partitions":0,"brokers":[],"partitions":[]}`); err == nil {
		t.Fatal("expected inconsistent count rejection")
	}
}
