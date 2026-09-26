package sdk

import (
	"errors"
	"strings"
	"testing"
)

func TestErrStandaloneBroker(t *testing.T) {
	requireError := ErrStandaloneBroker{}
	if !errors.Is(requireError, ErrStandaloneBroker{}) || requireError.Error() != "broker distribution is not enabled" {
		t.Fatalf("unexpected standalone broker error: %v", requireError)
	}
}

func TestParseClusterStatus(t *testing.T) {
	value, err := parseClusterStatus(`OK cluster={"raft_leader":"broker-1","raft_state":"leader","broker_count":2,"active_brokers":2,"inactive_brokers":0,"partition_count":1,"leaderless_partitions":0,"under_replicated_partitions":0,"brokers":[{"id":"broker-1","status":"active","addr":"10.0.0.1:7000","client_addr":"broker-1:9000"},{"id":"broker-2","status":"active","addr":"10.0.0.2:7000"}],"partitions":[{"key":"orders-0","topic":"orders","partition":0,"leader":"broker-1","replicas":["broker-1","broker-2"],"isr":["broker-1","broker-2"],"leader_available":true,"under_replicated":false}]}`)
	if err != nil {
		t.Fatalf("parse cluster status: %v", err)
	}
	if value.RaftLeader != "broker-1" || len(value.Brokers) != 2 || value.Partitions[0].Leader != "broker-1" {
		t.Fatalf("unexpected cluster status: %+v", value)
	}
	if value.Brokers[0].ClientAddr != "broker-1:9000" {
		t.Fatalf("client address was not decoded: %+v", value.Brokers[0])
	}
}

func TestParseClusterStatusRejectsInvalidResponses(t *testing.T) {
	tests := []struct {
		name     string
		response string
		wantErr  string
	}{
		{name: "missing prefix", response: `{"broker_count":0}`, wantErr: "invalid cluster status response"},
		{name: "empty payload", response: "OK cluster=", wantErr: "invalid cluster status response"},
		{name: "malformed JSON", response: `OK cluster={`, wantErr: "decode cluster status"},
		{name: "negative count", response: `OK cluster={"broker_count":-1}`, wantErr: "invalid negative cluster status count"},
		{name: "inconsistent counts", response: `OK cluster={"broker_count":1,"active_brokers":1,"inactive_brokers":0,"partition_count":0,"leaderless_partitions":0,"under_replicated_partitions":0,"brokers":[],"partitions":[]}`, wantErr: "inconsistent cluster status counts"},
		{name: "empty broker ID", response: `OK cluster={"broker_count":1,"active_brokers":1,"inactive_brokers":0,"partition_count":0,"leaderless_partitions":0,"under_replicated_partitions":0,"brokers":[{"status":"active"}],"partitions":[]}`, wantErr: "invalid cluster broker status"},
		{name: "unknown broker status", response: `OK cluster={"broker_count":1,"active_brokers":1,"inactive_brokers":0,"partition_count":0,"leaderless_partitions":0,"under_replicated_partitions":0,"brokers":[{"id":"broker-1","status":"joining"}],"partitions":[]}`, wantErr: "invalid cluster broker status"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseClusterStatus(test.response)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("parseClusterStatus() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}
