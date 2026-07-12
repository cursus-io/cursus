package controller

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestTextCommandResponseContract(t *testing.T) {
	ch, _, _ := newTestHandlerWithCoordinator(t)
	ctx := NewClientContext("", 0)

	assertContractSuccess(t, ch.HandleCommand("CREATE topic=contract-topic partitions=2", ctx), "CREATE")
	registerResp := ch.HandleCommand("REGISTER_GROUP topic=contract-topic group=contract-group", ctx)
	assertContractSuccess(t, registerResp, "REGISTER_GROUP")
	if !strings.Contains(registerResp, "registered=true") {
		t.Fatalf("REGISTER_GROUP response missing registered=true: %s", registerResp)
	}
	assertContractSuccess(t, ch.HandleCommand("JOIN_GROUP topic=contract-topic group=contract-group member=contract-member", ctx), "JOIN_GROUP")
	assertContractSuccess(t, ch.HandleCommand("SYNC_GROUP topic=contract-topic group=contract-group member="+ctx.MemberID, ctx), "SYNC_GROUP")
	assertContractSuccess(t, ch.HandleCommand("HEARTBEAT topic=contract-topic group=contract-group member="+ctx.MemberID, ctx), "HEARTBEAT")
	assertContractSuccess(t, ch.HandleCommand("COMMIT_OFFSET topic=contract-topic partition=0 group=contract-group offset=2", ctx), "COMMIT_OFFSET")
	assertContractSuccess(t, ch.HandleCommand("FETCH_OFFSET topic=contract-topic partition=0 group=contract-group", ctx), "FETCH_OFFSET")
	assertContractSuccess(t, ch.HandleCommand("BEGIN_TXN transactional_id=contract-tx producerId=contract-producer", ctx), "BEGIN_TXN")
	assertContractSuccess(t, ch.HandleCommand("TXN_PUBLISH transactional_id=contract-tx topic=contract-topic partition=0 producerId=contract-producer seqNum=1 message=tx", ctx), "TXN_PUBLISH")
	assertContractSuccess(t, ch.HandleCommand("SEND_OFFSETS_TO_TXN transactional_id=contract-tx producerId=contract-producer epoch=0 topic=contract-topic group=contract-group member="+ctx.MemberID+" generation="+strconv.Itoa(ctx.Generation)+" P0:2", ctx), "SEND_OFFSETS_TO_TXN")
	assertContractSuccess(t, ch.HandleCommand("TXN_STATUS transactional_id=contract-tx", ctx), "TXN_STATUS")
	assertContractSuccess(t, ch.HandleCommand("END_TXN transactional_id=contract-tx producerId=contract-producer epoch=0 result=abort", ctx), "END_TXN")
	assertContractSuccess(t, ch.HandleCommand("LIST_OFFSETS topic=contract-topic", ctx), "LIST_OFFSETS")

	batchCmd := fmt.Sprintf("BATCH_COMMIT topic=contract-topic group=contract-group generation=%d member=%s P0:3", ctx.Generation, ctx.MemberID)
	assertContractSuccess(t, ch.HandleCommand(batchCmd, ctx), "BATCH_COMMIT")
	assertContractSuccess(t, ch.HandleCommand("METADATA topic=contract-topic", ctx), "METADATA")
	assertContractSuccess(t, ch.HandleCommand("FIND_COORDINATOR group=contract-group", ctx), "FIND_COORDINATOR")
}

func TestTextCommandErrorContract(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	commands := []string{
		"CREATE partitions=2",
		"DELETE topic=missing-topic",
		"REGISTER_GROUP topic=missing-topic group=g1",
		"JOIN_GROUP topic=missing-topic group=g1 member=m1",
		"FETCH_OFFSET topic=t1 partition=0 group=g1",
		"LIST_OFFSETS topic=missing-topic",
		"COMMIT_OFFSET topic=t1 partition=0 group=g1 offset=1",
		"BEGIN_TXN producerId=p1",
		"TXN_PUBLISH transactional_id=missing topic=t1 producerId=p1 message=x",
		"SEND_OFFSETS_TO_TXN transactional_id=missing topic=t1 group=g1 P0:1",
		"END_TXN transactional_id=missing result=commit",
		"DESCRIBE topic=missing-topic",
	}

	for _, cmd := range commands {
		resp := ch.HandleCommand(cmd, ctx)
		if !strings.HasPrefix(resp, "ERROR:") {
			t.Fatalf("%s returned non-contract error response: %s", cmd, resp)
		}
	}
}

func assertContractSuccess(t *testing.T, resp, command string) {
	t.Helper()
	if !isContractSuccess(resp) {
		t.Fatalf("%s returned non-contract success response: %s", command, resp)
	}
}

func isContractSuccess(resp string) bool {
	if strings.HasPrefix(resp, "OK") {
		return true
	}
	var envelope struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal([]byte(resp), &envelope); err != nil {
		return false
	}
	return envelope.Status == "OK"
}
