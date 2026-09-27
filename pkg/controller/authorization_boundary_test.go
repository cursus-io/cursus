package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func authorizationHandler(t *testing.T, users []config.SASLUser) *CommandHandler {
	t.Helper()
	ch, _ := newTestHandler(t)
	ch.Config.EnableSASL = true
	ch.Config.SASLUsers = users
	return ch
}

func authenticateTestUser(t *testing.T, ch *CommandHandler, principal, token string) *ClientContext {
	t.Helper()
	ctx := NewClientContext("", 0)
	resp := ch.HandleCommand("AUTH principal="+principal+" token="+token, ctx)
	if !strings.HasPrefix(resp, "OK ") {
		t.Fatalf("authenticate %s: %s", principal, resp)
	}
	return ctx
}

func TestProtectedCommandsRequireAuthenticationWhenSASLIsEnabled(t *testing.T) {
	ch := authorizationHandler(t, []config.SASLUser{{Principal: "reader", Token: "secret", Permissions: []string{PermissionTopicRead}}})

	for _, command := range []string{
		"LIST",
		"CREATE topic=blocked partitions=1",
		"TRUNCATE topic=blocked expected_revision=1",
		"CLUSTER_STATUS",
		"ELECT_LEADER topic=orders partition=0 broker=broker-2",
		"JOIN_GROUP topic=missing group=workers member=m1",
		"LIST_GROUPS",
		"CONSUME topic=orders partition=0 offset=0 member=m1",
		"STREAM topic=orders partition=0 group=workers",
		"READ_STREAM topic=orders stream=aggregate key=id",
		"INIT_PRODUCER_ID transactional_id=tx-1",
	} {
		resp := ch.HandleCommand(command, NewClientContext("", 0))
		if !strings.Contains(resp, "authentication_required") {
			t.Fatalf("%s did not require authentication: %s", command, resp)
		}
	}
}

func TestCommandPermissionsAreEnforcedByCategory(t *testing.T) {
	ch := authorizationHandler(t, []config.SASLUser{
		{Principal: "reader", Token: "read-secret", Permissions: []string{PermissionTopicRead}},
		{Principal: "operator", Token: "ops-secret", Permissions: []string{PermissionAdmin, PermissionGroup}},
		{Principal: "processor", Token: "txn-secret", Permissions: []string{PermissionTransaction, PermissionTopicWrite, PermissionGroup}},
	})

	reader := authenticateTestUser(t, ch, "reader", "read-secret")
	if resp := ch.HandleCommand("LIST", reader); !strings.HasPrefix(resp, "OK ") {
		t.Fatalf("reader could not list topics: %s", resp)
	}
	if resp := ch.HandleCommand("CREATE topic=reader-blocked partitions=1", reader); !strings.Contains(resp, "permission=admin") {
		t.Fatalf("reader admin command was not denied: %s", resp)
	}
	if resp := ch.HandleCommand("TRUNCATE topic=reader-blocked expected_revision=1", reader); !strings.Contains(resp, "permission=admin") {
		t.Fatalf("reader truncate command was not denied: %s", resp)
	}
	if resp := ch.HandleCommand("INIT_PRODUCER_ID transactional_id=reader-tx", reader); !strings.Contains(resp, "permission=transaction") {
		t.Fatalf("reader transaction command was not denied: %s", resp)
	}
	if resp := ch.HandleCommand("LIST_GROUPS", reader); !strings.Contains(resp, "permission=group") {
		t.Fatalf("reader group query was not denied: %s", resp)
	}

	operator := authenticateTestUser(t, ch, "operator", "ops-secret")
	if resp := ch.HandleCommand("CREATE topic=operator-topic partitions=1", operator); !strings.HasPrefix(resp, "OK ") {
		t.Fatalf("operator could not create topic: %s", resp)
	}
	if resp := ch.HandleCommand("TRUNCATE topic=operator-topic expected_revision=1", operator); !strings.HasPrefix(resp, "OK ") {
		t.Fatalf("operator could not truncate topic: %s", resp)
	}
	if resp := ch.HandleCommand("FIND_COORDINATOR group=workers", operator); !strings.HasPrefix(resp, "OK ") {
		t.Fatalf("operator could not discover group coordinator: %s", resp)
	}
	if resp := ch.HandleCommand("LIST_GROUPS", operator); resp != "ERROR: coordinator_not_available" {
		t.Fatalf("operator did not pass LIST_GROUPS authorization: %s", resp)
	}
	if resp := ch.HandleCommand("INIT_PRODUCER_ID transactional_id=operator-tx", operator); !strings.Contains(resp, "permission=transaction") {
		t.Fatalf("operator transaction command was not denied: %s", resp)
	}

	processor := authenticateTestUser(t, ch, "processor", "txn-secret")
	initResp := ch.HandleCommand("INIT_PRODUCER_ID transactional_id=processor-tx", processor)
	if !strings.HasPrefix(initResp, "OK ") {
		t.Fatalf("processor could not initialize transaction: %s", initResp)
	}
}

func TestCompositeTransactionPermissionsAreRequired(t *testing.T) {
	ch := authorizationHandler(t, []config.SASLUser{
		{Principal: "txn-only", Token: "secret", Permissions: []string{PermissionTransaction}},
	})
	ctx := authenticateTestUser(t, ch, "txn-only", "secret")

	resp := ch.HandleCommand("TXN_PUBLISH transactional_id=tx-1 topic=missing partition=0 producerId=p1 seqNum=1 epoch=0 message=value", ctx)
	if !strings.Contains(resp, "permission=topic.write") {
		t.Fatalf("TXN_PUBLISH did not require topic.write: %s", resp)
	}
	resp = ch.HandleCommand("SEND_OFFSETS_TO_TXN transactional_id=tx-1 producerId=p1 epoch=0 topic=missing group=g1 member=m1 generation=1 offsets=P0:1", ctx)
	if !strings.Contains(resp, "permission=group") {
		t.Fatalf("SEND_OFFSETS_TO_TXN did not require group: %s", resp)
	}
}

func TestInlineAuthenticationAndInternalContextRespectBoundaries(t *testing.T) {
	ch := authorizationHandler(t, []config.SASLUser{
		{Principal: "admin", Token: "secret", Permissions: []string{PermissionAdmin}},
	})

	inlineCtx := NewClientContext("", 0)
	resp := ch.HandleCommand("CREATE topic=inline-admin partitions=1 principal=admin auth_token=secret", inlineCtx)
	if !strings.HasPrefix(resp, "OK ") || !inlineCtx.Authenticated {
		t.Fatalf("inline admin authentication failed: %s", resp)
	}

	internalCtx := NewInternalClientContext("", 0)
	resp = ch.HandleCommand("CREATE topic=internal-admin partitions=1", internalCtx)
	if !strings.HasPrefix(resp, "OK ") {
		t.Fatalf("internal context did not bypass client authorization: %s", resp)
	}
}

func TestEmptyPermissionListDeniesProtectedCommands(t *testing.T) {
	ch := authorizationHandler(t, []config.SASLUser{{Principal: "restricted", Token: "secret"}})
	ctx := authenticateTestUser(t, ch, "restricted", "secret")

	resp := ch.HandleCommand("CREATE topic=restricted-topic partitions=1", ctx)
	if !strings.Contains(resp, "permission=admin") {
		t.Fatalf("user with omitted permissions accessed a protected command: %s", resp)
	}
}

func TestBatchPublishUsesPublishAuthenticationAndPermission(t *testing.T) {
	ch, topics := newTestHandler(t)
	require.NoError(t, topics.CreateTopic("orders", 1, false, false))
	ch.Config.EnableSASL = true
	ch.Config.SASLUsers = []config.SASLUser{
		{Principal: "reader", Token: "read-secret", Permissions: []string{PermissionTopicRead}},
		{Principal: "writer", Token: "write-secret", Permissions: []string{PermissionTopicWrite}},
	}
	batch, err := util.EncodeBatchMessages("orders", 0, "1", false, []types.Message{{ProducerID: "p1", SeqNum: 1, Payload: "body"}})
	require.NoError(t, err)

	response, err := ch.HandleBatchMessage(batch, nil, NewClientContext("", 0))
	require.NoError(t, err)
	require.Contains(t, response, "authentication_required")

	reader := authenticateTestUser(t, ch, "reader", "read-secret")
	response, err = ch.HandleBatchMessage(batch, nil, reader)
	require.NoError(t, err)
	require.Contains(t, response, "permission=topic.write")

	writer := authenticateTestUser(t, ch, "writer", "write-secret")
	response, err = ch.HandleBatchMessage(batch, nil, writer)
	require.NoError(t, err)
	require.Contains(t, response, `"status":"OK"`)
}

func TestUnauthorizedDeleteIsRejectedBeforeLifecycleWait(t *testing.T) {
	ch, topics := newTestHandler(t)
	require.NoError(t, topics.CreateTopic("orders", 1, false, false))
	ch.Config.EnableSASL = true
	ch.Config.SASLUsers = []config.SASLUser{{Principal: "reader", Token: "read-secret", Permissions: []string{PermissionTopicRead}}}
	reader := authenticateTestUser(t, ch, "reader", "read-secret")
	release, err := ch.topicLifecycleGates.acquire(context.Background(), "orders", false)
	require.NoError(t, err)
	defer release()
	requestCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	responseCh := make(chan string, 1)
	go func() { responseCh <- ch.HandleCommandContext(requestCtx, "DELETE topic=orders", reader) }()
	select {
	case response := <-responseCh:
		require.Contains(t, response, "permission=admin")
	case <-time.After(time.Second):
		t.Fatal("unauthorized delete waited on the lifecycle gate")
	}
}

func TestPublicAndStandaloneSessionsCannotReplicateMessages(t *testing.T) {
	ch, _ := newTestHandler(t)
	response := ch.HandleCommand("REPLICATE_MESSAGE payload={}", NewClientContext("", 0))
	require.Contains(t, response, "internal_command_unauthorized")
	response = ch.HandleCommand("REPLICATE_MESSAGE payload={}", NewInternalClientContext("", 0))
	require.Contains(t, response, "distribution_required")
}
