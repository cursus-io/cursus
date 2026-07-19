package controller

import (
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
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
		"CLUSTER_STATUS",
		"ELECT_LEADER topic=orders partition=0 broker=broker-2",
		"JOIN_GROUP topic=missing group=workers member=m1",
		"LIST_GROUPS",
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
	resp = ch.HandleCommand("SEND_OFFSETS_TO_TXN transactional_id=tx-1 producerId=p1 epoch=0 topic=missing group=g1 member=m1 generation=1 P0:1", ctx)
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

func TestEmptyPermissionListPreservesLegacyAuthenticatedAccess(t *testing.T) {
	ch := authorizationHandler(t, []config.SASLUser{{Principal: "legacy", Token: "secret"}})
	ctx := authenticateTestUser(t, ch, "legacy", "secret")

	resp := ch.HandleCommand("CREATE topic=legacy-topic partitions=1", ctx)
	if !strings.HasPrefix(resp, "OK ") {
		t.Fatalf("legacy user with omitted permissions lost access: %s", resp)
	}
}
