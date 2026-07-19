package controller

import (
	"crypto/subtle"
	"fmt"
	"strings"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
)

const (
	PermissionAdmin       = "admin"
	PermissionTopicRead   = "topic.read"
	PermissionTopicWrite  = "topic.write"
	PermissionGroup       = "group"
	PermissionTransaction = "transaction"
	PermissionAll         = "*"
)

func (ch *CommandHandler) handleAuth(cmd string, ctx *ClientContext) string {
	args := parseKeyValueArgs(cmd[len("AUTH "):])
	principal := strings.TrimSpace(args["principal"])
	token := args["token"]
	if principal == "" || token == "" {
		return "ERROR: invalid_auth command=AUTH"
	}
	if !ch.validateSASLToken(principal, token) {
		return "ERROR: authentication_failed mechanism=PLAIN"
	}
	if ctx != nil {
		ctx.Principal = principal
		ctx.Authenticated = true
	}
	return fmt.Sprintf("OK principal=%s authenticated=true", principal)
}

func (ch *CommandHandler) authenticateInline(args map[string]string, ctx *ClientContext) string {
	principal := strings.TrimSpace(args["principal"])
	token := args["auth_token"]
	if principal == "" && token == "" {
		return ""
	}
	if principal == "" || token == "" {
		return "ERROR: invalid_auth reason=principal_and_auth_token_required"
	}
	if !ch.validateSASLToken(principal, token) {
		return "ERROR: authentication_failed mechanism=PLAIN"
	}
	if ctx != nil {
		ctx.Principal = principal
		ctx.Authenticated = true
	}
	return ""
}

func (ch *CommandHandler) validateSASLToken(principal, token string) bool {
	user := ch.saslUser(principal)
	return user != nil && constantTimeStringEqual(user.Token, token)
}

func (ch *CommandHandler) saslUser(principal string) *config.SASLUser {
	if ch == nil || ch.Config == nil || !ch.Config.EnableSASL {
		return nil
	}
	for i := range ch.Config.SASLUsers {
		if ch.Config.SASLUsers[i].Principal == principal {
			return &ch.Config.SASLUsers[i]
		}
	}
	return nil
}

func constantTimeStringEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func principalFromContext(ctx *ClientContext) string {
	if ctx == nil || !ctx.Authenticated {
		return ""
	}
	return ctx.Principal
}

func (ch *CommandHandler) authorizeTopicRead(policy topic.Policy, ctx *ClientContext) string {
	if policy.CanReadPrincipal(principalFromContext(ctx)) {
		return ""
	}
	return "ERROR: NOT_AUTHORIZED_FOR_TOPIC operation=read"
}

func (ch *CommandHandler) authorizeTopicWrite(policy topic.Policy, ctx *ClientContext) string {
	if policy.CanWritePrincipal(principalFromContext(ctx)) {
		return ""
	}
	return "ERROR: NOT_AUTHORIZED_FOR_TOPIC operation=write"
}

func (ch *CommandHandler) authorizeClientCommand(input commandInput, ctx *ClientContext) string {
	permissions := commandPermissions(input)
	if len(permissions) == 0 {
		return ""
	}

	if authResp := ch.authenticateInline(input.Args, ctx); authResp != "" {
		return authResp
	}
	return ch.authorizeClientPermissions(input.Name, input.Args, ctx, permissions...)
}

func (ch *CommandHandler) authorizeClientPermissions(command string, args map[string]string, ctx *ClientContext, permissions ...string) string {
	if ch == nil || ch.Config == nil || !ch.Config.EnableSASL || (ctx != nil && ctx.Internal) {
		return ""
	}
	if ctx == nil || !ctx.Authenticated {
		return fmt.Sprintf("ERROR: authentication_required command=%s", command)
	}

	user := ch.saslUser(ctx.Principal)
	if user == nil {
		return fmt.Sprintf("ERROR: authentication_failed principal=%s", ctx.Principal)
	}
	if len(user.Permissions) > 0 {
		for _, permission := range permissions {
			if !hasPermission(user.Permissions, permission) {
				return fmt.Sprintf("ERROR: NOT_AUTHORIZED_FOR_OPERATION command=%s permission=%s", command, permission)
			}
		}
	}

	topicName := args["topic"]
	if topicName == "" || ch.TopicManager == nil {
		return ""
	}
	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return ""
	}
	for _, permission := range permissions {
		switch permission {
		case PermissionTopicRead:
			if resp := ch.authorizeTopicRead(t.Policy, ctx); resp != "" {
				return fmt.Sprintf("%s topic=%s", resp, topicName)
			}
		case PermissionTopicWrite:
			if resp := ch.authorizeTopicWrite(t.Policy, ctx); resp != "" {
				return fmt.Sprintf("%s topic=%s", resp, topicName)
			}
		}
	}
	return ""
}

func hasPermission(granted []string, required string) bool {
	for _, permission := range granted {
		if strings.EqualFold(strings.TrimSpace(permission), PermissionAll) ||
			strings.EqualFold(strings.TrimSpace(permission), required) {
			return true
		}
	}
	return false
}

func commandPermissions(input commandInput) []string {
	upper := input.Upper
	switch {
	case upper == "LIST_CLUSTER", upper == "CLUSTER_STATUS", strings.HasPrefix(upper, "ELECT_LEADER "), strings.HasPrefix(upper, "CREATE "), strings.HasPrefix(upper, "DELETE "):
		return []string{PermissionAdmin}
	case upper == "LIST", strings.HasPrefix(upper, "DESCRIBE "), strings.HasPrefix(upper, "METADATA "),
		upper == "LIST_OFFSETS", strings.HasPrefix(upper, "LIST_OFFSETS "),
		strings.HasPrefix(upper, "READ_STREAM "), strings.HasPrefix(upper, "READ_SNAPSHOT "),
		strings.HasPrefix(upper, "STREAM_VERSION "):
		return []string{PermissionTopicRead}
	case strings.HasPrefix(upper, "PUBLISH "), strings.HasPrefix(upper, "APPEND_STREAM "),
		strings.HasPrefix(upper, "SAVE_SNAPSHOT "):
		return []string{PermissionTopicWrite}
	case strings.HasPrefix(upper, "TXN_PUBLISH "):
		return []string{PermissionTransaction, PermissionTopicWrite}
	case strings.HasPrefix(upper, "SEND_OFFSETS_TO_TXN "):
		return []string{PermissionTransaction, PermissionGroup}
	case strings.HasPrefix(upper, "INIT_PRODUCER_ID "), strings.HasPrefix(upper, "BEGIN_TXN "),
		strings.HasPrefix(upper, "END_TXN "), strings.HasPrefix(upper, "TXN_STATUS "):
		return []string{PermissionTransaction}
	case strings.HasPrefix(upper, "FIND_COORDINATOR "):
		if input.Args["transactional_id"] != "" || input.Args["txn"] != "" || input.Args["transaction"] != "" {
			return []string{PermissionTransaction}
		}
		return []string{PermissionGroup}
	case upper == "LIST_GROUPS", strings.HasPrefix(upper, "REGISTER_GROUP "), strings.HasPrefix(upper, "JOIN_GROUP "),
		strings.HasPrefix(upper, "SYNC_GROUP "), strings.HasPrefix(upper, "LEAVE_GROUP "),
		strings.HasPrefix(upper, "FETCH_OFFSET "), strings.HasPrefix(upper, "GROUP_STATUS "),
		strings.HasPrefix(upper, "HEARTBEAT "), strings.HasPrefix(upper, "COMMIT_OFFSET "),
		strings.HasPrefix(upper, "BATCH_COMMIT "):
		return []string{PermissionGroup}
	default:
		return nil
	}
}
