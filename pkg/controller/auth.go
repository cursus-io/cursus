package controller

import (
	"crypto/subtle"
	"fmt"
	"strings"

	"github.com/cursus-io/cursus/pkg/topic"
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
	if ch == nil || ch.Config == nil || !ch.Config.EnableSASL {
		return false
	}
	for _, user := range ch.Config.SASLUsers {
		if user.Principal == principal && constantTimeStringEqual(user.Token, token) {
			return true
		}
	}
	return false
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
