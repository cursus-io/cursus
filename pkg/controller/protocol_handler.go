package controller

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	wireprotocol "github.com/cursus-io/cursus/pkg/protocol"
)

func (ch *CommandHandler) handleProtocolInfo() string {
	return fmt.Sprintf(
		"OK protocol=cursus min_version=%d max_version=%d default_version=%d features=%s error_classes=authorization,availability,conflict,fencing,internal,not_found,routing,validation",
		wireprotocol.MinimumVersion,
		wireprotocol.CurrentVersion,
		wireprotocol.CurrentVersion,
		wireprotocol.JoinFeatures(wireprotocol.SupportedFeatures()),
	)
}

func (ch *CommandHandler) handleNegotiate(cmd string, ctx *ClientContext) string {
	args := parseKeyValueArgs(strings.TrimSpace(cmd[len("NEGOTIATE"):]))
	versionValue := args["version"]
	if versionValue == "" {
		return "ERROR: missing_protocol_version class=validation retryable=false command=NEGOTIATE"
	}
	version, err := strconv.Atoi(versionValue)
	if err != nil {
		return fmt.Sprintf("ERROR: invalid_protocol_version class=validation retryable=false requested=%q", versionValue)
	}
	if version < wireprotocol.MinimumVersion || version > wireprotocol.CurrentVersion {
		return fmt.Sprintf(
			"ERROR: UNSUPPORTED_PROTOCOL_VERSION class=validation retryable=false requested=%d min=%d max=%d",
			version,
			wireprotocol.MinimumVersion,
			wireprotocol.CurrentVersion,
		)
	}
	if ctx == nil {
		return "ERROR: negotiation_context_required class=internal retryable=false"
	}

	requireFeatures := false
	if value := args["require_features"]; value != "" {
		requireFeatures, err = strconv.ParseBool(value)
		if err != nil {
			return fmt.Sprintf("ERROR: invalid_require_features class=validation retryable=false value=%q", value)
		}
	}
	enabled, unsupported, err := wireprotocol.NegotiateFeatures(args["features"])
	if err != nil {
		return fmt.Sprintf("ERROR: invalid_protocol_features class=validation retryable=false reason=%q", err.Error())
	}
	if requireFeatures && len(unsupported) > 0 {
		return fmt.Sprintf(
			"ERROR: UNSUPPORTED_FEATURE class=validation retryable=false features=%s",
			strings.Join(unsupported, ","),
		)
	}

	ctx.SetProtocol(version, enabled)
	sort.Strings(unsupported)
	return fmt.Sprintf(
		"OK protocol_version=%d enabled=%s unsupported=%s",
		version,
		wireprotocol.JoinFeatures(enabled),
		strings.Join(unsupported, ","),
	)
}

func decorateProtocolResponse(response string, ctx *ClientContext) string {
	if ctx == nil || !ctx.HasFeature(wireprotocol.FeatureStructuredErrorsV1) {
		return response
	}
	return wireprotocol.EnrichErrorResponse(response)
}
