package sdk

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	wireprotocol "github.com/cursus-io/cursus/pkg/protocol"
)

type ProtocolInfo struct {
	Protocol       string
	MinimumVersion int
	MaximumVersion int
	DefaultVersion int
	Features       []string
	ErrorClasses   []string
}

type ProtocolNegotiation struct {
	Version         int
	Features        []string
	RequireFeatures bool
}

type NegotiatedProtocol struct {
	Version     int
	Enabled     []string
	Unsupported []string
}

func FetchProtocolInfo(conn net.Conn) (*ProtocolInfo, error) {
	resp, err := executeProtocolCommand(conn, "PROTOCOL_INFO")
	if err != nil {
		return nil, err
	}
	fields, err := parseOKResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("protocol info: %w", err)
	}

	minimum, err := requiredIntField(fields, "min_version")
	if err != nil {
		return nil, fmt.Errorf("protocol info: %w", err)
	}
	maximum, err := requiredIntField(fields, "max_version")
	if err != nil {
		return nil, fmt.Errorf("protocol info: %w", err)
	}
	defaultVersion, err := requiredIntField(fields, "default_version")
	if err != nil {
		return nil, fmt.Errorf("protocol info: %w", err)
	}
	protocolName := fields["protocol"]
	if protocolName == "" {
		return nil, fmt.Errorf("protocol info: missing protocol")
	}
	if minimum < 1 || maximum < minimum || defaultVersion < minimum || defaultVersion > maximum {
		return nil, fmt.Errorf(
			"protocol info: invalid version range min=%d max=%d default=%d",
			minimum,
			maximum,
			defaultVersion,
		)
	}

	return &ProtocolInfo{
		Protocol:       protocolName,
		MinimumVersion: minimum,
		MaximumVersion: maximum,
		DefaultVersion: defaultVersion,
		Features:       splitListField(fields["features"]),
		ErrorClasses:   splitListField(fields["error_classes"]),
	}, nil
}

func NegotiateProtocol(conn net.Conn, request ProtocolNegotiation) (*NegotiatedProtocol, error) {
	version := request.Version
	if version == 0 {
		version = wireprotocol.CurrentVersion
	}
	features, err := normalizeFeatureNames(request.Features)
	if err != nil {
		return nil, err
	}

	cmd := fmt.Sprintf(
		"NEGOTIATE version=%d features=%s require_features=%t",
		version,
		strings.Join(features, ","),
		request.RequireFeatures,
	)
	resp, err := executeProtocolCommand(conn, cmd)
	if err != nil {
		return nil, err
	}
	fields, err := parseOKResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("protocol negotiation: %w", err)
	}
	selected, err := requiredIntField(fields, "protocol_version")
	if err != nil {
		return nil, fmt.Errorf("protocol negotiation: %w", err)
	}
	if selected != version {
		return nil, fmt.Errorf("protocol negotiation: broker selected version %d, requested %d", selected, version)
	}
	enabled, err := normalizeFeatureNames(splitListField(fields["enabled"]))
	if err != nil {
		return nil, fmt.Errorf("protocol negotiation: invalid enabled features: %w", err)
	}
	unsupported, err := normalizeFeatureNames(splitListField(fields["unsupported"]))
	if err != nil {
		return nil, fmt.Errorf("protocol negotiation: invalid unsupported features: %w", err)
	}
	result := &NegotiatedProtocol{Version: selected, Enabled: enabled, Unsupported: unsupported}
	if err := validateNegotiatedFeatures(features, result, request.RequireFeatures); err != nil {
		return nil, err
	}
	return result, nil
}

func negotiateConfiguredProtocol(conn net.Conn, version int, features []string, require bool, timeoutMS int) error {
	if version == 0 && len(features) == 0 {
		if require {
			return fmt.Errorf("required protocol negotiation has no configured features")
		}
		return nil
	}
	if timeoutMS <= 0 {
		timeoutMS = 5000
	}
	if err := conn.SetDeadline(time.Now().Add(time.Duration(timeoutMS) * time.Millisecond)); err != nil {
		return fmt.Errorf("set protocol negotiation deadline: %w", err)
	}
	defer func() { _ = conn.SetDeadline(time.Time{}) }()
	_, err := NegotiateProtocol(conn, ProtocolNegotiation{
		Version:         version,
		Features:        features,
		RequireFeatures: require,
	})
	return err
}

func validateNegotiatedFeatures(requested []string, result *NegotiatedProtocol, require bool) error {
	if len(requested) == 1 && requested[0] == "*" {
		if len(result.Unsupported) > 0 {
			return fmt.Errorf("protocol negotiation: wildcard request returned unsupported features: %s", strings.Join(result.Unsupported, ","))
		}
		return nil
	}
	requestedSet := make(map[string]struct{}, len(requested))
	for _, feature := range requested {
		requestedSet[feature] = struct{}{}
	}
	enabledSet := make(map[string]struct{}, len(result.Enabled))
	for _, feature := range result.Enabled {
		if _, ok := requestedSet[feature]; !ok {
			return fmt.Errorf("protocol negotiation: broker enabled unrequested feature %s", feature)
		}
		enabledSet[feature] = struct{}{}
	}
	unsupportedSet := make(map[string]struct{}, len(result.Unsupported))
	for _, feature := range result.Unsupported {
		if _, ok := requestedSet[feature]; !ok {
			return fmt.Errorf("protocol negotiation: broker rejected unrequested feature %s", feature)
		}
		if _, enabled := enabledSet[feature]; enabled {
			return fmt.Errorf("protocol negotiation: feature reported as both enabled and unsupported: %s", feature)
		}
		unsupportedSet[feature] = struct{}{}
	}
	for _, feature := range requested {
		_, enabled := enabledSet[feature]
		_, unsupported := unsupportedSet[feature]
		if !enabled && !unsupported {
			return fmt.Errorf("protocol negotiation: broker omitted requested feature %s", feature)
		}
		if require && !enabled {
			return fmt.Errorf("required protocol feature not enabled: %s", feature)
		}
	}
	return nil
}

func executeProtocolCommand(conn net.Conn, command string) (string, error) {
	if conn == nil {
		return "", fmt.Errorf("protocol command connection is nil")
	}
	if err := WriteWithLength(conn, EncodeMessage("", command)); err != nil {
		return "", fmt.Errorf("send protocol command: %w", err)
	}
	resp, err := ReadWithLength(conn)
	if err != nil {
		return "", fmt.Errorf("read protocol response: %w", err)
	}
	value := strings.TrimSpace(string(resp))
	if brokerErr, ok := ParseBrokerError(value); ok {
		return "", brokerErr
	}
	return value, nil
}

func parseOKResponse(response string) (map[string]string, error) {
	parts := strings.Fields(strings.TrimSpace(response))
	if len(parts) == 0 || parts[0] != "OK" {
		return nil, fmt.Errorf("unexpected response: %s", response)
	}
	fields := make(map[string]string, len(parts)-1)
	for _, part := range parts[1:] {
		key, value, ok := strings.Cut(part, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("malformed response field %q", part)
		}
		if _, duplicate := fields[key]; duplicate {
			return nil, fmt.Errorf("duplicate response field %q", key)
		}
		fields[key] = value
	}
	return fields, nil
}

func requiredIntField(fields map[string]string, key string) (int, error) {
	value, ok := fields[key]
	if !ok || value == "" {
		return 0, fmt.Errorf("missing %s", key)
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q", key, value)
	}
	return parsed, nil
}

func splitListField(value string) []string {
	if value == "" {
		return nil
	}
	values := strings.Split(value, ",")
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func normalizeFeatureNames(features []string) ([]string, error) {
	if len(features) == 1 && strings.TrimSpace(features[0]) == "*" {
		return []string{"*"}, nil
	}
	seen := make(map[string]struct{}, len(features))
	for _, feature := range features {
		feature = strings.TrimSpace(feature)
		if feature == "" {
			return nil, fmt.Errorf("protocol feature name is empty")
		}
		if !wireprotocol.IsValidFeatureName(feature) {
			return nil, fmt.Errorf("invalid protocol feature %q", feature)
		}
		seen[feature] = struct{}{}
	}
	result := make([]string, 0, len(seen))
	for feature := range seen {
		result = append(result, feature)
	}
	sort.Strings(result)
	return result, nil
}
