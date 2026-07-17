package protocol

import (
	"fmt"
	"sort"
	"strings"
)

const (
	MinimumVersion        = 1
	CurrentVersion        = 1
	MaxFeatureNameLength  = 64
	MaxRequestedFeatures  = 64
	MaxFeatureRequestSize = 4096
)

type Feature string

const (
	FeatureEventSourcingV1      Feature = "event_sourcing_v1"
	FeatureIdempotentProducerV1 Feature = "idempotent_producer_v1"
	FeatureOffsetResumeV1       Feature = "offset_resume_v1"
	FeatureStreamControlV1      Feature = "stream_control_v1"
	FeatureStructuredErrorsV1   Feature = "structured_errors_v1"
	FeatureTopicCompactionV1    Feature = "topic_compaction_v1"
)

var supportedFeatures = []Feature{
	FeatureEventSourcingV1,
	FeatureIdempotentProducerV1,
	FeatureOffsetResumeV1,
	FeatureStreamControlV1,
	FeatureStructuredErrorsV1,
	FeatureTopicCompactionV1,
}

func SupportedFeatures() []Feature {
	features := make([]Feature, len(supportedFeatures))
	copy(features, supportedFeatures)
	return features
}

func IsSupportedFeature(feature Feature) bool {
	for _, supported := range supportedFeatures {
		if feature == supported {
			return true
		}
	}
	return false
}

func NegotiateFeatures(requested string) (enabled []Feature, unsupported []string, err error) {
	names, wildcard, err := ParseFeatureRequest(requested)
	if err != nil {
		return nil, nil, err
	}
	if wildcard {
		return SupportedFeatures(), nil, nil
	}

	seenEnabled := make(map[Feature]struct{})
	seenUnsupported := make(map[string]struct{})
	for _, name := range names {
		feature := Feature(name)
		if IsSupportedFeature(feature) {
			seenEnabled[feature] = struct{}{}
			continue
		}
		seenUnsupported[name] = struct{}{}
	}

	for feature := range seenEnabled {
		enabled = append(enabled, feature)
	}
	for feature := range seenUnsupported {
		unsupported = append(unsupported, feature)
	}
	sort.Slice(enabled, func(i, j int) bool { return enabled[i] < enabled[j] })
	sort.Strings(unsupported)
	return enabled, unsupported, nil
}

func ParseFeatureRequest(requested string) (names []string, wildcard bool, err error) {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return nil, false, nil
	}
	if len(requested) > MaxFeatureRequestSize {
		return nil, false, fmt.Errorf("feature request exceeds %d bytes", MaxFeatureRequestSize)
	}
	parts := strings.Split(requested, ",")
	if len(parts) > MaxRequestedFeatures {
		return nil, false, fmt.Errorf("feature request exceeds %d entries", MaxRequestedFeatures)
	}
	if len(parts) == 1 && strings.TrimSpace(parts[0]) == "*" {
		return nil, true, nil
	}

	seen := make(map[string]struct{}, len(parts))
	for _, raw := range parts {
		name := strings.TrimSpace(raw)
		if !IsValidFeatureName(name) {
			return nil, false, fmt.Errorf("invalid feature name %q", name)
		}
		seen[name] = struct{}{}
	}
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, false, nil
}

func IsValidFeatureName(name string) bool {
	if name == "" || len(name) > MaxFeatureNameLength {
		return false
	}
	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

func JoinFeatures(features []Feature) string {
	values := make([]string, len(features))
	for i, feature := range features {
		values[i] = string(feature)
	}
	sort.Strings(values)
	return strings.Join(values, ",")
}
