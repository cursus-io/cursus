package controller

import (
	"context"
	"sort"

	wireprotocol "github.com/cursus-io/cursus/pkg/protocol"
)

type ClientContext struct {
	ConsumerGroup   string
	ConsumerIdx     int
	CurrentTopics   map[string]struct{}
	MemberID        string
	Generation      int
	OffsetCache     map[string]uint64
	Principal       string
	Authenticated   bool
	Internal        bool
	ProtocolVersion int
	Features        map[wireprotocol.Feature]struct{}
	requestContext  context.Context
}

func NewClientContext(group string, idx int) *ClientContext {
	return &ClientContext{
		ConsumerGroup:   group,
		ConsumerIdx:     idx,
		CurrentTopics:   make(map[string]struct{}),
		MemberID:        "",
		Generation:      0,
		OffsetCache:     make(map[string]uint64),
		ProtocolVersion: wireprotocol.CurrentVersion,
		Features:        make(map[wireprotocol.Feature]struct{}),
		requestContext:  context.Background(),
	}
}

func NewInternalClientContext(group string, idx int) *ClientContext {
	ctx := NewClientContext(group, idx)
	ctx.Internal = true
	return ctx
}

func firstClientContext(contexts []*ClientContext) *ClientContext {
	if len(contexts) == 0 {
		return nil
	}
	return contexts[0]
}

func (ctx *ClientContext) SetRequestContext(requestContext context.Context) {
	if ctx == nil {
		return
	}
	if requestContext == nil {
		requestContext = context.Background()
	}
	ctx.requestContext = requestContext
}

func (ctx *ClientContext) RequestContext() context.Context {
	if ctx == nil || ctx.requestContext == nil {
		return context.Background()
	}
	return ctx.requestContext
}

func (ctx *ClientContext) SetConsumerGroup(groupName string) {
	ctx.ConsumerGroup = groupName
}

func (ctx *ClientContext) SetProtocol(version int, features []wireprotocol.Feature) {
	ctx.ProtocolVersion = version
	ctx.Features = make(map[wireprotocol.Feature]struct{}, len(features))
	for _, feature := range features {
		ctx.Features[feature] = struct{}{}
	}
}

func (ctx *ClientContext) HasFeature(feature wireprotocol.Feature) bool {
	if ctx == nil {
		return false
	}
	_, ok := ctx.Features[feature]
	return ok
}

func (ctx *ClientContext) EnabledFeatures() []wireprotocol.Feature {
	if ctx == nil {
		return nil
	}
	features := make([]wireprotocol.Feature, 0, len(ctx.Features))
	for feature := range ctx.Features {
		features = append(features, feature)
	}
	sort.Slice(features, func(i, j int) bool { return features[i] < features[j] })
	return features
}
