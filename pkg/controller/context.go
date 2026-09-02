package controller

import (
	"context"
)

type ClientContext struct {
	ConsumerGroup  string
	ConsumerIdx    int
	CurrentTopics  map[string]struct{}
	MemberID       string
	Generation     int
	OffsetCache    map[string]uint64
	Principal      string
	Authenticated  bool
	Internal       bool
	requestContext context.Context
}

func NewClientContext(group string, idx int) *ClientContext {
	return &ClientContext{
		ConsumerGroup:  group,
		ConsumerIdx:    idx,
		CurrentTopics:  make(map[string]struct{}),
		MemberID:       "",
		Generation:     0,
		OffsetCache:    make(map[string]uint64),
		requestContext: context.Background(),
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
