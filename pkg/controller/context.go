package controller

type ClientContext struct {
	ConsumerGroup string
	ConsumerIdx   int
	CurrentTopics map[string]struct{}
	MemberID      string
	Generation    int
	OffsetCache   map[string]uint64
	Principal     string
	Authenticated bool
}

func NewClientContext(group string, idx int) *ClientContext {
	return &ClientContext{
		ConsumerGroup: group,
		ConsumerIdx:   idx,
		CurrentTopics: make(map[string]struct{}),
		MemberID:      "",
		Generation:    0,
		OffsetCache:   make(map[string]uint64),
	}
}

func (ctx *ClientContext) SetConsumerGroup(groupName string) {
	ctx.ConsumerGroup = groupName
}
