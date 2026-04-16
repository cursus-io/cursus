package types

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMessageString(t *testing.T) {
	m := Message{
		ProducerID: "p1",
		SeqNum:     100,
		Payload:    "hello",
		Offset:     50,
		Key:        "k1",
		Epoch:      1,
		RetryCount: 0,
	}

	expected := "Message { ID: p1-100, Payload:hello, Offset:50, Key:k1, Epoch:1, RetryCount:0 }"
	assert.Equal(t, expected, m.String())
}
