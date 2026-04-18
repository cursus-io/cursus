package util

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufferReadWrite(t *testing.T) {
	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()

	data := []byte("hello world")

	go func() {
		err := WriteWithLength(c1, data)
		assert.NoError(t, err)
	}()

	received, err := ReadWithLength(c2)
	assert.NoError(t, err)
	assert.Equal(t, data, received)
}

func TestBufferErrorCases(t *testing.T) {
	c1, c2 := net.Pipe()
	_ = c1.Close()
	_ = c2.Close()

	_, err := ReadWithLength(c2)
	assert.Error(t, err)

	err = WriteWithLength(c1, []byte("fail"))
	assert.Error(t, err)

	// Max size error
	largeData := make([]byte, MaxMessageSize+1)
	err = WriteWithLength(c1, largeData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum")
}
