package sdk

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"

	"github.com/cursus-io/cursus/pkg/wire"
)

func acceptWireTestRequest(conn net.Conn) (*wire.Connection, wire.Frame, string, error) {
	connection, err := wire.ServerHandshake(conn, []wire.Compression{
		wire.CompressionNone, wire.CompressionGZIP, wire.CompressionSnappy, wire.CompressionLZ4,
	})
	if err != nil {
		return nil, wire.Frame{}, "", err
	}
	request, err := connection.ReadFrame()
	if err != nil {
		return nil, wire.Frame{}, "", err
	}
	if request.Kind != wire.KindRequest {
		return nil, wire.Frame{}, "", fmt.Errorf("unexpected request kind %d", request.Kind)
	}
	command, err := decodeWireTestCommand(request)
	if err != nil {
		return nil, wire.Frame{}, "", err
	}
	return connection, request, command, nil
}

func decodeWireTestCommand(request wire.Frame) (string, error) {
	if wire.IsBatch(request.Payload) {
		return "PUBLISH_BATCH", nil
	}
	if wire.IsCommandPayload(request.Payload) {
		payload, err := wire.DecodeCommandPayload(request.Payload)
		if err != nil {
			return "", err
		}
		return wire.RenderCommand(request.Command, payload)
	}
	payload := request.Payload
	if len(payload) >= 2 {
		topicLength := int(binary.BigEndian.Uint16(payload[:2]))
		if topicLength <= len(payload)-2 {
			return string(payload[2+topicLength:]), nil
		}
	}
	return string(payload), nil
}

func writeWireTestResponse(connection *wire.Connection, request wire.Frame, response string) error {
	status := wire.StatusOK
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(response)), "ERROR:") {
		status = wire.StatusError
		brokerError, ok := ParseBrokerError(response)
		if !ok {
			return fmt.Errorf("invalid test broker error %q", response)
		}
		class, err := wire.ParseErrorClass(string(brokerError.Class))
		if err != nil {
			return err
		}
		fields := make(map[string]string, len(brokerError.Fields))
		for key, value := range brokerError.Fields {
			if key != "class" && key != "retryable" {
				fields[key] = value
			}
		}
		encoded, err := wire.EncodeError(wire.ErrorPayload{
			Code: brokerError.Code, Class: class, Retryable: brokerError.Retryable,
			Message: strings.Join(brokerError.Details, " "), Fields: fields,
		})
		if err != nil {
			return err
		}
		response = string(encoded)
	}
	return connection.WriteFrame(wire.Frame{
		Kind: wire.KindResponse, Command: request.Command, Status: status, RequestID: request.RequestID, Payload: []byte(response),
	})
}
