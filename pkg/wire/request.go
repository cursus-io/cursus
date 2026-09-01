package wire

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
)

const (
	commandPayloadMagic   uint32 = 0x43525132 // CRQ2
	commandPayloadVersion uint16 = 2
	maxCommandArguments          = 1024
)

// CommandPayload is the deterministic request schema for commands that do not
// carry a native record batch. Command identity lives in the frame header.
type CommandPayload struct {
	Positionals []string
	Fields      map[string]string
	FieldOrder  []string
}

func ParseCommandText(text string) (Command, CommandPayload, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return CommandUnknown, CommandPayload{}, fmt.Errorf("command is empty")
	}
	commandName, rest, _ := strings.Cut(text, " ")
	command, err := ParseCommand(commandName)
	if err != nil {
		return CommandUnknown, CommandPayload{}, err
	}
	payload := CommandPayload{Fields: make(map[string]string)}
	addField := func(key, value string) error {
		if !validFieldName(key) {
			return fmt.Errorf("invalid command field %q", key)
		}
		if _, duplicate := payload.Fields[key]; duplicate {
			return fmt.Errorf("duplicate command field %q", key)
		}
		payload.Fields[key] = value
		payload.FieldOrder = append(payload.FieldOrder, key)
		return nil
	}

	var trailing [][2]string
	if messageIndex := strings.Index(rest, "message="); messageIndex >= 0 &&
		(messageIndex == 0 || isCommandSpace(rest[messageIndex-1])) {
		message := strings.TrimSpace(rest[messageIndex+len("message="):])
		rest = strings.TrimSpace(rest[:messageIndex])
		if metadataIndex := strings.Index(rest, "metadata="); metadataIndex >= 0 &&
			(metadataIndex == 0 || isCommandSpace(rest[metadataIndex-1])) {
			metadata := strings.TrimSpace(rest[metadataIndex+len("metadata="):])
			rest = strings.TrimSpace(rest[:metadataIndex])
			trailing = append(trailing, [2]string{"metadata", metadata})
		}
		trailing = append(trailing, [2]string{"message", message})
	} else if payloadIndex := strings.Index(rest, "payload="); payloadIndex >= 0 &&
		(payloadIndex == 0 || isCommandSpace(rest[payloadIndex-1])) {
		value := strings.TrimSpace(rest[payloadIndex+len("payload="):])
		rest = strings.TrimSpace(rest[:payloadIndex])
		trailing = append(trailing, [2]string{"payload", value})
	}

	for _, part := range strings.Fields(rest) {
		key, value, field := strings.Cut(part, "=")
		if !field {
			payload.Positionals = append(payload.Positionals, part)
			continue
		}
		if err := addField(key, value); err != nil {
			return CommandUnknown, CommandPayload{}, err
		}
	}
	for _, field := range trailing {
		if err := addField(field[0], field[1]); err != nil {
			return CommandUnknown, CommandPayload{}, err
		}
	}
	return command, payload, nil
}

func isCommandSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}

func EncodeCommandPayload(payload CommandPayload) ([]byte, error) {
	if len(payload.Positionals) > maxCommandArguments || len(payload.Fields) > maxCommandArguments {
		return nil, fmt.Errorf("command argument count exceeds maximum %d", maxCommandArguments)
	}
	keys := append([]string(nil), payload.FieldOrder...)
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("duplicate command field order entry %q", key)
		}
		if _, exists := payload.Fields[key]; !exists {
			return nil, fmt.Errorf("field order references missing field %q", key)
		}
		seen[key] = struct{}{}
	}
	for key := range payload.Fields {
		if !validFieldName(key) {
			return nil, fmt.Errorf("invalid command field %q", key)
		}
		if _, ordered := seen[key]; !ordered {
			keys = append(keys, key)
		}
	}
	if len(payload.FieldOrder) == 0 {
		sort.Strings(keys)
	}
	encoder := newBinaryEncoder(MaxFramePayload)
	encoder.uint32(commandPayloadMagic)
	encoder.uint16(commandPayloadVersion)
	encoder.uint16(uint16(len(payload.Positionals)))
	for _, positional := range payload.Positionals {
		if positional == "" || strings.ContainsAny(positional, " \t\r\n") {
			return nil, fmt.Errorf("invalid positional command argument %q", positional)
		}
		encoder.string(positional)
	}
	encoder.uint16(uint16(len(keys)))
	for _, key := range keys {
		encoder.string(key)
		encoder.string(payload.Fields[key])
	}
	return encoder.result()
}

func DecodeCommandPayload(data []byte) (CommandPayload, error) {
	decoder := newBinaryDecoder(data)
	if magic := decoder.uint32(); magic != commandPayloadMagic {
		return CommandPayload{}, fmt.Errorf("invalid Wire v2 command payload magic")
	}
	if version := decoder.uint16(); version != commandPayloadVersion {
		return CommandPayload{}, fmt.Errorf("unsupported command payload version %d", version)
	}
	positionalCount := decoder.uint16()
	if positionalCount > maxCommandArguments {
		return CommandPayload{}, fmt.Errorf("positional argument count %d exceeds maximum", positionalCount)
	}
	payload := CommandPayload{
		Fields: make(map[string]string),
	}
	if positionalCount > 0 {
		payload.Positionals = make([]string, 0, positionalCount)
	}
	for range positionalCount {
		value := decoder.string()
		if value == "" || strings.ContainsAny(value, " \t\r\n") {
			return CommandPayload{}, fmt.Errorf("invalid positional command argument %q", value)
		}
		payload.Positionals = append(payload.Positionals, value)
	}
	fieldCount := decoder.uint16()
	if fieldCount > maxCommandArguments {
		return CommandPayload{}, fmt.Errorf("field count %d exceeds maximum", fieldCount)
	}
	for range fieldCount {
		key, value := decoder.string(), decoder.string()
		if !validFieldName(key) {
			return CommandPayload{}, fmt.Errorf("invalid command field %q", key)
		}
		if _, duplicate := payload.Fields[key]; duplicate {
			return CommandPayload{}, fmt.Errorf("duplicate command field %q", key)
		}
		payload.Fields[key] = value
		payload.FieldOrder = append(payload.FieldOrder, key)
	}
	if err := decoder.finish(); err != nil {
		return CommandPayload{}, err
	}
	return payload, nil
}

func RenderCommand(command Command, payload CommandPayload) (string, error) {
	if !command.valid() || command == CommandNegotiate {
		return "", fmt.Errorf("invalid application command %s", command)
	}
	keys := append([]string(nil), payload.FieldOrder...)
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		seen[key] = struct{}{}
	}
	for key := range payload.Fields {
		if !validFieldName(key) {
			return "", fmt.Errorf("invalid command field %q", key)
		}
		if _, ordered := seen[key]; !ordered {
			keys = append(keys, key)
		}
	}
	if len(payload.FieldOrder) == 0 {
		sort.Strings(keys)
	}
	parts := make([]string, 0, 1+len(payload.Positionals)+len(keys))
	parts = append(parts, command.String())
	parts = append(parts, payload.Positionals...)
	for _, key := range keys {
		parts = append(parts, key+"="+payload.Fields[key])
	}
	return strings.Join(parts, " "), nil
}

func IsCommandPayload(data []byte) bool {
	return len(data) >= 6 && binary.BigEndian.Uint32(data[:4]) == commandPayloadMagic &&
		binary.BigEndian.Uint16(data[4:6]) == commandPayloadVersion
}

func validFieldName(value string) bool {
	if value == "" {
		return false
	}
	for index, char := range value {
		if (char >= 'a' && char <= 'z') || char == '_' ||
			(index > 0 && ((char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9'))) {
			continue
		}
		return false
	}
	return true
}
