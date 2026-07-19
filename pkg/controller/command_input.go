package controller

import "strings"

type commandInput struct {
	Raw   string
	Upper string
	Name  string
	Args  map[string]string
}

func decodeCommandInput(raw string) commandInput {
	cmd := strings.TrimSpace(raw)
	input := commandInput{Raw: cmd, Upper: strings.ToUpper(cmd), Args: map[string]string{}}
	if cmd == "" {
		return input
	}

	separator := strings.IndexAny(cmd, " \t\r\n")
	if separator == -1 {
		input.Name = strings.ToUpper(cmd)
		return input
	}
	input.Name = strings.ToUpper(cmd[:separator])
	input.Args = parseKeyValueArgs(strings.TrimSpace(cmd[separator+1:]))
	return input
}
