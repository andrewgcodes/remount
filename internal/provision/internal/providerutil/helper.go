package providerutil

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
)

// Helper invokes a shell-free provider adapter using a bounded JSON stdin/stdout
// protocol. PrefixArgs and operation must never contain credentials.
type Helper struct {
	Runner     Runner
	Executable string
	PrefixArgs []string
	Env        map[string]string
	MaxOutput  int
}

// Call sends input only on stdin and decodes one bounded JSON response.
func (h Helper) Call(ctx context.Context, operation string, input, output any) error {
	if operation == "" || strings.ContainsAny(operation, " \t\r\n\x00/\\") {
		return errors.New("provision: helper operation is invalid")
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return errors.New("provision: encode helper request")
	}
	runner := h.Runner
	if runner == nil {
		runner = OSRunner{}
	}
	args := append(append([]string(nil), h.PrefixArgs...), operation)
	response, err := runner.Run(ctx, Command{Executable: h.Executable, Args: args, Stdin: payload, Env: h.Env, MaxOutput: h.MaxOutput})
	if err != nil {
		return err
	}
	if output != nil {
		if err := json.Unmarshal(response, output); err != nil {
			return errors.New("provision: decode helper response")
		}
	}
	return nil
}
