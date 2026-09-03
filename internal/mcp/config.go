package mcp

import (
	"encoding/json"
	"fmt"
	"strings"
)

var jsonHosts = map[string]func(string) any{
	"claude": func(command string) any {
		return map[string]any{"mcpServers": map[string]any{"remount": map[string]any{"type": "stdio", "command": command, "args": []string{"mcp", "serve"}}}}
	},
	"cursor": func(command string) any {
		return map[string]any{"mcpServers": map[string]any{"remount": map[string]any{"command": command, "args": []string{"mcp", "serve"}}}}
	},
	"gemini": func(command string) any {
		return map[string]any{"mcpServers": map[string]any{"remount": map[string]any{"command": command, "args": []string{"mcp", "serve"}, "trust": false}}}
	},
	"opencode": func(command string) any {
		return map[string]any{"$schema": "https://opencode.ai/config.json", "mcp": map[string]any{"servers": map[string]any{"remount": map[string]any{"type": "local", "command": []string{command, "mcp", "serve"}, "disabled": false}}}}
	},
}

// ConfigSnippet returns a host-native stdio configuration. It contains no
// credentials: the host must explicitly provide REMOUNT_SERVER and
// REMOUNT_TOKEN in the server process environment.
func ConfigSnippet(host, command string) (string, error) {
	host = strings.ToLower(strings.TrimSpace(host))
	if command == "" {
		command = "remount"
	}
	if build, ok := jsonHosts[host]; ok {
		raw, err := json.MarshalIndent(build(command), "", "  ")
		if err != nil {
			return "", err
		}
		return string(raw) + "\n", nil
	}
	switch host {
	case "claude-code":
		return ConfigSnippet("claude", command)
	case "codex":
		return fmt.Sprintf("[mcp_servers.remount]\ncommand = %q\nargs = [\"mcp\", \"serve\"]\nenv_vars = [\"REMOUNT_SERVER\", \"REMOUNT_TOKEN\", \"REMOUNT_PRINCIPAL\"]\nrequired = true\n", command), nil
	case "goose":
		return fmt.Sprintf("goose session --with-extension %q\n", command+" mcp serve"), nil
	case "agents", "agents.md":
		return "Use the remount MCP tools for workspace and durable-agent lifecycle. Reuse idempotency_key when retrying a mutation. Prefer agent_create, message, get, transcript_tail, approve, and events_tail. Never request, read, or print credentials or .remount/env; workspace_info returns only the workspace id and broker URL.\n", nil
	default:
		return "", fmt.Errorf("unsupported MCP host %q (choose claude, codex, cursor, goose, gemini, opencode, or agents)", host)
	}
}
