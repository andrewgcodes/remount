// Package providerauth defines the provider-native subscription commands that
// a node permits through the confidential auth-session path.
package providerauth

import (
	"fmt"
	"slices"
	"strings"

	"remount.dev/remount/internal/proto"
)

// Spec is the node-enforced command and environment contract for one provider.
type Spec struct {
	Login    []string
	Status   []string
	Logout   []string
	UnsetEnv []string
}

var specs = map[string]Spec{
	"claude": {
		Login:    []string{"claude", "auth", "login", "--claudeai"},
		Status:   []string{"claude", "auth", "status"},
		Logout:   []string{"claude", "auth", "logout"},
		UnsetEnv: []string{"ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL"},
	},
	"codex": {
		Login:    []string{"codex", "login", "--device-auth"},
		Status:   []string{"codex", "login", "status"},
		Logout:   []string{"codex", "logout"},
		UnsetEnv: []string{"CODEX_API_KEY", "OPENAI_API_KEY", "OPENAI_BASE_URL", "AZURE_OPENAI_API_KEY"},
	},
}

// Lookup returns a copy of a supported provider contract.
func Lookup(recipe string) (Spec, bool) {
	spec, ok := specs[recipe]
	if !ok {
		return Spec{}, false
	}
	spec.Login = slices.Clone(spec.Login)
	spec.Status = slices.Clone(spec.Status)
	spec.Logout = slices.Clone(spec.Logout)
	spec.UnsetEnv = slices.Clone(spec.UnsetEnv)
	return spec, true
}

// Program returns the exact shell program accepted by the node. The shell is
// needed only to remove ambient API-key overrides and prepend recipe tools.
func Program(recipe, action string) ([]string, error) {
	spec, ok := Lookup(recipe)
	if !ok {
		return nil, fmt.Errorf("provider subscription auth is unsupported for recipe %q", recipe)
	}
	var command []string
	switch action {
	case proto.AuthActionLogin:
		command = spec.Login
	case proto.AuthActionStatus:
		command = spec.Status
	case proto.AuthActionLogout:
		command = spec.Logout
	default:
		return nil, fmt.Errorf("unknown provider auth action %q", action)
	}
	var script strings.Builder
	for _, name := range spec.UnsetEnv {
		fmt.Fprintf(&script, "unset %s\n", name)
	}
	script.WriteString(`PATH="$HOME/.local/bin:$PATH"` + "\nexport PATH\nexec")
	for _, arg := range command {
		script.WriteByte(' ')
		script.WriteString(shellQuote(arg))
	}
	script.WriteByte('\n')
	return []string{"/bin/sh", "-c", script.String()}, nil
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
