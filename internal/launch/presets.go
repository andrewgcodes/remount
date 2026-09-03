package launch

import (
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
)

// Preset describes how a model provider is reached through the broker: which
// hosts its binding must cover, which environment variables SDKs and
// harnesses read, and where the API root sits behind the broker's
// reverse-proxy path. Presets keep recipes provider-neutral: a recipe lists
// preset names and the user picks bindings.
type Preset struct {
	Name string `json:"name"`
	// Hosts the provider binding must cover; a HostParam host contains
	// "{host}" and is supplied per binding.
	Hosts []string `json:"hosts"`
	// HostParam names a per-deployment host component the user supplies as
	// --binding ID:PRESET?host=…; empty for fixed-host providers.
	HostParam string `json:"host_param,omitempty"`
	// KeyEnv is the variable harnesses read the API key from; the workspace
	// sees the binding's placeholder there.
	KeyEnv string `json:"key_env"`
	// BaseURLEnv is the variable harnesses read the API root from.
	BaseURLEnv string `json:"base_url_env,omitempty"`
	// BaseURLPath is appended to the broker base URL: /d/<host>/<path>.
	BaseURLPath string `json:"base_url_path,omitempty"`
	// Header documents how the key travels, for `binding preset ls`.
	Header string `json:"header,omitempty"`
	// ExtraEnv is exported verbatim by the launcher.
	ExtraEnv map[string]string `json:"extra_env,omitempty"`
	// Note is one line of operator guidance.
	Note string `json:"note,omitempty"`
}

var presets = map[string]Preset{
	"anthropic": {
		Name: "anthropic", Hosts: []string{"api.anthropic.com"},
		KeyEnv: "ANTHROPIC_API_KEY", BaseURLEnv: "ANTHROPIC_BASE_URL", BaseURLPath: "/d/api.anthropic.com",
		Header: "x-api-key",
	},
	"openai": {
		Name: "openai", Hosts: []string{"api.openai.com"},
		KeyEnv: "OPENAI_API_KEY", BaseURLEnv: "OPENAI_BASE_URL", BaseURLPath: "/d/api.openai.com/v1",
		Header: "Authorization: Bearer",
	},
	"google": {
		Name: "google", Hosts: []string{"generativelanguage.googleapis.com"},
		KeyEnv: "GEMINI_API_KEY", BaseURLEnv: "GOOGLE_GEMINI_BASE_URL", BaseURLPath: "/d/generativelanguage.googleapis.com",
		Header:   "x-goog-api-key",
		ExtraEnv: map[string]string{"GOOGLE_API_KEY": "$GEMINI_API_KEY"},
	},
	"openrouter": {
		Name: "openrouter", Hosts: []string{"openrouter.ai"},
		KeyEnv: "OPENROUTER_API_KEY", BaseURLEnv: "OPENROUTER_BASE_URL", BaseURLPath: "/d/openrouter.ai/api/v1",
		Header: "Authorization: Bearer",
	},
	"bedrock": {
		Name: "bedrock", Hosts: []string{"bedrock-runtime.{host}.amazonaws.com"}, HostParam: "region",
		KeyEnv: "AWS_BEARER_TOKEN_BEDROCK", BaseURLEnv: "BEDROCK_BASE_URL", BaseURLPath: "/d/bedrock-runtime.{host}.amazonaws.com",
		Header: "Authorization: Bearer",
		Note:   "Bedrock API keys (bearer tokens); SigV4 signing at the broker arrives with the cloud connectors",
	},
	"vertex": {
		Name: "vertex", Hosts: []string{"aiplatform.googleapis.com"},
		KeyEnv: "GOOGLE_VERTEX_API_KEY", BaseURLEnv: "GOOGLE_VERTEX_BASE_URL", BaseURLPath: "/d/aiplatform.googleapis.com/v1",
		Header: "x-goog-api-key",
		Note:   "Vertex AI express-mode API keys; OAuth service accounts arrive with the cloud connectors",
	},
	"azure-openai": {
		Name: "azure-openai", Hosts: []string{"{host}.openai.azure.com"}, HostParam: "resource",
		KeyEnv: "AZURE_OPENAI_API_KEY", BaseURLEnv: "AZURE_OPENAI_ENDPOINT", BaseURLPath: "/d/{host}.openai.azure.com",
		Header: "api-key",
	},
	"mistral": {
		Name: "mistral", Hosts: []string{"api.mistral.ai"},
		KeyEnv: "MISTRAL_API_KEY", BaseURLEnv: "MISTRAL_BASE_URL", BaseURLPath: "/d/api.mistral.ai/v1",
		Header: "Authorization: Bearer",
	},
	"groq": {
		Name: "groq", Hosts: []string{"api.groq.com"},
		KeyEnv: "GROQ_API_KEY", BaseURLEnv: "GROQ_BASE_URL", BaseURLPath: "/d/api.groq.com/openai/v1",
		Header: "Authorization: Bearer",
	},
	"together": {
		Name: "together", Hosts: []string{"api.together.xyz"},
		KeyEnv: "TOGETHER_API_KEY", BaseURLEnv: "TOGETHER_BASE_URL", BaseURLPath: "/d/api.together.xyz/v1",
		Header: "Authorization: Bearer",
	},
	"fireworks": {
		Name: "fireworks", Hosts: []string{"api.fireworks.ai"},
		KeyEnv: "FIREWORKS_API_KEY", BaseURLEnv: "FIREWORKS_BASE_URL", BaseURLPath: "/d/api.fireworks.ai/inference/v1",
		Header: "Authorization: Bearer",
	},
	"deepseek": {
		Name: "deepseek", Hosts: []string{"api.deepseek.com"},
		KeyEnv: "DEEPSEEK_API_KEY", BaseURLEnv: "DEEPSEEK_BASE_URL", BaseURLPath: "/d/api.deepseek.com/v1",
		Header: "Authorization: Bearer",
	},
	"xai": {
		Name: "xai", Hosts: []string{"api.x.ai"},
		KeyEnv: "XAI_API_KEY", BaseURLEnv: "XAI_BASE_URL", BaseURLPath: "/d/api.x.ai/v1",
		Header: "Authorization: Bearer",
	},
}

// PresetNames returns every preset name, sorted.
func PresetNames() []string {
	names := make([]string, 0, len(presets))
	for n := range presets {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// LookupPreset returns the preset called name.
func LookupPreset(name string) (Preset, bool) {
	p, ok := presets[name]
	return p, ok
}

// Presets returns all presets sorted by name.
func Presets() []Preset {
	out := make([]Preset, 0, len(presets))
	for _, n := range PresetNames() {
		out = append(out, presets[n])
	}
	return out
}

// Binding is one --binding argument resolved against a preset.
type Binding struct {
	// ID is the binding id the control plane knows (b_…).
	ID string
	// Preset is the provider preset the binding stands for.
	Preset Preset
	// Host is the resolved deployment host for HostParam presets.
	Host string
}

var bindingIDPattern = regexp.MustCompile(`^b_[A-Za-z0-9._-]{1,63}$`)
var hostLabelPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// ParseBinding parses `ID`, `ID:PRESET` or `ID:PRESET?host=…`. A bare ID is
// matched to the preset named by its suffix (`b_openai` → openai). A recipe
// that lists no matching preset is a mismatch reported by the caller.
func ParseBinding(spec string) (Binding, error) {
	id, rest, _ := strings.Cut(spec, ":")
	if !bindingIDPattern.MatchString(id) {
		return Binding{}, fmt.Errorf("binding %q: id must look like b_name", spec)
	}
	presetName, query, _ := strings.Cut(rest, "?")
	if presetName == "" {
		presetName = strings.TrimPrefix(id, "b_")
		presetName = strings.ToLower(strings.NewReplacer("_", "-").Replace(presetName))
	}
	p, ok := LookupPreset(presetName)
	if !ok {
		return Binding{}, fmt.Errorf("binding %q: no provider preset %q; name the preset as ID:PRESET (have %s)", spec, presetName, strings.Join(PresetNames(), ", "))
	}
	b := Binding{ID: id, Preset: p}
	params, err := url.ParseQuery(query)
	if err != nil {
		return Binding{}, fmt.Errorf("binding %q: %w", spec, err)
	}
	host := params.Get("host")
	for k := range params {
		if k != "host" {
			return Binding{}, fmt.Errorf("binding %q: unknown parameter %q", spec, k)
		}
	}
	if p.HostParam != "" {
		if host == "" {
			return Binding{}, fmt.Errorf("binding %q: preset %s needs ?host=<%s>", spec, p.Name, p.HostParam)
		}
		if !hostLabelPattern.MatchString(host) {
			return Binding{}, fmt.Errorf("binding %q: host %q must be a single DNS label", spec, host)
		}
		b.Host = host
	} else if host != "" {
		return Binding{}, fmt.Errorf("binding %q: preset %s has a fixed host", spec, p.Name)
	}
	return b, nil
}

// BaseURLPath returns the broker path for this binding with any host
// parameter substituted.
func (b Binding) BaseURLPath() string {
	return strings.ReplaceAll(b.Preset.BaseURLPath, "{host}", b.Host)
}

// Hosts returns the destinations the binding must cover, substituted.
func (b Binding) Hosts() []string {
	out := make([]string, 0, len(b.Preset.Hosts))
	for _, h := range b.Preset.Hosts {
		out = append(out, strings.ReplaceAll(h, "{host}", b.Host))
	}
	return out
}

// SessionEnv is the environment the session opens with: the key variable
// holds the binding reference the node resolves to a placeholder, and the
// base URL points at the broker. Nothing here is a secret.
func (b Binding) SessionEnv() map[string]string {
	return map[string]string{
		b.Preset.KeyEnv:     "ref:" + b.ID,
		b.Preset.BaseURLEnv: "${REMOUNT_BROKER}" + b.BaseURLPath(),
	}
}
