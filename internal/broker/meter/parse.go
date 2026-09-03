package meter

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"strconv"
	"strings"
)

type accumulator struct {
	input         uint64
	output        uint64
	total         uint64
	hasInput      bool
	hasOutput     bool
	hasTotal      bool
	sawUsage      bool
	model         string
	modelConflict bool
}

func (a *accumulator) merge(input token, output token, total token, model string) {
	if input.present && (!a.hasInput || input.value > a.input) {
		a.input, a.hasInput = input.value, true
	}
	if output.present && (!a.hasOutput || output.value > a.output) {
		a.output, a.hasOutput = output.value, true
	}
	if total.present && (!a.hasTotal || total.value > a.total) {
		a.total, a.hasTotal = total.value, true
	}
	if model = safeModel(model); model != "" && !a.modelConflict {
		if a.model == "" {
			a.model = model
		} else if a.model != model {
			a.model = ""
			a.modelConflict = true
		}
	}
}

func (a *accumulator) result() (uint64, uint64, uint64, string, error) {
	if a.hasTotal {
		if a.hasInput && !a.hasOutput && a.total >= a.input {
			a.output, a.hasOutput = a.total-a.input, true
		}
		if a.hasOutput && !a.hasInput && a.total >= a.output {
			a.input, a.hasInput = a.total-a.output, true
		}
	}
	if !a.hasInput || !a.hasOutput {
		if a.sawUsage {
			return 0, 0, 0, "", ErrMalformed
		}
		return 0, 0, 0, "", ErrUsageMissing
	}
	computed, overflow := add(a.input, a.output)
	if overflow || computed > math.MaxInt64 {
		return 0, 0, 0, "", ErrMalformed
	}
	if !a.hasTotal {
		a.total = computed
		a.hasTotal = true
	}
	if a.total < computed {
		return 0, 0, 0, "", ErrMalformed
	}
	return a.input, a.output, a.total, a.model, nil
}

func add(a, b uint64) (uint64, bool) {
	if b > math.MaxUint64-a {
		return 0, true
	}
	return a + b, false
}

type token struct {
	value   uint64
	present bool
}

func consumeJSONBody(body []byte, usage *accumulator, provider Provider, limits Limits) error {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return ErrMalformed
	}
	if trimmed[0] != '[' {
		if len(trimmed) > limits.MaxEventBytes {
			return ErrLimit
		}
		return consumeJSON(trimmed, usage, provider)
	}

	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	start, err := decoder.Token()
	if err != nil || start != json.Delim('[') {
		return ErrMalformed
	}
	events := 0
	for decoder.More() {
		if events >= limits.MaxEvents {
			return ErrLimit
		}
		var item json.RawMessage
		if err := decoder.Decode(&item); err != nil {
			return ErrMalformed
		}
		if len(bytes.TrimSpace(item)) > limits.MaxEventBytes {
			return ErrLimit
		}
		if err := consumeObject(item, usage, provider); err != nil {
			return err
		}
		events++
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim(']') {
		return ErrMalformed
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return ErrMalformed
	}
	return nil
}

func consumeJSON(data []byte, usage *accumulator, provider Provider) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return ErrMalformed
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return ErrMalformed
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return ErrMalformed
	}
	if trimmed[0] == '[' {
		return ErrMalformed
	}
	return consumeObject(raw, usage, provider)
}

func consumeObject(data []byte, usage *accumulator, provider Provider) error {
	var root map[string]json.RawMessage
	if err := decode(data, &root); err != nil || root == nil {
		return ErrMalformed
	}
	switch provider {
	case ProviderOpenAI, ProviderOpenRouter:
		return consumeOpenAI(root, usage)
	case ProviderAnthropic:
		return consumeAnthropic(root, usage)
	case ProviderGoogle:
		return consumeGoogle(root, usage)
	default:
		return nil
	}
}

func consumeOpenAI(root map[string]json.RawMessage, usage *accumulator) error {
	model := stringField(root, "model")
	if nested, ok, err := objectField(root, "response"); err != nil {
		return err
	} else if ok {
		if model == "" {
			model = stringField(nested, "model")
		}
		if _, exists := root["usage"]; !exists {
			root = nested
		}
	}
	u, ok, err := objectField(root, "usage")
	if err != nil || !ok {
		if err == nil {
			usage.merge(token{}, token{}, token{}, model)
		}
		return err
	}
	usage.sawUsage = true
	input, err := eitherToken(u, "input_tokens", "prompt_tokens")
	if err != nil {
		return err
	}
	output, err := eitherToken(u, "output_tokens", "completion_tokens")
	if err != nil {
		return err
	}
	total, err := tokenField(u, "total_tokens")
	if err != nil {
		return err
	}
	usage.merge(input, output, total, model)
	return nil
}

func consumeAnthropic(root map[string]json.RawMessage, usage *accumulator) error {
	model := stringField(root, "model")
	if message, ok, err := objectField(root, "message"); err != nil {
		return err
	} else if ok {
		if model == "" {
			model = stringField(message, "model")
		}
		if _, exists := root["usage"]; !exists {
			root = message
		}
	}
	u, ok, err := objectField(root, "usage")
	if err != nil || !ok {
		if err == nil {
			usage.merge(token{}, token{}, token{}, model)
		}
		return err
	}
	usage.sawUsage = true
	input, err := tokenField(u, "input_tokens")
	if err != nil {
		return err
	}
	for _, name := range []string{"cache_creation_input_tokens", "cache_read_input_tokens"} {
		cached, fieldErr := tokenField(u, name)
		if fieldErr != nil {
			return fieldErr
		}
		if cached.present {
			if !input.present {
				input.present = true
			}
			var overflow bool
			input.value, overflow = add(input.value, cached.value)
			if overflow || input.value > math.MaxInt64 {
				return ErrMalformed
			}
		}
	}
	output, err := tokenField(u, "output_tokens")
	if err != nil {
		return err
	}
	total, err := tokenField(u, "total_tokens")
	if err != nil {
		return err
	}
	usage.merge(input, output, total, model)
	return nil
}

func consumeGoogle(root map[string]json.RawMessage, usage *accumulator) error {
	model := stringField(root, "modelVersion")
	u, ok, err := objectField(root, "usageMetadata")
	if err != nil || !ok {
		if err == nil {
			usage.merge(token{}, token{}, token{}, model)
		}
		return err
	}
	usage.sawUsage = true
	input, err := tokenField(u, "promptTokenCount")
	if err != nil {
		return err
	}
	output, err := tokenField(u, "candidatesTokenCount")
	if err != nil {
		return err
	}
	total, err := tokenField(u, "totalTokenCount")
	if err != nil {
		return err
	}
	usage.merge(input, output, total, model)
	return nil
}

func decode(data []byte, dst any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	return nil
}

func objectField(root map[string]json.RawMessage, name string) (map[string]json.RawMessage, bool, error) {
	raw, ok := root[name]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, false, nil
	}
	var result map[string]json.RawMessage
	if err := decode(raw, &result); err != nil || result == nil {
		return nil, false, ErrMalformed
	}
	return result, true, nil
}

func stringField(root map[string]json.RawMessage, name string) string {
	raw, ok := root[name]
	if !ok {
		return ""
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return value
}

func eitherToken(root map[string]json.RawMessage, first, second string) (token, error) {
	one, err := tokenField(root, first)
	if err != nil || one.present {
		return one, err
	}
	return tokenField(root, second)
}

func tokenField(root map[string]json.RawMessage, name string) (token, error) {
	raw, ok := root[name]
	if !ok {
		return token{}, nil
	}
	value := strings.TrimSpace(string(raw))
	if value == "" || value[0] == '-' || strings.ContainsAny(value, ".eE+") {
		return token{}, ErrMalformed
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil || n > math.MaxInt64 {
		return token{}, ErrMalformed
	}
	return token{value: n, present: true}, nil
}

func safeModel(model string) string {
	if model == "" || len(model) > 256 {
		return ""
	}
	for _, r := range model {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || strings.ContainsRune("._:/-@", r) {
			continue
		}
		return ""
	}
	return model
}
