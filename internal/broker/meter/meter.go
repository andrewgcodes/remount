// Package meter observes model-provider responses and extracts token usage.
// It never grants budget authority; callers reserve and settle against their
// authoritative budget store.
package meter

import (
	"errors"
	"io"
	"mime"
	"strconv"
	"strings"
	"sync"
)

// Provider identifies a response grammar.
type Provider string

const (
	// ProviderUnknown identifies a provider whose usage grammar is unsupported.
	ProviderUnknown Provider = "unknown"
	// ProviderOpenAI identifies the OpenAI API.
	ProviderOpenAI Provider = "openai"
	// ProviderAnthropic identifies the Anthropic Messages API.
	ProviderAnthropic Provider = "anthropic"
	// ProviderGoogle identifies the Google Gemini API.
	ProviderGoogle Provider = "google"
	// ProviderOpenRouter identifies the OpenRouter API.
	ProviderOpenRouter Provider = "openrouter"
)

// UnmeteredReason explains why a response has no trustworthy token count.
type UnmeteredReason string

const (
	// UnmeteredUnsupported means the response grammar is not supported.
	UnmeteredUnsupported UnmeteredReason = "unsupported_provider"
	// UnmeteredIncomplete means the response did not reach EOF.
	UnmeteredIncomplete UnmeteredReason = "observation_incomplete"
	// UnmeteredLimit means an observation bound was exceeded.
	UnmeteredLimit UnmeteredReason = "observation_limit"
	// UnmeteredMalformed means usage was invalid or internally inconsistent.
	UnmeteredMalformed UnmeteredReason = "malformed_usage"
	// UnmeteredMissing means a complete response did not contain usage.
	UnmeteredMissing UnmeteredReason = "usage_missing"
)

var (
	// ErrIncomplete means the response was not observed through EOF.
	ErrIncomplete = errors.New("meter: response observation incomplete")
	// ErrLimit means a configured observation bound was exceeded.
	ErrLimit = errors.New("meter: response observation limit exceeded")
	// ErrMalformed means provider usage data was malformed or inconsistent.
	ErrMalformed = errors.New("meter: malformed provider usage")
	// ErrUsageMissing means a complete known-provider response had no usage.
	ErrUsageMissing = errors.New("meter: provider usage missing")
)

// Usage is the settled token observation for one brokered response.
// Metered is true only when all three token counts are trustworthy.
type Usage struct {
	Provider        Provider
	Model           string
	InputTokens     uint64
	OutputTokens    uint64
	TotalTokens     uint64
	Metered         bool
	Streaming       bool
	UnmeteredReason UnmeteredReason
}

// Limits bounds CPU-adjacent parser work and retained response bytes.
type Limits struct {
	MaxBytes      int
	MaxEventBytes int
	MaxEvents     int
}

const (
	defaultMaxBytes      = 8 << 20
	defaultMaxEventBytes = 256 << 10
	defaultMaxEvents     = 10_000
	maximumMaxBytes      = 64 << 20
	maximumMaxEventBytes = 1 << 20
	maximumMaxEvents     = 1_000_000
)

func normalizeLimits(l Limits) (Limits, error) {
	if l.MaxBytes == 0 {
		l.MaxBytes = defaultMaxBytes
	}
	if l.MaxEventBytes == 0 {
		l.MaxEventBytes = defaultMaxEventBytes
	}
	if l.MaxEvents == 0 {
		l.MaxEvents = defaultMaxEvents
	}
	if l.MaxBytes < 1 || l.MaxBytes > maximumMaxBytes ||
		l.MaxEventBytes < 1 || l.MaxEventBytes > maximumMaxEventBytes ||
		l.MaxEventBytes > l.MaxBytes || l.MaxEvents < 1 || l.MaxEvents > maximumMaxEvents {
		return Limits{}, ErrLimit
	}
	return l, nil
}

type streamKind uint8

const (
	streamJSON streamKind = iota
	streamSSE
	streamNDJSON
)

// Observer incrementally inspects bytes without retaining an unbounded body.
// Its methods are safe for concurrent use, although a response body itself
// should still have only one reader.
type Observer struct {
	mu       sync.Mutex
	provider Provider
	limits   Limits
	kind     streamKind
	parser   eventParser
	jsonBody []byte
	usage    accumulator
	bytes    int
	finished bool
	err      error
}

// NewObserver creates an observer. Unknown providers deliberately retain no
// response bytes and always report request-only metering.
func NewObserver(provider Provider, contentType string, limits Limits) (*Observer, error) {
	limits, err := normalizeLimits(limits)
	if err != nil {
		return nil, err
	}
	kind := contentKind(contentType)
	o := &Observer{provider: normalizedProvider(provider), limits: limits, kind: kind}
	if o.provider != ProviderUnknown {
		switch kind {
		case streamSSE:
			o.parser = newSSEParser(limits)
		case streamNDJSON:
			o.parser = newNDJSONParser(limits)
		}
	}
	return o, nil
}

func normalizedProvider(provider Provider) Provider {
	switch provider {
	case ProviderOpenAI, ProviderAnthropic, ProviderGoogle, ProviderOpenRouter:
		return provider
	default:
		return ProviderUnknown
	}
}

func contentKind(contentType string) streamKind {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		mediaType = strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
	}
	switch strings.ToLower(mediaType) {
	case "text/event-stream":
		return streamSSE
	case "application/x-ndjson", "application/jsonl":
		return streamNDJSON
	default:
		return streamJSON
	}
}

// Observe records a forwarded byte slice. It never modifies p and reports
// parse failures later through Result so metering cannot corrupt delivery.
func (o *Observer) Observe(p []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.observeLocked(p)
}

func (o *Observer) observeLocked(p []byte) {
	if o.finished || o.err != nil || o.provider == ProviderUnknown || len(p) == 0 {
		return
	}
	if len(p) > o.limits.MaxBytes-o.bytes {
		o.err = ErrLimit
		o.releaseLocked()
		return
	}
	o.bytes += len(p)
	switch o.kind {
	case streamJSON:
		o.jsonBody = append(o.jsonBody, p...)
	default:
		if err := o.parser.feed(p, &o.usage, o.provider); err != nil {
			o.err = err
			o.releaseLocked()
		}
	}
}

// Finish marks a successfully observed EOF and completes parsing.
func (o *Observer) Finish() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.finished {
		return
	}
	o.finished = true
	if o.err != nil || o.provider == ProviderUnknown {
		return
	}
	var err error
	if o.kind == streamJSON {
		err = consumeJSONBody(o.jsonBody, &o.usage, o.provider, o.limits)
	} else {
		err = o.parser.finish(&o.usage, o.provider)
	}
	o.releaseLocked()
	if err != nil {
		o.err = err
	}
}

func (o *Observer) markIncomplete() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.finished {
		return
	}
	o.finished = true
	o.err = ErrIncomplete
	o.releaseLocked()
}

func (o *Observer) releaseLocked() {
	clear(o.jsonBody)
	o.jsonBody = nil
	if o.parser != nil {
		o.parser.release()
	}
}

// Result returns a trustworthy usage record only after the wrapped body
// reached EOF. Errors contain no response payload or provider-supplied model.
func (o *Observer) Result() (Usage, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := Usage{Provider: o.provider, Streaming: o.kind != streamJSON}
	if o.provider == ProviderUnknown {
		result.UnmeteredReason = UnmeteredUnsupported
		return result, nil
	}
	if !o.finished {
		result.UnmeteredReason = UnmeteredIncomplete
		return result, ErrIncomplete
	}
	if o.err != nil {
		switch {
		case errors.Is(o.err, ErrLimit):
			result.UnmeteredReason = UnmeteredLimit
		case errors.Is(o.err, ErrIncomplete):
			result.UnmeteredReason = UnmeteredIncomplete
		default:
			result.UnmeteredReason = UnmeteredMalformed
		}
		return result, o.err
	}
	input, output, total, model, err := o.usage.result()
	if err != nil {
		if errors.Is(err, ErrUsageMissing) {
			result.UnmeteredReason = UnmeteredMissing
		} else {
			result.UnmeteredReason = UnmeteredMalformed
		}
		return result, err
	}
	result.Model = model
	result.InputTokens = input
	result.OutputTokens = output
	result.TotalTokens = total
	result.Metered = true
	return result, nil
}

// Wrap returns a byte-transparent response body. Parser failures never replace
// upstream read errors or stop forwarding bytes.
func (o *Observer) Wrap(body io.ReadCloser) io.ReadCloser {
	return &observedBody{body: body, observer: o}
}

type observedBody struct {
	body     io.ReadCloser
	observer *Observer
}

func (b *observedBody) Read(p []byte) (int, error) {
	n, err := b.body.Read(p)
	if n > 0 {
		b.observer.Observe(p[:n])
	}
	if err == io.EOF {
		b.observer.Finish()
	} else if err != nil {
		b.observer.markIncomplete()
	}
	return n, err
}

func (b *observedBody) Close() error {
	b.observer.markIncomplete()
	return b.body.Close()
}

// ProviderForHost maps only canonical model-provider API hosts. Unknown hosts
// remain request-only rather than being guessed from substrings.
func ProviderForHost(hostport string) Provider {
	host := strings.ToLower(strings.TrimSpace(hostport))
	if strings.HasPrefix(host, "[") {
		return ProviderUnknown
	}
	if i := strings.LastIndexByte(host, ':'); i >= 0 {
		if strings.Count(host, ":") != 1 {
			return ProviderUnknown
		}
		port, err := strconv.ParseUint(host[i+1:], 10, 16)
		if err != nil || port == 0 {
			return ProviderUnknown
		}
		host = host[:i]
	}
	host = strings.TrimSuffix(host, ".")
	switch host {
	case "api.openai.com":
		return ProviderOpenAI
	case "api.anthropic.com":
		return ProviderAnthropic
	case "openrouter.ai", "api.openrouter.ai":
		return ProviderOpenRouter
	case "generativelanguage.googleapis.com", "aiplatform.googleapis.com":
		return ProviderGoogle
	}
	if strings.HasSuffix(host, "-aiplatform.googleapis.com") {
		prefix := strings.TrimSuffix(host, "-aiplatform.googleapis.com")
		if validDNSLabel(prefix) {
			return ProviderGoogle
		}
	}
	return ProviderUnknown
}

func validDNSLabel(label string) bool {
	if len(label) < 1 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
		return false
	}
	for _, r := range label {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			continue
		}
		return false
	}
	return true
}
