package meter

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

func TestNonStreamingProviders(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		provider Provider
		body     string
		want     Usage
	}{
		{
			name:     "openai chat completions",
			provider: ProviderOpenAI,
			body:     `{"model":"gpt-5","usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18}}`,
			want:     Usage{Provider: ProviderOpenAI, Model: "gpt-5", InputTokens: 11, OutputTokens: 7, TotalTokens: 18, Metered: true},
		},
		{
			name:     "openai responses",
			provider: ProviderOpenAI,
			body:     `{"response":{"model":"gpt-5-mini","usage":{"input_tokens":12,"output_tokens":8,"total_tokens":20}}}`,
			want:     Usage{Provider: ProviderOpenAI, Model: "gpt-5-mini", InputTokens: 12, OutputTokens: 8, TotalTokens: 20, Metered: true},
		},
		{
			name:     "openai input only endpoint",
			provider: ProviderOpenAI,
			body:     `{"model":"text-embedding-3-small","usage":{"prompt_tokens":12,"total_tokens":12}}`,
			want:     Usage{Provider: ProviderOpenAI, Model: "text-embedding-3-small", InputTokens: 12, OutputTokens: 0, TotalTokens: 12, Metered: true},
		},
		{
			name:     "openrouter",
			provider: ProviderOpenRouter,
			body:     `{"model":"openai/gpt-5","usage":{"prompt_tokens":9,"completion_tokens":4,"total_tokens":13,"cost":0.1}}`,
			want:     Usage{Provider: ProviderOpenRouter, Model: "openai/gpt-5", InputTokens: 9, OutputTokens: 4, TotalTokens: 13, Metered: true},
		},
		{
			name:     "anthropic includes cached input",
			provider: ProviderAnthropic,
			body:     `{"model":"claude-sonnet-4-5","usage":{"input_tokens":10,"cache_creation_input_tokens":2,"cache_read_input_tokens":3,"output_tokens":7}}`,
			want:     Usage{Provider: ProviderAnthropic, Model: "claude-sonnet-4-5", InputTokens: 15, OutputTokens: 7, TotalTokens: 22, Metered: true},
		},
		{
			name:     "google",
			provider: ProviderGoogle,
			body:     `{"modelVersion":"gemini-2.5-pro","usageMetadata":{"promptTokenCount":21,"candidatesTokenCount":5,"totalTokenCount":29,"thoughtsTokenCount":3}}`,
			want:     Usage{Provider: ProviderGoogle, Model: "gemini-2.5-pro", InputTokens: 21, OutputTokens: 5, TotalTokens: 29, Metered: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := mustObserver(t, tt.provider, "application/json", Limits{})
			gotBytes, err := io.ReadAll(o.Wrap(io.NopCloser(strings.NewReader(tt.body))))
			if err != nil {
				t.Fatal(err)
			}
			if string(gotBytes) != tt.body {
				t.Fatalf("forwarded body changed: %q", gotBytes)
			}
			got, err := o.Result()
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("usage = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestStreamingProvidersAndArbitraryReadSplits(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		provider Provider
		ctype    string
		body     string
		want     Usage
	}{
		{
			name:     "openai sse",
			provider: ProviderOpenAI,
			ctype:    "text/event-stream; charset=utf-8",
			body:     ": keepalive\r\n\r\ndata: {\"model\":\"gpt-5\",\"choices\":[{\"delta\":{}}],\"usage\":null}\r\n\r\ndata: {\"model\":\"gpt-5\",\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":6,\"total_tokens\":10}}\r\n\r\ndata: [DONE]\r\n\r\n",
			want:     Usage{Provider: ProviderOpenAI, Model: "gpt-5", InputTokens: 4, OutputTokens: 6, TotalTokens: 10, Metered: true, Streaming: true},
		},
		{
			name:     "openai responses sse",
			provider: ProviderOpenAI,
			ctype:    "text/event-stream",
			body:     "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-5-mini\",\"usage\":{\"input_tokens\":5,\"output_tokens\":4,\"total_tokens\":9}}}\n\n",
			want:     Usage{Provider: ProviderOpenAI, Model: "gpt-5-mini", InputTokens: 5, OutputTokens: 4, TotalTokens: 9, Metered: true, Streaming: true},
		},
		{
			name:     "openrouter sse lone carriage returns",
			provider: ProviderOpenRouter,
			ctype:    "text/event-stream",
			body:     "data: {\"model\":\"openai/gpt-5\",\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3,\"total_tokens\":5}}\r\rdata: [DONE]\r\r",
			want:     Usage{Provider: ProviderOpenRouter, Model: "openai/gpt-5", InputTokens: 2, OutputTokens: 3, TotalTokens: 5, Metered: true, Streaming: true},
		},
		{
			name:     "anthropic sse partial usage",
			provider: ProviderAnthropic,
			ctype:    "text/event-stream",
			body:     "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-3-7-sonnet\",\"usage\":{\"input_tokens\":8,\"cache_read_input_tokens\":2,\"output_tokens\":0}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":7}}\n\n",
			want:     Usage{Provider: ProviderAnthropic, Model: "claude-3-7-sonnet", InputTokens: 10, OutputTokens: 7, TotalTokens: 17, Metered: true, Streaming: true},
		},
		{
			name:     "google sse",
			provider: ProviderGoogle,
			ctype:    "text/event-stream",
			body:     "data: {\"modelVersion\":\"gemini-2.5-flash\",\"candidates\":[]}\n\ndata: {\"modelVersion\":\"gemini-2.5-flash\",\"usageMetadata\":{\"promptTokenCount\":3,\"candidatesTokenCount\":2,\"totalTokenCount\":5}}\n\n",
			want:     Usage{Provider: ProviderGoogle, Model: "gemini-2.5-flash", InputTokens: 3, OutputTokens: 2, TotalTokens: 5, Metered: true, Streaming: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := mustObserver(t, tt.provider, tt.ctype, Limits{})
			source := &oneByteReadCloser{reader: strings.NewReader(tt.body)}
			gotBytes, err := io.ReadAll(o.Wrap(source))
			if err != nil {
				t.Fatal(err)
			}
			if string(gotBytes) != tt.body {
				t.Fatal("observer changed streaming bytes")
			}
			got, err := o.Result()
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("usage = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestGoogleStreamingJSONArray(t *testing.T) {
	t.Parallel()
	body := `[{"modelVersion":"gemini-2.0-flash","candidates":[]},{"modelVersion":"gemini-2.0-flash","usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":1,"totalTokenCount":3}}]`
	o := mustObserver(t, ProviderGoogle, "application/json", Limits{})
	_, _ = io.Copy(io.Discard, o.Wrap(io.NopCloser(strings.NewReader(body))))
	got, err := o.Result()
	if err != nil || !got.Metered || got.TotalTokens != 3 {
		t.Fatalf("Result() = %+v, %v", got, err)
	}
}

func TestJSONEventLimits(t *testing.T) {
	t.Parallel()
	t.Run("array event count", func(t *testing.T) {
		body := `[{"modelVersion":"gemini","candidates":[]},{"modelVersion":"gemini","candidates":[]},{"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}]`
		o := mustObserver(t, ProviderGoogle, "application/json", Limits{MaxBytes: 512, MaxEventBytes: 256, MaxEvents: 2})
		o.Observe([]byte(body))
		o.Finish()
		got, err := o.Result()
		if !errors.Is(err, ErrLimit) || got.UnmeteredReason != UnmeteredLimit {
			t.Fatalf("Result() = %+v, %v", got, err)
		}
	})
	t.Run("array item bytes", func(t *testing.T) {
		body := `[{"padding":"` + strings.Repeat("x", 80) + `"},{"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}]`
		o := mustObserver(t, ProviderGoogle, "application/json", Limits{MaxBytes: 512, MaxEventBytes: 64, MaxEvents: 3})
		o.Observe([]byte(body))
		o.Finish()
		if _, err := o.Result(); !errors.Is(err, ErrLimit) {
			t.Fatalf("Result() error = %v", err)
		}
	})
	t.Run("single event bytes", func(t *testing.T) {
		body := `{"padding":"` + strings.Repeat("x", 80) + `","usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`
		o := mustObserver(t, ProviderOpenAI, "application/json", Limits{MaxBytes: 512, MaxEventBytes: 64, MaxEvents: 1})
		o.Observe([]byte(body))
		o.Finish()
		if _, err := o.Result(); !errors.Is(err, ErrLimit) {
			t.Fatalf("Result() error = %v", err)
		}
	})
}

func TestUnknownProviderIsExplicitlyRequestOnly(t *testing.T) {
	t.Parallel()
	o := mustObserver(t, Provider("other"), "application/json", Limits{MaxBytes: 1, MaxEventBytes: 1, MaxEvents: 1})
	o.Observe(bytes.Repeat([]byte("secret"), 100))
	o.Finish()
	got, err := o.Result()
	if err != nil {
		t.Fatal(err)
	}
	if got.Provider != ProviderUnknown || got.Metered || got.UnmeteredReason != UnmeteredUnsupported {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestIncompleteAndLimitsAreDistinct(t *testing.T) {
	t.Parallel()
	t.Run("close before eof", func(t *testing.T) {
		o := mustObserver(t, ProviderOpenAI, "application/json", Limits{})
		body := o.Wrap(io.NopCloser(strings.NewReader(`{"usage":{"prompt_tokens":1,"completion_tokens":1}}`)))
		buf := make([]byte, 1)
		if _, err := body.Read(buf); err != nil {
			t.Fatal(err)
		}
		if err := body.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := o.Result()
		if !errors.Is(err, ErrIncomplete) || got.UnmeteredReason != UnmeteredIncomplete {
			t.Fatalf("Result() = %+v, %v", got, err)
		}
	})
	t.Run("upstream read error", func(t *testing.T) {
		upstreamErr := errors.New("upstream transport failed")
		o := mustObserver(t, ProviderOpenAI, "application/json", Limits{})
		body := o.Wrap(&errorReadCloser{err: upstreamErr})
		if _, err := io.ReadAll(body); !errors.Is(err, upstreamErr) {
			t.Fatalf("ReadAll error = %v", err)
		}
		got, err := o.Result()
		if !errors.Is(err, ErrIncomplete) || got.UnmeteredReason != UnmeteredIncomplete {
			t.Fatalf("Result() = %+v, %v", got, err)
		}
	})
	t.Run("body limit still forwards", func(t *testing.T) {
		body := strings.Repeat("x", 65)
		o := mustObserver(t, ProviderOpenAI, "application/json", Limits{MaxBytes: 64, MaxEventBytes: 32, MaxEvents: 2})
		forwarded, err := io.ReadAll(o.Wrap(io.NopCloser(strings.NewReader(body))))
		if err != nil || string(forwarded) != body {
			t.Fatalf("forwarded=%d err=%v", len(forwarded), err)
		}
		got, err := o.Result()
		if !errors.Is(err, ErrLimit) || got.UnmeteredReason != UnmeteredLimit {
			t.Fatalf("Result() = %+v, %v", got, err)
		}
	})
	t.Run("event byte limit", func(t *testing.T) {
		o := mustObserver(t, ProviderOpenAI, "text/event-stream", Limits{MaxBytes: 128, MaxEventBytes: 16, MaxEvents: 4})
		o.Observe([]byte("data: 0123456789012345\n\n"))
		o.Finish()
		got, err := o.Result()
		if !errors.Is(err, ErrLimit) || got.UnmeteredReason != UnmeteredLimit {
			t.Fatalf("Result() = %+v, %v", got, err)
		}
	})
	t.Run("comment events count", func(t *testing.T) {
		o := mustObserver(t, ProviderOpenAI, "text/event-stream", Limits{MaxBytes: 128, MaxEventBytes: 32, MaxEvents: 2})
		o.Observe([]byte(": a\n\n: b\n\n: c\n\n"))
		o.Finish()
		if _, err := o.Result(); !errors.Is(err, ErrLimit) {
			t.Fatalf("Result() error = %v", err)
		}
	})
}

func TestLargeReadWithSmallEventsDoesNotTripEventByteLimit(t *testing.T) {
	t.Parallel()
	var body strings.Builder
	for range 20 {
		body.WriteString(": keepalive\n\n")
	}
	body.WriteString("data: {\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n")
	o := mustObserver(t, ProviderOpenAI, "text/event-stream", Limits{MaxBytes: 1024, MaxEventBytes: 128, MaxEvents: 32})
	// One large Observe call proves the retention limit applies per event, not
	// to the transport's arbitrary read size.
	o.Observe([]byte(body.String()))
	o.Finish()
	got, err := o.Result()
	if err != nil || got.TotalTokens != 3 {
		t.Fatalf("Result() = %+v, %v", got, err)
	}
}

func TestMalformedTerminalEventInvalidatesPartialUsage(t *testing.T) {
	t.Parallel()
	canary := "RESPONSE_SECRET_CANARY"
	body := "data: {\"model\":\"gpt-5\",\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\ndata: {\"" + canary + "\"\n\n"
	o := mustObserver(t, ProviderOpenAI, "text/event-stream", Limits{})
	forwarded, readErr := io.ReadAll(o.Wrap(io.NopCloser(strings.NewReader(body))))
	if readErr != nil || string(forwarded) != body {
		t.Fatalf("forwarded body changed: %v", readErr)
	}
	got, err := o.Result()
	if !errors.Is(err, ErrMalformed) || got.Metered || got.UnmeteredReason != UnmeteredMalformed {
		t.Fatalf("Result() = %+v, %v", got, err)
	}
	if strings.Contains(err.Error(), canary) {
		t.Fatalf("error exposed response content: %v", err)
	}
}

func TestMalformedAndMissingNeverReturnPartialUsage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		body   string
		want   error
		reason UnmeteredReason
	}{
		{"malformed token", `{"model":"CANARY_MODEL","usage":{"prompt_tokens":1,"completion_tokens":-2,"total_tokens":3}}`, ErrMalformed, UnmeteredMalformed},
		{"trailing data", `{"usage":{"prompt_tokens":1,"completion_tokens":2}} CANARY_TRAILER`, ErrMalformed, UnmeteredMalformed},
		{"missing usage", `{"model":"CANARY_MODEL","choices":[]}`, ErrUsageMissing, UnmeteredMissing},
		{"incomplete usage", `{"usage":{"prompt_tokens":5}}`, ErrMalformed, UnmeteredMalformed},
		{"int64 overflow", `{"usage":{"prompt_tokens":9223372036854775808,"completion_tokens":0,"total_tokens":9223372036854775808}}`, ErrMalformed, UnmeteredMalformed},
		{"computed int64 overflow", `{"usage":{"prompt_tokens":9223372036854775807,"completion_tokens":1}}`, ErrMalformed, UnmeteredMalformed},
		{"inconsistent total", `{"usage":{"prompt_tokens":5,"completion_tokens":6,"total_tokens":2}}`, ErrMalformed, UnmeteredMalformed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := mustObserver(t, ProviderOpenAI, "application/json", Limits{})
			o.Observe([]byte(tt.body))
			o.Finish()
			got, err := o.Result()
			if !errors.Is(err, tt.want) || got.Metered || got.UnmeteredReason != tt.reason {
				t.Fatalf("Result() = %+v, %v", got, err)
			}
			if strings.Contains(err.Error(), "CANARY") {
				t.Fatalf("error exposed response content: %v", err)
			}
		})
	}
}

func TestModelExtractionIsConservative(t *testing.T) {
	t.Parallel()
	body := "data: {\"model\":\"valid-model\"}\n\ndata: {\"model\":\"changed-model\",\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n"
	o := mustObserver(t, ProviderOpenAI, "text/event-stream", Limits{})
	o.Observe([]byte(body))
	o.Finish()
	got, err := o.Result()
	if err != nil {
		t.Fatal(err)
	}
	if got.Model != "" {
		t.Fatalf("conflicting model was accepted: %q", got.Model)
	}
}

func TestProviderForHost(t *testing.T) {
	t.Parallel()
	tests := map[string]Provider{
		"api.openai.com":                          ProviderOpenAI,
		"API.ANTHROPIC.COM.:443":                  ProviderAnthropic,
		"openrouter.ai":                           ProviderOpenRouter,
		"us-central1-aiplatform.googleapis.com":   ProviderGoogle,
		"generativelanguage.googleapis.com":       ProviderGoogle,
		"api.openai.com.attacker.example":         ProviderUnknown,
		"attacker-aiplatform.googleapis.com.evil": ProviderUnknown,
		"api.openai.com:notaport":                 ProviderUnknown,
		"[::1]:443":                               ProviderUnknown,
		"":                                        ProviderUnknown,
	}
	for host, want := range tests {
		if got := ProviderForHost(host); got != want {
			t.Errorf("ProviderForHost(%q) = %q, want %q", host, got, want)
		}
	}
}

func TestConcurrentResultObservation(t *testing.T) {
	t.Parallel()
	body := `{"model":"gpt-5","usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`
	o := mustObserver(t, ProviderOpenAI, "application/json", Limits{})
	wrapped := o.Wrap(&oneByteReadCloser{reader: strings.NewReader(body)})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				_, _ = o.Result()
			}
		}()
	}
	forwarded, err := io.ReadAll(wrapped)
	if err != nil || string(forwarded) != body {
		t.Fatalf("ReadAll = %q, %v", forwarded, err)
	}
	wg.Wait()
	got, err := o.Result()
	if err != nil || got.TotalTokens != 30 {
		t.Fatalf("Result() = %+v, %v", got, err)
	}
}

func TestConcurrentReadCloseIsIncomplete(t *testing.T) {
	t.Parallel()
	source := newConcurrentCloseBody([]byte(`{"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`))
	o := mustObserver(t, ProviderOpenAI, "application/json", Limits{})
	body := o.Wrap(source)
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(body)
		readDone <- err
	}()
	<-source.started
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	got, err := o.Result()
	if !errors.Is(err, ErrIncomplete) || got.Metered || got.UnmeteredReason != UnmeteredIncomplete {
		t.Fatalf("Result() = %+v, %v", got, err)
	}
}

func TestLimitsValidation(t *testing.T) {
	t.Parallel()
	bad := []Limits{
		{MaxBytes: -1},
		{MaxBytes: 4, MaxEventBytes: 5, MaxEvents: 1},
		{MaxBytes: maximumMaxBytes + 1, MaxEventBytes: 1, MaxEvents: 1},
		{MaxBytes: 1, MaxEventBytes: 1, MaxEvents: maximumMaxEvents + 1},
	}
	for _, limits := range bad {
		if _, err := NewObserver(ProviderOpenAI, "application/json", limits); !errors.Is(err, ErrLimit) {
			t.Errorf("NewObserver(%+v) error = %v", limits, err)
		}
	}
}

func mustObserver(t *testing.T, provider Provider, contentType string, limits Limits) *Observer {
	t.Helper()
	o, err := NewObserver(provider, contentType, limits)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

type oneByteReadCloser struct {
	reader *strings.Reader
}

func (r *oneByteReadCloser) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.reader.Read(p)
}

func (r *oneByteReadCloser) Close() error { return nil }

type errorReadCloser struct {
	err error
}

func (r *errorReadCloser) Read([]byte) (int, error) { return 0, r.err }
func (r *errorReadCloser) Close() error             { return nil }

type concurrentCloseBody struct {
	data    []byte
	started chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func newConcurrentCloseBody(data []byte) *concurrentCloseBody {
	return &concurrentCloseBody{data: data, started: make(chan struct{}), closed: make(chan struct{})}
}

func (b *concurrentCloseBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.closed
	return copy(p, b.data), io.EOF
}

func (b *concurrentCloseBody) Close() error {
	close(b.closed)
	return nil
}
