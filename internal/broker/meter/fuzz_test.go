package meter

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func FuzzObserver(f *testing.F) {
	f.Add([]byte(`{"model":"gpt-5","usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`), uint8(0), uint8(7))
	f.Add([]byte("data: {\"usage\":{\"input_tokens\":1,\"output_tokens\":2}}\n\n"), uint8(1), uint8(1))
	f.Add([]byte(": comment\n\ndata: [DONE]\n\n"), uint8(4), uint8(3))
	f.Fuzz(func(t *testing.T, data []byte, selector, chunkSize uint8) {
		if len(data) > 8<<10 {
			t.Skip()
		}
		providers := [...]Provider{ProviderOpenAI, ProviderAnthropic, ProviderGoogle, ProviderOpenRouter, ProviderUnknown}
		contentTypes := [...]string{"application/json", "text/event-stream", "application/x-ndjson"}
		provider := providers[int(selector)%len(providers)]
		contentType := contentTypes[int(selector)%len(contentTypes)]
		o, err := NewObserver(provider, contentType, Limits{MaxBytes: 4096, MaxEventBytes: 256, MaxEvents: 32})
		if err != nil {
			t.Fatal(err)
		}
		chunk := int(chunkSize%64) + 1
		for start := 0; start < len(data); start += chunk {
			end := min(start+chunk, len(data))
			o.Observe(data[start:end])
		}
		o.Finish()
		got, resultErr := o.Result()
		if got.Metered {
			if resultErr != nil || got.UnmeteredReason != "" || got.TotalTokens < got.InputTokens || got.TotalTokens-got.InputTokens < got.OutputTokens {
				t.Fatalf("invalid metered result: %+v, %v", got, resultErr)
			}
		} else if provider != ProviderUnknown && resultErr == nil {
			t.Fatalf("known provider was silently unmetered: %+v", got)
		}
		if resultErr != nil && !errors.Is(resultErr, ErrIncomplete) && !errors.Is(resultErr, ErrLimit) && !errors.Is(resultErr, ErrMalformed) && !errors.Is(resultErr, ErrUsageMissing) {
			t.Fatalf("unexpected error: %v", resultErr)
		}

		// The wrapping contract is byte-transparent even when parsing fails.
		o2, err := NewObserver(provider, contentType, Limits{MaxBytes: 4096, MaxEventBytes: 256, MaxEvents: 32})
		if err != nil {
			t.Fatal(err)
		}
		forwarded, readErr := io.ReadAll(o2.Wrap(io.NopCloser(bytes.NewReader(data))))
		if readErr != nil || !bytes.Equal(forwarded, data) {
			t.Fatalf("observer changed bytes: %v", readErr)
		}
	})
}
