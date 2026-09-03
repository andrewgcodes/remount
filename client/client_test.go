package client

import (
	"errors"
	"testing"

	"remount.dev/remount/api"
	"remount.dev/remount/internal/proto"
)

func TestNormalizeLinkURL(t *testing.T) {
	tests := map[string]string{
		"":                               "ws://127.0.0.1:7443/v1/link",
		"http://example.test":            "ws://example.test/v1/link",
		"https://example.test/base/":     "wss://example.test/base/v1/link",
		"wss://example.test/v1/link?q=x": "wss://example.test/v1/link?q=x",
	}
	for input, want := range tests {
		got, err := normalizeLinkURL(input)
		if err != nil {
			t.Fatalf("normalize %q: %v", input, err)
		}
		if got != want {
			t.Errorf("normalize %q = %q, want %q", input, got, want)
		}
	}
	for _, input := range []string{"example.test", "ftp://example.test", "http://user@example.test", "http://example.test/#fragment"} {
		if _, err := normalizeLinkURL(input); err == nil {
			t.Errorf("normalize %q succeeded", input)
		}
	}
}

func TestNewRejectsNegativeLimits(t *testing.T) {
	if _, err := New(Options{Retries: -1}); err == nil {
		t.Fatal("negative retries accepted")
	}
	if _, err := New(Options{MaxReadBytes: -1}); err == nil {
		t.Fatal("negative read limit accepted")
	}
	if _, err := New(Options{MaxRunOutputBytes: -1}); err == nil {
		t.Fatal("negative run-output limit accepted")
	}
}

func TestPublicErrorTaxonomy(t *testing.T) {
	err := errors.Join(errors.New("transport context"), &api.Error{Code: api.CodeResourceExhausted})
	if got := api.ErrorCode(err); got != api.CodeResourceExhausted || !api.IsErrorCode(err, api.CodeResourceExhausted) {
		t.Fatalf("code=%q match=%v", got, api.IsErrorCode(err, api.CodeResourceExhausted))
	}
}

func TestOutputGapIsNeverReportedAsComplete(t *testing.T) {
	gap, err := parseOutputGap(proto.MustMarshal(api.Gap{From: 4, To: 9}))
	if err != nil || gap.From != 4 || gap.To != 9 {
		t.Fatalf("gap = %+v, parse error = %v", gap, err)
	}
	if !errors.Is(outputGapEvictedError(gap), &api.Error{Code: api.CodeEvicted}) {
		t.Fatal("valid output gap did not produce a typed eviction error")
	}
	if _, err := parseOutputGap(proto.MustMarshal(api.Gap{From: 9, To: 4})); err == nil {
		t.Fatal("reversed output gap was accepted")
	}
}
