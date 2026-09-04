package modelfake_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"remount.dev/remount/internal/modelfake"
)

const bearer = "sk-modelfake-upstream-000000000000000000"

func script() []modelfake.Step {
	return []modelfake.Step{
		{Calls: []modelfake.Call{{Name: "write", Arguments: map[string]any{"filePath": "/work/GREETING.txt", "content": "hello\n"}}}},
		{Calls: []modelfake.Call{{Name: "bash", Arguments: map[string]any{"command": "echo ok"}}}},
		{Text: "done"},
	}
}

func newServer(t *testing.T, adjust func(*modelfake.Options)) (*modelfake.Server, *httptest.Server) {
	t.Helper()
	opts := modelfake.Options{Bearer: bearer, Model: "m", Script: script()}
	if adjust != nil {
		adjust(&opts)
	}
	s, err := modelfake.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return s, srv
}

// post sends body to path with the synthetic bearer unless auth overrides it.
func post(t *testing.T, srv *httptest.Server, path, auth string, body any) (*http.Response, []byte) {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	res, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	out, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res, out
}

// turn is one Responses request carrying results tool outputs.
func turn(model string, results int, stream bool) map[string]any {
	input := []any{map[string]any{"role": "user", "content": "go"}}
	for i := 0; i < results; i++ {
		input = append(input, map[string]any{
			"type": "function_call_output", "call_id": fmt.Sprintf("call_%d", i), "output": "ok",
		})
	}
	return map[string]any{"model": model, "stream": stream, "input": input}
}

// TestNewRefusesAScriptThatWouldProveNothing keeps the lane honest at
// construction: a server without a bearer would let a broker-less run pass.
func TestNewRefusesAScriptThatWouldProveNothing(t *testing.T) {
	cases := map[string]modelfake.Options{
		"no bearer":    {Model: "m", Script: script()},
		"no model":     {Bearer: bearer, Script: script()},
		"no script":    {Bearer: bearer, Model: "m"},
		"empty step":   {Bearer: bearer, Model: "m", Script: []modelfake.Step{{}}},
		"unnamed call": {Bearer: bearer, Model: "m", Script: []modelfake.Step{{Calls: []modelfake.Call{{}}}}},
		"valid":        {Bearer: bearer, Model: "m", Script: script()},
	}
	for name, opts := range cases {
		_, err := modelfake.New(opts)
		if (err == nil) != (name == "valid") {
			t.Fatalf("%s: err = %v", name, err)
		}
	}
}

// TestRequestWithoutTheSyntheticBearerIsRefused is the property that makes the
// deterministic lane evidence: a run that skipped the broker cannot pass.
func TestRequestWithoutTheSyntheticBearerIsRefused(t *testing.T) {
	s, srv := newServer(t, nil)
	for _, auth := range []string{"", "Bearer ref:b_openai", "Bearer " + bearer + "x", "Basic " + bearer} {
		res, body := post(t, srv, "/v1/responses", auth, turn("m", 0, false))
		if res.StatusCode != http.StatusUnauthorized {
			t.Fatalf("authorization %q = %d, want 401 (%s)", auth, res.StatusCode, body)
		}
	}
	if got := s.Count(); got != 4 {
		t.Fatalf("refused requests recorded = %d, want 4", got)
	}
	// A refusal is still inspectable: the test can prove what arrived.
	if !s.Saw("ref:b_openai") {
		t.Fatal("the server did not record the placeholder it refused")
	}
	res, _ := post(t, srv, "/v1/responses", "Bearer "+bearer, turn("m", 0, false))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("authorized request = %d", res.StatusCode)
	}
}

// TestAnswerIsAPureFunctionOfTheConversation is why a reattach or a retry
// replays rather than advances: the script index comes from the request.
func TestAnswerIsAPureFunctionOfTheConversation(t *testing.T) {
	_, srv := newServer(t, nil)
	first := map[string][]byte{}
	for _, results := range []int{0, 1, 2} {
		_, a := post(t, srv, "/v1/responses", "Bearer "+bearer, turn("m", results, true))
		_, b := post(t, srv, "/v1/responses", "Bearer "+bearer, turn("m", results, true))
		if !bytes.Equal(a, b) {
			t.Fatalf("results=%d: a replayed request answered differently", results)
		}
		first[fmt.Sprint(results)] = a
	}
	if bytes.Equal(first["0"], first["1"]) || bytes.Equal(first["1"], first["2"]) {
		t.Fatal("distinct conversations answered identically")
	}
	if !bytes.Contains(first["0"], []byte(`"name":"write"`)) ||
		!bytes.Contains(first["1"], []byte(`"name":"bash"`)) ||
		!bytes.Contains(first["2"], []byte("done")) {
		t.Fatal("the script steps did not answer in order")
	}
}

// TestStreamCarriesTheToolCallInResponsesOrder checks the event sequence a
// harness's streaming assembly actually depends on.
func TestStreamCarriesTheToolCallInResponsesOrder(t *testing.T) {
	_, srv := newServer(t, nil)
	res, body := post(t, srv, "/v1/responses", "Bearer "+bearer, turn("m", 0, true))
	if ct := res.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}
	var types []string
	var arguments strings.Builder
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event struct {
			Type  string `json:"type"`
			Delta string `json:"delta"`
			Seq   *int   `json:"sequence_number"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatalf("undecodable event %q: %v", line, err)
		}
		if event.Seq == nil {
			t.Fatalf("event %s has no sequence number", event.Type)
		}
		if *event.Seq != len(types) {
			t.Fatalf("event %s sequence = %d, want %d", event.Type, *event.Seq, len(types))
		}
		types = append(types, event.Type)
		if event.Type == "response.function_call_arguments.delta" {
			arguments.WriteString(event.Delta)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"response.created", "response.in_progress", "response.output_item.added",
		"response.function_call_arguments.delta",
	}
	for i, typ := range want {
		if i >= len(types) || types[i] != typ {
			t.Fatalf("event %d = %q, want %q (%v)", i, types[min(i, len(types)-1)], typ, types)
		}
	}
	if types[len(types)-1] != "response.completed" || types[len(types)-2] != "response.output_item.done" {
		t.Fatalf("stream did not close in order: %v", types)
	}
	// Deltas reassemble to the exact scripted arguments, key order included.
	var decoded map[string]any
	if err := json.Unmarshal([]byte(arguments.String()), &decoded); err != nil {
		t.Fatalf("argument deltas do not reassemble: %v (%q)", err, arguments.String())
	}
	if decoded["filePath"] != "/work/GREETING.txt" || decoded["content"] != "hello\n" {
		t.Fatalf("arguments = %v", decoded)
	}
}

// TestAuxModelNeverConsumesAScriptStep protects the script from a harness's
// own side calls, such as OpenCode's separate title generator.
func TestAuxModelNeverConsumesAScriptStep(t *testing.T) {
	s, srv := newServer(t, func(o *modelfake.Options) { o.Aux = "Side Call" })
	for i := 0; i < 3; i++ {
		_, body := post(t, srv, "/v1/responses", "Bearer "+bearer, turn("other", 0, false))
		if !bytes.Contains(body, []byte("Side Call")) {
			t.Fatalf("aux answer = %s", body)
		}
	}
	_, body := post(t, srv, "/v1/responses", "Bearer "+bearer, turn("m", 0, false))
	if !bytes.Contains(body, []byte(`"name":"write"`)) {
		t.Fatalf("the script did not start at step 0: %s", body)
	}
	for _, r := range s.Requests() {
		if r.Model == "other" && r.Step != -1 {
			t.Fatalf("aux request resolved to script step %d", r.Step)
		}
	}
}

// TestRunningPastTheScriptIsAnError: a looping harness must end the test with
// a legible reason, never with an empty turn that reads as success.
func TestRunningPastTheScriptIsAnError(t *testing.T) {
	_, srv := newServer(t, nil)
	res, body := post(t, srv, "/v1/responses", "Bearer "+bearer, turn("m", len(script()), false))
	if res.StatusCode != http.StatusConflict || !bytes.Contains(body, []byte("script_exhausted")) {
		t.Fatalf("exhausted script = %d %s", res.StatusCode, body)
	}
}

// TestScriptNamingAnUndeclaredToolIsAnError keeps a stale script legible: a
// newer harness that renamed a tool must fail here, not silently drop the turn.
func TestScriptNamingAnUndeclaredToolIsAnError(t *testing.T) {
	_, srv := newServer(t, nil)
	request := turn("m", 0, false)
	request["tools"] = []any{map[string]any{"type": "function", "name": "edit"}}
	res, body := post(t, srv, "/v1/responses", "Bearer "+bearer, request)
	if res.StatusCode != http.StatusConflict || !bytes.Contains(body, []byte("unknown_tool")) {
		t.Fatalf("undeclared tool = %d %s", res.StatusCode, body)
	}
	request["tools"] = []any{map[string]any{"type": "function", "name": "write"}}
	if res, _ := post(t, srv, "/v1/responses", "Bearer "+bearer, request); res.StatusCode != http.StatusOK {
		t.Fatalf("declared tool = %d", res.StatusCode)
	}
	// A request that declares no tools is a plain completion and is not checked.
	if res, _ := post(t, srv, "/v1/responses", "Bearer "+bearer, turn("m", 0, false)); res.StatusCode != http.StatusOK {
		t.Fatalf("toolless request = %d", res.StatusCode)
	}
}

// TestBoundsStopARunawayHarness proves the caps are enforced rather than
// documented.
func TestBoundsStopARunawayHarness(t *testing.T) {
	_, srv := newServer(t, func(o *modelfake.Options) { o.MaxBodyBytes = 512; o.MaxRequests = 3 })
	big := turn("m", 0, false)
	big["padding"] = strings.Repeat("x", 4096)
	res, _ := post(t, srv, "/v1/responses", "Bearer "+bearer, big)
	if res.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body = %d", res.StatusCode)
	}
	var last int
	for i := 0; i < 5; i++ {
		res, _ := post(t, srv, "/v1/responses", "Bearer "+bearer, turn("m", 0, false))
		last = res.StatusCode
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("request bound not enforced: last = %d", last)
	}
}

// TestChatCompletionsFollowsTheSameScript keeps a harness pinned to the older
// API on the same proof.
func TestChatCompletionsFollowsTheSameScript(t *testing.T) {
	_, srv := newServer(t, nil)
	messages := []any{map[string]any{"role": "user", "content": "go"}}
	_, body := post(t, srv, "/v1/chat/completions", "Bearer "+bearer, map[string]any{"model": "m", "messages": messages})
	var completion struct {
		Choices []struct {
			FinishReason string `json:"finish_reason"`
			Message      struct {
				ToolCalls []struct {
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &completion); err != nil {
		t.Fatalf("%v: %s", err, body)
	}
	if len(completion.Choices) != 1 || completion.Choices[0].FinishReason != "tool_calls" {
		t.Fatalf("completion = %s", body)
	}
	if completion.Choices[0].Message.ToolCalls[0].Function.Name != "write" {
		t.Fatalf("tool call = %s", body)
	}
	messages = append(messages, map[string]any{"role": "tool", "content": "ok"}, map[string]any{"role": "tool", "content": "ok"})
	res, streamed := post(t, srv, "/v1/chat/completions", "Bearer "+bearer, map[string]any{"model": "m", "messages": messages, "stream": true})
	if res.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("stream content-type = %q", res.Header.Get("Content-Type"))
	}
	if !bytes.Contains(streamed, []byte("done")) || !bytes.HasSuffix(streamed, []byte("data: [DONE]\n\n")) {
		t.Fatalf("stream = %s", streamed)
	}
}

// TestModelsListsOnlyTheScriptedModel: the lane replaces external metadata
// discovery, so the catalog is local and fixed.
func TestModelsListsOnlyTheScriptedModel(t *testing.T) {
	_, srv := newServer(t, nil)
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/models", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	res, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"id":"m"`)) {
		t.Fatalf("models = %d %s", res.StatusCode, body)
	}
}

// TestUnknownPathIsRecordedAndRefused keeps a harness's unexpected call
// visible instead of silently 404-ing outside the record.
func TestUnknownPathIsRecordedAndRefused(t *testing.T) {
	s, srv := newServer(t, nil)
	res, _ := post(t, srv, "/v1/embeddings", "Bearer "+bearer, map[string]any{})
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown path = %d", res.StatusCode)
	}
	requests := s.Requests()
	if len(requests) != 1 || requests[0].Path != "/v1/embeddings" {
		t.Fatalf("requests = %+v", requests)
	}
}

// TestConcurrentRequestsAreRecordedExactlyOnce guards the record under -race.
func TestConcurrentRequestsAreRecordedExactlyOnce(t *testing.T) {
	s, srv := newServer(t, func(o *modelfake.Options) { o.MaxRequests = 32; o.MaxConcurrent = 8 })
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			post(t, srv, "/v1/responses", "Bearer "+bearer, turn("m", 0, false))
		}()
	}
	wg.Wait()
	if got := s.Count(); got != 16 {
		t.Fatalf("recorded %d requests, want 16", got)
	}
}
