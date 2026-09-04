package modelfake

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// chatReq is the subset of the Chat Completions request the lane depends on.
type chatReq struct {
	Model    string            `json:"model"`
	Stream   bool              `json:"stream"`
	Messages []json.RawMessage `json:"messages"`
	Tools    []struct {
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	} `json:"tools"`
}

// chat answers the older Chat Completions shape from the same script, so a
// harness pinned to either API proves the same broker path.
func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	if !s.enter(w, r) {
		return
	}
	defer s.exit()
	if r.Method != http.MethodPost {
		s.record(r, "", nil, http.StatusMethodNotAllowed, -1)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "modelfake accepts POST")
		return
	}
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	if _, ok := s.authorize(w, r, body); !ok {
		return
	}
	var req chatReq
	if err := json.Unmarshal(body, &req); err != nil {
		s.record(r, "", body, http.StatusBadRequest, -1)
		writeError(w, http.StatusBadRequest, "invalid_request", "modelfake could not decode the request")
		return
	}
	if req.Model != s.opts.Model {
		s.record(r, req.Model, body, http.StatusOK, -1)
		s.writeChat(w, req.Stream, Step{Text: s.opts.Aux}, "aux")
		return
	}
	idx, err := s.step(req.Messages)
	if err != nil {
		s.record(r, req.Model, body, http.StatusConflict, idx)
		writeError(w, http.StatusConflict, "script_exhausted", "modelfake: "+err.Error())
		return
	}
	names := make([]string, 0, len(req.Tools))
	for _, tool := range req.Tools {
		names = append(names, tool.Function.Name)
	}
	if err := checkTools(names, s.opts.Script[idx]); err != nil {
		s.record(r, req.Model, body, http.StatusConflict, idx)
		writeError(w, http.StatusConflict, "unknown_tool", "modelfake: "+err.Error())
		return
	}
	s.record(r, req.Model, body, http.StatusOK, idx)
	s.writeChat(w, req.Stream, s.opts.Script[idx], fmt.Sprintf("%d", idx))
}

func (s *Server) writeChat(w http.ResponseWriter, stream bool, step Step, stem string) {
	toolCalls := make([]any, 0, len(step.Calls))
	for i, call := range step.Calls {
		arguments := "{}"
		if call.Arguments != nil {
			if encoded, err := json.Marshal(call.Arguments); err == nil {
				arguments = string(encoded)
			}
		}
		toolCalls = append(toolCalls, map[string]any{
			"index": i, "id": fmt.Sprintf("call_%s_%d", stem, i), "type": "function",
			"function": map[string]any{"name": call.Name, "arguments": arguments},
		})
	}
	finish := "stop"
	if len(toolCalls) > 0 {
		finish = "tool_calls"
	}
	message := map[string]any{"role": "assistant", "content": nil}
	if step.Text != "" {
		message["content"] = step.Text
	}
	if len(toolCalls) > 0 {
		message["tool_calls"] = toolCalls
	}
	envelope := func(key string, payload map[string]any, done bool) map[string]any {
		choice := map[string]any{"index": 0, key: payload, "finish_reason": nil, "logprobs": nil}
		if done {
			choice["finish_reason"] = finish
		}
		object := "chat.completion"
		if key == "delta" {
			object = "chat.completion.chunk"
		}
		out := map[string]any{
			"id": "chatcmpl_" + stem, "object": object, "created": createdAt,
			"model": s.opts.Model, "system_fingerprint": systemFP, "choices": []any{choice},
		}
		if !done && key == "delta" {
			return out
		}
		out["usage"] = map[string]any{"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120}
		return out
	}
	if !stream {
		writeJSON(w, http.StatusOK, envelope("message", message, true))
		return
	}
	flusher, canFlush := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	send := func(payload map[string]any) {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return
		}
		_, _ = fmt.Fprintf(w, "data: %s\n\n", encoded)
		if canFlush {
			flusher.Flush()
		}
	}
	send(envelope("delta", map[string]any{"role": "assistant", "content": ""}, false))
	for _, chunk := range chunks(step.Text) {
		send(envelope("delta", map[string]any{"content": chunk}, false))
	}
	if len(toolCalls) > 0 {
		send(envelope("delta", map[string]any{"tool_calls": toolCalls}, false))
	}
	send(envelope("delta", map[string]any{}, true))
	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	if canFlush {
		flusher.Flush()
	}
}
