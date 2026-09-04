package modelfake

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// responsesReq is the subset of the Responses API request the lane depends on.
type responsesReq struct {
	Model  string            `json:"model"`
	Stream bool              `json:"stream"`
	Input  []json.RawMessage `json:"input"`
	Tools  []struct {
		Name string `json:"name"`
	} `json:"tools"`
}

// step resolves the answer from the request alone: the script index is the
// number of tool results already in the conversation. A retried or replayed
// request therefore gets the same answer, which is what makes a reattach
// byte-identical rather than merely similar.
func (s *Server) step(input []json.RawMessage) (int, error) {
	results := 0
	for _, raw := range input {
		var item struct {
			Type string `json:"type"`
			Role string `json:"role"`
		}
		if json.Unmarshal(raw, &item) != nil {
			continue
		}
		if item.Type == "function_call_output" || item.Role == "tool" {
			results++
		}
	}
	if results >= len(s.opts.Script) {
		return results, fmt.Errorf("conversation carries %d tool results but the script has %d steps", results, len(s.opts.Script))
	}
	return results, nil
}

func (s *Server) responses(w http.ResponseWriter, r *http.Request) {
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
	var req responsesReq
	if err := json.Unmarshal(body, &req); err != nil {
		s.record(r, "", body, http.StatusBadRequest, -1)
		writeError(w, http.StatusBadRequest, "invalid_request", "modelfake could not decode the request")
		return
	}
	// A model that is not the scripted one is the harness's own auxiliary
	// call (OpenCode titles a session with a separate small model). Answering
	// it from the script would silently consume a turn.
	if req.Model != s.opts.Model {
		s.record(r, req.Model, body, http.StatusOK, -1)
		s.writeResponses(w, req.Stream, Step{Text: s.opts.Aux}, "aux")
		return
	}
	idx, err := s.step(req.Input)
	if err != nil {
		s.record(r, req.Model, body, http.StatusConflict, idx)
		writeError(w, http.StatusConflict, "script_exhausted", "modelfake: "+err.Error())
		return
	}
	names := make([]string, 0, len(req.Tools))
	for _, tool := range req.Tools {
		names = append(names, tool.Name)
	}
	if err := checkTools(names, s.opts.Script[idx]); err != nil {
		s.record(r, req.Model, body, http.StatusConflict, idx)
		writeError(w, http.StatusConflict, "unknown_tool", "modelfake: "+err.Error())
		return
	}
	s.record(r, req.Model, body, http.StatusOK, idx)
	s.writeResponses(w, req.Stream, s.opts.Script[idx], fmt.Sprintf("%d", idx))
}

// writeResponses renders one scripted turn, streaming or not. The event order
// is the real API's: created, then each output item opened, filled and closed,
// then completed.
func (s *Server) writeResponses(w http.ResponseWriter, stream bool, step Step, stem string) {
	items := outputItems(step, stem)
	if !stream {
		writeJSON(w, http.StatusOK, responseObject("resp_"+stem, s.opts.Model, items))
		return
	}
	flusher, canFlush := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	seq := 0
	send := func(typ string, payload map[string]any) {
		payload["type"] = typ
		payload["sequence_number"] = seq
		seq++
		encoded, err := json.Marshal(payload)
		if err != nil {
			return
		}
		_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", typ, encoded)
		if canFlush {
			flusher.Flush()
		}
	}
	send("response.created", map[string]any{"response": responseObject("resp_"+stem, s.opts.Model, nil)})
	send("response.in_progress", map[string]any{"response": responseObject("resp_"+stem, s.opts.Model, nil)})
	for i, item := range items {
		opening := map[string]any{}
		for k, v := range item {
			opening[k] = v
		}
		id, _ := item["id"].(string)
		switch item["type"] {
		case "message":
			opening["content"] = []any{}
			opening["status"] = "in_progress"
			text := messageText(item)
			send("response.output_item.added", map[string]any{"output_index": i, "item": opening})
			send("response.content_part.added", map[string]any{
				"item_id": id, "output_index": i, "content_index": 0,
				"part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}},
			})
			for _, chunk := range chunks(text) {
				send("response.output_text.delta", map[string]any{
					"item_id": id, "output_index": i, "content_index": 0, "delta": chunk,
				})
			}
			send("response.output_text.done", map[string]any{
				"item_id": id, "output_index": i, "content_index": 0, "text": text,
			})
			send("response.content_part.done", map[string]any{
				"item_id": id, "output_index": i, "content_index": 0,
				"part": map[string]any{"type": "output_text", "text": text, "annotations": []any{}},
			})
		case "function_call":
			arguments, _ := item["arguments"].(string)
			opening["arguments"] = ""
			opening["status"] = "in_progress"
			send("response.output_item.added", map[string]any{"output_index": i, "item": opening})
			for _, chunk := range chunks(arguments) {
				send("response.function_call_arguments.delta", map[string]any{
					"item_id": id, "output_index": i, "delta": chunk,
				})
			}
			send("response.function_call_arguments.done", map[string]any{
				"item_id": id, "output_index": i, "arguments": arguments,
			})
		}
		send("response.output_item.done", map[string]any{"output_index": i, "item": item})
	}
	send("response.completed", map[string]any{"response": responseObject("resp_"+stem, s.opts.Model, items)})
}

// chunks splits text into deterministic deltas so the harness exercises its
// streaming assembly rather than receiving one whole string.
func chunks(text string) []string {
	if text == "" {
		return nil
	}
	const size = 24
	var out []string
	for len(text) > size {
		// Split on a rune boundary; a delta cut mid-rune is invalid JSON text.
		cut := size
		for cut < len(text) && !isRuneStart(text[cut]) {
			cut++
		}
		out = append(out, text[:cut])
		text = text[cut:]
	}
	return append(out, text)
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }

func messageText(item map[string]any) string {
	parts, _ := item["content"].([]any)
	var b strings.Builder
	for _, part := range parts {
		if p, ok := part.(map[string]any); ok {
			if text, ok := p["text"].(string); ok {
				b.WriteString(text)
			}
		}
	}
	return b.String()
}

// outputItems renders a step as Responses output items. Ids are derived from
// the step stem so two runs of one script produce identical ids.
func outputItems(step Step, stem string) []map[string]any {
	var items []map[string]any
	if step.Text != "" {
		items = append(items, map[string]any{
			"id": "msg_" + stem, "type": "message", "status": "completed", "role": "assistant",
			"content": []any{map[string]any{"type": "output_text", "text": step.Text, "annotations": []any{}}},
		})
	}
	for i, call := range step.Calls {
		arguments := "{}"
		if call.Arguments != nil {
			// Marshalling a map sorts keys, so the argument bytes are stable.
			if encoded, err := json.Marshal(call.Arguments); err == nil {
				arguments = string(encoded)
			}
		}
		id := fmt.Sprintf("fc_%s_%d", stem, i)
		items = append(items, map[string]any{
			"id": id, "type": "function_call", "status": "completed",
			"call_id": "call_" + stem + "_" + fmt.Sprint(i), "name": call.Name, "arguments": arguments,
		})
	}
	return items
}

func responseObject(id, model string, output []map[string]any) map[string]any {
	status := "in_progress"
	rendered := []any{}
	if output != nil {
		status = "completed"
		for _, item := range output {
			rendered = append(rendered, item)
		}
	}
	return map[string]any{
		"id": id, "object": "response", "created_at": createdAt, "status": status,
		"model": model, "output": rendered, "parallel_tool_calls": true,
		"tool_choice": "auto", "tools": []any{}, "temperature": 1, "top_p": 1,
		"usage": map[string]any{
			"input_tokens": 100, "output_tokens": 20, "total_tokens": 120,
			"input_tokens_details":  map[string]any{"cached_tokens": 0},
			"output_tokens_details": map[string]any{"reasoning_tokens": 0},
		},
	}
}
