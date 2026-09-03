package server

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"text/template"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// webhookBodyLimit bounds one inbound webhook body.
const webhookBodyLimit = 1 << 20

// webhookTemplateLimit bounds one rendered template (a task or a message);
// the control plane's own message bound is the real ceiling.
const webhookTemplateLimit = proto.MaxAgentMessageSize

// webhookRequest is the body of POST /v1/events. The event fields are the
// original webhook wake; Agent asks the control plane to act on the event
// as well (ADR 0057).
type webhookRequest struct {
	Type    string          `json:"type"`
	Stream  string          `json:"stream"`
	Payload json.RawMessage `json:"payload"`
	Agent   *webhookAgent   `json:"agent,omitempty"`
}

// webhookAgent maps an event onto an Agent. Templates are Go text/template
// over the decoded payload with missing keys an error, so a payload that
// does not carry what the mapping expects is rejected rather than half
// rendered into a prompt.
type webhookAgent struct {
	// Wake names an existing Agent that receives Message as a follow-up
	// (waking it if it sleeps): an id, or "name:<template>" for the one live
	// agent of that name the credential may read. Create makes a new Agent
	// whose Spec.Task is rendered as a template. Exactly one of the two is
	// set.
	Wake    string                `json:"wake,omitempty"`
	Message string                `json:"message,omitempty"`
	Create  *proto.AgentCreateReq `json:"create,omitempty"`
	// IdempotencyKey is a template too ("gh-{{.issue.number}}"), so a
	// redelivered webhook maps onto the same Agent or the same message.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// webhookSignature headers carry an HMAC-SHA256 of the raw body as
// "sha256=<hex>". The GitHub header is accepted so a repository can post
// straight to the control plane.
var webhookSignatureHeaders = []string{"X-Remount-Signature", "X-Hub-Signature-256"}

// webhookSigned reports whether body carries a valid signature under the
// configured secret. No secret means no signature is ever valid.
func (s *Server) webhookSigned(r *http.Request, body []byte) bool {
	if s.opts.WebhookSecret == "" {
		return false
	}
	mac := hmac.New(sha256.New, []byte(s.opts.WebhookSecret))
	mac.Write(body)
	want := mac.Sum(nil)
	for _, h := range webhookSignatureHeaders {
		v := r.Header.Get(h)
		hexSig, ok := strings.CutPrefix(v, "sha256=")
		if !ok {
			continue
		}
		got, err := hex.DecodeString(hexSig)
		if err != nil {
			continue
		}
		if hmac.Equal(got, want) {
			return true
		}
	}
	return false
}

// handleEvents accepts a JSON event and appends it (webhook wake). With an
// agent mapping it also wakes or creates an Agent. A caller authenticates
// with the API bearer or with an HMAC signature; a signed request acts as the
// configured webhook credential, and without one it may only append.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, webhookBodyLimit+1))
	if err != nil || len(body) > webhookBodyLimit {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	bearer, _, hasBearer := credential(r, false)
	signed := s.webhookSigned(r, body)
	// Three ways in: the shared bearer, a credential the authenticator knows
	// (checked by acquiring its client), or a valid signature.
	shared := s.authed(r) && (hasBearer || s.opts.Token == "")
	var cl *client.Client
	if hasBearer && !shared && s.opts.Authenticator != nil {
		c, release, err := s.clients.acquire(r.Context(), bearer)
		if err != nil {
			metrics.WebhookRejected.Inc()
			writeError(w, err)
			return
		}
		defer release()
		cl = c
	} else if !signed && !shared {
		metrics.WebhookRejected.Inc()
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var in webhookRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil || in.Type == "" {
		http.Error(w, "bad event", http.StatusBadRequest)
		return
	}
	var payload any
	if len(in.Payload) > 0 {
		if err := json.Unmarshal(in.Payload, &payload); err != nil {
			http.Error(w, "bad payload", http.StatusBadRequest)
			return
		}
	}
	// Deliberately not r.Context(): the append, the timers it fires and the
	// offers that follow outlive this HTTP response.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 30*time.Second)
	defer cancel()

	var acted *proto.Agent
	if in.Agent != nil {
		if cl == nil {
			tok := s.opts.WebhookToken
			if shared && hasBearer {
				tok = bearer
			}
			if tok == "" && (s.opts.Token != "" || s.opts.Authenticator != nil) {
				writeError(w, proto.Err(proto.CodeDenied, "agent actions from a signed webhook need --webhook-token"))
				return
			}
			c, release, err := s.clients.acquire(ctx, tok)
			if err != nil {
				writeError(w, err)
				return
			}
			defer release()
			cl = c
		}
		var err error
		acted, err = s.webhookAct(ctx, cl, in.Agent, payload)
		if err != nil {
			writeError(w, err)
			return
		}
		if in.Stream == "" {
			in.Stream = acted.WS
		}
	}
	e := proto.Event{Type: in.Type, Stream: in.Stream, Principal: "webhook"}
	if payload != nil {
		e.Payload = proto.MustMarshal(payload)
	}
	if err := s.Control.PostEvents(ctx, "webhook", []proto.Event{e}); err != nil {
		writeError(w, err)
		return
	}
	metrics.WebhookAccepted.Inc()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	out := map[string]any{"accepted": true}
	if acted != nil {
		out["agent"] = acted.ID
		out["ws"] = acted.WS
		out["status"] = acted.Status
	}
	_ = json.NewEncoder(w).Encode(out)
}

// webhookAct performs the mapping's Agent action as the acquired credential.
func (s *Server) webhookAct(ctx context.Context, cl agentActor, m *webhookAgent, payload any) (*proto.Agent, error) {
	if (m.Wake == "") == (m.Create == nil) {
		return nil, proto.Err(proto.CodeBadRequest, "agent mapping needs exactly one of wake or create")
	}
	idem, err := renderWebhookTemplate("idempotency_key", m.IdempotencyKey, payload)
	if err != nil {
		return nil, err
	}
	if m.Wake != "" {
		text, err := renderWebhookTemplate("message", m.Message, payload)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(text) == "" {
			return nil, proto.Err(proto.CodeBadRequest, "agent mapping wake needs a message")
		}
		id, err := resolveWebhookAgent(ctx, cl, m.Wake, payload)
		if err != nil {
			return nil, err
		}
		res, err := cl.MessageAgent(ctx, proto.AgentMessageReq{ID: id, Text: text, IdempotencyKey: idem})
		if err != nil {
			return nil, err
		}
		return &res.Agent, nil
	}
	req := *m.Create
	if req.Spec.Task, err = renderWebhookTemplate("task", req.Spec.Task, payload); err != nil {
		return nil, err
	}
	if req.Name, err = renderWebhookTemplate("name", req.Name, payload); err != nil {
		return nil, err
	}
	if req.Workspace != nil {
		ws := *req.Workspace
		if ws.Name, err = renderWebhookTemplate("workspace.name", ws.Name, payload); err != nil {
			return nil, err
		}
		req.Workspace = &ws
	}
	if idem != "" {
		req.IdempotencyKey = idem
	}
	return cl.CreateAgent(ctx, req)
}

// resolveWebhookAgent turns a wake target into an agent id. "name:<template>"
// must match exactly one live agent: none is not_found, several is conflict,
// so a mapping never messages the wrong agent.
func resolveWebhookAgent(ctx context.Context, cl agentActor, wake string, payload any) (string, error) {
	spec, ok := strings.CutPrefix(wake, "name:")
	if !ok {
		return wake, nil
	}
	name, err := renderWebhookTemplate("wake", spec, payload)
	if err != nil {
		return "", err
	}
	if name == "" {
		return "", proto.Err(proto.CodeBadRequest, "agent mapping wake name is empty")
	}
	agents, err := cl.ListAgents(ctx, proto.AgentListReq{})
	if err != nil {
		return "", err
	}
	var id string
	for _, a := range agents {
		if a.Name != name || agentTerminalStatus(a.Status) {
			continue
		}
		if id != "" {
			return "", proto.Err(proto.CodeConflict, "several live agents are named %q", name)
		}
		id = a.ID
	}
	if id == "" {
		return "", proto.Err(proto.CodeNotFound, "no live agent named %q", name)
	}
	return id, nil
}

func agentTerminalStatus(s string) bool {
	return s == proto.AgentFailed || s == proto.AgentFinished || s == proto.AgentDestroyed
}

// agentActor is the slice of the SDK the webhook needs; *client.Client
// satisfies it.
type agentActor interface {
	CreateAgent(ctx context.Context, req proto.AgentCreateReq, options ...client.OperationOption) (*proto.Agent, error)
	MessageAgent(ctx context.Context, req proto.AgentMessageReq, options ...client.OperationOption) (*proto.AgentMessageRes, error)
	ListAgents(ctx context.Context, req proto.AgentListReq) ([]proto.Agent, error)
}

// renderWebhookTemplate renders src against payload. A template that names a
// missing key fails: a webhook that changed shape must not silently produce
// an empty task.
func renderWebhookTemplate(field, src string, payload any) (string, error) {
	if !strings.Contains(src, "{{") {
		return src, nil
	}
	t, err := template.New(field).Option("missingkey=error").Parse(src)
	if err != nil {
		return "", proto.Err(proto.CodeBadRequest, "agent mapping %s: %v", field, err)
	}
	var b bytes.Buffer
	if err := t.Execute(&limitedWriter{w: &b, n: webhookTemplateLimit}, payload); err != nil {
		if errors.Is(err, errTemplateTooLarge) {
			return "", proto.Err(proto.CodeBadRequest, "agent mapping %s renders past %d bytes", field, webhookTemplateLimit)
		}
		return "", proto.Err(proto.CodeBadRequest, "agent mapping %s: %v", field, err)
	}
	return b.String(), nil
}

var errTemplateTooLarge = errors.New("template output too large")

type limitedWriter struct {
	w *bytes.Buffer
	n int
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	if len(p) > l.n {
		return 0, fmt.Errorf("%w", errTemplateTooLarge)
	}
	l.n -= len(p)
	return l.w.Write(p)
}
