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
	"strconv"
	"strings"
	"text/template"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// webhookBodyLimit bounds one inbound webhook body.
const webhookBodyLimit = 1 << 20

const (
	githubProvider           = "github"
	slackProvider            = "slack"
	linearProvider           = "linear"
	genericProvider          = "generic"
	defaultSlackReplayWindow = 5 * time.Minute
)

// WebhookProviderConfig contains the independent credentials used to verify
// provider-native webhook requests. GenericBearer is accepted only when the
// request explicitly selects the generic adapter with X-Remount-Provider.
type WebhookProviderConfig struct {
	GitHubSecret       string
	SlackSigningSecret string
	LinearSecret       string
	GenericBearer      string
	// SlackReplayWindow defaults to five minutes, as required by Slack's
	// signing protocol. A negative duration is invalid and zero selects the
	// default.
	SlackReplayWindow time.Duration
}

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

// webhookProvider authenticates and translates a provider-native request.
// The bool reports whether a provider adapter was selected; selected requests
// fail closed instead of falling through to ordinary bearer authentication.
func (s *Server) webhookProvider(r *http.Request, body []byte, bearer string) (webhookRequest, any, string, bool, error) {
	provider := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Remount-Provider")))
	switch {
	case r.Header.Get("X-GitHub-Event") != "":
		provider = githubProvider
	case r.Header.Get("X-Slack-Signature") != "" || r.Header.Get("X-Slack-Request-Timestamp") != "":
		provider = slackProvider
	case r.Header.Get("Linear-Signature") != "":
		provider = linearProvider
	}
	if provider == "" {
		return webhookRequest{}, nil, "", false, nil
	}

	var typ string
	switch provider {
	case githubProvider:
		if !verifyPrefixedHMAC(s.opts.WebhookProviders.GitHubSecret, body, r.Header.Get("X-Hub-Signature-256"), "sha256=") {
			return webhookRequest{}, nil, provider, true, proto.Err(proto.CodeUnauthorized, "invalid github webhook signature")
		}
		typ = "webhook.github." + webhookEventPart(r.Header.Get("X-GitHub-Event"))
	case slackProvider:
		window := s.opts.WebhookProviders.SlackReplayWindow
		if window == 0 {
			window = defaultSlackReplayWindow
		}
		if window < 0 {
			return webhookRequest{}, nil, provider, true, proto.Err(proto.CodeUnauthorized, "slack webhook verification unavailable")
		}
		stamp, err := strconv.ParseInt(r.Header.Get("X-Slack-Request-Timestamp"), 10, 64)
		if err != nil || stamp <= 0 || time.Since(time.Unix(stamp, 0)) > window || time.Until(time.Unix(stamp, 0)) > window {
			return webhookRequest{}, nil, provider, true, proto.Err(proto.CodeUnauthorized, "stale slack webhook timestamp")
		}
		base := append([]byte("v0:"+strconv.FormatInt(stamp, 10)+":"), body...)
		if !verifyPrefixedHMAC(s.opts.WebhookProviders.SlackSigningSecret, base, r.Header.Get("X-Slack-Signature"), "v0=") {
			return webhookRequest{}, nil, provider, true, proto.Err(proto.CodeUnauthorized, "invalid slack webhook signature")
		}
		var envelope struct {
			Type  string `json:"type"`
			Event struct {
				Type string `json:"type"`
			} `json:"event"`
		}
		if err := decodeWebhookJSON(body, &envelope); err != nil {
			return webhookRequest{}, nil, provider, true, proto.Err(proto.CodeBadRequest, "invalid slack webhook body")
		}
		name := envelope.Type
		if envelope.Type == "event_callback" && envelope.Event.Type != "" {
			name = envelope.Event.Type
		}
		typ = "webhook.slack." + webhookEventPart(name)
	case linearProvider:
		if !verifyPrefixedHMAC(s.opts.WebhookProviders.LinearSecret, body, r.Header.Get("Linear-Signature"), "") {
			return webhookRequest{}, nil, provider, true, proto.Err(proto.CodeUnauthorized, "invalid linear webhook signature")
		}
		var envelope struct {
			Type   string `json:"type"`
			Action string `json:"action"`
		}
		if err := decodeWebhookJSON(body, &envelope); err != nil {
			return webhookRequest{}, nil, provider, true, proto.Err(proto.CodeBadRequest, "invalid linear webhook body")
		}
		typ = "webhook.linear." + webhookEventPart(envelope.Type)
		if action := webhookEventPart(envelope.Action); action != "unknown" {
			typ += "." + action
		}
	case genericProvider:
		if !constantTimeTextEqual(bearer, s.opts.WebhookProviders.GenericBearer) || bearer == "" {
			return webhookRequest{}, nil, provider, true, proto.Err(proto.CodeUnauthorized, "invalid generic webhook bearer")
		}
		var in webhookRequest
		if err := decodeWebhookJSON(body, &in); err != nil || in.Type == "" {
			return webhookRequest{}, nil, provider, true, proto.Err(proto.CodeBadRequest, "generic event needs a non-empty type")
		}
		in.Type = "webhook.generic." + webhookEventPart(in.Type)
		payload, err := decodeWebhookPayload(in.Payload)
		return in, payload, provider, true, err
	default:
		return webhookRequest{}, nil, provider, true, proto.Err(proto.CodeBadRequest, "unsupported webhook provider")
	}
	if strings.HasSuffix(typ, ".unknown") {
		return webhookRequest{}, nil, provider, true, proto.Err(proto.CodeBadRequest, "provider event type is missing")
	}
	payload, err := decodeWebhookPayload(json.RawMessage(body))
	return webhookRequest{Type: typ, Payload: append(json.RawMessage(nil), body...)}, payload, provider, true, err
}

func verifyPrefixedHMAC(secret string, body []byte, supplied, prefix string) bool {
	if secret == "" {
		return false
	}
	hexSignature, ok := strings.CutPrefix(supplied, prefix)
	if !ok {
		return false
	}
	got, err := hex.DecodeString(hexSignature)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return hmac.Equal(got, mac.Sum(nil))
}

func constantTimeTextEqual(a, b string) bool {
	// Compare fixed-size digests so length is not an early-exit oracle.
	aDigest, bDigest := sha256.Sum256([]byte(a)), sha256.Sum256([]byte(b))
	return hmac.Equal(aDigest[:], bDigest[:])
}

func webhookEventPart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' {
			b.WriteRune(r)
		} else if b.Len() > 0 && !strings.HasSuffix(b.String(), "_") {
			b.WriteByte('_')
		}
		if b.Len() == 64 {
			break
		}
	}
	part := strings.Trim(b.String(), "_")
	if part == "" {
		return "unknown"
	}
	return part
}

func decodeWebhookJSON(body []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return errors.New("trailing webhook JSON")
	}
	return nil
}

func decodeWebhookPayload(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var payload any
	if err := decodeWebhookJSON(raw, &payload); err != nil {
		return nil, proto.Err(proto.CodeBadRequest, "payload must be JSON")
	}
	return payload, nil
}

// handleEvents accepts a JSON event and appends it (webhook wake). With an
// agent mapping it also wakes or creates an Agent. A caller authenticates
// with the API bearer or with an HMAC signature; a signed request acts as the
// configured webhook credential, and without one it may only append.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, apiErrorBody{Error: apiError{Code: proto.CodeUnsupported, Message: "method not allowed"}})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, webhookBodyLimit+1))
	if err != nil || len(body) > webhookBodyLimit {
		writeJSON(w, http.StatusRequestEntityTooLarge, apiErrorBody{Error: apiError{Code: proto.CodeBadRequest, Message: fmt.Sprintf("body is larger than %d bytes", webhookBodyLimit)}})
		return
	}
	bearer, _, hasBearer := credential(r, false)
	in, payload, provider, providerSelected, providerErr := s.webhookProvider(r, body, bearer)
	if providerErr != nil {
		metrics.WebhookRejected.Inc()
		writeError(w, providerErr)
		return
	}
	signed := providerSelected || s.webhookSigned(r, body)
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
		writeError(w, proto.Err(proto.CodeUnauthorized, "missing credential or signature"))
		return
	}
	if !providerSelected {
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&in); err != nil || in.Type == "" {
			badRequest(w, "event must be a JSON object with a non-empty type")
			return
		}
		// Numbers stay as their literals: a template that renders an issue
		// or comment id must not turn 2147483648 into 2.147483648e+09.
		payload, err = decodeWebhookPayload(in.Payload)
		if err != nil {
			badRequest(w, "payload must be JSON")
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
	principal := "webhook"
	if provider != "" {
		principal += ":" + provider
	}
	e := proto.Event{Type: in.Type, Stream: in.Stream, Principal: principal}
	if payload != nil {
		e.Payload = proto.MustMarshal(payload)
	}
	if err := s.Control.PostEvents(ctx, principal, []proto.Event{e}); err != nil {
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
