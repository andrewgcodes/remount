package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/proto"
)

func sign(secret, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func slackSign(secret string, timestamp int64, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = fmt.Fprintf(mac, "v0:%d:%s", timestamp, body)
	return "v0=" + hex.EncodeToString(mac.Sum(nil))
}

// Provider adapters verify their provider's exact raw-body scheme before
// translating provider-native bodies into stable event names.
func TestWebhookProviderSignaturesAndMapping(t *testing.T) {
	// GitHub's published test vector catches verification over decoded or
	// re-encoded JSON instead of the bytes delivered on the wire.
	const githubDigest = "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17"
	if !verifyPrefixedHMAC("It's a Secret to Everybody", []byte("Hello, World!"), githubDigest, "sha256=") {
		t.Fatal("GitHub published signature fixture did not verify")
	}
	const slackFixtureBody = `{"type":"event_callback","event":{"type":"app_mention"}}`
	const slackDigest = "v0=3228c02cb5d71de76c2d48ba6aab79458570489724b6f414d4f22df4e00f2493"
	if !verifyPrefixedHMAC("slack-test-secret", []byte("v0:1770000000:"+slackFixtureBody), slackDigest, "v0=") {
		t.Fatal("recorded Slack v0 signature fixture did not verify")
	}

	s, hs := newAPIServer(t, func(o *Options) {
		o.WebhookProviders = WebhookProviderConfig{
			GitHubSecret:       "github-test-secret",
			SlackSigningSecret: "slack-test-secret",
			LinearSecret:       "linear-test-secret",
			GenericBearer:      "generic-test-token",
		}
	})
	post := func(body string, headers map[string]string, wantStatus int, wantType string) {
		t.Helper()
		resp, responseBody := apiCall(t, hs, http.MethodPost, "/v1/events", "", body, headers)
		if resp.StatusCode != wantStatus {
			t.Fatalf("POST provider = %d %s", resp.StatusCode, responseBody)
		}
		if wantType == "" {
			return
		}
		events, err := s.Log.Read(context.Background(), 1, "", 100)
		if err != nil || len(events) == 0 || events[len(events)-1].Type != wantType {
			t.Fatalf("mapped events = %+v, %v; want %s", events, err, wantType)
		}
	}

	githubBody := `{"action":"created","repository":{"full_name":"acme/widgets"},"comment":{"body":"wake"}}`
	post(githubBody, map[string]string{
		"X-GitHub-Event":      "issue_comment",
		"X-Hub-Signature-256": sign("github-test-secret", githubBody),
	}, http.StatusAccepted, "webhook.github.issue_comment")
	post(githubBody, map[string]string{
		"X-GitHub-Event":      "issue_comment",
		"X-Hub-Signature-256": sign("wrong", githubBody),
	}, http.StatusUnauthorized, "")

	slackBody := `{"type":"event_callback","event":{"type":"app_mention","text":"hi"}}`
	now := time.Now().Unix()
	post(slackBody, map[string]string{
		"X-Slack-Request-Timestamp": fmt.Sprint(now),
		"X-Slack-Signature":         slackSign("slack-test-secret", now, slackBody),
	}, http.StatusAccepted, "webhook.slack.app_mention")
	old := now - int64((defaultSlackReplayWindow+time.Minute)/time.Second)
	post(slackBody, map[string]string{
		"X-Slack-Request-Timestamp": fmt.Sprint(old),
		"X-Slack-Signature":         slackSign("slack-test-secret", old, slackBody),
	}, http.StatusUnauthorized, "")

	linearBody := `{"action":"create","type":"Issue","webhookTimestamp":1710000000000}`
	const linearDigest = "5c1a2f0cc7610128ba02aa83ef2fe695e99da28ee66ef5c6031ee6f3021fcea3"
	post(linearBody, map[string]string{"Linear-Signature": linearDigest}, http.StatusAccepted, "webhook.linear.issue.create")

	genericBody := `{"type":"deploy.completed","payload":{"environment":"staging"}}`
	post(genericBody, map[string]string{
		"Authorization":      "Bearer generic-test-token",
		"X-Remount-Provider": "generic",
	}, http.StatusAccepted, "webhook.generic.deploy_completed")
	post(genericBody, map[string]string{
		"Authorization":      "Bearer wrong",
		"X-Remount-Provider": "generic",
	}, http.StatusUnauthorized, "")
}

// A signed webhook creates an Agent from a templated task, a redelivery
// maps onto the same Agent, a follow-up wakes it, and a bad signature,
// a template that names a missing key, or a signed request without a webhook
// credential never reaches the control plane.
func TestWebhookMapsEventsOntoAgents(t *testing.T) {
	s, hs := newAPIServer(t, func(o *Options) {
		o.Authenticator = control.StaticAuthenticator{"hook-tok": {ID: "hook", Tenant: "team"}}
		o.WebhookSecret = "s3cret"
		o.WebhookToken = "hook-tok"
	})
	payload := `{"action":"labeled","issue":{"number":42,"title":"Fix login","body":"It breaks"},"label":{"name":"agent"}}`
	create := `{"type":"github.issues","payload":` + payload + `,"agent":{"idempotency_key":"gh-{{.issue.number}}","create":{"name":"issue-{{.issue.number}}","workspace":{"name":"issue-{{.issue.number}}"},"spec":{"recipe":"custom","task":"Fix #{{.issue.number}}: {{.issue.title}}\n\n{{.issue.body}}","acp_command":["/bin/true"]}}}}`

	if resp, _ := apiCall(t, hs, "POST", "/v1/events", "", create, map[string]string{"X-Hub-Signature-256": sign("wrong", create)}); resp.StatusCode != 401 {
		t.Fatalf("bad signature = %d", resp.StatusCode)
	}
	if resp, _ := apiCall(t, hs, "POST", "/v1/events", "", create, nil); resp.StatusCode != 401 {
		t.Fatalf("unsigned = %d", resp.StatusCode)
	}
	resp, body := apiCall(t, hs, "POST", "/v1/events", "", create, map[string]string{"X-Hub-Signature-256": sign("s3cret", create)})
	if resp.StatusCode != 202 {
		t.Fatalf("signed create = %d %s", resp.StatusCode, body)
	}
	var out struct {
		Agent string `json:"agent"`
		WS    string `json:"ws"`
	}
	if err := json.Unmarshal(body, &out); err != nil || out.Agent == "" {
		t.Fatalf("response = %s", body)
	}
	resp, body = apiCall(t, hs, "GET", "/v1/agents/"+out.Agent, "hook-tok", "", nil)
	var a proto.Agent
	if resp.StatusCode != 200 || json.Unmarshal(body, &a) != nil {
		t.Fatalf("get = %d %s", resp.StatusCode, body)
	}
	if a.Owner != "hook" || a.Name != "issue-42" || a.Spec.Task != "Fix #42: Fix login\n\nIt breaks" {
		t.Fatalf("agent = %+v", a)
	}
	hook, release, err := s.clients.acquire(context.Background(), "hook-tok")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if ws, err := hook.GetWorkspace(context.Background(), out.WS); err != nil || ws.Spec.Name != "issue-42" {
		t.Fatalf("workspace = %+v %v", ws, err)
	}
	// Redelivery: same key, same agent.
	resp, body = apiCall(t, hs, "POST", "/v1/events", "", create, map[string]string{"X-Remount-Signature": sign("s3cret", create)})
	if resp.StatusCode != 202 || !strings.Contains(string(body), out.Agent) {
		t.Fatalf("redelivery = %d %s", resp.StatusCode, body)
	}
	// A comment wakes the agent with a follow-up (a bearer works too).
	wake := `{"type":"github.issue_comment","payload":{"comment":{"body":"also fix logout"}},"agent":{"wake":"` + out.Agent + `","message":"{{.comment.body}}"}}`
	if resp, body := apiCall(t, hs, "POST", "/v1/events", "hook-tok", wake, nil); resp.StatusCode != 202 {
		t.Fatalf("wake = %d %s", resp.StatusCode, body)
	}
	resp, body = apiCall(t, hs, "GET", "/v1/agents/"+out.Agent, "hook-tok", "", nil)
	if resp.StatusCode != 200 || json.Unmarshal(body, &a) != nil || len(a.Inbox) != 2 || a.Inbox[1].Text != "also fix logout" {
		t.Fatalf("after wake = %d %s", resp.StatusCode, body)
	}
	// The same agent by name, from a payload that only knows the issue.
	byName := `{"type":"github.issue_comment","payload":{"issue":{"number":42},"comment":{"body":"and the tests"}},"agent":{"wake":"name:issue-{{.issue.number}}","message":"{{.comment.body}}"}}`
	if resp, body := apiCall(t, hs, "POST", "/v1/events", "", byName, map[string]string{"X-Remount-Signature": sign("s3cret", byName)}); resp.StatusCode != 202 || !strings.Contains(string(body), out.Agent) {
		t.Fatalf("wake by name = %d %s", resp.StatusCode, body)
	}
	unknown := strings.Replace(byName, "issue-{{.issue.number}}", "nobody", 1)
	if resp, body := apiCall(t, hs, "POST", "/v1/events", "", unknown, map[string]string{"X-Remount-Signature": sign("s3cret", unknown)}); resp.StatusCode != 404 {
		t.Fatalf("wake unknown name = %d %s", resp.StatusCode, body)
	}
	// Missing key, both actions, neither action: rejected before any effect.
	for name, bad := range map[string]string{
		"missing":   `{"type":"x","payload":{},"agent":{"wake":"` + out.Agent + `","message":"{{.nope.body}}"}}`,
		"both":      `{"type":"x","agent":{"wake":"a","create":{}}}`,
		"neither":   `{"type":"x","agent":{}}`,
		"unknown":   `{"type":"x","bogus":1}`,
		"no-type":   `{"payload":{}}`,
		"bad-templ": `{"type":"x","agent":{"wake":"a","message":"{{.x"}}`,
	} {
		resp, body := apiCall(t, hs, "POST", "/v1/events", "hook-tok", bad, nil)
		var e apiErrorBody
		if resp.StatusCode != 400 || json.Unmarshal(body, &e) != nil || e.Error.Code != proto.CodeBadRequest {
			t.Fatalf("%s = %d %s", name, resp.StatusCode, body)
		}
	}
	// Every refusal is the documented JSON error shape, not a text/plain line.
	if resp, body := apiCall(t, hs, "GET", "/v1/events", "hook-tok", "", nil); resp.StatusCode != 405 || !strings.Contains(string(body), `"code":"unsupported"`) {
		t.Fatalf("GET = %d %s", resp.StatusCode, body)
	}
	if resp, body := apiCall(t, hs, "POST", "/v1/events", "", `{"type":"x"}`, nil); resp.StatusCode != 401 || !strings.Contains(string(body), `"code":"unauthorized"`) || resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatalf("unauthenticated = %d %s", resp.StatusCode, body)
	}
	if resp, body := apiCall(t, hs, "POST", "/v1/events", "hook-tok", `{"type":"x","payload":"`+strings.Repeat("x", webhookBodyLimit)+`"}`, nil); resp.StatusCode != 413 || !strings.Contains(string(body), `"code":"bad_request"`) {
		t.Fatalf("oversized = %d %.80s", resp.StatusCode, body)
	}
	// Numbers render as their literals; GitHub ids are past float precision.
	bigID := `{"type":"github.issue_comment","payload":{"comment":{"id":3000000001234567,"body":"n"}},"agent":{"wake":"` + out.Agent + `","message":"comment {{.comment.id}}"}}`
	if resp, body := apiCall(t, hs, "POST", "/v1/events", "hook-tok", bigID, nil); resp.StatusCode != 202 {
		t.Fatalf("big id = %d %s", resp.StatusCode, body)
	}
	resp, body = apiCall(t, hs, "GET", "/v1/agents/"+out.Agent, "hook-tok", "", nil)
	if resp.StatusCode != 200 || json.Unmarshal(body, &a) != nil || a.Inbox[len(a.Inbox)-1].Text != "comment 3000000001234567" {
		t.Fatalf("big id rendered = %d %s", resp.StatusCode, body)
	}
	// Events without a mapping still append as before.
	if resp, _ := apiCall(t, hs, "POST", "/v1/events", "hook-tok", `{"type":"ping"}`, nil); resp.StatusCode != 202 {
		t.Fatalf("plain event = %d", resp.StatusCode)
	}
}

// Without a webhook credential a signed request may append events but not
// act on agents.
func TestWebhookSignedWithoutTokenIsAppendOnly(t *testing.T) {
	_, hs := newAPIServer(t, func(o *Options) { o.WebhookSecret = "s3cret" })
	plain := `{"type":"ping","payload":{"n":1}}`
	if resp, body := apiCall(t, hs, "POST", "/v1/events", "", plain, map[string]string{"X-Remount-Signature": sign("s3cret", plain)}); resp.StatusCode != 202 {
		t.Fatalf("signed append = %d %s", resp.StatusCode, body)
	}
	act := `{"type":"x","agent":{"create":{"workspace":{},"spec":{"recipe":"custom","task":"t","acp_command":["/bin/true"]}}}}`
	if resp, body := apiCall(t, hs, "POST", "/v1/events", "", act, map[string]string{"X-Remount-Signature": sign("s3cret", act)}); resp.StatusCode != 403 {
		t.Fatalf("signed act = %d %s", resp.StatusCode, body)
	}
	if resp, _ := apiCall(t, hs, "GET", "/v1/agents", "tok", "", nil); resp.StatusCode != 200 {
		t.Fatalf("list = %d", resp.StatusCode)
	}
}
