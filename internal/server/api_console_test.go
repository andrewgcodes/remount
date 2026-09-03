package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/proto"
)

func TestConsoleAPIRequiresOperatorForMutations(t *testing.T) {
	_, hs := newAPIServer(t, func(options *Options) {
		options.Authenticator = control.StaticAuthenticator{
			"operator": {ID: "op", Tenant: "team", Roles: []string{"operator"}},
			"viewer":   {ID: "read", Tenant: "team", Roles: []string{"viewer"}},
		}
	})
	for _, test := range []struct {
		name, token string
		headers     map[string]string
		want        int
	}{
		{name: "missing idempotency", token: "operator", want: http.StatusBadRequest},
		{name: "viewer", token: "viewer", headers: map[string]string{"Idempotency-Key": "create-viewer"}, want: http.StatusForbidden},
		{name: "operator", token: "operator", headers: map[string]string{"Idempotency-Key": "create-operator"}, want: http.StatusCreated},
	} {
		t.Run(test.name, func(t *testing.T) {
			response, body := apiCall(t, hs, http.MethodPost, "/v1/console/workspaces", test.token, `{"name":"console"}`, test.headers)
			if response.StatusCode != test.want {
				t.Fatalf("response = %d %s, want %d", response.StatusCode, body, test.want)
			}
			if test.want == http.StatusCreated {
				var workspace consoleWorkspaceDetail
				if err := json.Unmarshal(body, &workspace); err != nil || workspace.Name != "console" || workspace.Tenant != "team" {
					t.Fatalf("workspace = %+v, err=%v", workspace, err)
				}
			}
		})
	}
}

func TestConsoleEventsAreBoundedFilteredAndTenantScoped(t *testing.T) {
	s, hs := newAPIServer(t, func(options *Options) {
		options.Authenticator = control.StaticAuthenticator{
			"team":  {ID: "reader", Tenant: "team", Roles: []string{"viewer"}},
			"rival": {ID: "reader", Tenant: "rival", Roles: []string{"viewer"}},
		}
	})
	for _, event := range []proto.Event{
		{Type: proto.EvCredUsed, Tenant: "team", Principal: "alice", Payload: proto.MustMarshal(map[string]string{"binding": "openai"})},
		{Type: "leak_blocked", Tenant: "rival", Principal: "mallory"},
	} {
		copy := event
		if err := s.Log.Append(context.Background(), &copy); err != nil {
			t.Fatal(err)
		}
	}
	response, body := apiCall(t, hs, http.MethodGet,
		"/v1/console/events?limit=1&types=cred.used&principal=alice&credential=openai", "team", "", nil)
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), `"type":"cred.used"`) || strings.Contains(string(body), "mallory") {
		t.Fatalf("events = %d %s", response.StatusCode, body)
	}
	if response, _ := apiCall(t, hs, http.MethodGet, "/v1/console/events?types=BAD%20TYPE", "team", "", nil); response.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid event filter = %d", response.StatusCode)
	}
}

func TestConsoleStaticAssetsAreEmbedded(t *testing.T) {
	_, hs := newAPIServer(t, nil)
	response, body := apiCall(t, hs, http.MethodGet, "/console/", "", "", nil)
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), `<div id="app">`) {
		t.Fatalf("console = %d %s", response.StatusCode, body)
	}
	if response.Header.Get("Content-Security-Policy") == "" || response.Header.Get("Cache-Control") != "no-cache" {
		t.Fatalf("console headers = %v", response.Header)
	}
}
