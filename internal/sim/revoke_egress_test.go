package sim

import (
	"context"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/proto"
)

// TestFleetRevokeEgressKillsGatewayAccess is the revocation half of the
// brokered-credential contract. "Revoking network kills existing gateway
// access" is a claim about a transfer that is already running, not only about
// the next one, and the two fail differently: a broker that stops accepting
// connections still leaks whatever is mid-stream.
//
// The workspace never holds the real key. The test drives the broker through
// the capability URL the workspace itself was given, so what it exercises is
// the gateway a workspace has, not a private path.
func TestFleetRevokeEgressKillsGatewayAccess(t *testing.T) {
	// The upstream answers with one flushed chunk and then holds the response
	// open. That parked response is the in-flight transfer under test.
	release := make(chan struct{})
	arrived := make(chan struct{}, 1)
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer sk-REAL-SECRET" {
			http.Error(w, "the broker did not substitute", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "part-one\n")
		w.(http.Flusher).Flush()
		select {
		case arrived <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-time.After(30 * time.Second):
		}
	}))
	defer upstream.Close()
	defer close(release)
	upstreamHost := strings.TrimPrefix(upstream.URL, "https://")

	w := newWorld(t, control.Binding{
		ID: "b_egress", Secret: "sk-REAL-SECRET", Destinations: []string{upstreamHost}, TTLSec: 60,
	})
	roots := x509.NewCertPool()
	roots.AddCert(upstream.Certificate())
	w.nodeWithBrokerRoots("n1", nil, roots)
	c := w.client("incident-commander")
	ctx := ctxT(t, 90*time.Second)
	ws := mustWS(t, c, proto.WorkspaceSpec{
		Run:      "revoke-egress-run",
		Bindings: []string{"b_egress"},
		Env:      map[string]string{"API_KEY": "ref:b_egress"},
	})

	// The broker address is discovered the way a harness discovers it: from
	// the workspace's own environment.
	session, base := liveBrokerSession(t, ctx, c, ws.ID)
	defer func() { _ = session.Close(context.Background(), true) }()

	gateway := &http.Client{Transport: &http.Transport{Proxy: nil}}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		strings.TrimSuffix(base, "/")+"/d/"+upstreamHost+"/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer ref:b_egress")
	response, err := gateway.Do(request)
	if err != nil {
		t.Fatalf("open the allowed transfer: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("allowed transfer = %d %q", response.StatusCode, body)
	}
	first := make([]byte, len("part-one\n"))
	if _, err := io.ReadFull(response.Body, first); err != nil {
		t.Fatalf("read the first chunk: %v", err)
	}
	if string(first) != "part-one\n" {
		t.Fatalf("first chunk = %q", first)
	}
	select {
	case <-arrived:
	case <-ctx.Done():
		t.Fatal("the upstream never saw the allowed transfer")
	}

	// The stream is parked on the upstream, so nothing but revocation can end
	// it. Read in the background and watch for the cut.
	cut := make(chan error, 1)
	go func() {
		buf := make([]byte, 256)
		for {
			if _, err := response.Body.Read(buf); err != nil {
				cut <- err
				return
			}
		}
	}()
	select {
	case err := <-cut:
		t.Fatalf("the transfer ended before revocation: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	operation, err := c.QuarantineFleet(ctx, proto.FleetQuarantineReq{
		Selector:       proto.WorkspaceSelector{Workspace: ws.ID},
		Action:         proto.FleetActionRevokeEgress,
		IdempotencyKey: "revoke-egress-one",
	})
	if err != nil {
		t.Fatal(err)
	}
	operation, err = c.WaitFleetOperation(ctx, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if operation.State != proto.FleetStateCompleted || len(operation.Results) != 1 || !operation.Results[0].Acknowledged {
		t.Fatalf("revoke_egress operation = %#v", operation)
	}

	// 1. The in-flight transfer is cut.
	select {
	case err := <-cut:
		if err == nil {
			t.Fatal("the in-flight transfer ended without an error, so it was not cut")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("an allowed transfer survived revoke_egress; gateway access outlived the revocation")
	}

	// 2. A subsequent connection is refused. The listener is gone, so this is
	// a transport failure rather than a policy answer.
	next, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(base, "/")+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		resp, err := gateway.Do(next.Clone(ctx))
		if err != nil {
			break
		}
		_ = resp.Body.Close()
		if time.Now().After(deadline) {
			t.Fatalf("the broker still served %d after revoke_egress", resp.StatusCode)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 3. The revocation is in the record, and so is the credential use it
	// ended. The binding and type filters are applied by the control plane,
	// so this is also the audit question an operator asks.
	credential, err := c.ReadEvents(ctx, 1, ws.ID, client.WithEventBinding("b_egress"), client.WithEventTypes("cred", "egress"))
	if err != nil {
		t.Fatal(err)
	}
	if len(credential) == 0 {
		t.Fatal("no credential event was recorded for the allowed transfer")
	}
	for _, event := range credential {
		if !strings.HasPrefix(event.Type, "cred") && !strings.HasPrefix(event.Type, "egress") {
			t.Fatalf("the server-side type filter returned %s", event.Type)
		}
		payload := map[string]any{}
		if err := proto.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["binding"] != "b_egress" {
			t.Fatalf("the binding filter returned %s for binding %v", event.Type, payload["binding"])
		}
	}
	// A destination the binding never touched returns nothing, so the filter
	// is narrowing rather than passing everything through.
	elsewhere, err := c.ReadEvents(ctx, 1, ws.ID, client.WithEventHost("api.github.com"))
	if err != nil {
		t.Fatal(err)
	}
	if len(elsewhere) != 0 {
		t.Fatalf("the host filter returned %d events for an untouched destination", len(elsewhere))
	}

	all, err := c.ReadEvents(ctx, 1, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	fenced := false
	for _, event := range all {
		if event.Type != proto.EvWSFenced {
			continue
		}
		payload := map[string]any{}
		if err := proto.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["action"] == proto.FleetActionRevokeEgress {
			fenced = true
		}
	}
	if !fenced {
		t.Fatalf("revoke_egress emitted no ws.fenced record; %d workspace events", len(all))
	}
}
