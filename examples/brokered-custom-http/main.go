// Command brokered-custom-http calls an internal HTTP service that takes its
// token inside a JSON body, from a workspace that never holds the token.
//
// The binding declares `substitution: body_json:/auth/token`, so the broker
// buffers the request, replaces the JSON string at that RFC 6901 pointer, and
// forwards the result. This is the shape a company's own service usually has,
// and the one that used to force an adopter either to put the real token in
// the workspace or to write their own proxy.
//
// Two properties are worth knowing before choosing this location. The broker
// buffers the request body (bounded, 1 MiB by default) for a workspace whose
// bindings declare a body location, and a rewritten JSON document is
// re-serialized, so its keys come back in sorted order. A caller that needs
// byte-identical requests declares a header or query location instead.
//
//	go run ./cmd/remount standalone --listen 127.0.0.1:7443 --data ./remount-data
//	export REMOUNT_SERVER=http://127.0.0.1:7443
//	export REMOUNT_SERVICE_HOST=service.internal.example SERVICE_TOKEN=...
//	go run ./examples/brokered-custom-http
//
// Environment: REMOUNT_SERVER, REMOUNT_TOKEN, REMOUNT_SERVICE_HOST (required),
// REMOUNT_SERVICE_TOKEN_ENV (default SERVICE_TOKEN), REMOUNT_SERVICE_PATH
// (default /v1/records), REMOUNT_SERVICE_POINTER (default /auth/token) and
// REMOUNT_FOREIGN_HOST (default api.openai.com).
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"remount.dev/remount/api"
	"remount.dev/remount/client"
	"remount.dev/remount/examples/brokered"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "brokered-custom-http:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	host := os.Getenv("REMOUNT_SERVICE_HOST")
	if host == "" {
		return errors.New("REMOUNT_SERVICE_HOST is empty: name the HTTP service this example should call")
	}
	path := brokered.EnvOr("REMOUNT_SERVICE_PATH", "/v1/records")
	pointer := brokered.EnvOr("REMOUNT_SERVICE_POINTER", "/auth/token")
	foreign := brokered.EnvOr("REMOUNT_FOREIGN_HOST", "api.openai.com")
	tokenEnv := brokered.EnvOr("REMOUNT_SERVICE_TOKEN_ENV", "SERVICE_TOKEN")
	secret := os.Getenv(tokenEnv)
	if secret == "" {
		return fmt.Errorf("%s is empty: export the service token in the shell that runs this program", tokenEnv)
	}

	c, err := client.New(client.Options{
		Server:    brokered.EnvOr("REMOUNT_SERVER", "http://127.0.0.1:7443"),
		Token:     os.Getenv("REMOUNT_TOKEN"),
		Principal: "a_brokered_custom_http",
	})
	if err != nil {
		return err
	}
	defer c.Close()

	bindingID := "b_example_service_" + brokered.Suffix()
	binding, err := c.CreateBinding(ctx, api.BindingSpec{
		ID: bindingID, Kind: api.BindingKindBearer, Secret: secret,
		Destinations: []string{host}, TTLSec: 900,
		Methods:      []string{"POST"},
		Substitution: &api.BindingSubstitution{Location: api.SubstitutionBodyJSON, JSONPointer: pointer},
	})
	if err != nil {
		return err
	}
	workspaceID := ""
	defer func() { brokered.Cleanup(ctx, c, out, workspaceID, bindingID) }()
	fmt.Fprintf(out, "binding %s substitutes at JSON pointer %s for %s\n",
		binding.ID, binding.Substitution.JSONPointer, host)

	ws, err := c.CreateWorkspace(ctx, api.WorkspaceSpec{
		Name:     "brokered-custom-http",
		Bindings: []string{bindingID},
		Env: map[string]string{
			"SERVICE_TOKEN": "ref:" + bindingID,
			"SERVICE_URL":   "${REMOUNT_BROKER}/d/" + host + path,
			"FOREIGN_URL":   "${REMOUNT_BROKER}/d/" + foreign + path,
		},
	})
	if err != nil {
		return err
	}
	if ws, err = c.WaitClaimed(ctx, ws.ID); err != nil {
		return err
	}
	workspaceID = ws.ID
	fmt.Fprintf(out, "workspace %s claimed by node %s\n", ws.ID, ws.Node)

	// The workspace writes its placeholder into the document it was going to
	// send anyway. Nothing in the program knows a credential is involved.
	post := func(target string) string {
		return `curl -sS -o body -D head -w '%{http_code}' -X POST -H 'Content-Type: application/json' ` +
			`--data-binary "{\"auth\":{\"token\":\"$SERVICE_TOKEN\"},\"record\":{\"note\":\"brokered\"}}" "$` + target + `"`
	}

	// 1. The declared location.
	status, _, body, err := brokered.Probe(ctx, c, ws.ID, post("SERVICE_URL"))
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "record posted through the broker: HTTP %s\n", status)
	if status != "200" {
		return fmt.Errorf("the bound host answered %s: %s", status, body)
	}

	// 2. The leaked placeholder. A placeholder in a request body is scanned
	//    for scope exactly like one in a header, so it is blocked here too.
	status, reason, body, err := brokered.Probe(ctx, c, ws.ID, post("FOREIGN_URL"))
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "placeholder at foreign host %s: HTTP %s reason=%s\n", foreign, status, reason)
	if status != "403" || reason != "egress_denied" {
		return fmt.Errorf("the foreign host answered %s reason=%q: %s", status, reason, body)
	}

	copies, err := brokered.CountCredentialInEnv(ctx, c, ws.ID, secret)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "workspace environment holds %d copies of the real credential\n", copies)
	if copies != 0 {
		return fmt.Errorf("the real credential reached the workspace environment %d times", copies)
	}
	return nil
}
