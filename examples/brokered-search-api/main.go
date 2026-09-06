// Command brokered-search-api calls a search API that takes its key in the
// query string, from a workspace that never holds the key.
//
// It is the same shape as brokered-model-call with one difference that
// matters: the binding declares `substitution: query:key`, so the broker
// replaces the placeholder in the named query parameter and nowhere else. A
// provider that wants `?key=…` needed a hand-written proxy before; here it is
// one field on the binding.
//
// The example also shows the fail-closed half of that rule. Sending the same
// placeholder in an Authorization header instead of the declared parameter is
// refused, because guessing which occurrence the workspace meant is how a
// credential reaches a location nobody authorized.
//
//	go run ./cmd/remount standalone --listen 127.0.0.1:7443 --data ./remount-data
//	export REMOUNT_SERVER=http://127.0.0.1:7443
//	export REMOUNT_SEARCH_HOST=api.search.example SEARCH_API_KEY=...
//	go run ./examples/brokered-search-api
//
// Environment: REMOUNT_SERVER, REMOUNT_TOKEN, REMOUNT_SEARCH_HOST (required —
// there is no universal search API to default to), REMOUNT_SEARCH_KEY_ENV
// (default SEARCH_API_KEY), REMOUNT_SEARCH_PATH (default /search) and
// REMOUNT_FOREIGN_HOST (default api.openai.com).
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"remount.dev/remount/api"
	"remount.dev/remount/client"
	"remount.dev/remount/examples/brokered"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "brokered-search-api:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	host := os.Getenv("REMOUNT_SEARCH_HOST")
	if host == "" {
		return errors.New("REMOUNT_SEARCH_HOST is empty: name the search API this example should call")
	}
	path := brokered.EnvOr("REMOUNT_SEARCH_PATH", "/search")
	foreign := brokered.EnvOr("REMOUNT_FOREIGN_HOST", "api.openai.com")
	keyEnv := brokered.EnvOr("REMOUNT_SEARCH_KEY_ENV", "SEARCH_API_KEY")
	secret := os.Getenv(keyEnv)
	if secret == "" {
		return fmt.Errorf("%s is empty: export the provider key in the shell that runs this program", keyEnv)
	}

	c, err := client.New(client.Options{
		Server:    brokered.EnvOr("REMOUNT_SERVER", "http://127.0.0.1:7443"),
		Token:     os.Getenv("REMOUNT_TOKEN"),
		Principal: "a_brokered_search_api",
	})
	if err != nil {
		return err
	}
	defer c.Close()

	// `query:key` is the whole difference from a header binding. The control
	// plane validates the declaration once, when the binding is registered,
	// rather than failing one request at a time.
	bindingID := "b_example_search_" + brokered.Suffix()
	binding, err := c.CreateBinding(ctx, api.BindingSpec{
		ID: bindingID, Kind: "api_key", Secret: secret,
		Destinations: []string{host}, TTLSec: 900,
		Substitution: &api.BindingSubstitution{Location: "query", Name: "key"},
	})
	if err != nil {
		return err
	}
	workspaceID := ""
	defer func() { brokered.Cleanup(ctx, c, out, workspaceID, bindingID) }()
	fmt.Fprintf(out, "binding %s substitutes in query parameter %q for %s\n",
		binding.ID, binding.Substitution.Name, strings.Join(binding.Destinations, ","))

	ws, err := c.CreateWorkspace(ctx, api.WorkspaceSpec{
		Name:     "brokered-search-api",
		Bindings: []string{bindingID},
		Env: map[string]string{
			"SEARCH_API_KEY": "ref:" + bindingID,
			"SEARCH_URL":     "${REMOUNT_BROKER}/d/" + host + path,
			"FOREIGN_URL":    "${REMOUNT_BROKER}/d/" + foreign + path,
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

	// 1. The declared location. The workspace puts its placeholder in ?key=
	//    and the broker replaces exactly that parameter.
	status, _, body, err := brokered.Probe(ctx, c, ws.ID,
		`curl -sS -o body -D head -w '%{http_code}' -G --data-urlencode "q=remount" --data-urlencode "key=$SEARCH_API_KEY" "$SEARCH_URL"`)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "search through the broker: HTTP %s\n", status)
	if status != "200" {
		return fmt.Errorf("the bound host answered %s: %s", status, body)
	}

	// 2. The wrong location. A binding that declares a query parameter and
	//    finds its placeholder in a header is refused, not helpfully
	//    substituted in both.
	status, reason, body, err := brokered.Probe(ctx, c, ws.ID,
		`curl -sS -o body -D head -w '%{http_code}' -H "Authorization: Bearer $SEARCH_API_KEY" "$SEARCH_URL?q=remount"`)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "placeholder in a header instead of the query: HTTP %s reason=%s\n", status, reason)
	if status != "400" || reason != "egress_denied" {
		return fmt.Errorf("the misplaced placeholder answered %s reason=%q: %s", status, reason, body)
	}

	// 3. The leaked placeholder, refused before DNS.
	status, reason, body, err = brokered.Probe(ctx, c, ws.ID,
		`curl -sS -o body -D head -w '%{http_code}' -G --data-urlencode "q=remount" --data-urlencode "key=$SEARCH_API_KEY" "$FOREIGN_URL"`)
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
