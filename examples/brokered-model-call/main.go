// Command brokered-model-call calls an OpenAI-compatible model API from a
// workspace that never holds the key.
//
// It creates a binding — the credential plus the host, methods and paths it
// may be spent on — hands the workspace only the opaque placeholder, and then
// proves both halves of the claim: the call works against the bound host, and
// the same placeholder aimed at any other host is refused with a typed reason
// before a byte reaches the network.
//
// The credential is read from an environment variable of *this* process. It is
// never a command-line argument, never written to a file, and never placed in
// the workspace environment; the program checks that last one rather than
// asserting it.
//
//	go run ./cmd/remount standalone --listen 127.0.0.1:7443 --data ./remount-data
//	export REMOUNT_SERVER=http://127.0.0.1:7443 OPENAI_API_KEY=...
//	go run ./examples/brokered-model-call
//
// Environment: REMOUNT_SERVER, REMOUNT_TOKEN, REMOUNT_MODEL_HOST (default
// api.openai.com), REMOUNT_MODEL_KEY_ENV (default OPENAI_API_KEY) and
// REMOUNT_FOREIGN_HOST (default api.anthropic.com), the host used to prove the
// placeholder is useless off its binding.
package main

import (
	"context"
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
		fmt.Fprintln(os.Stderr, "brokered-model-call:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	host := brokered.EnvOr("REMOUNT_MODEL_HOST", "api.openai.com")
	foreign := brokered.EnvOr("REMOUNT_FOREIGN_HOST", "api.anthropic.com")
	keyEnv := brokered.EnvOr("REMOUNT_MODEL_KEY_ENV", "OPENAI_API_KEY")
	secret := os.Getenv(keyEnv)
	if secret == "" {
		return fmt.Errorf("%s is empty: export the provider key in the shell that runs this program", keyEnv)
	}

	c, err := client.New(client.Options{
		Server:    brokered.EnvOr("REMOUNT_SERVER", "http://127.0.0.1:7443"),
		Token:     os.Getenv("REMOUNT_TOKEN"),
		Principal: "a_brokered_model_call",
	})
	if err != nil {
		return err
	}
	defer c.Close()

	// The binding is the whole policy: this credential, for this host, for
	// these methods and paths, for fifteen minutes at a time. Nothing else in
	// the deployment may spend it.
	bindingID := "b_example_model_" + brokered.Suffix()
	binding, err := c.CreateBinding(ctx, api.BindingSpec{
		ID: bindingID, Kind: "api_key", Secret: secret,
		Destinations: []string{host}, TTLSec: 900,
		Methods: []string{"GET", "POST"}, PathPrefixes: []string{"/v1/"},
	})
	if err != nil {
		return err
	}
	workspaceID := ""
	defer func() { brokered.Cleanup(ctx, c, out, workspaceID, bindingID) }()
	fmt.Fprintf(out, "binding %s covers %s for GET,POST /v1/ (revision %d)\n",
		binding.ID, strings.Join(binding.Destinations, ","), binding.Revision)

	// The workspace receives the placeholder and the broker's own base URL.
	// ${REMOUNT_BROKER} is resolved on the node at materialize time, because
	// the broker's address is different on every node and after every move.
	placeholder := "ref:" + bindingID
	ws, err := c.CreateWorkspace(ctx, api.WorkspaceSpec{
		Name:     "brokered-model-call",
		Bindings: []string{bindingID},
		Env: map[string]string{
			"OPENAI_API_KEY":  placeholder,
			"OPENAI_BASE_URL": "${REMOUNT_BROKER}/d/" + host + "/v1",
			"FOREIGN_URL":     "${REMOUNT_BROKER}/d/" + foreign + "/v1",
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

	// 1. The bound host. The workspace sends its placeholder; the node
	//    substitutes the real credential at the network edge.
	status, _, body, err := brokered.Probe(ctx, c, ws.ID,
		`curl -sS -o body -D head -w '%{http_code}' -H "Authorization: Bearer $OPENAI_API_KEY" "$OPENAI_BASE_URL/models"`)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "model list through the broker: HTTP %s\n", status)
	if status != "200" {
		return fmt.Errorf("the bound host answered %s: %s", status, body)
	}

	// 2. The same credential on a POST body the binding's path prefix covers.
	status, _, body, err = brokered.Probe(ctx, c, ws.ID,
		`curl -sS -o body -D head -w '%{http_code}' -X POST -H "Authorization: Bearer $OPENAI_API_KEY" `+
			`-H 'Content-Type: application/json' `+
			`-d '{"model":"fake-model-small","messages":[{"role":"user","content":"ping"}]}' `+
			`"$OPENAI_BASE_URL/chat/completions"`)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "chat completion through the broker: HTTP %s\n", status)
	if status != "200" {
		return fmt.Errorf("the completion answered %s: %s", status, body)
	}

	// 3. The leaked placeholder. It is refused before DNS, with the reason a
	//    harness matches on rather than prose it would have to parse.
	status, reason, body, err := brokered.Probe(ctx, c, ws.ID,
		`curl -sS -o body -D head -w '%{http_code}' -H "Authorization: Bearer $OPENAI_API_KEY" "$FOREIGN_URL/models"`)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "placeholder at foreign host %s: HTTP %s reason=%s\n", foreign, status, reason)
	if status != "403" || reason != "egress_denied" {
		return fmt.Errorf("the foreign host answered %s reason=%q: %s", status, reason, body)
	}

	// 4. The claim this example exists to make: the real key is not in the
	//    workspace. Counting occurrences of the value the program holds is a
	//    stronger check than trusting the pattern of a provider's key format.
	copies, err := brokered.CountCredentialInEnv(ctx, c, ws.ID, secret)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "workspace environment holds %d copies of the real credential\n", copies)
	if copies != 0 {
		return fmt.Errorf("the real credential reached the workspace environment %d times", copies)
	}

	// 5. The audit answers the question in one call.
	records, err := c.CredentialEvents(ctx, client.CredentialFilter{WS: ws.ID, Binding: bindingID, Since: 1})
	if err != nil {
		return err
	}
	substituted, blocked := 0, 0
	for _, record := range records {
		switch record.Decision {
		case "substituted":
			substituted++
		case "leak_blocked":
			blocked++
		}
	}
	fmt.Fprintf(out, "audit for %s: %d substituted, %d leak_blocked\n", bindingID, substituted, blocked)
	if substituted < 2 || blocked < 1 {
		return fmt.Errorf("the audit recorded %d substitutions and %d blocks", substituted, blocked)
	}
	return nil
}
