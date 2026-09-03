// Command agentloop is the smallest honest agent harness on Remount.
//
// It creates a workspace, then loops: ask the model for one shell command,
// run it in the workspace, feed the output back, repeat until the model says
// DONE. The model call itself is made from inside the workspace by exec'ing
// curl, so the workspace's placeholder key is substituted by the broker and
// this program never touches a real credential either.
//
// Prerequisites: a running server with a binding named b_openai whose
// destination is api.openai.com (see docs/harness-integration.md).
//
//	export REMOUNT_SERVER=http://127.0.0.1:7443
//	go run ./examples/agentloop "list the files here, then create hello.txt containing hi"
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"remount.dev/remount/api"
	"remount.dev/remount/client"
)

const system = `You are an agent operating a Unix shell. Reply with exactly one line, either:
CMD: <a single shell command to run next>
or, only after you have seen the output you needed:
DONE: <one sentence summary>
Never send both. Never explain. Never use markdown.`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: agentloop \"<task>\"")
		os.Exit(2)
	}
	task := os.Args[1]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// One connection to the relay; the client redials on its own.
	server := envOr("REMOUNT_SERVER", "http://127.0.0.1:7443")
	c, err := client.New(client.Options{
		Server:    server,
		Token:     os.Getenv("REMOUNT_TOKEN"),
		Principal: "a_agentloop",
	})
	must(err)
	defer c.Close()

	// The workspace holds a reference to the binding, never the secret.
	ws, err := c.CreateWorkspace(ctx, api.WorkspaceSpec{
		Name:     "agentloop",
		Bindings: []string{"b_openai"},
		Env: map[string]string{
			"OPENAI_API_KEY":  "ref:b_openai",
			"OPENAI_BASE_URL": "${REMOUNT_BROKER}/d/api.openai.com/v1",
		},
	})
	must(err)
	ws, err = c.WaitClaimed(ctx, ws.ID)
	must(err)
	fmt.Fprintf(os.Stderr, "workspace %s on node %s\n", ws.ID, ws.Node)

	// The conversation is an ordinary chat transcript.
	messages := []map[string]string{
		{"role": "system", "content": system},
		{"role": "user", "content": task},
	}

	for step := 1; step <= 12; step++ {
		reply, err := chat(ctx, c, ws.ID, messages)
		must(err)
		messages = append(messages, map[string]string{"role": "assistant", "content": reply})
		fmt.Fprintf(os.Stderr, "[%d] model: %s\n", step, reply)

		// Models sometimes send a CMD line and a DONE line together. Run the
		// first CMD if there is one; finish only when DONE stands alone.
		cmd, done := parse(reply)
		if cmd == "" && done != "" {
			fmt.Println(done)
			return
		}
		if cmd == "" {
			messages = append(messages, map[string]string{"role": "user", "content": "Reply with exactly one line: CMD: <command> or DONE: <summary>."})
			continue
		}

		// Run the command in the workspace. IdempotencyKey means a retry
		// after a dropped connection resumes this exact process rather than
		// starting a second one.
		s, err := c.Exec(ctx, api.SessionOpenRequest{
			WS:             ws.ID,
			Kind:           api.SessionExec,
			Program:        []string{"sh", "-c", cmd},
			TimeoutSec:     60,
			IdempotencyKey: fmt.Sprintf("agentloop-%s-%d", ws.ID, step),
		})
		must(err)
		var out strings.Builder
		for ch := range s.Chunks() {
			if ch.Stream == api.StreamStdout || ch.Stream == api.StreamStderr {
				out.Write(ch.Data)
			}
		}
		exit := s.Exit()
		text := out.String()
		if len(text) > 4000 {
			text = text[:4000] + "\n[truncated]"
		}
		fmt.Fprintf(os.Stderr, "[%d] exit %d\n", step, exit.Code)
		messages = append(messages, map[string]string{
			"role":    "user",
			"content": fmt.Sprintf("exit code %d\n%s", exit.Code, text),
		})
	}
	fmt.Fprintln(os.Stderr, "step limit reached")
	os.Exit(1)
}

// chat asks the model for the next step. The request is made by curl inside
// the workspace: the placeholder in OPENAI_API_KEY is only ever swapped for
// the real key at the broker, for api.openai.com, and audited as cred.used.
func chat(ctx context.Context, c *client.Client, wsID string, messages []map[string]string) (string, error) {
	body, err := json.Marshal(map[string]any{
		"model":      envOr("AGENTLOOP_MODEL", "gpt-4o-mini"),
		"messages":   messages,
		"max_tokens": 300,
	})
	if err != nil {
		return "", err
	}
	// The body goes in via a file so the shell never sees the JSON.
	if err := c.WriteFile(ctx, wsID, ".agentloop/req.json", body, 0o600); err != nil {
		return "", err
	}
	stdout, stderr, exit, err := c.Run(ctx, wsID, "sh", "-c",
		`curl -sS "$OPENAI_BASE_URL/chat/completions" -H "Content-Type: application/json" `+
			`-H "Authorization: Bearer $OPENAI_API_KEY" --data-binary @.agentloop/req.json`)
	if err != nil {
		return "", err
	}
	if exit.Code != 0 {
		return "", fmt.Errorf("curl exit %d: %s", exit.Code, stderr)
	}
	var resp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(stdout, &resp); err != nil {
		return "", fmt.Errorf("bad response: %w: %s", err, stdout)
	}
	if resp.Error != nil {
		return "", fmt.Errorf("api: %s", resp.Error.Message)
	}
	if len(resp.Choices) == 0 {
		return "", fmt.Errorf("no choices in %s", stdout)
	}
	return strings.TrimSpace(resp.Choices[0].Message.Content), nil
}

// parse extracts the first CMD line and any DONE line from a reply.
func parse(reply string) (cmd, done string) {
	for _, line := range strings.Split(reply, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case cmd == "" && strings.HasPrefix(line, "CMD:"):
			cmd = strings.TrimSpace(strings.TrimPrefix(line, "CMD:"))
		case strings.HasPrefix(line, "DONE:"):
			done = strings.TrimSpace(strings.TrimPrefix(line, "DONE:"))
		}
	}
	return cmd, done
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "agentloop:", err)
		os.Exit(1)
	}
}
