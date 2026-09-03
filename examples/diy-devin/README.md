# diy-devin

Three reference clients for the Agent HTTP API (`docs/api.md`) and one
trigger, together a minimal "Devin": start an agent on a task from anywhere,
watch it, answer it, look at its diff, come back tomorrow.

| File | What |
|---|---|
| `remount_agent.py` | a standard-library Python CLI: `run`, `watch`, `say`, `approve`, `diff`, … ; SSE transcript that resumes across disconnects |
| `remount.ts` | a dependency-free typed client for Node/Deno/Bun/browsers with an `AsyncGenerator` transcript |
| `web/index.html` | one static page: create or open an agent, follow the transcript over WebSocket, answer approvals, send follow-ups, open the preview and diff |
| `github/remount-issue-agent.yml` | a GitHub Actions workflow: labeling an issue `agent` starts one, every comment is a follow-up, signed with the webhook secret |

```sh
# The standalone has no token; --cors names the one page origin allowed to
# call it from a browser. Use `remount server --token … --cors …` for anything
# that is not your own laptop.
go run ./cmd/remount standalone --data ./data --cors http://localhost:8000
export REMOUNT_SERVER=http://127.0.0.1:7443
./examples/diy-devin/remount_agent.py run --recipe codex --repo https://github.com/you/app -- "add a health endpoint"
./examples/diy-devin/remount_agent.py say ag_… "and a test for it"
python3 -m http.server -d examples/diy-devin/web 8000     # then open http://localhost:8000/#ag_…
```

The same agent is reachable from all four at once: the CLI's `remount agent
watch`, the Python script, the page and a comment on the issue all read one
durable transcript and write one inbox, so a conversation started at a laptop
continues from a phone.

None of these hold a credential the workspace could see. The token is the
API bearer; provider keys stay behind the node's broker and the page's cookie
is `HttpOnly`, minted by `POST /v1/session`, and only ever accepted on the
preview proxy and the `/a/{id}` link, never on a route that reads a file,
opens a terminal or wakes an agent.
