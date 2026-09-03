# Tutorial: from zero to a moved workspace

This walkthrough takes about twenty minutes. It uses one machine for most of
it and a second machine for the move. Every command below is the real CLI; if
something differs, `remount help` is the authority.

## 1. Build

Remount is one Go binary with no runtime dependencies.

```sh
git clone https://github.com/remount-dev/remount
cd remount
go build -o remount ./cmd/remount
./remount version
```

```
remount dev
```

## 2. Start standalone and run a command

`remount standalone` runs the control plane, the relay and one node in a single
process. It needs no token and no account. It is how you try things out.

```sh
./remount standalone --data ./data &
```

```
remount standalone: server http://127.0.0.1:7443 (no token), node n_06g67jsx1an8pfy8r9exe5tgyw
  export REMOUNT_SERVER=http://127.0.0.1:7443
```

Export the server address so the client commands find it, then create a
workspace. A workspace is the agent's computer: a directory plus whatever
processes you start in it.

```sh
export REMOUNT_SERVER=http://127.0.0.1:7443
WS=$(./remount ws create --name hello)
echo $WS
```

```
ws_06g67jv89e2jx29427b99pq8jg
```

`ws create` waits until a node has claimed the workspace. Run something in it:

```sh
./remount exec $WS -- sh -c 'echo hello from $(hostname); pwd'
```

```
session s_06g67k1hy1rf4fb2c78tbawzdr
hello from mac-mini.local
/Users/you/remount/data/node/ws/ws_06g67jv89e2jx29427b99pq8jg
```

The exit code of the command becomes the exit code of `remount exec`. Stdout and
stderr come through on the same streams you would expect.

## 3. An interactive shell

`remount sh` opens a session of kind `pty`. A pty session has a real terminal
behind it, so line editing, colours, `top` and resizing all work. The default
program is `sh -l`; pass one to override it.

```sh
./remount sh $WS
```

```
session s_06g67k3287pz8s60eahcr99gj0
$ ls
$ echo $HOME
/Users/you/remount/data/node/ws/ws_06g67jv89e2jx29427b99pq8jg
$ exit
```

`HOME` is the workspace root, so dotfiles and tool caches stay inside it and
travel with it. Terminal resizes are forwarded, and the shell exits when you
type `exit`.

## 4. The filesystem

Every filesystem operation is served by the node and jailed to the workspace
root. Paths are workspace-relative; `/src/a.go` and `src/a.go` name the same
file.

```sh
./remount fs write $WS src/notes.md < README.md
./remount fs ls $WS src
./remount fs stat $WS src/notes.md
```

```
- 0644       5321 2026-09-02 13:31 notes.md
```

Search runs on the node, not on your laptop. Over a slow link that is the
difference between one round trip and thousands. Binary files and files over
8 MiB are skipped, and `.git` is not searched.

```sh
./remount fs search $WS 'movable value' src
./remount fs search --glob '*.md' --max 5 $WS 'workspace'
```

```
/src/notes.md:11:The agent's computer is a **workspace**, and Remount treats it as a movable value
```

Edits are atomic find and replace. A non-`--all` edit must match exactly once,
or nothing is written and the command tells you why.

```sh
./remount fs edit $WS src/notes.md 'movable value' 'movable thing'
./remount fs edit $WS src/notes.md 'nothing here' 'x'
```

```
1 replacement(s)
remount: conflict: edit 0: old string not found
```

## 5. Reconnect without losing output

Start a command that runs for a while, then kill the client half way through.

```sh
./remount exec $WS -- sh -c 'for i in $(seq 1 60); do echo line $i; sleep 1; done'
```

```
session s_06g67kajt02bz3htt6zdas8z9w
line 1
line 2
line 3
^C
```

The process is still running on the node. Session output is an append-only log
with a sequence number per chunk, and your client was only a cursor over it.
Reattach from the beginning and everything replays, then the live tail follows:

```sh
./remount attach $WS s_06g67kajt02bz3htt6zdas8z9w --from 0
```

```
line 1
line 2
line 3
line 4
line 5
...
```

Pass a higher `--from` to skip what you already saw. If the requested sequence
has aged out of the node's buffer you get an explicit marker, never a silent
gap:

```
[remount: output seq 0-311 elided]
```

## 6. A second machine, and a move

On another computer, enroll it as a node. Only outbound access to the server is
needed; the node exposes no inbound listener. Labels are how you address groups
of machines later.

```sh
./remount up --server http://server.example:7443 --label zone=gpu --label owner=you
```

```
time=... level=INFO msg="remount node starting" id=n_06g67csz3wmes8mengggemdaa4 server=http://server.example:7443
time=... level=INFO msg="uplink established" node=n_06g67csz3wmes8mengggemdaa4 server=remount
```

Back on the first machine, confirm both nodes are online:

```sh
./remount nodes
```

```
ID                              ONLINE  OS/ARCH       CPU  MEM_MiB  BACKENDS  PROTOCOL       LABELS                      WORKSPACES
n_06g67csz3wmes8mengggemdaa4    true    linux/amd64   32   131072   process   v1,authz-push  map[owner:you zone:gpu]     0
n_06g67jsx1an8pfy8r9exe5tgyw    true    darwin/arm64  10   32768    process   v1,authz-push  map[standalone:true]        1
```

Now move the workspace. The current node snapshots the filesystem, the control
plane re-queues the workspace with the new placement, and a matching node
claims it and restores the snapshot.

```sh
./remount ws move $WS --label zone=gpu
```

```
moved: node=n_06g67csz3wmes8mengggemdaa4 gen=2 restored_from=art_sha256:6768a59fbdbb1c26f4…
```

Your files are there:

```sh
./remount fs read $WS src/notes.md | head -2
./remount exec $WS -- sh -c 'nproc; hostname'
```

```
# Remount
32
gpu-box-01
```

Running processes do not move; only files do. The generation number bumped
from 1 to 2, which is how every stale grant and stale session from the old
node is refused.

`ws move` also accepts `--cpu`, `--mem`, `--backend` and `--node` to pin to a
specific node id.

You can capture the tree without moving it. The default is intentionally a live
snapshot: concurrent writes may be observed and the artifact does not replace
failover state. Request an authoritative checkpoint when recovery must use it.

```sh
./remount ws snapshot $WS
# ... consistency=live, authoritative=false

./remount ws snapshot $WS --authoritative
# ... consistency=quiesced, authoritative=true
```

The authoritative path fences Remount-managed execution, uploads the blob, and
commits the digest for the current generation before it reports success.

## 7. Sleep and wake

A sleeping workspace has no node. Its last snapshot is kept, its timers are
durable, and it costs only storage.

```sh
./remount ws sleep $WS --after 2m
./remount ws get $WS | grep -E '"state"|"last_snapshot"'
```

```
  "state": "paused",
  "last_snapshot": "art_sha256:e61c30d7cd3e20273e27443d51ab6ff23543b5bd8af49fbc9f32f53bd4913dfc",
```

While it sleeps, it is unreachable by design. Two minutes later the timer fires,
an eligible node claims it, and it is back with its files intact. Wake it
early with `ws wake`.

You can also sleep until something happens in the world:

```sh
./remount ws sleep $WS --on github.pr.merged
```

Any HTTP client can post that event. The server requires the bearer token for
this endpoint, so in standalone mode use `remount server` with `--token` to try
it, or wake with `ws wake` instead.

```sh
curl -X POST http://server.example:7443/v1/events \
  -H "Authorization: Bearer $REMOUNT_TOKEN" \
  -d '{"type":"github.pr.merged","payload":{"pr":42}}'
```

The workspace wakes within a second of the post.

```sh
./remount timers
```

```
[
  {
    "id": "t_06g67es8g2qzp9b0hhy1j99r2g",
    "ws": "ws_06g67jv89e2jx29427b99pq8jg",
    "on": "github.pr.merged",
    "action": "resume",
    "fired": true,
    "created_at": 1788379668837
  }
]
```

## 8. Secrets the workspace never holds

Stop the standalone process and write a bindings file. A binding is a secret,
the destinations it may be sent to, and the placeholder the workspace will hold
instead. The secret can be an `$ENV` reference so the file itself stays clean.

```sh
cat > bindings.json <<'EOF'
[
  {
    "id": "b_openai",
    "secret": "$OPENAI_API_KEY",
    "destinations": ["api.openai.com"],
    "placeholder": "sk-proj-REMOUNT-PLACEHOLDER-NOT-A-REAL-KEY",
    "ttl_sec": 900
  }
]
EOF
export OPENAI_API_KEY=sk-proj-...
./remount standalone --data ./data --bindings ./bindings.json --allow api.openai.com &
```

Create a workspace that references the binding. Values of the form
`ref:<binding>` become the placeholder, and `${REMOUNT_BROKER}` becomes the
per-workspace broker address.

```sh
WS=$(./remount ws create --name live \
  --binding b_openai \
  --env OPENAI_API_KEY=ref:b_openai \
  --env OPENAI_BASE_URL='${REMOUNT_BROKER}/d/api.openai.com/v1')
```

What the workspace sees:

```sh
./remount exec $WS -- sh -c 'echo $OPENAI_API_KEY; echo $OPENAI_BASE_URL'
```

```
sk-proj-REMOUNT-PLACEHOLDER-NOT-A-REAL-KEY
http://127.0.0.1:53120/d/api.openai.com/v1
```

The call still works, because the broker substitutes the real key on the way
to the bound host:

```sh
./remount exec $WS -- sh -c 'curl -s "$OPENAI_BASE_URL/chat/completions" \
  -H "Content-Type: application/json" -H "Authorization: Bearer $OPENAI_API_KEY" \
  -d "{\"model\":\"gpt-4o-mini\",\"messages\":[{\"role\":\"user\",\"content\":\"Reply with exactly: remount works\"}]}"' \
  | grep -o '"content": "[^"]*"'
```

```
"content": "remount works"
```

Send the same placeholder anywhere else and the broker refuses before a byte
leaves the machine. An unlisted host with no credential is refused too.

```sh
./remount exec $WS -- sh -c 'curl -s -o /dev/null -w "%{http_code}\n" \
  "$REMOUNT_BROKER/d/api.anthropic.com/v1/messages" -H "Authorization: Bearer $OPENAI_API_KEY"'
./remount exec $WS -- sh -c 'curl -s -o /dev/null -w "%{http_code}\n" "$REMOUNT_BROKER/d/example.com/"'
```

```
403
403
```

Nothing on the workspace's disk or in its environment ever contained the real
key. The audit for all three requests is in the event log.

### Sharing a workspace, and taking it back

A workspace's ACL names who else may use it. Only the owner (or an
administrator) changes it, and every change advances the workspace's
authorization revision so that every grant already handed out stops verifying.

```sh
./remount ws acl $WS --writer bob --reader carol
./remount ws acl $WS --reader carol            # bob is out
```

Bob is refused a new grant immediately. The node learns the new revision on
its next lease renew, at most a third of the lease later (10 s by default),
and ends bob's live sessions with `exit{reason: "revoked"}`. Carol's shell
keeps running; her client fetches a fresh grant the next time it needs one.
In standalone mode every client is the same local administrator, so try this
against `remount server` with an authenticator that tells principals apart.

## 9. Watch everything

Transactional resource rows are the source of lifecycle/recovery truth. The
event log is the ordered audit and observation history. Follow it live:

```sh
./remount events --follow --ws $WS
```

```
     1 13:24:18.402 ws.created         ws_06g67jv89e2jx29427b99pq8jg  andrewgao      {"name":"live",...}
     4 13:24:18.551 ws.claiming        ws_06g67jv89e2jx29427b99pq8jg                 {"gen":1,"restore_from":""}
     6 13:24:18.702 ws.claimed         ws_06g67jv89e2jx29427b99pq8jg                 {"gen":1,"restore_from":""}
     8 13:24:31.102 s.opened           ws_06g67jv89e2jx29427b99pq8jg  andrewgao      {"kind":"exec","program":["sh","-c","curl ..."],"s":"s_06g67k…"}
     9 13:24:31.461 cred.used          ws_06g67jv89e2jx29427b99pq8jg  andrewgao      {"binding":"b_openai","decision":"substituted","host":"api.openai.com","method":"POST","path":"/v1/chat/completions","status":200}
    12 13:24:59.942 egress.denied      ws_06g67jv89e2jx29427b99pq8jg  andrewgao      {"binding":"b_openai","decision":"leak_blocked","host":"api.anthropic.com","reason":"placeholder for b_openai sent to api.anthropic.com"}
    15 13:24:59.983 egress.denied      ws_06g67jv89e2jx29427b99pq8jg  andrewgao      {"decision":"denied","host":"example.com","reason":"destination not in bindings or allow list"}
```

Drop `--ws` to see the whole deployment, `--from N` to start at a sequence, and
`--json` for one JSON object per line.

## Where to go next

- [harness-integration.md](harness-integration.md) runs Claude Code, Codex and OpenCode inside a workspace with the same secret-blind setup.
- [operations.md](operations.md) covers tokens, TLS, backends, leases and backups for a real deployment.
- [design.md](design.md) explains why each of the pieces you just used is shaped the way it is.
