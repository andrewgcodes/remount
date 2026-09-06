# Tutorial: from zero to a moved workspace

This walkthrough takes about twenty minutes. It uses one machine for most of
it and a second machine for the move. Every command below is the real CLI; if
something differs, `remount help` is the authority.

## 1. Build

The core is one Go binary; selected backends and harness recipes have additional
runtime prerequisites. Use an accessible checkout and the Go version required
by `go.mod` (currently 1.27.1). Public installation has separate
[release prerequisites](releases.md).

```sh
git clone https://github.com/andrewgcodes/remount.git
cd remount
make build
./remount version
```

```
remount <source-revision>
```

## 2. One command

If you have a provider key in your environment, the whole thing is one
command. `remount run` finds nothing at `http://127.0.0.1:7443`, starts
`remount standalone` in the background with a data directory under
`~/.local/share/remount` (or `$XDG_DATA_HOME/remount`, or `$REMOUNT_DATA`),
writes a bindings file that references your keys by variable name — never by
value — and picks the binding the recipe consumes.

```sh
export OPENAI_API_KEY=sk-...
./remount run opencode --dir . -- 'Create GREETING.txt containing hello.'
```

```
started remount standalone in the background (pid 168858, data /home/you/.local/share/remount, log .../standalone.log); stop it with: kill 168858
provider bindings from the environment: b_openai ($OPENAI_API_KEY)
using binding b_openai ($OPENAI_API_KEY) for opencode
workspace ws_06g6d1z9g849pkqcxxfy7z99m4 created
installing opencode
session s_06g6d1za3qk2nvrsjq5y3jwn94 (Ctrl-C detaches; reattach with: remount attach ws_… s_…)
```

The next `remount run` or `remount resume` reuses that standalone. Everything
in the rest of this tutorial works against it too; skip the `standalone` and
`export` lines below if you went this way. `REMOUNT_AUTOSTART=0` disables the
background start, and any explicit `--server` / `REMOUNT_SERVER` or token
does as well: the CLI only ever starts a server at the default local address.
The standalone inherits the environment of the `remount run` that started it,
so after changing a key, stop it (the pid is in `standalone.pid` in the data
directory) and let the next run start a fresh one.

## 3. Start standalone by hand and run a command

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
hello from your-mac.local
/Users/you/remount/data/node/ws/ws_06g67jv89e2jx29427b99pq8jg
```

The exit code of the command becomes the exit code of `remount exec`. Stdout and
stderr come through on the same streams you would expect.

## 4. An interactive shell

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

## 5. The filesystem

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

## 6. Reconnect without losing output

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

## 7. A second machine, and a move

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

Control durably orders each release attempt before it reaches the node. Exact
retries are idempotent, but a delayed request from an older aborted move,
sleep, or destroy cycle cannot fence a workspace that has already been
restored.

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

On this process/Docker filesystem move, running processes do not move. The generation number bumped
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

### Working from a local checkout

A workspace can start as a copy of a directory on your machine, and changes
can flow both ways. Packing honors `.gitignore` and `.remountignore`, skips
`node_modules`, `.venv`, `target`, `dist` and `__pycache__`, and includes
`.git` so the agent can commit (pass `--include-git=false` to leave it out).

```sh
WS=$(./remount ws create --dir ~/src/myapp --json | jq -r .id)

# after editing locally: overlay the changed tree onto the workspace
./remount push $WS --dir ~/src/myapp

# after the agent has worked: bring its tree back, refusing to clobber
# uncommitted local changes unless you say so
./remount pull $WS --dir ~/src/myapp
./remount pull $WS --dir ~/src/myapp --force
```

`push` never deletes files the archive does not name, and every file lands
atomically; `pull` never deletes local files either and reports the ones it
left in place.

Applications using a workspace as several independent subtrees can target an
uploaded overlay at one existing directory through the public SDK. Python uses
`apply_tar(..., path="state")`; Go uses `ApplyTarAt(..., "state")`. The node
resolves the destination through the workspace jail and keeps the existing
root-overlay behavior when the path is empty.

### Bases: a prepared workspace many runs start from

Once a workspace has the toolchain installed and the repo cloned, pin its
snapshot under a name. Every member of your tenant can start from it, and the
artifact is exempt from garbage collection until the base is removed.

```sh
./remount ws snapshot $WS --as-base golden
./remount base ls
NEW=$(./remount ws create --base golden --json | jq -r .id)
./remount base rm golden        # workspaces already created from it are unaffected
```

A base names an artifact, not a workspace: re-snapshotting `$WS` does not move
`golden`. To update it, `base rm` and snapshot `--as-base` again.

The third way to seed a workspace is a repository. The node clones it through
the broker before the workspace becomes `claimed`, using a binding's
placeholder for a private repository (a public one needs no binding), and
emits `repo.cloned` with the commit it checked out:

```sh
NEW=$(./remount ws create --repo github.com/acme/app@main --repo-depth 1 --json | jq -r .id)
./remount exec $NEW -- git log -1 --oneline
./remount events --ws $NEW | grep repo.cloned
```

Inside the workspace `git fetch` and `git push` route through
`$REMOUNT_GIT_CONNECTOR` and reach only that repository.

## 8. Sleep and wake

A sleeping workspace has no node. Its last snapshot is kept, its timers are
durable, and it retains storage rather than a workspace compute assignment.
The provider can still charge for an idle node until that node is scaled down
or terminated.

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

### Hold a workspace until a deadline

`ws sleep` schedules a resume. The opposite need — keep this workspace claimed
because a background job, a download, a code server or an approval wait is
still running, then put it to sleep if nobody extends the deadline — is
`ws lease`.

The distinction that matters is where the deadline lives. A `sleep` in the
process that started the job is only as durable as that process; a deploy,
a scale-down, an OOM kill or a partition takes it with it and leaves the
workspace running. `ws lease` writes the deadline into the control plane's own
durable state, so it survives the client, the node, and a restart of the
control plane itself.

```sh
LEASE=$(./remount ws lease $WS --max 20m --min 30s --reason background_job)
./remount ws lease get $WS
```

```
lease=wl_06g792n9ap91y3d82yztbdmyk4 live=true on_expiry=sleep max_alive_until=2026-09-06T02:27:39Z
lifecycle deadline: sleep at 2026-09-06T02:27:39Z (in 19m58s, source=lease)
```

| Command | What it does |
|---|---|
| `ws lease WS --max D [--min D] [--on-expiry sleep\|destroy] [--reason R]` | takes the hold. `--max` is the hard deadline, `--min` is the earliest the idle policy may act. One hold per workspace: a second call replaces the first |
| `ws lease renew WS LEASE --extend D` | moves the hard deadline to `D` from **now**, not from the original grant |
| `ws lease cancel WS LEASE` | drops the hold. The workspace stays `claimed` under its idle policy, if it has one |
| `ws lease get WS` | the hold and the derived deadline |
| `ws idle-policy WS [--sleep-after D] [--destroy-after D]` | the no-work cleanup rule. Both omitted removes the policy |
| `ws mark-idle WS [--reason R]` | starts the idle clock |
| `ws mark-active WS [--reason R]` | stops it and clears any pending idle deadline |

Durations are Go durations: `30s`, `20m`, `2h`. All of these accept `--json`
and `--idem` for a stable idempotency key.

**Activity is explicit.** `ws mark-active`, `ws lease` and `ws lease renew` are
the activity signals. Session traffic is not. That cuts both ways and both are
deliberate: a workspace running `sleep 300` with nobody renewing still sleeps
at its deadline, and a workspace with an idle shell attached and nothing to do
is still idle. Inferring activity from a session open would also make every
`s.open` a durable control-plane write on a hot path. Mark activity each turn.

Watch a hold expire. Take a short one, close the terminal, and come back:

```sh
./remount ws lease $WS --max 30s --reason background_job
```

Thirty seconds later, from any process at all:

```sh
./remount ws get $WS | grep '"state"'
./remount events --ws $WS | tail -4
```

```
  "state": "paused",
   126 19:11:01.270 ws.lifecycle.expired  {"action":"sleep","at":...,"source":"lease","timer":"t_lc_ws_..."}
   127 19:11:01.270 ws.lease.expired      {"lease":"wl_...","reason":"deadline"}
   134 19:11:01.353 ws.paused             {"reason":"lifecycle_deadline_expired","snapshot":"art_sha256:..."}
```

What the expiry does is exactly what `ws sleep` does — the same release,
checkpoint, fencing and volume detach — with two additions. The node sends
`SIGTERM`, waits `LifecycleGrace` (five seconds by default) and then `SIGKILL`,
joining every session before the checkpoint is taken; on Windows a job object
has no distinct polite signal, so the grace degrades to immediate termination
and the recorded reason is the same. And the session's exit chunk carries
`reason: "lifecycle_deadline_expired"`, so replaying the log tells you a policy
decision ended the work rather than leaving you to correlate timestamps against
another stream:

```sh
./remount ws wake $WS
./remount attach $WS $SESSION --from 0
```

A hold is a filesystem checkpoint, not a memory checkpoint. Files survive;
processes do not. And a hold that expires says only that nobody extended it, so
no wake is armed — use `ws sleep --after` or `--on` when you know when the work
should resume.

The idle policy is the other half. Its clock only runs while the workspace is
marked idle, which is why `ws mark-idle` exists as a separate call:

```sh
./remount ws idle-policy $WS --sleep-after 15s
./remount ws mark-idle $WS --reason turn_settled
```

```
lifecycle deadline: sleep at 2026-09-06T02:09:24Z (in 15s, source=idle)
```

`ws mark-active` before it fires clears the deadline, and the workspace stays
`claimed`. When both a hold and a policy exist, the pending deadline is the
earlier of the two, floored by the hold's `--min`; `ws get` publishes it as
`lifecycle_deadline` with a `source` of `lease` or `idle`, so a client never
has to read `remount timers` to know its position.

Refusals are typed, and each names what to do about it:

| Code and reason | When |
|---|---|
| `conflict` / `workspace_not_ready` | `ws lease` against a workspace that is not `claimed` |
| `conflict` / `lifecycle_deadline_expired` | renewing after the deadline fired or the hold was cancelled — take a fresh hold |
| `conflict` / `generation_mismatch` | renewing after `ws move` re-placed the workspace — the hold named the old placement |
| `resource_exhausted` / `quota_exceeded` | the tenant's concurrent-hold limit, alongside a `ws.hold.max_reached` event |

`--max` and every idle-policy duration are bounded by the deployment's maximum
hold (24 hours by default), and a tenant may pin at most 256 workspaces awake
at once by default. If the release underneath an expiry fails, it is retried
with backoff and then reported as `ws.lifecycle.expiry_failed` with the attempt
count and the error; the workspace is degraded and operator-actionable rather
than quietly still running.

A `ws move` or `ws destroy` retires the pending deadline in its own
transaction, recorded as `ws.lease.expired` with the reason `moved` or
`destroyed`. A re-placement you did not ask for — a lost node, an expired claim
lease — keeps the deadline, because dropping it there would leak the workspace
the hold exists to reclaim.

`examples/long-running-autosleep` runs this whole cycle in one program against
a local standalone server. The design record is
[ADR 0090](adr/0090-durable-workspace-leases-and-idle-policy.md) and the
normative surface is `spec/PROTOCOL.md` §5.5.

## 9. Secrets the workspace never holds

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

### Run a coding agent with one command

`remount run` does the create, the harness install, the provider wiring and
the launch in one step. `--binding b_openai` picks the `openai` preset by
name, so the harness gets `OPENAI_API_KEY=ref:b_openai` and an
`OPENAI_BASE_URL` that resolves to the broker at run time. Allow the hosts the
recipe installs from when you start standalone:

```sh
./remount standalone --data ./data --bindings ./bindings.json \
  --allow api.openai.com --allow registry.npmjs.org --allow models.dev &
mkdir -p proj && echo hello > proj/README.md
./remount run opencode --dir proj --binding b_openai --model openai/gpt-4o-mini \
  -- 'Create a file named GREETING.txt containing exactly the word hello. Do nothing else.'
```

```
workspace ws_06g6c49ka10ptnrcpzk8an8298 created
installing opencode
> build · gpt-4o-mini
← Write GREETING.txt
I have created a file named **GREETING.txt** containing the word "hello."
```

Ctrl-C detaches and leaves the agent running; `remount attach WS SID` picks the
output back up from the start. `--detach` skips the attach and prints the two
ids and the attach line. The run is in the log as `run.started` and
`run.finished`, with a hash of the task rather than the task:

```
    37 06:59:58.574 run.started   ws_06g6c49ka10…  local-user  {"auth":"api_key","recipe":"opencode","s":"s_06g6c49k…","sandbox":"workspace-write","task_hash":"56d7ee2b45f68478"}
    45 07:01:13.157 run.finished  ws_06g6c49ka10…  local-user  {"exit":0,"recipe":"opencode","s":"s_06g6c49k…","signal":""}
```

`remount run custom --binding b_openai -- python agent.py` runs anything that
honors `OPENAI_API_KEY` and `OPENAI_BASE_URL`; `remount binding preset ls`
lists the other providers. [harness-integration.md](harness-integration.md)
has the full recipe table and the two kinds of auth.

### Hand off a conversation, and a night's worth of tasks

`remount handoff` moves the checkout you are in *and* the harness's
conversation into a workspace and keeps it going. The recipe is detected from
the state under your home directory; the tree lands at the same absolute path
inside the workspace when the harness keys its history on it, which is why
this one wants a `docker` node:

```sh
cd ~/proj
remount handoff --binding b_anthropic --task "carry on with the failing test"
```

```
uploaded 212 files (1840233 bytes) as art_sha256:…
handed off /home/me/proj (212 files) with claude state .claude,.claude.json to ws_06g6cq… at /home/me/proj
attach: remount attach ws_06g6cq… s_06g6cq…
resume later: remount resume ws_06g6cq…
bring it back: remount pull ws_06g6cq…
```

Later, from any machine, `remount resume ws_06g6cq…` attaches if the agent is
still going, wakes the workspace if it went to sleep, and otherwise starts the
harness's own resume command in the same conversation.

A list of tasks runs the same way, one after another in one workspace, with
the progress kept on the server so the run survives the workspace sleeping
and moving:

```sh
cat > tonight.txt <<'TASKS'
# one task per line; \ continues a line
Fix the flaky TestReconnect and make the suite green.
Update CHANGELOG.md for the 0.4 release.
Open a PR titled "0.4" with a summary of the changes.
TASKS
remount run codex --dir proj --binding b_openai --queue tonight.txt --sleep-after 20m
```

Each task is a `run` session; between tasks the workspace checkpoints (or here
sleeps for twenty minutes on a durable timer). A task that exits non-zero stops
the queue with the cursor on it, and `remount run codex --queue-continue
q_06g6cr…` retries from there. `remount events --ws WS` shows
`queue.advanced{index, exit}` per task; the tasks themselves are never in the
log.

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

## 10. Watch everything

Transactional resource rows are the source of lifecycle/recovery truth. The
event log is the ordered audit and observation history. Follow it live:

```sh
./remount events --follow --ws $WS
```

```
     1 13:24:18.402 ws.created         ws_06g67jv89e2jx29427b99pq8jg  you      {"name":"live",...}
     4 13:24:18.551 ws.claiming        ws_06g67jv89e2jx29427b99pq8jg                 {"gen":1,"restore_from":""}
     6 13:24:18.702 ws.claimed         ws_06g67jv89e2jx29427b99pq8jg                 {"gen":1,"restore_from":""}
     8 13:24:31.102 s.opened           ws_06g67jv89e2jx29427b99pq8jg  you      {"kind":"exec","program":["sh","-c","curl ..."],"s":"s_06g67k…"}
     9 13:24:31.461 cred.used          ws_06g67jv89e2jx29427b99pq8jg  you      {"binding":"b_openai","decision":"substituted","host":"api.openai.com","method":"POST","path":"/v1/chat/completions","status":200}
    12 13:24:59.942 egress.denied      ws_06g67jv89e2jx29427b99pq8jg  you      {"binding":"b_openai","decision":"leak_blocked","host":"api.anthropic.com","reason":"placeholder for b_openai sent to api.anthropic.com"}
    15 13:24:59.983 egress.denied      ws_06g67jv89e2jx29427b99pq8jg  you      {"decision":"denied","host":"example.com","reason":"destination not in bindings or allow list"}
```

Drop `--ws` to see the whole deployment, `--from N` to start at a sequence, and
`--json` for one JSON object per line.

## Where to go next

- [harness-integration.md](harness-integration.md) runs Claude Code, Codex and OpenCode inside a workspace with the same secret-blind setup.
- [operations.md](operations.md) covers tokens, TLS, backends, leases and backups for a real deployment.
- [design.md](design.md) explains why each of the pieces you just used is shaped the way it is.
