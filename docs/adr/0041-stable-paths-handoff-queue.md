# ADR 0041: Stable logical paths, hand-off and the durable task queue

## Status

Accepted.

## Context

`remount run` (ADR 0039) starts a harness in a fresh workspace. The unattended
workflow needs three more things, and each one tripped over the same fact:
harnesses key their state on where they run.

- Claude Code, Codex, OpenCode and Gemini store per-project conversation
  history and settings in directories named after the absolute path of the
  working tree. Move the tree from `/home/me/proj` to `/work` and the harness
  starts a new conversation.
- Hand-off ("take what I have open on my laptop and keep it going in the
  cloud") therefore has to carry the checkout *and* the harness's state
  directory, and the tree has to appear at the same path inside the workspace.
- A queue of tasks that runs overnight has to survive the workspace being put
  to sleep between tasks and moved to another node while asleep, without the
  cursor living in the tree the harness is rewriting.

Three shapes for the path problem were considered.

- **Symlink the host path at the real location.** On a process-backend node
  this means creating `/home/me/proj -> $DATA/ws_x/tree` on a machine that is
  not the user's. It couples the node to paths it does not own, it weakens the
  jail (a path outside the data directory that resolves inside it), and it
  breaks the moment two workspaces want the same path.
- **Rewrite the harness's state to a new path.** Feasible per harness (a
  `rehome` hook that renames the project key), fragile in general, and it
  changes the harness's own files — exactly the state the hand-off promised to
  preserve.
- **Mount the tree at the requested path inside the workspace's own mount
  namespace.** Docker and gVisor already give every workspace a private view
  of `/`; the bind mount's destination is free to choose. Nothing on the host
  changes.

## Decision

1. **`WorkspaceSpec.MountPath` is where the tree appears inside the
   workspace.** Default `/work`. It must be absolute, clean, at most 1024
   bytes and outside the system directories (`/`, `/proc`, `/sys`, `/dev`,
   `/etc`, `/bin`, `/sbin`, `/lib`, `/usr`, `/var`, `/run`, `/boot`).
   `ws.create` validates and normalises it (`/work` is stored as empty). The
   path is part of the spec, so it survives snapshots, sleep, wake and moves.

2. **Mirroring happens only inside a backend namespace.** A backend
   advertises `RuntimeCaps.MountPath`. Docker sets it and uses `MountPath` as
   the bind destination, working directory and `HOME`; adoption after a node
   restart reads the destination back from `docker inspect`. The process
   backend has no namespace, advertises `false`, and refuses to create a
   workspace with a non-default path. The control plane only places such a
   workspace on a node whose backend advertises the capability; a workspace
   nobody can claim stays `pending` and the client reports why. No host
   symlink is ever created.

3. **Recipes declare `resume_command`, `state_dirs` and `path_keyed`.**
   `state_dirs` are workspace-relative (`HOME` is the tree root inside the
   workspace), so the same names locate the state under the user's home
   locally and under the tree after hand-off. `path_keyed: true` makes
   hand-off pin `MountPath` to the checkout's absolute path; a recipe whose
   state is path-independent keeps `/work` and works on every backend.

4. **`remount handoff` is one deterministic artifact plus one `run`.** It
   packs the checkout (gitignore-aware, `.git` included) and each present
   `state_dir` into one tar.gz (`artifact.SnapshotTrees`; two trees may share
   directories but a file/file or file/directory conflict is an error, and a
   state file the checkout already carries is left out with a note), uploads
   it, creates the workspace with `restore_from`, `MountPath` (if path-keyed)
   and labels `remount.recipe`, `remount.bindings`, `remount.model`,
   `remount.origin`, and starts the recipe's `resume_command` with the task
   (default "Continue where you left off."). The recipe is auto-detected when
   exactly one candidate's state directory exists locally; zero or several is
   an error naming the candidates.

5. **`remount resume WS` continues from the labels.** If the workspace is
   claimed and a `run` session is still live it attaches to the newest one and
   starts nothing. Otherwise it wakes a paused workspace, waits for `claimed`,
   rebuilds the bindings and model from the labels, and starts
   `resume_command`. It never re-uploads: the tree and state are already the
   workspace's.

6. **A queue is a control-plane resource, never a file in the tree.**
   `Queue{id, ws, tenant, owner, recipe, items[], cursor, status,
   sleep_after_sec, sleep_until}` lives in its own SQLite table and is loaded
   on start. One unfinished (`running` or `failed`) queue per workspace, so a
   second driver cannot interleave with a stopped one; finish or continue it
   first. `queue.create` rejects an
   empty list, more than 256 tasks or a task over 16 KiB. `queue.advance
   {index, session, exit, signal}` requires `index == cursor`; a zero exit
   moves the cursor (and marks `done` at the end), a non-zero exit or signal
   leaves the cursor on the task and marks the queue `failed`, so the next
   `--queue-continue` retries it (`attempts` counts). Both operations take an
   idempotency key; the resource write, the mutation record and the events
   commit in one transaction (ADR 0050). Queues are deleted with their
   workspace.

7. **Events carry positions, never prompts.** `queue.created {queue, ws,
   items}` and `queue.advanced {queue, index, exit, signal, status, cursor}`
   are emitted on the workspace stream. Task text is retrievable with
   `queue.get` by the owner or tenant; it is not in the log.

8. **The driver is a client.** `remount run RECIPE --queue FILE` reads one
   task per line (`#` comments, blank lines, trailing `\` continuations),
   creates the workspace and the queue, then for each task: opens a `run`
   session, drains it, records the exit with `queue.advance` (idempotency key
   derived from the session id, so a retried call replays), and before the
   next task either checkpoints the workspace or sleeps it — `--sleep-after
   DUR` via the existing timer, `--sleep-until HH:MM` by computing the next
   such local wall-clock instant and sending `WSSleepReq.AtMillis` — and waits
   for `claimed` again. A driver that dies is replaced with `remount run
   RECIPE --queue-continue QUEUE`, which reads the cursor and the recorded
   sleep settings back from the control plane, wakes the workspace wherever it
   is now, and carries on. Nothing about progress is read from the tree.

## Consequences

- Hand-off of a path-keyed harness needs a Docker (or gVisor) node. On a
  process-only deployment the workspace stays `pending` and `handoff` says
  "no node with a namespaced backend"; `--backend process` is refused before
  anything is created. This is deliberate: the alternative was a symlink on
  someone else's machine.
- The process backend still runs every recipe whose state is not keyed on the
  path, and every recipe at the default `/work`.
- A queue's workspace is shared by all its tasks, so a task that leaves the
  tree broken breaks the following ones. That is the same contract a human
  gets from running the tasks by hand, and the checkpoint before each task
  means `ws snapshot` history can show which task did it.
- `ws sleep --on-event TYPE` already exists; the queue driver does not use it
  yet. Waking on an external event (a GitHub review, a Slack reply) is the
  intended extension and needs only a webhook that emits the named event.
- A failed queue keeps its workspace from taking a new one until it is
  continued to completion or the workspace is destroyed. A `queue.cancel`
  operation is the obvious addition when that becomes a nuisance.
- `queue.advance` from a driver that was not the one running the task is
  accepted if the index matches; the control plane cannot tell drivers apart.
  Two concurrent drivers on one queue are a user error the cursor check turns
  into a `conflict` for the loser rather than a double-run.
