# long-running-autosleep

A durable lifecycle deadline, end to end. The program takes a twenty-second
hold on a workspace, starts a `sleep 300` that will outlive it, renews once,
then stops renewing and watches Remount's control plane act on its own: the
session ends with the exit reason `lifecycle_deadline_expired`, the workspace
publishes `paused`, and `ws.lifecycle.expired` records what happened. Waking it
afterwards shows exactly what a hold preserves — the filesystem — and what it
does not — the processes.

It imports only the two supported public packages, `remount.dev/remount/api`
and `remount.dev/remount/client`. It needs no credentials, no bindings and no
network access beyond the Remount server.

## Why this is not a `time.Sleep`

The obvious way to bound a background job is a timer in the process that
started it. That timer is only as durable as the process. A deploy, a
scale-down, an OOM kill or a partition takes it with it, and the workspace runs
until somebody notices the bill — which is the leak this API exists to close.

`ws.lease` writes the deadline into the control plane's own durable state. It
survives this program dying, the machine it runs on going away, and a restart
of the control plane itself. Nothing in this example holds the deadline; after
the renewal, the program is a spectator.

## Run it

```sh
go run ./cmd/remount standalone --listen 127.0.0.1:7443 --data ./remount-data
```

In a second shell:

```sh
REMOUNT_SERVER=http://127.0.0.1:7443 go run ./examples/long-running-autosleep
```

It takes about half a minute. Expected output, with ids and timestamps
differing every run. `background job started` is streamed from the session on
its own goroutine, so it may land anywhere after the hold is taken:

```
workspace ws_… claimed by node n_…
held ws_… until 2026-…Z (lease wl_…, on_expiry sleep)
background job started
renewed wl_… until 2026-…Z (renewals 1)
pending deadline: sleep at 2026-…Z (source lease)
deadline fired: workspace ws_… is paused, checkpoint art_sha256:…
session s_… exit code 143 reason "lifecycle_deadline_expired"
event ws.lease.granted lease=wl_… reason=background_job on_expiry=sleep
event ws.lease.renewed lease=wl_… renewals=1
event ws.lifecycle.expired action=sleep source=lease
event ws.lease.expired lease=wl_… reason=deadline
event ws.paused reason=lifecycle_deadline_expired
woken: node n_… gen 2
filesystem survived: written before the hold expired
processes after wake: processes-gone
workspace ws_… destroyed
```

Prove the durability claim yourself: take a hold from the CLI, kill the
terminal, and come back after the deadline.

```sh
export REMOUNT_SERVER=http://127.0.0.1:7443
WS=$(remount ws create --wait)
remount ws lease "$WS" --max 30s --reason background_job
remount ws lease get "$WS"       # from any process; the deadline is not local
# ... 30 seconds later, from a fresh shell ...
remount ws get "$WS"             # paused
remount events --ws "$WS"        # ws.lifecycle.expired, ws.lease.expired
```

## What each step demonstrates

| Step | Why it is in the example |
|---|---|
| `LeaseWorkspace` | the deadline is a durable control-plane row, not a timer in this process |
| `Exec` with `sleep 300` | session traffic is *not* activity; a running process does not extend a hold |
| `RenewLease` | the activity signal for a hold; it moves the hard deadline to now + `extend_sec` |
| `GetLease` | a fresh reader learns the schedule from the workspace, never from the process that armed it |
| polling for `paused` | clients read lifecycle position from `ws.get`; `lifecycle_deadline` is the derived view |
| the exit reason | a replayed log says a policy decision ended the work, not a crash |
| `ws.lifecycle.expired` | the durable explanation: action, source and timer |
| `WakeWorkspace` + `ReadFile` | a hold is a filesystem checkpoint, not a memory checkpoint |

## Rules worth knowing before you copy this

- **Activity is explicit.** `MarkActive`, `LeaseWorkspace` and `RenewLease`
  reset the clock. An open session does not. A workspace with a running process
  and no renewal sleeps on time; that is the contract, because a deadline
  nobody has to renew is a deadline that never fires.
- **A hold is not a wake.** Expiry says only that nobody extended the hold. Use
  `SleepWorkspace` with `after_sec` or `on_event` to schedule a resume.
- **A move needs a fresh hold.** Renewing after `ws.move` is refused with
  `conflict` and reason `generation_mismatch`.
- **Renewing after the deadline fired is refused** with `conflict` and reason
  `lifecycle_deadline_expired`. Take a new lease instead of assuming you still
  hold one.
- **Holds are quota'd.** `max_alive_sec` may not exceed the deployment's
  maximum hold (24 hours by default), and a tenant may pin only so many
  workspaces awake at once (256 by default) before `resource_exhausted` /
  `quota_exceeded`.

For the idle-policy half of the API — a no-work cleanup rule driven by
`MarkIdle`/`MarkActive` rather than a job deadline — see "Hold a workspace for
background work" in [docs/using-remount.md](../../docs/using-remount.md).

## Proof

`integration/examples` boots a whole system in-process on a free loopback port,
builds and runs this program against it, and asserts on its output — including
that the workspace really reached `paused` and that the exit reason really was
`lifecycle_deadline_expired`:

```sh
go test -count=1 -timeout 600s ./integration/examples/ -run TestLongRunningAutosleepExample
```
