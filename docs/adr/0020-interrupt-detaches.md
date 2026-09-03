# ADR 0020: Ctrl-C detaches; the timeout is the node's

## Status

Accepted.

## Context

A session belongs to the node. It survives the client that opened it, and
`remount attach WS SID --from N` resumes it from any machine with the output
replayed. The CLI contradicted that story: Ctrl-C in `exec` cancelled the
process-wide context, which neither stopped the remote process nor released
the terminal cleanly, and operators read the hang as "Ctrl-C kills my build".
An agent's long build or test run should not die with the terminal that
happened to start it.

`exec --timeout` was also read as a client-side limit. It is not: the value
travels as `SOpenReq.TimeoutSec` and the node's session runner kills the
process when it elapses. There is no client-side ceiling; the only ceiling
that was ever observed came from the tooling around the CLI, not from
Remount. `0` (the default) means no timeout.

## Decision

- SIGINT in `exec`, `sh` and `attach` **detaches**: the CLI sends
  `s.close{kill:false}`, flushes output that already arrived, restores the
  terminal, and prints `detached; reattach with: remount attach WS SID`. The
  exit status is 0 because nothing failed.
- `--kill-on-interrupt` forwards the first SIGINT as `s.signal{INT}` to the
  remote process and keeps streaming so the operator sees the exit record. A
  second Ctrl-C detaches, so a process that ignores SIGINT cannot trap the
  operator.
- In raw (pty) mode Ctrl-C is a byte on stdin that the remote shell
  interprets; no signal reaches the CLI. `sh` therefore behaves like ssh.
- Session calls made from the interrupt path run under
  `context.WithoutCancel`, because the same signal cancels the process-wide
  `signal.NotifyContext` and a cancelled detach would strand the terminal in
  raw mode.
- `--timeout` is documented as the server-side session timeout. Negative
  values are rejected at the flag; no maximum is imposed by the client.

## Consequences

- `drive` takes a `liveSession` interface rather than `*client.Session`, so
  the interrupt paths are covered by `cmd/remount` unit tests with a fake
  session and no node.
- Operators who relied on Ctrl-C terminating a foreground command pass
  `--kill-on-interrupt` or run `remount ws destroy`.
- Long-running work is `exec` + detach + `attach`; nothing in the client
  bounds it.
