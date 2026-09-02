# 5. Backends are compiled in, not plugins

**Status:** accepted

## Context

Remount needs to run a workspace on many substrates: a plain directory, a
container, a microVM, someone else's sandbox API. The obvious extensibility
mechanism is a plugin interface so third parties can add backends without
touching our code.

## Decision

No plugin ABI in the supervisor. Backends satisfy a Go interface and are
compiled into the binary. Extension happens by adding a backend and rebuilding.

```go
type Backend interface {
    Name() string
    Caps() Caps
    Create(ctx, id, spec, restore) (Handle, error)
    Adopt(ctx, id) (Handle, error)
}
```

`Caps` is where a backend tells the truth about what it cannot do: its isolation
level, whether it can enforce egress, whether its snapshots include memory.

## Consequences

The supervisor is the trusted computing base. Everything the agent controls is
untrusted. A plugin ABI would put third-party code inside the TCB, on the same
side of the boundary as the credential broker. That is not a trade worth making
for the convenience of not rebuilding.

A dynamic ABI would also be the hardest part of the system to keep stable across
versions, and it would be load-bearing for security. Hyrum's law applies harder
to ABIs than to APIs.

The cost is real: adding a backend means a pull request, not a shared object.
We think that is the right friction for code that runs as the supervisor.

`Caps` carries the honesty requirement. A backend that cannot force egress
through the broker must say so, and policy must be able to refuse to place a
workload that needs enforcement onto a node that cannot provide it.
