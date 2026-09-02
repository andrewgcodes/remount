# 2. One frame type

**Status:** accepted

## Context

A protocol for this boundary needs requests, responses, streamed output,
notifications and liveness. The obvious approach is a message type per concern,
which is how most RPC systems grow: a `Request`, a `Response`, an `Output`, an
`Event`, a `Ping`, each with its own envelope and its own versioning story.

## Decision

There is exactly one frame struct. The kind is a short string in one field.
Bodies are CBOR, and their shape is determined by the kind plus the operation
name.

```
Frame { v, t, id, seq, s, ws, to, from, op, body, err }
```

CBOR with deterministic encoding, because we sign grant claims and need the
bytes to be stable. Unknown fields are ignored on decode and unknown kinds are
skipped, so a newer peer can talk to an older one.

## Consequences

The relay can route without understanding anything: it reads `to` and forwards.
That is what makes the relay dumb enough to be trustworthy (see ADR 8).

A whole implementation of the wire format is a struct and two functions, which
is the real test of whether someone else will implement this protocol.

The cost is that the body's type is not statically implied by the frame type.
We pay it with a table in the spec and a `decode[T]` helper on both sides. In
exchange, adding an operation is additive and needs no version bump.

## Alternatives

Protobuf with a service definition would give typed stubs. It would also make
the relay depend on the schema of everything it forwards, and make a
weekend-long implementation in another language considerably less likely.
