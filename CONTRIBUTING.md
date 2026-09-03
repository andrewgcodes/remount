# Contributing

## The bar

Remount is infrastructure that runs as a supervisor and holds credentials. Two
rules follow from that:

1. **A change to the failure model needs a test in `internal/sim`.** That
   package runs the whole system in one process over in-memory transports with
   fault injection, so a claim to survive some failure is checkable.
2. **A lifecycle change that emits no event is a bug.** Transactional SQLite
   resource rows are recovery truth; the ordered event log is the durable audit
   and observation record. Both must describe the same successful transition.

## Getting started

```sh
make            # vet, test, build
make race       # the suite under the race detector
make conformance # hostile-input and failure-model packages under -race
make fuzz       # every registered fuzz target (FUZZTIME=... to extend)
make public-api # compile the SDK from an external module
make dist       # static binaries for every platform
```

Tests must pass under `-race`. Several real bugs in this codebase were found
only there, including a lease that expired while a node was restoring the
workspace it had just claimed.

## Design changes

If you are changing something the ADRs decided, add an ADR rather than editing
one. `docs/adr/0011-ready-handshake.md` is an example: the original design was
wrong, a test found it, and the record says so.

## Protocol changes

`spec/PROTOCOL.md` is normative. Adding an operation is additive and needs no
version bump, because an implementation that does not know an operation answers
`unsupported`. Version 1 is negotiated exactly during hello and requires the
`v1` capability. Changing the meaning of an existing field requires a
new frame version. Update the normative protocol, its golden wire fixture, and
the capability/compatibility tests together.

## Security

The trust table in `docs/adr/0010-trust-boundaries.md` decides most questions.
The workspace is trusted with nothing. Anything that moves a secret, a
credential decision, or a policy evaluation closer to the workspace needs a very
good argument.

Report vulnerabilities privately rather than in an issue.
