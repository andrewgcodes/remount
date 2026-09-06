# 0097. Node enrollment is an operator command, and the floor is checked before the credential is spent

Status: accepted (2026-09-05)

## Context

Production modes refuse a shared node token. §3 of the protocol says a node's
`token` is a one-time enrollment credential, `internal/identity` has minted
those since the beginning, and `internal/pool` calls `IssueWithLabels` for
every machine a provider driver creates.

None of that was reachable by hand. There was no control operation to mint an
enrollment credential, so there was no CLI command either, so there was
nothing between "a production control plane is running" and "a node is
attached to it". The 2026-09-05 credentials run recorded the consequence:
`remount server --mode production-single-tenant` came up healthy,
`remount up --token "$(cat op.token)"` — the only credential an operator had —
was refused `unauthorized: node enrollment failed`, and the run ended with "no
deployment on this host has both a principal authority and a claimed
workspace". The only supported way to acquire a first node was to configure a
provider driver and let the pool reconciler do it, which is a strange
prerequisite for a machine you already own.

Two smaller things fell out of building the command and running it live.

The control plane consumed the one-time credential in `AuthenticateNode` and
only then checked the node's backend descriptors against the deployment's
security floor. A node whose backend can never satisfy the floor therefore
burned a credential on every connection attempt. The live run made this
concrete: a `process`-backed node on macOS against a `production-single-tenant`
control plane emitted `node.enrolled` on its first refused hello and then
retried forever, and the operator's only recovery was to mint another
credential.

And `remount node enroll --out FILE` wrote the file after the round trip, so a
path collision was discovered only once a live credential existed. That
credential was then discarded, still live, still counting against the
deployment's bounded enrollment capacity until it expired.

## Decision

Add one control operation, `node.enroll`, and one operator command,
`remount node enroll --name NAME [--tenant T] [--labels k=v] [--ttl 10m]
(--out FILE | --stdout)`.

The operation is tenant-operator authority over a `node-enrollment` resource.
It takes an idempotency key like every other mutating request, and — like
`principal.token.issue` and `principal.session.create` — deliberately does not
record its result for replay. The response is a bearer, and a bearer must
never enter durable state; a replay of the key therefore mints a second
credential rather than handing back the first. That is the honest trade: the
alternative is storing a credential so it can be returned twice.

It emits `identity.node_enrollment_issued`, committed in the same transaction
as the enrollment digest by the identity store, naming the issuing operator
and carrying no bearer. The pool reconciler keeps emitting
`identity.enrollment_issued`, so the audit log distinguishes a credential a
human minted from one the scheduler minted.

`remount up` grows `--enrollment-file FILE` / `REMOUNT_ENROLLMENT_FILE`, which
reads a mode-0600 file and refuses one any other local user can read. It also
honours `REMOUNT_ENROLL_TOKEN`, which the Fly and E2B drivers have always set
inside a machine they create and which the binary had never read. Precedence
is: the file the operator named, then `--token`/`REMOUNT_TOKEN`, then the
provisioner variable. The file wins over the ambient bearer because
`REMOUNT_TOKEN` is routinely exported for the client CLI, and a node that
quietly enrolled with an operator bearer instead of the credential just named
on the command line is a confusing failure rather than a convenient one.

The deployment security floor is now evaluated before the credential is
consumed. It is decided entirely from the hello's own backend descriptors, so
it never needed the credential; checking it first means a misconfigured node
is refused without cost and can be fixed and restarted with the same file.
`remount node enroll --out` reserves its destination `O_EXCL|0600` before it
calls the control plane, for the same reason.

## Consequences

An operator can bring up a production deployment and attach a node to it with
four commands and no provider account. `docs/operations.md` has an "Enroll a
node" section that is the exact sequence, and it was run.

A refused hello is now free. The refusal for a spent, expired or unknown
credential carries reason `revoked` and never distinguishes those three cases
to the caller; `unauthorized` plus that reason is all an unauthenticated peer
learns.

Enrollment remains one credential per machine. Nothing here makes a credential
reusable, and `remount node enroll` is not a way to avoid `remount pool` for a
fleet — it is the hand path for a machine you own or a first node.

The floor still decides which hosts can join. On macOS neither `process` nor
`docker` advertises `enforced_gateway`, so neither can join any
production-mode control plane; that is the floor working, and it is why the
live run in `docs/engineering/verification-2026-09.md` proves the enrollment
path end to end but leaves the session capability to `internal/sim`.
