# ADR 0081: The relay routes payloads it cannot read

## Status

Accepted. The mechanism, the negotiation and the proofs are implemented in
`internal/e2ee`, `internal/proto/e2ee.go` and `internal/sim`. Issuing the peer
bindings the handshake authenticates against is a control-plane operation that
is specified here and not yet implemented; until it lands, a deployment
supplies bindings through the `e2ee.Config.Bind` seam. See "What is not wired
yet".

## Context

ADR 0008 made the relay dumb: it forwards frames by destination id and
interprets nothing. Dumb is not the same as blind. Until now the relay could
read every operation body and every response it carried — file contents, exec
arguments, session output, error messages — because those travel as plaintext
CBOR in `Frame.Body`. The relay is also the process that authenticates hellos,
so an operator running the relay has always been able to observe the whole
data path between a client and a node.

That is a meaningful difference between a router and a confidential router.
Plan B §14 closes it: peers that both implement the capability agree keys the
relay never sees, and the relay keeps routing exactly as before.

This changes confidentiality only. Identity, enrollment, generations, grants,
revocation epochs and every authorization check are untouched. A sealed frame
carries the same grant, is checked by the same code, and is refused for the
same reasons as before.

## Decision

### Where it lives

Confidentiality is a `transport.Conn` wrapper (`e2ee.Guard`), not a change to
the frame codec or to any peer. A peer opts in by dialing through
`e2ee.Dialer`; nothing about how it speaks the protocol changes. This is what
lets the mechanism land without touching `internal/client`, `internal/node` or
`internal/control`, and it is why the sim lane runs the real client, the real
node and the real relay.

Frames addressed to or from `control` are never sealed. The control plane is a
party to that traffic and must read it; sealing it would hide nothing from the
relay, which is the same process, and would break the protocol. Ping and pong
carry no payload and pass through.

### What a sealed frame exposes

```
Frame { v, t, id, to, from, controller_epoch, op: "e2ee.sealed",
        body: E2EESealed{ sid, n, ct } }
```

`op`, `s`, `ws`, `seq`, `err` and the real `body` move inside the ciphertext.
The operation name is hidden with them: the relay routes by `to`, so it does
not need to know that a client asked a node to write a file.

### Key agreement

X25519 ephemeral keys, authenticated by ed25519 signatures over a transcript,
one agreement per connection:

```
offerer  -> accepter : offer  { sid, from, to, eph_o, nonce_o, ts, binding_o, sig_o }
accepter -> offerer  : accept { sid, from, to, eph_a, nonce_a,     binding_a, sig_a }
```

`binding` is a `proto.PeerBinding`: the control plane's ed25519 signature over
`{peer, key, tenant, issued, exp}`. The control plane is the only party that
knows which identity owns a peer id, so it is the only party that can issue
one. A directory served by the relay was rejected outright: it would make the
relay the trust anchor, which is the party this ADR removes trust from. Both
peers already receive the control plane's ed25519 key in `HelloOK.pubkey` —
the same key nodes verify grants with — so verification needs nothing new on
the wire.

`sig_o` covers the offer half of the transcript, including both peer ids, so
an offer cannot be redirected at another peer. `sig_a` covers the whole
transcript, so both ephemeral keys are authenticated to both parties. The
transcript is a SHA-256 over length-prefixed fields with a domain separator:
protocol version, suite id, suite name, offerer id, accepter id, session id,
offer key, offer nonce, offer timestamp, accept key, accept nonce. Every field
is length-prefixed, so the half transcript is not a prefix of any full one and
no two different field sets hash alike.

Keys come from `HKDF-Extract(SHA-256, ikm = X25519 shared, salt = transcript
hash)` and four `HKDF-Expand` labels: a 32-byte key and a 12-byte nonce prefix
for each direction. Directional keys mean a record can never be reflected back
at its sender even by a relay holding both halves of the traffic.

### The AAD binding

This is the heart of the design. Every record authenticates, and refuses to
open without, exactly:

```
"remount/e2ee/v1 aad\0"
  proto.Version           1 byte
  suite id                1 byte
  direction               1 byte   (0 offerer->accepter, 1 accepter->offerer)
  session id              len16 + 16 bytes
  record counter          len16 + 8 bytes big endian
  frame correlation id    len16 + 8 bytes big endian
  frame kind              len16 + bytes
  source peer id          len16 + bytes
  destination peer id     len16 + bytes
```

Source and destination come from the session — the peers that authenticated
each other — not from the `from` the relay stamps, and the stamped `from` must
agree with the session or the frame is refused before the AEAD runs. So:

- a record moved to another destination fails: the destination is in the AAD
  and the key belongs to one pair;
- a record replayed under another correlation id fails: the id is in the AAD;
- a record reflected at its sender fails: the direction and the key differ;
- a record replayed at another position fails: the counter is in the AAD and
  the receiver's window has already seen it;
- a record from a peer speaking an older protocol version or another suite
  fails: both are in the AAD and in the transcript that made the key.

### Nonce uniqueness

AES-256-GCM nonces are `prefix XOR counter`, the prefix per direction from
HKDF. Uniqueness is the counter's, and the counter is unique by construction,
not by chance:

1. Each direction has its own key and its own counter.
2. The counter is incremented under the same mutex that owns the AEAD, so two
   concurrent sends can never be handed the same value.
3. The counter never resets within a key: a new agreement produces new keys.
4. The counter never wraps. At 2^48 records `Seal` returns
   `ErrRekeyRequired` rather than reusing a value.

### Rekey and bounded state

A session belongs to one connection, so a reconnect is a rekey: the new
connection has a new guard, a new agreement and a new session id. A key never
outlives the connection it was agreed on.

Every piece of retained state has a bound, because a peer on the far side of a
relay can otherwise drive it:

| State | Bound |
|---|---|
| peer sessions | `MaxSessions`, default 64, least recently used evicted |
| idle sessions | `IdleTimeout`, default 10 minutes, swept on every lookup |
| sessions of a departed peer | dropped on `peer.gone` from the control plane |
| replay window | one 64-bit high-water mark and one 64-bit bitmap per direction |
| in-flight sealed request ids | `MaxOutstanding`, default 4096, fixed ring |
| in-flight key agreements | one per peer, each bounded by `HandshakeTimeout` |
| failed negotiations | remembered for `FailureTTL`, default 5 s |
| honoured restart hints | 16 per session |
| refused inbound records | `MaxAuthFailures`, default 64, then the connection closes |

### Reordering, restarts and relay-authored frames

The relay may reorder, so a strictly increasing counter would drop honest
frames. The receiver keeps a 64-record sliding window: reordering inside it is
accepted once, a duplicate is refused, and anything older than the window is
refused rather than guessed at.

A peer that reconnected while its counterpart did not will receive records
under a key it no longer holds. It answers with an unauthenticated `restart`
hint. The hint is a liveness aid only: it is honoured at most 16 times per
session, only for the current session id, and only once that session has been
used. A relay that forges them achieves nothing it could not achieve by
dropping frames.

The relay still authors one frame about sealed traffic: the `unreachable`
response when a destination is not connected. The guard restores the operation
name from its own record of the request and replaces the relay's message with
a fixed one, so no relay-authored text reaches a caller.

### Policy

`PolicyDisabled` is today's behaviour. `PolicyPreferred` seals when the peer
answers a key agreement and falls back to plaintext when it does not, so a
peer built before the capability keeps working unchanged. `PolicyRequired`
fails closed in both directions: an outbound payload for a peer that cannot
negotiate is refused before the frame reaches the wire, and an inbound
plaintext payload from a peer is dropped.

The refusal is `unsupported`, wrapped in `transport.ErrNotSent`. That sentinel
is new and says the frame never reached the wire and the connection is still
healthy — without it, `transport.Peer.Send` treats every send error as a dead
connection and one refused operation becomes a reconnect for every other
caller sharing the link.

### The lifecycle boundary

This ADR adds no lifecycle authority, which is the point. Stated in full:

- **Authority**: unchanged. A grant signed by the control plane, verified by
  the node, bound to workspace generation, tenant and authorization revision.
  A peer binding confers no authority; it only says which ed25519 key answers
  for a peer id.
- **Resource**: the confidentiality of one connection's peer-to-peer frames.
- **Irreversible action**: none. Nothing is committed, nothing is destroyed. A
  failed negotiation refuses a send; it never half-applies one.
- **Durable commit point**: none. Key material is process memory for the life
  of one connection and is never written anywhere.
- **Observable postcondition**: with `PolicyRequired`, every peer-to-peer
  frame on the connection is sealed, and any operation that could not be
  sealed was refused before transmission.

## What is not wired yet

`Bind` is a seam, not an implementation. A deployment needs the control plane
to issue peer bindings, and needs the client and the node to offer the
capability at hello. Neither is in this change, and both are specified in
`docs/engineering/pending-protocol-rows-b6.md`:

- a control-plane operation that returns a `PeerBinding` for the calling peer,
  signed with the grant-signing key;
- `e2ee-payloads` added to `knownCapabilities` and to `implementedCapabilities`
  in the same release that lands both sides, and to the `isolated` and
  `multi_tenant` profile requirements once it is implemented everywhere;
- `spec/PROTOCOL.md` rows for the capability, the two operations and the
  sealed frame shape.

Until those land, `e2ee-payloads` is a reserved identifier: it is named in the
peer-to-peer key exchange, which is where the two endpoints actually negotiate
with each other, and it is not offered at hello.

## Residual metadata exposure

The exit condition in Plan B §14 is explicit that this does not conceal
traffic analysis metadata, and it does not. A relay still sees:

- **peer ids**, source and destination — it cannot route without them;
- **frame kind** (`req`, `res`, `chunk`, `ev`) and **correlation id**, so it
  knows a request was made and when its response came back;
- **frame length**, so payload sizes and their distribution are visible; there
  is no padding;
- **timing**, so request latency, think time and interactive typing rhythm are
  visible;
- **connection timing**: when a peer connects, reconnects and disconnects, and
  how long a key agreement takes;
- **traffic volume and direction**, so the size of a file, the length of a
  session's output, and which peer is talking to which are all inferable;
- **the controller epoch**, which the relay stamps itself and which is
  therefore outside the AAD; and
- **every control-plane frame**, which is plaintext by design — workspace
  lifecycle, grants, events, renewals. The relay and the control plane are the
  same process today, so this is a distinction that matters only when they are
  operated separately.

A relay also retains every denial-of-service power it already had: it can drop
frames, delay them, refuse to route, or close connections. Encryption removes
its ability to read and to forge, not its ability to interfere.

## Alternatives rejected

**A relay-served key directory.** Simplest to build and worthless: the relay
would vouch for the keys of the peers it is not trusted with, so it could
substitute its own and read everything.

**Reusing node ed25519 identity keys directly.** Nodes have one, clients do
not, and a client has no way to learn a node's key without asking the control
plane anyway. A control-signed binding covers both peer roles with one
mechanism and keeps the long-term identity key out of the online path.

**Sealing at the frame codec.** It would have put crypto in the one type every
package depends on and made the plaintext path harder to keep. A Conn wrapper
is opt-in per connection and lets the disabled path be exactly today's code,
which is what makes the leak scan's positive control honest.

**Encrypting control-plane frames too.** The control plane must read them.
Encrypting them to itself would add a key exchange and hide nothing.

**Strictly increasing counters instead of a replay window.** A relay may
reorder frames; strict monotonicity would let it cause honest losses.

## Verification

| Scenario | Layer | Test |
|---|---|---|
| B24 relay capture holds no plaintext payload or canary | sim | `TestPlanBRelayCaptureHasNoPlaintextPayload` |
| B25 mutation, replay and cross-destination substitution refused | sim | `TestPlanBRelayRejectsMutationReplayAndSubstitution` |
| B26 reconnect rekeys without losing cursor or idempotency | sim | `TestPlanBRelayReconnectRekeysWithoutLosingCursorOrIdempotency` |
| B27 required encryption with an incompatible peer fails before mutation | code, sim | `TestRequiredPolicyRefusesBeforeAnythingIsSent`, `TestPlanBRelayRequiredEncryptionFailsBeforeMutation` |

B24 is a positive-control scan. The identical workload runs first with the
guard disabled and the scan must find the canary; only then is its silence
against the sealed capture evidence. `internal/e2ee` adds record-level proofs
for the AAD binding field by field, counter uniqueness under concurrency, the
refusal at the counter bound, the replay window, binding verification and
transcript separation.
