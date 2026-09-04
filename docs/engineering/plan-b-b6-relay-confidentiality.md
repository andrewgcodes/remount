# Plan B B6: closing the relay confidentiality gap

What landed, how to run it, what it proves, and what it deliberately does not
prove. The design decision is ADR 0081; this is the operator's and reviewer's
view of the same change.

## What landed

| Path | What it owns |
|---|---|
| `internal/proto/e2ee.go` | the capability identifier, the two operation names, and the wire types: `PeerBinding`, `E2EEKeyExchange`, `E2EESealed`, `E2EEPayload` |
| `internal/e2ee/e2ee.go` | policy, errors, and every bound with the reason it exists |
| `internal/e2ee/keys.go` | transcript, key schedule, nonce, and the record AAD |
| `internal/e2ee/identity.go` | peer bindings: minting, control-plane signing, verification |
| `internal/e2ee/handshake.go` | offer, accept and their verification |
| `internal/e2ee/session.go` | directional AEADs, record counters, replay window |
| `internal/e2ee/guard.go` | the `transport.Conn` wrapper and `e2ee.Dialer` |
| `internal/transport/conn.go`, `peer.go` | `ErrNotSent`: a refused frame is not a broken connection |
| `internal/sim/planb_relay_lane_test.go` | the malicious-relay lane and B24–B27 |

Nothing in `internal/control`, `internal/node` or `internal/client` changed.
A peer opts in by dialing through `e2ee.Dialer`, which is why the sim lane
runs the real client, the real node, the real control plane and the real
relay with no test doubles in the data path.

## Running it

```sh
go test -count=1 -timeout 20m ./internal/sim/ -run 'PlanBRelay'
go test -count=1 ./internal/e2ee/...
go test -race -count=1 ./internal/e2ee/... ./internal/transport/... ./internal/relay/...
```

## The lane

`internal/sim/planb_relay_lane_test.go` splices an adversary into the relay
position on every peer connection. It sits between the peer's pipe and the
real server, records every frame in both directions, and can drop, duplicate,
reorder, rewrite or re-address any of them. A redirected frame is routed by
the real relay to the peer the adversary chose, so substitution is tested
end to end rather than simulated.

The capture is kept twice: everything the adversary carried, and the subset
that travelled between two peers. Control-plane traffic is plaintext by
design — the control plane is a party to it — so claims about what the relay
cannot read are made against the peer-to-peer subset, and the canary claim is
made against the whole capture.

## What each scenario proves

**B24, `TestPlanBRelayCaptureHasNoPlaintextPayload`.** The same workload runs
twice: a file written with a credential-shaped canary, read back, and printed
through a session so the canary travels in a request body, a response body and
a chunk stream. Run once with the guard disabled, the scan must find the
canary — it finds six occurrences in about 15 KB of capture. Only then is the
sealed run's silence evidence. The sealed run additionally asserts that the
capture is not implausibly small, that both connections carried sealed frames,
and that the operation names and the file path do not appear in peer-to-peer
traffic either.

The positive control is the whole point. A scan that cannot see anything
reports "clean" for the wrong reason, and this repository has been bitten by
exactly that; see `hardening-lessons.md` and the `grep | head` note in
`AGENTS.md`.

**B25, `TestPlanBRelayRejectsMutationReplayAndSubstitution`.** Three phases
against a live workspace. A flipped ciphertext bit: the write fails, the file
does not exist, and the node reports a refused record. Every sealed request
delivered twice: the node opens each once, reports the duplicate, and the
result is undisturbed. Every sealed request re-addressed to a second node: the
second node refuses them all and the workspace is unchanged.

**B26, `TestPlanBRelayReconnectRekeysWithoutLosingCursorOrIdempotency`.** A
1500-line stream is cut three times mid-flight. The output is byte-identical,
the cursor never goes backwards, no gap chunk appears, and the exit is clean —
the same assertions as `TestReconnectMidStreamIsLossless`, with encryption on.
Each reconnect produced a distinct key agreement and no session id was reused.
Afterwards the idempotency contract still holds in both directions: replaying
a key is a no-op and reusing it with different arguments is refused.

This is the scenario that could have quietly traded a real feature for a
theoretical one. Rekeying is per connection precisely because reconnect is
already the boundary the session cursor and the idempotency key are designed
around; the crypto rides on that boundary instead of inventing one.

**B27, `TestPlanBRelayRequiredEncryptionFailsBeforeMutation`, plus
`TestRequiredPolicyRefusesBeforeAnythingIsSent`.** A client requiring
encryption, a node that answers no key exchange. The write fails with
`unsupported`; no sealed frame and no plaintext operation frame reached the
relay; the canary is nowhere in the capture; and a second, unencrypted client
confirms the file does not exist. The ordering claim is the point: the failure
is in `Send` before transmission, so the destination could not have applied
what it never received. The same test then writes through the unencrypted
client to show that a peer which does not require encryption is unaffected.

## Operating notes

- **Policy is per connection.** `PolicyPreferred` is the safe default for a
  mixed fleet: it seals where it can and falls back where it cannot, and every
  fallback is observable through `Config.Observe`.
- **`PolicyRequired` fails closed in both directions.** It refuses to send a
  peer payload it cannot seal, and it drops an inbound peer payload that
  arrived in the clear. Turning it on before every peer in the fleet can
  negotiate will strand operations, not silently downgrade them.
- **A refusal is not a disconnect.** `transport.ErrNotSent` keeps one refused
  operation from tearing down a link every other caller is sharing. If you add
  another `Conn` that declines frames, wrap that sentinel or you will get
  reconnect storms.
- **Repeated forgeries close the connection.** After `MaxAuthFailures`
  refused inbound records, default 64, the guard closes. A relay that mutates
  traffic loses the connection rather than getting unlimited attempts.
- **Key material never touches disk.** It lives for one connection.

## What this does not claim

The relay still sees who is talking to whom, when, how often, in what
direction and how much. Frame lengths are not padded and timing is not
smoothed. The exit condition in Plan B §14 says this plainly and so does ADR
0081's residual-exposure section: end-to-end encryption removes the relay's
ability to read and to forge, not its ability to observe traffic patterns or
to interfere by dropping.

The control plane still reads control-plane traffic, by design. Today the
relay and the control plane are one process, so an operator of that process
sees workspace lifecycle, grants and events regardless. The confidentiality
claim is about the client-to-node data path.

Finally, the deployment story is incomplete on purpose: the control-plane
operation that issues peer bindings, and the client and node opt-in, are
staged in `pending-protocol-rows-b6.md` rather than implemented here. Until
they land, a deployment supplies bindings through `e2ee.Config.Bind` and the
capability is not offered at hello.
