# Pending `spec/PROTOCOL.md` rows for Plan B B6

`spec/PROTOCOL.md` is owned by another change in flight, so the normative rows
for end-to-end payload confidentiality (ADR 0081) are staged here and merged
by the spec owner. Nothing below is implemented in the control plane yet; the
mechanism is implemented in `internal/e2ee` and negotiated peer to peer.

## §3.1 named capabilities — new row

| Capability | Introduced for | An old peer ignoring it would |
|---|---|---|
| `e2ee-payloads` | end-to-end sealed operation payloads between peers | hand the relay every operation body and response in plaintext |

Add `e2ee-payloads` to `knownCapabilities` in `internal/proto/proto.go`, after
`encrypted-artifacts`, in the same commit that merges the row. Add it to
`implementedCapabilities` only in the release that lands both the control
plane's binding operation and the client and node opt-in, and add it to the
`isolated` and `multi_tenant` rows of `profileCapabilities` in that same
release, per the §3.1 rule.

Until then the identifier is reserved: it is named inside the peer-to-peer key
exchange, where the two endpoints negotiate with each other, and it is not
offered in `Hello.caps`.

## §2 frames — sealed frame paragraph

When two peers negotiate `e2ee-payloads`, every frame between them carries
`op: "e2ee.sealed"` and a body of:

```
E2EESealed { sid: bytes(16), n: uint64, ct: bytes }
```

`ct` is AES-256-GCM over the deterministic CBOR of:

```
E2EEPayload { op, s, ws, seq, body, err }
```

`v`, `t`, `id`, `to`, `from` and `controller_epoch` stay in the clear because
the relay routes on them. A receiver MUST refuse a sealed frame whose
associated data does not match the frame it arrived in; the associated data is
specified in ADR 0081 and covers the protocol version, the cipher suite, the
direction, the session id, the record counter, the frame kind, the source and
destination peer ids and the correlation id.

## §4 grants — new subsection 4.2, peer bindings

```
PeerBinding { peer, key: bytes (ed25519), tenant, issued: int64,
              exp: int64, sig: bytes }
```

`sig` is ed25519 over the deterministic CBOR encoding of the binding with
`sig` omitted, made with the same control-plane key `HelloOK.pubkey` carries
and grants are signed with. A binding says which key answers for a peer id. It
confers no authority: authorization remains the grant, unchanged.

A peer MUST refuse a binding whose `peer` does not match the peer the frame
came from, whose `exp` has passed, or whose signature fails. A peer MUST NOT
accept a binding from any source other than the control plane's signature —
in particular, never from the relay's view of who is connected.

## §6 control-plane operations — new row

| op | body | result | notes |
|---|---|---|---|
| `e2ee.bind` | `E2EEBindReq { key: bytes }` | `E2EEBindRes { binding: PeerBinding }` | issues a binding for the calling peer's own id and tenant; the control plane never signs a binding for a peer id other than the caller's |

Suggested bounds for the implementation: `exp` no further out than the lease
interval times a small constant, refuse a key that is not 32 bytes, and rate
limit per peer, since a peer may ask on every reconnect.

## §7 node operations

No new node operation. Sealing is below the operation layer, so every node
operation is unchanged.

## §11 events

No new event type. `peer.gone` gains a second meaning for peers that
negotiated the capability: the receiving peer discards the keys it agreed with
the departed peer.

## §13 versioning

The sealed frame is not a version bump. A peer that has not negotiated the
capability never receives one, because sealing follows a key agreement that
such a peer never answers.
