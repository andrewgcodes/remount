# ADR 0059: a peer's connect and disconnect are ordered per peer id

## Status

Accepted. Refines the relay's replacement rule in ADR 0008.

## Context

A client that loses its connection redials at once and sends a hello with the
same peer id. The relay replaces the old connection with the new one and, when
the old connection's `Serve` observes its peer closing, calls
`Controller.PeerGone` and sends `peer.gone` to every peer the old connection
talked to.

Nothing ordered those two paths. The cut and the redial are the same event
seen from two goroutines, so on a slow or oversubscribed scheduler the new
hello could authenticate first and the stale teardown run second. Two things
then went wrong, both silently:

- the control plane's `PeerGone` deleted the subject the new hello had just
  registered, so every later control call from the client failed
  `unauthorized` until the SDK's retries ran out; and
- a `peer.gone` for the client could reach a node after that node had accepted
  the reconnected client's `s.attach`, so the node cancelled the fresh
  subscription and the stream stopped for good.

The reconnect-losslessness sim test caught the first form on macOS CI only:
after two cuts the client held 5,074 of 18,890 bytes. Linux ran it hundreds of
times without a failure. The relay's `remove` already refused to delete a newer
replacement from its own tables (`r.peers[id] != peer`), which is why routing
kept working; the controller and the correspondents had no such guard.

## Decision

The relay serializes the lifecycle of one peer id. `Serve` takes a per-id lock
before it calls `Authenticate` and releases it after `PeerConnected`; `remove`
takes the same lock around the whole teardown, including the `peer.gone` sends.
The lock is keyed by the id the hello claims, since that is the id a reconnect
reuses; a hello that is assigned a fresh id re-locks under it. Ids are
reference-counted so the map does not grow with every peer ever seen.

The invariant this buys: once a newer connection for an id has been reported
connected, no observer learns the older one is gone. The older connection's
teardown either runs entirely before the new hello is authenticated, or it
finds itself already replaced and reports nothing.

## Consequences

- A reconnect waits for the old connection's teardown to finish delivering
  `peer.gone`, bounded by the transport write timeout. That delay is the price
  of the ordering and is far shorter than a lost stream.
- The controller keeps its contract simple: `PeerGone` for an id always refers
  to the connection it last saw `PeerConnected` for.
- Nodes may rely on a `peer.gone` for a client arriving before any frame from
  that client's replacement connection.
- `internal/relay` has a regression test that holds the second hello open
  inside `Authenticate` while the first connection dies and asserts the
  controller sees `connected, connected` and never `gone` between them.
