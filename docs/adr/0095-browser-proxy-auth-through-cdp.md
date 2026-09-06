# ADR 0095: A browser's proxy challenge is answered over CDP, by the node

## Status

Accepted. Refines ADR 0088.

## Context

ADR 0088 said a computer's egress is the workspace's egress: the browser is
started with `sessionEnv`, so `HTTP_PROXY`/`HTTPS_PROXY` already name the
workspace's broker. The first real Chromium proved half of that. Traffic did
travel through the broker — every navigation produced a broker audit — and
every one of those audits said `unauthenticated`.

The reason is not subtle. The broker authenticates a proxy client by the
workspace capability in `Proxy-Authorization` (`Broker.EnvForCapability` puts it
in the proxy URL's user-info). **Chromium discards proxy user-info.** It never
volunteers `Proxy-Authorization` at all: it sends a bare `CONNECT`, waits for a
`407` with `Proxy-Authenticate`, and then asks *someone* for the credential. In
a headless browser under automation there is nobody to ask.

So the browser was contained but useless: an unbound host was refused, and so
was a host the broker allowed. The verification entry for the first browser lane
said so in as many words and made no claim of usable brokered browsing.

Three ways out were available.

- **A credential-injecting shim in the workspace.** A local listener that adds
  `Proxy-Authorization` and forwards to the broker. It puts the workspace
  capability *inside the workspace*, which is exactly the thing ADR 10 says the
  workspace is trusted with nothing of. It is also one more process in every
  browser image and one more thing to supervise.
- **Give up on authentication for browsers.** An unauthenticated proxy path for
  computer sessions would be a hole big enough to drive every other session
  through.
- **Answer the challenge from the node, over the connection the node already
  holds.** CDP's `Fetch` domain does exactly this, and it is how Playwright
  implements `proxy.username`/`proxy.password`.

## Decision

**The node answers the browser's proxy authentication challenge itself, over
CDP, with the credential the browser's own `HTTPS_PROXY` already carries.**

- `computer.Options.ProxyAuth` is a `ProxyCredentials{Username, Password}`.
  When it is unset nothing below happens at all: no interception is enabled and
  a browser with no broker in its path pays nothing.
- The node reads it out of the session environment it just built for that
  browser (`proxyCredentialsFromEnv`), preferring `HTTPS_PROXY`, then
  `https_proxy`, `HTTP_PROXY`, `http_proxy`, taking the last assignment of a
  name the way every backend's environment does. **There is deliberately no
  second credential path.** The node can never answer a challenge with an
  authority the browser was not itself handed, and a workspace whose env a
  caller overrode with an unauthenticated proxy simply yields nothing.
- On the page session the client calls `Fetch.enable{handleAuthRequests: true}`
  and `Target.setAutoAttach{autoAttach, waitForDebuggerOnStart, flatten}`. Each
  target that attaches afterwards — an out-of-process iframe is its own target —
  gets the same two calls and then `Runtime.runIfWaitingForDebugger`, sent
  unconditionally so a target we failed to arm still runs rather than hanging.
- `Fetch.authRequired` with `authChallenge.source == "Proxy"` is answered
  `Fetch.continueWithAuth{response: "ProvideCredentials", username, password}`.
- Every other paused request is `Fetch.continueRequest{requestId}` and nothing
  else. No url, method, header or body override: this interception exists to
  answer a challenge, not to rewrite traffic.

### Origin challenges get `CancelAuth`

`authChallenge.source` is `Proxy` or `Server`. A `Server` challenge is a *site*
asking a user for a password. The workspace capability is not that credential,
and handing it to an origin would post the workspace's entire egress authority
to a third party that merely asked for one. Those challenges are answered
`CancelAuth`, which is also what a headless browser with no user in front of it
would do anyway. The request fails; nothing is sent.

### The credential is answered once per request

A proxy that refuses the capability challenges the same request again. Answering
twice would retry forever against a broker that will never take it, so the
second challenge for a request already given the credential is cancelled and the
failure reaches the page as the browser's own proxy error. The table of
challenged requests is capped and dropped whole at the cap rather than grown for
the life of the browser.

### What is redacted, and what was never there

The capability authorizes every egress the workspace has, so:

- It is written into exactly one CDP call, `Fetch.continueWithAuth`. A unit
  test scans every recorded call for a synthetic canary and requires it in that
  one call and nowhere else — the same run proves the scan can find it.
- Nothing in `internal/computer` logs; the package has no logger. The
  fire-and-forget dispatcher deliberately drops the error from a failed write
  rather than reporting text built from parameters that carry the credential.
- `computer.get`, `computer.eval` and every `computer.*` event have no field it
  could ride out on, and the node redacts it from the browser command line it
  publishes with `s.opened` (`redactProxyCredential`), so a `Launch.Program`
  that names the proxy URL cannot put it in the durable event log.

### Interception is scoped to what can meet a proxy

`Fetch.enable` is armed with the patterns `http://*` and `https://*`. A
`file://`, `data:` or `blob:` load never reaches the broker, so pausing one
would buy a round trip and nothing else. Within those schemes every request is
paused and immediately continued, which is a real cost: one extra node round
trip per request. It is the cost of the only mechanism Chromium offers.

### Fetch responses are written from the read loop

`conn.send` writes a CDP call and does not wait for its reply. The Fetch answers
are issued from the read loop's own event handler, where waiting for a reply
would deadlock: the reply can only arrive on the loop that is waiting. An
unmatched reply is dropped by the read loop, which is what should happen to one
nobody is waiting for. Nothing about the request/response path changes; `call`
is untouched.

### A `407` in the audit trail is the handshake, not a refusal

Chromium cannot present the capability before it is challenged, so the first
`CONNECT` of a proxy connection is recorded as an `unauthenticated`
`egress.denied` and the retry that carries the credential is recorded as
`allowed`. **An `unauthenticated` record for a host that also has an `allowed`
record is the 407 handshake.** A conformance assertion must therefore wait for
the decision it means rather than for the first event that names the host.

Chromium's own component, sync, metrics and first-run traffic is issued by the
network service outside any page target, so CDP interception never sees it and
the node cannot authenticate it. It is refused, which is containment working
correctly, but it also files `unauthenticated` denials against Google hosts
nobody asked for. The default launch now carries
`--disable-background-networking`, `--disable-component-update`,
`--disable-default-apps`, `--disable-sync`, `--metrics-recording-only`,
`--no-first-run` and `--no-default-browser-check`, which reduces that traffic
without eliminating it. What remains is denied and stays denied.

## Consequences

- Brokered browsing to an allowed host works, and the B34 lane proves it against
  a real public host: `https://example.com/` loads with HTTP 200 over `https:`
  and the broker records `egress.allowed`, while an unbound host is refused with
  decision `denied` and the reason "CONNECT requires an explicit allow or typed
  CONNECT rule" — host policy, not a missing capability.
- Egress policy is still per host and never per URL. Nothing here changes what
  an opaque CONNECT tunnel lets the broker see (ADR 0088).
- A popup or a target the computer does not drive is out of scope: the client
  drives one page and arms that page and everything auto-attached beneath it.
- A workspace with no broker is unchanged in every observable way, including the
  CDP calls it makes.
- No new error code and no protocol change. `ProxyAuth` is a node-side option;
  nothing about it is on the wire.
