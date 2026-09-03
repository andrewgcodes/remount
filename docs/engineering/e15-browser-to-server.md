# E15 browser-to-server evidence

`handoff-2026-09-03-codex-wrap.md` §2.2 recorded that the E15 operator-console
proof was real in `internal/sim` but that "the browser DOM/Playwright suite
still uses mocked HTTP responses", and §6 listed "the browser mock suite is not
a browser-to-real-server proof" as a known honesty constraint.

This document records what replaced the mock, what is now proven end to end
through a real browser against the real Go binary, and what is still not.

## What changed

`web/tests/mock-server.mjs` (a 63-line hand-written HTTP fixture) and the
`page.routeWebSocket(...)` terminal interception in `tests/e2e/operator.spec.ts`
are gone. Playwright now starts the real `remount` binary and drives the console
the binary embeds and sha256-verifies at start-up.

| Path | Role |
|---|---|
| `web/tests/e15/env.mjs` | ports, run directory and binary resolution |
| `web/tests/e15/harness.mjs` | process spawn/reap, bounded readiness waits, CLI invocation |
| `web/tests/e15/global-setup.mjs` | brings up both servers and seeds the durable state the specs assert on |
| `web/tests/e15/global-teardown.mjs` | reaps every process group by recorded pid and removes the run directory |
| `web/tests/e15/upstream.mjs` | two plain-HTTP upstreams (`allowed`, `foreign`) that record what actually reached them |
| `web/tests/e15/state.ts` | typed access to the setup's state, with a drift check against the ports the specs use |
| `web/tests/e2e/standalone-*.spec.ts` | the operator flow and the credential boundary |
| `web/tests/e2e/rbac.spec.ts` | operator / viewer / never-issued credential |
| `web/scripts/build-remount.mjs` | builds `./cmd/remount` unless `REMOUNT_BIN` names a prebuilt binary |

Two Playwright projects, because one server cannot show both stories:

- **`standalone`** — `remount standalone` plus a second node enrolled with
  `remount up`. A real control plane, two real nodes, a real process backend, a
  real broker. Standalone mode promotes every credential to `operator`+`admin`
  (`internal/server/api_console.go`), so it cannot show roles.
- **`rbac`** — `remount server --mode production-single-tenant` with
  `--bootstrap-principal`. This is the only mode where a viewer is a viewer.

Ports default to 7681–7684 and are overridable
(`E15_STANDALONE_PORT`, `E15_RBAC_PORT`, `E15_UPSTREAM_PORT`,
`E15_FOREIGN_PORT`), deliberately clear of the 7443/7455/7456 demo ports.
Every byte a server writes lives under a per-run directory in the system temp
directory, never in the repository; `E15_KEEP_RUN_DIR=1` preserves it for
debugging.

## What is now real

Every response, WebSocket frame and authorization decision below is produced by
the Go server under test. Nothing in the browser path is stubbed.

1. **Fleet.** The console renders two node ids minted by real enrollment, both
   `online`, with the axe accessibility audit still clean of serious and
   critical violations.
2. **Create.** The browser's "New workspace" form performs a real
   `POST /v1/console/workspaces` with an `Idempotency-Key`; the workspace
   reaches `claimed` on a real node.
3. **PTY over the real terminal WebSocket.** "Run command" opens a real session;
   the socket is `ws://127.0.0.1:<port>/v1/console/workspaces/<id>/terminal` and
   the bytes arrive as `{"type":"chunk"}` frames from the node. Changing the
   replay window re-dials with `since=` and the marker is replayed **from the
   server's session log**, not from anything the page retained.
4. **Files.** The console reads `hello.txt` from the node's filesystem, writes it
   back with `If-Match`, and the test verifies the new content by reading it
   out of band rather than trusting the textarea.
5. **Snapshot and cross-node move.** Snapshot produces a real
   `art_sha256:…` artifact; the move relocates the workspace to the *other*
   real node, advancing the generation. The receiving node logs
   `workspace claimed … gen=2 … restore=art_sha256:…`.
6. **RBAC.** Bearers minted by the production server's own identity manager:
   - operator (`e15-operator@acme`): fleet renders, create succeeds, no alert;
   - viewer (`e15-viewer@acme`): the fleet read is refused with
     `authorization denied: role does not permit action`, the create mutation is
     refused with `operator role required`, and the same credential still reads
     the canonical timeline — which is what distinguishes a denied *role* from a
     rejected *credential*;
   - a never-issued bearer: `invalid credential` on every surface.
7. **The credential boundary.** A binding `b_e15` is seeded with a distinctive
   synthetic secret (`sk-e15-<48 hex>`) whose only covered destination is the
   allowed upstream. Then, in the browser:
   - the workspace's own process environment, streamed live over the terminal
     WebSocket, shows `API_KEY=ref:b_e15`;
   - `/.remount/env`, read through the console's file surface, shows
     `REMOUNT_REF_E15=ref:b_e15`;
   - the Security page shows a real `egress.denied` decision named by binding;
   - a real parked approve-mode request is released only after the browser
     commits the decision, and the allowed upstream records the release;
   - **the secret appears in no HTTP response body, no decoded WebSocket frame,
     no rendered DOM and no form-field value**, and the foreign upstream — the
     host the binding does not cover — recorded zero requests.

### The scan has teeth

`AGENTS.md` warns that a leak scan which cannot find anything reports "clean"
for the wrong reason. The secrets spec therefore carries a positive control: a
second synthetic string is planted in a workspace file the browser is *supposed*
to read, and each channel (response bodies, decoded WebSocket frames, DOM plus
field values) must demonstrably contain it before the absence of the secret
counts as evidence. WebSocket chunk payloads are base64-decoded before scanning,
because a leaked secret would travel inside `data` where a scan of the raw JSON
would never see it.

The control was verified by mutation: substituting the control string for the
secret makes the "must not contain" assertions fail, so the scan does detect a
planted canary of the same shape.

## What is still NOT proven here

Be precise about this; the point of the exercise is to stop over-claiming.

- **A successful credential substitution (`cred.used`) is not driven from the
  browser fixture.** The broker refuses to send a credential over plaintext
  HTTP ("remount broker: credentials are never sent over plaintext HTTP"), and
  its upstream TLS trust (`broker.Options.RootCAs`) is only settable in-process
  — there is no CLI flag to trust a locally generated CA. So the fixture proves
  the *blocked* path (`leak_blocked`) and the *placeholder-only* workspace view
  with a real binding lease, while the substituted-and-allowed path remains
  covered by `TestConsoleE15SimulatedOperatorFlow` in `internal/sim`. Closing
  this would need a Go change (an operator-facing broker root-CA option), which
  was out of scope for this change.
- **The production-mode fixture has no node.** `remount server` does not start
  one, so the RBAC project exercises authorization on the console surface, not
  PTY or lifecycle under a role.
- **Only Chromium is exercised.** No Firefox or WebKit project.
- **No TLS.** Both fixtures listen on plaintext `127.0.0.1`, so the console's
  `wss:` path and any HSTS/secure-cookie behavior are unexercised.
- **`retries` is 0 on purpose.** The specs mutate durable server state (a
  workspace is created, snapshotted and moved; an approval is decided). A retry
  would replay against already-changed state, so a failure is reported rather
  than re-rolled.

## Gotchas found while building this

- **`--bootstrap-ttl` above one hour mints an unusable bearer.** The identity
  manager rejects an access token whose lifetime exceeds its `AccessTTL`
  (default 1h, `internal/identity/identity.go`), and the failure surfaces as a
  plain `unauthorized: authentication failed` with nothing in the server log.
  The fixture uses `55m`.
- **`principal create --tenant '*'` is refused** ("an exact tenant is
  required"), so there is no wildcard-tenant viewer; a viewer is always
  tenant-scoped and therefore cannot read the global fleet view.
- **An approve-mode request is answered 403 with `Retry-After` once the
  broker's bounded wait elapses**, while the approval stays durable and
  retryable. A fixture that issues the request once and waits sees a 403 and a
  decision that releases nothing. The fixture retries, which is the contract
  `AGENTS.md` states ("timeout remains a retryable durable approval").
- **A failed create leaves the modal open**, so a spec that continues after a
  denied mutation must dismiss it.

## Console change made for this proof

One behavior change in `web/src`, and the reason for it:

`Fleet` and `Workspace` rendered `<ErrorNotice error={state.error ?? mutationError}/>`,
so a mutation failure was invisible whenever the page's own load had also
failed. That is exactly the viewer's situation — the fleet read fails *and* the
create fails — and it meant a denied mutation produced no operator-visible
message. The two notices are now rendered separately, with the mutation notice
titled "Could not complete that action." `ErrorNotice` gained an optional
`title`. `web/dist` and `web/dist.sha256` were regenerated accordingly, and the
Go binary must be rebuilt after the console build or the browser would be
driven against a stale embedded console — the CI job orders those two steps
that way.

## How to run it

```sh
cd web
npm ci
npm test                 # vitest
npm run build            # tsc + vite + check-dist --write
npm run check:dist
npm run build:server     # go build ./cmd/remount  (or set REMOUNT_BIN)
npx playwright install chromium
npx playwright test
```

`npm run test:e2e` chains the console build, the binary build and the suite.

## Recorded result, 2026-09-03

Darwin arm64, Go 1.27.1, Node 25.9.0, Chromium via Playwright 1.62.1.

```
$ npm test
 Test Files  3 passed (3)
      Tests  9 passed (9)

$ npm run build && npm run check:dist
console assets: 99523 / 1500000 gzipped bytes

$ npx playwright test --reporter=list
Running 7 tests using 1 worker

  ✓  1 [standalone] › tests/e2e/standalone-operator.spec.ts:15:1 › the fleet the browser renders is the real fleet, and creating a workspace is a real mutation (844ms)
  ✓  2 [standalone] › tests/e2e/standalone-operator.spec.ts:43:1 › the terminal streams real PTY bytes over the real terminal WebSocket, and replay comes from the server (545ms)
  ✓  3 [standalone] › tests/e2e/standalone-operator.spec.ts:74:1 › files, snapshot and a cross-node move are real control-plane operations (772ms)
  ✓  4 [standalone] › tests/e2e/standalone-secrets.spec.ts:20:1 › the browser sees the binding name and the placeholder, never the upstream secret (2.0s)
  ✓  5 [rbac] › tests/e2e/rbac.spec.ts:15:1 › an operator may read the fleet and complete a mutation (298ms)
  ✓  6 [rbac] › tests/e2e/rbac.spec.ts:30:1 › a viewer is refused the operator surface but keeps its read scope (327ms)
  ✓  7 [rbac] › tests/e2e/rbac.spec.ts:59:1 › a credential the server never issued is refused on every surface (215ms)

  7 passed (8.5s)
```

Corroborating server-side evidence from the same run
(`E15_KEEP_RUN_DIR=1`, node log of the second node):

```
msg="workspace claimed" node=n_06g6jyp8ya7ys4h2q6ydzgfv24 ws=ws_06g6jypa1w0xkbfn233bv65v38 gen=2 \
  backend=process restore=art_sha256:55175fce95d94e0c9683cf46920468aa0d3da839b7fa18eb0d57a8035d9e9683
```

and the workspace's own retry loop for the parked egress:

```
session s_06g6jypc0rwpyxmqnyzz34hxvm
released
```

## Remaining P1 items in this bullet

`handoff-2026-09-03-codex-wrap.md`'s "P1 — close E15 browser-to-server and
Gate 5 distribution evidence" also lists E12 published-package evidence, E13
with two real MCP-capable harnesses, and the under-five-minute clean-machine
README flow in a disposable VM. **None of those were touched by this change.**
They remain open.

## Timing sensitivity under load

Observed 2026-09-03: running the full suite while `make race` saturated the
same host made `standalone-operator.spec.ts` "the terminal streams real PTY
bytes" fail after its 45 s replay poll expired, with the re-dialed `since=`
socket having received nothing. The same test passes in about 1 s alone, and
the whole suite passes in under 9 s on an unloaded host.

This is a real-server suite, so its waits are bounded by wall-clock rather than
by a mock's immediate response. Treat a failure of that test as "the host was
starved" only after confirming it passes alone; do not raise the timeout to
hide a genuine replay regression, and do not run this suite concurrently with
the race detector on the same machine.
