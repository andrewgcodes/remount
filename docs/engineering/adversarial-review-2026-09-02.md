# Adversarial review — 2026-09-02

Six independent reviewers audited one dimension each of the implementation.
Every candidate defect was then handed to a separate agent instructed to
refute it, with a standing instruction to call a finding refuted whenever it
could not be shown to be real and reachable. This document records what
survived that pass.

| | |
|---|---|
| Candidate defects reported | 65 |
| Refuted on verification | 28 |
| **Survived refutation** | **37** |
| Critical | 3 |
| High | 16 |
| Medium | 16 |
| Low | 2 |

Severities below are the verifier's corrected severity, not the reporter's.
Several findings were downgraded during verification and are recorded at the
lower value.

## What this changes about the project's status

The README and design document previously described this implementation as
working and tested. That remains true of its functional behaviour, and the
cross-vendor migration and secret-blind execution demonstrations were real. It
is not true of its security posture. Three findings are sandbox escapes or
credential-boundary breaks with reproduced proofs of concept, and several more
lose data outright.

Do not run this on infrastructure you care about, or with credentials that
matter, until at least the critical and high findings are closed.

## Findings by file

| File | Findings |
|---|---|
| `internal/client/client.go` | 8 |
| `internal/control/control.go` | 6 |
| `internal/broker/broker.go` | 6 |
| `internal/fsops/fsops.go` | 4 |
| `internal/node/node.go` | 3 |
| `internal/artifact/artifact.go` | 3 |
| `internal/session/session.go` | 3 |
| `internal/session/log.go` | 2 |
| `internal/relay/relay.go` | 1 |
| `internal/transport/peer.go` | 1 |

## Themes

**Lexical checks standing in for real containment.** Both filesystem escapes come from validating a path as a string and then
handing that string to a syscall that follows symlinks. The check and the use
are separated by a window, and the check never inspects link targets at all.

**Identity inferred from correlation rather than proven.** The relay and the transport peer both match a response to a request by frame
identifier alone. Nothing binds the reply to the peer that was asked, so a
response can be forged.

**Trust granted by position rather than by credential.** The broker's loopback listener treats any local connection as the workspace it
serves. Anything else on the node can spend that workspace's credentials.

**Destroying the last copy on inference.** Several paths delete a workspace because the control plane's view disagrees
with the node's, without first trying to reconcile or snapshot. The
disagreement is usually recoverable; the deletion is not.

**Errors reported as success.** A rejected readiness handshake, a failed snapshot upload and a failed spill
write are each logged and then ignored, leaving the caller believing the
operation succeeded.


## Critical

### `internal/control/control.go:211` — Control-plane restart clears ws.Node, so the node that holds the data cannot re-adopt it

**Severity:** critical

**Defect**

load() demotes every held workspace to pending and sets ws.Node = "", relying
on the comment at 206-208 that "Nodes that still hold them will reclaim ...
and Adopt their local copy". But with Node cleared, wsReady's identity check
(851) and wsClaim's re-adoption branch (804) can no longer recognise the
previous holder: the holder's resync gets a conflict (and destroys, see the
resync finding), and the pending workspace is offered to every eligible node
with RestoreFrom = LastSnapshot, which is "" for a workspace that has never
been released. Nothing records affinity for the node that actually has the
bytes.

**How it triggers**

Two nodes n_A and n_B are eligible. ws_1 is claimed and live on n_A with
unsaved work (LastSnapshot==""). The broker process restarts. load() makes
ws_1 pending with Node="". n_B reconnects first, PeerConnected → offerPending
→ n_B claims (gen 6) and materializes an empty workspace because RestoreFrom
is empty; ws.ready promotes it to claimed. n_A then reconnects and its resync
is rejected, so it deletes its copy. The claim "no data is lost on a node
restart" fails for a broker restart, deterministically, with no race needed
when n_A reconnects at all.

**Suggested fix**

Keep the holder in the record across the restart — e.g. leave ws.Node set
(with State=WSClaiming and LeaseUntil=0) so wsClaim's re-adoption branch and
wsReady's identity check still match the previous holder, and let the lease-
expiry path in Tick release it to other nodes only after the holder has failed
to come back.

**Verification**

Confirmed and reproduced deterministically, not refuted. load()
(control.go:209-214) sets ws.State=pending, ws.Node="", LeaseUntil=0,
RestoreFrom=LastSnapshot. Because ws.Node is cleared, wsReady's identity check
(control.go:851, ws.Node != node -> CodeConflict) and wsClaim's re-adoption
branch (control.go:804, requires held(state) && ws.Node==node) can no longer
recognise the node that still holds the bytes, and no affinity is recorded
anywhere (proto.NodeStatus.Workspaces is declared but never written;
eligibleLocked only consults user-set Spec.Placement). On reconnect a node
whose process never died still has the workspace in n.workspaces, so
node.go:399-425 resync() runs before reclaimLocal(), receives the conflict,
and dropWorkspace -> handle.Destroy -> os.RemoveAll deletes the only copy; it
then re-claims with adopt=false and RestoreFrom=="" and materializes an empty
workspace. I reproduced this in a scratchpad copy of the repo (internal/sim
probe test: one node + one client, write keep.txt, then restart only the
control plane over the same DataDir): the log shows "dropping workspace we no
longer own reason=conflict", then "workspace claimed gen=2 restore=\"\"
adopted=false", and keep.txt is gone. No second node and no race are needed;
the n_B-steals-first ordering in the stated scenario is racy but only affects
which node ends up holding the empty workspace, not whether the data is
destroyed. This contradicts the project's own contract (docs/design.md:178
lists only in-flight session output at risk on a control-plane restart;
docs/operations.md:233-237 promises nodes re-adopt what is on their disk) and
matches the repo's own audit entries RM-008 / P0.7. The real repo was not
modified (git status clean).

### `internal/node/node.go:420` — resync destroys the only live copy of a workspace when control reports a conflict

**Severity:** critical

**Defect**

On a reconnect, resync sends ws.ready for every workspace it still holds; any
CodeConflict/CodeNotFound answer causes dropWorkspace, whose w.handle.Destroy
is os.RemoveAll(root) (workspace.go:200). Nothing snapshots first, and there
is no idle-snapshot loop at all (Idle.SnapshotEverySec is declared in
proto/types.go but referenced nowhere), so control's LastSnapshot is only ever
set at release time and is "" for a workspace that was never moved or slept.
The node therefore deletes the only copy of the data on the basis of a
control-plane state it has not tried to re-claim.

**How it triggers**

ws_1 is created, claimed by n_A (gen 5), and an agent writes 4 hours of work;
it is never moved or slept, so ws.LastSnapshot == "". The broker restarts (or
the uplink is down longer than LeaseSec=30s: node.go:226-229 backs off to 30s
while the lease is 30s). Control's load() (control.go:209-214) or Tick
(control.go:1218-1229) sets ws_1 to pending with Node="". n_A redials,
helloAndServe calls resync (node.go:281) before reclaimLocal, ws.ready hits
control.go:851 (ws.Node "" != n_A) and returns CodeConflict, and node.go:420
removes /data/ws/ws_1 from disk. reclaimLocal then finds no directory, and the
next claimant creates an empty workspace from RestoreFrom=="": no node, no
snapshot, and the data is gone.

**Suggested fix**

Do not destroy on conflict. Either re-run tryClaim(ws, adopt=true) first — the
workspace is pending and the node still has the bytes, so the CAS in wsClaim
would hand it back — or, if control truly has moved it elsewhere, snapshot and
upload before Destroy so the copy is recoverable. The same unconditional
destroy exists in tryClaim's adopt path at node.go:1092-1094.

**Verification**

Confirmed real and reachable; I could not refute it. (1) node.go:399-425
resync sends ws.ready for every entry in n.workspaces on every reconnect —
that map is never cleared on uplink loss (helloAndServe's teardown only
cancels n.subs; the only delete(n.workspaces,…) sites are dropWorkspace and
release). (2) On CodeConflict/CodeNotFound it calls dropWorkspace
(node.go:461-482), which kills sessions, closes the broker and calls
handle.Destroy = os.RemoveAll(root) (workspace.go:200 process, :377 docker)
with no snapshot on the path. (3) The conflict is reachable without split-
brain: PeerGone deliberately keeps ws.Node and the lease (so the brief-
reconnect happy path is fine), but Control.load (control.go:209-214) on a
control restart and Control.Tick (control.go:1218-1229) on lease expiry both
set State=pending and Node=""; wsReady (control.go:850-853) then takes ws.Node
!= node and returns CodeConflict. Peer.roundTrip returns res.Err as
*proto.Error, so resync's errors.As + code check matches. (4) No snapshot ever
exists for a never-moved workspace: n.snapshot is called only from
OpWSSnapshot (node.go:861-874) and release with req.Snapshot (node.go:1326);
Idle.SnapshotEverySec and Idle.DestroyAfterSec (proto/types.go:97-100) are
referenced nowhere — no idle loop. load even overwrites the spec's original
RestoreFrom with the empty LastSnapshot. (5) The recovery that would have
worked is ordered after the destruction: helloAndServe calls resync
(node.go:281) before reclaimLocal (node.go:282), and materialize's explicit
"with no snapshot the leftover is the only copy, so adopt it" branch
(node.go:1141-1149) never fires because os.ReadDir finds nothing. Even the
racing offer-driven tryClaim is no save — resync holds the *ws pointer and
destroys the root regardless. Refutation attempts that fail: "intentional
fencing" contradicts load's own comment ("Nodes that still hold them will
reclaim … and Adopt their local copy") and materialize's opposite rule, and
fencing does not require deleting the only copy; "another node has it" applies
only to lease expiry and only with a snapshot — with RestoreFrom=="" any
claimant creates an empty workspace, and the control-restart case involves no
second node; no lock or earlier check gates it (wsReady's conflict is decided
under c.mu with Node=="", and the node does no uniqueness check before
Destroy). The repo's own audit records it as unfixed: docs/engineering/code-
audit-2026-09-02.md:846-875 (RM-008) and codebase-deep-
dive-2026-09-02.md:745,1105; the tree has one commit and the code still
matches.

### `internal/artifact/artifact.go:339` — Restore creates unvalidated symlinks, then later entries write through them outside root

**Severity:** critical

**Defect**

Restore's containment check (lines 298-310) is purely lexical on hdr.Name and
never inspects hdr.Linkname. tar.TypeSymlink at line 339 creates a symlink to
any absolute or relative target. A later entry whose lexically-clean name
traverses that symlink passes the HasPrefix check at line 308, and os.MkdirAll
(313, 318), os.OpenFile (321), os.Chmod (332, 348) and os.Chtimes (333) all
follow symlinks, so the write lands outside root. The
tarhelper/TestRestoreRejectsEscape test only covers a literal "../evil" name,
which the line 298 loop catches; it never tests a symlink entry.

**How it triggers**

PoC run (verified): tar containing entry 1 = {Name:"evil",
Typeflag:TypeSymlink, Linkname:"/abs/path/outside"}, entry 2 =
{Name:"evil/victim", Typeflag:TypeReg, body:"PWNED"}. Restore(root, tar)
returned nil and /abs/path/outside/victim now contains "PWNED"; a TypeDir
entry "evil/newdir" also created a directory outside root. Simpler variant
also verified: entry 1 = {Name:"x", TypeSymlink, Linkname:"/abs/victim"},
entry 2 = {Name:"x", TypeReg, mode 0777} overwrote /abs/victim with attacker
content and chmod'd it to 0777. Reachable end to end: any authed client can
PUT the crafted tar.gz to /v1/artifacts/<its own digest>
(internal/server/server.go:186 stores whatever hashes to the named id) and
then create a workspace with Spec.RestoreFrom set to that id;
internal/node/node.go:1126 fetches it and internal/workspace/workspace.go:148
(process) / :269 (docker) calls artifact.Restore on it. Result is arbitrary
file write on the node host as the node's user, outside every workspace.

**Suggested fix**

Reject or sanitize hdr.Linkname: refuse absolute targets and any target whose
path.Join(path.Dir(name), linkname) leaves root. Independently, stop trusting
the lexical check: extract with openat2(RESOLVE_IN_ROOT) or an os.Root-style
handle, or lstat each parent component and refuse to descend into a symlink;
open regular files with O_NOFOLLOW and use
os.Lchown/fchmodat(AT_SYMLINK_NOFOLLOW) semantics. Add a regression test for
the symlink-then-write-through pattern.

**Verification**

Confirmed real and reachable; every refutation angle failed. CODE
(internal/artifact/artifact.go:290-345). The per-entry guard is entirely
lexical on hdr.Name: the loop at 298-302 rejects a literal ".." component,
then 303-310 does path.Clean + filepath.Join + strings.HasPrefix on the *name
only*. hdr.Linkname is never examined anywhere in the package — line 339
passes it verbatim to os.Symlink after os.Remove(host) at 338. Nothing in
Restore uses openat/O_NOFOLLOW or lstats the parent chain, and os.MkdirAll
(313, 318), os.OpenFile (321), os.Chmod (332, 348) and os.Chtimes (333) all
resolve symlinks. VERIFIED PoC. I copied Restore verbatim into a standalone
program (scratchpad, no repo file touched) and fed it a tar.gz of {"evil"
TypeSymlink -> /abs/outside}, {"evil/victim" TypeReg mode 0777 body "PWNED"},
{"evil/newdir" TypeDir}. Output: `Restore err: <nil>`, `outside/victim =
"PWNED"`, `outside/newdir dir? true`, `mode: -rwxrwxrwx`. Both the file write
and the directory creation land outside root and Restore reports success.
REACHABILITY, end to end, all confirmed by reading the callers: -
internal/server/server.go handleArtifact PUT stores whatever the authed client
sends as long as it hashes to the named id (it only compares got != id) — no
tar inspection. - internal/control/control.go:592 wsCreate does `Spec:
req.Spec` wholesale; Spec.RestoreFrom (internal/proto/types.go:86) is client-
supplied and never checked against artifacts the caller produced or owns. -
internal/node/node.go:1125-1131 fetchArtifact (1224-1257) only re-verifies the
digest, never the contents. - Extraction is host-side for BOTH backends:
internal/workspace/workspace.go:148 (process, root = p.Dir/<id>) and :269
(docker, root = d.Dir/<id>, before the container is even created and bind-
mounted). So the docker backend does not contain it. - The `_ =
os.RemoveAll(root)` error path in both callers never fires (Restore returns
nil), and RemoveAll would not follow the symlink anyway. NOT INTENTIONAL.
docs/design.md:154-155 and docs/adr/0010-trust-boundaries.md:33-34 both assert
the opposite invariant ("a path that escapes the workspace root is denied even
if a symlink inside the workspace points at it"), and the repo's own audits
already log this exact issue as source-confirmed P0:
docs/engineering/codebase-deep-dive-2026-09-02.md:691-700 ("P0.3 Archive
restore can escape through symlinks") and docs/engineering/code-
audit-2026-09-02.md:341-352. TEST GAP as claimed:
internal/artifact/artifact_test.go:126-142 TestRestoreRejectsEscape only
builds {"../evil": "bad"}, which the 298 loop catches; no symlink entry is
ever tested. Impact: any authenticated client gets arbitrary file
create/overwrite/chmod on the node host as the node supervisor's user, outside
every workspace — the node holds real credentials and binding leases, so this
is a full node-trust-boundary break.


## High

### `internal/broker/broker.go:192` — Real secrets are substituted onto cleartext HTTP legs, contradicting the TLS re-origination claim

**Severity:** high

**Defect**

The `/http/` route (line 190-192) and the absolute-URI forward-proxy route
(line 184-186, which takes `r.URL.Scheme` verbatim) both reach the same
substitution path at line 242 with scheme `http`. Nothing checks the scheme
before injecting `l.Secret`, and `proto.BindingLease.Destinations` carries no
scheme. The package doc at line 9 claims the broker 're-originates the
connection over TLS'; it does not.

**How it triggers**

A binding b_gh (secret ghp_REAL) is bound to an internal host. The workspace
requests `http://127.0.0.1:PORT/http/<host>/user` with `Authorization: Bearer
ref:b_gh`; verified the plaintext upstream received `Authorization: Bearer
ghp_REAL` over an unencrypted socket, audited as `substituted`. The existing
test already encodes this as intended behaviour (broker_test.go:245 asserts
the plain-HTTP upstream sees `plain:Token S3`). Any on-path observer between
the node and the upstream — a different network segment after a workspace
move, a corporate MITM, a hostile LAN — captures the real credential, and the
workspace itself chooses the scheme.

**Suggested fix**

Refuse substitution when the outbound scheme is not https: if `len(used) > 0
&& scheme != "https"`, emit DecisionDenied and 403 before forwarding.
Optionally allow an explicit per-binding opt-in (e.g. a `plaintext_ok` flag or
an `http://` prefix in Destinations) for deliberate localhost/loopback cases,
and fix the package doc.

**Verification**

Confirmed real and reachable. broker.go:186 forwards r.URL.Scheme verbatim
from the workspace's absolute-URI proxy request and broker.go:192 hardcodes
"http" for the /http/ route; both enter proxy(), which gates substitution only
on hostMatches(host, l.Destinations) (line 230) and lease expiry (line 236)
before writing the real secret into the header at line 242. `scheme` is not
read until line 257, after substitution. proto.BindingLease.Destinations
(proto/types.go:240) and control.Binding (control/control.go:41) are
host[:port] patterns with no scheme field, and a grep of node/control/proto
finds no scheme validation anywhere, so the broker is the sole enforcement
point and it enforces nothing. dial() (line 360) refuses private addresses
unless AllowPrivate matches, so the cleartext leg is normally a public hop.
Refutation attempts all fail: (1) it is intentional and test-encoded
(broker_test.go:245, sim_test.go:602) but not correct — the repo's own audit
lists it as RM-015 under "## 4. P0 findings" at docs/engineering/code-
audit-2026-09-02.md:441 with "Required fix: never substitute credentials over
plaintext transport" and the exact regression test the claim describes; (2) it
is not an operator opt-in, since the operator binds a host pattern with no way
to express HTTPS-only while the workspace picks the scheme; (3) ADR 0007
acknowledges only the CONNECT limitation and states there is no passthrough
mode in v0, while broker.go:9 affirmatively claims TLS re-origination that
this path does not perform; (4) the workspace does gain something — cleartext
means no certificate verification, so an on-path adversary can both sniff and
impersonate the bound host, turning the one destination the workspace is
permitted to reach into an exfiltration channel for the credential ADR 0007
exists to protect.

### `internal/client/client.go:471` — Session-scoped node ops never attach a Grant, so they fail with Unauthorized after any reconnect or move, and nodeCall's retry cannot repair it

**Severity:** high

**Defect**

WorkspaceInfo (471), ListSessions (478), Input (693), Resize (698), Signal
(703) and Close (717) build their request body ignoring the `g *proto.Grant`
argument, relying on the node's per-connection grant cache. The node calls
authorize(client, ws, nil) for these ops (node.go:736, 743, 869; sessionFor at
node.go:902 passes nil), and control.VerifyGrant(nil) returns CodeUnauthorized
"missing grant" (control.go:1170). The node drops cached grants when the
client's peer disappears (EvPeerGone -> dropClient, node.go:527) and on its
own uplink reconnect (node.go:274). nodeCall retries once on
Unauthorized/Conflict (client.go:385-388) via forgetGrant + re-fetch, but re-
invokes the same body(g) closure that discards the grant, so attempt 2 fails
identically.

**How it triggers**

Fresh Client with no prior fs call: c.WorkspaceInfo(ctx, ws) -> grant()
fetches a valid grant -> ws.info is sent with no grant -> node has no cache
entry -> Unauthorized -> forgetGrant -> re-fetch -> Unauthorized -> error.
Same after MoveWorkspace: the new node has no cache entry, so info2 in
internal/sim/sim_test.go:571 is an empty struct (the test hides it with
`info2, _ :=` and a Contains check that then passes vacuously). For
Input/Resize/Signal it is a race against reattach's s.attach, the only op that
re-seeds the node's cache, so input sent right after a reconnect fails.

**Suggested fix**

Carry Grant in SInputReq/SResizeReq/SSignalReq/SCloseReq/SListReq and in the
ws.info request, populating it from the closure argument (e.g.
`proto.SInputReq{..., Grant: g}`), so every node request is self-authorizing
and the retry after forgetGrant actually carries the fresh grant.

**Verification**

Reproduced end to end in a copy of the repo (user's tree untouched). The six
methods do drop the `*proto.Grant`, and the corresponding proto request types
(WSGetReq, SInputReq, SResizeReq, SSignalReq, SCloseReq, SListReq at
internal/proto/types.go:145,396,403,409,419,424) have no Grant field at all,
so the node's authorize(client, ws, nil) can only use its per-connection
cache. Sim runs: (1) fresh client -> c.WorkspaceInfo returns `unauthorized:
missing grant` with a zero WSInfoRes; (2) fresh client -> c.ListSessions
returns `unauthorized: missing grant`; (3) the same WorkspaceInfo succeeds if
any grant-bearing op (ListDir) ran first on that connection, proving the
dependency on the cache; (4) after MoveWorkspace+WaitClaimed it fails with
`conflict: grant generation 1 != workspace generation 2` and nodeCall's single
retry re-runs the same body(g) closure, discarding the refreshed grant, so
attempt 2 fails identically. The existing test at internal/sim/sim_test.go:571
does pass vacuously (info2.Broker == "" makes the Contains check
`Contains(second, "REMOUNT_BROKER=")`). The session case is worse than
claimed: cutting only the node's uplink (node.go:274 clears n.grants) leaves
the client connection up, so Session.reattach never fires and nothing re-seeds
the cache -- Input, Resize and Signal then all return `unauthorized: missing
grant` permanently, not as a race, contradicting the package doc's "survives
connection loss / one unbroken stream" contract. Failure is fail-closed
(denial, no privilege escalation).

### `internal/client/client.go:661` — deliver blocks the peer read loop while holding s.mu; a consumer that stops reading wedges the whole client and deadlocks Session.Close

**Severity:** high

**Defect**

push() and the gap branch send on s.out (cap 1024) while holding s.mu, and
deliver is called synchronously from Peer.readLoop (peer.go:101, whose
contract at peer.go:14-15 is "must not block for long"). Once out is full, the
read loop goroutine parks forever holding s.mu: no responses are correlated,
no pings are answered, no other session is delivered, and every in-flight Call
hangs. Session.Close (client.go:711) then blocks on s.mu.Lock() forever.

**How it triggers**

Run(ctx, ws, "sh","-c","yes") with a ctx that expires. On ctx.Done Run stops
draining s.Chunks() and calls s.Close (client.go:750). Chunks keep arriving;
after 1024 buffered chunks deliver blocks in push at line 661 holding s.mu;
Close blocks at line 711 acquiring s.mu; Run never returns, and every other
Session and every pending c.call on that Client hangs until its own ctx fires.
No goroutine can ever unblock the send because the only reader (Run's loop)
has exited.

**Suggested fix**

Do not do a blocking channel send under the session lock on the transport
goroutine. Either build the ordered batch under s.mu and send it after
unlocking with a select on a session-death channel (select { case s.out <- ch:
case <-s.dead: }), or hand deliver off to a per-session pump goroutine so the
read loop never blocks. Session.Close should close a `dead` channel first so a
parked deliver wakes.

**Verification**

Confirmed real and reachable; I could not refute it. (1) Chunk delivery is
synchronous on the transport read loop: client.go:117 installs
transport.HandlerFunc(c.handle), and transport/peer.go:68-101 invokes
HandleFrame inline in readLoop (the same goroutine that correlates responses
via p.pending and answers KindPing); c.handle calls s.deliver(f) directly at
client.go:183. (2) deliver acquires s.mu with defer unlock (client.go:620-623)
and then performs unguarded sends on the cap-1024 channel s.out (created at
client.go:596) at line 636 (gap branch) and inside push at line 661 — no
select, no default, no escape hatch. (3) The only thing that would stop those
sends, Session.Close, must itself take s.mu at client.go:711 before it can set
closed and close(s.out), so it parks behind the blocked sender. (4) The
scenario is reachable through the SDK's own helper: Run (client.go:731-758)
stops draining on ctx.Done and calls s.Close(context.Background(), true) at
line 750; with a producer faster than the consumer (e.g. `yes`), the steady
state already is "buffer full, read loop parked in the send at line 661", so
ctx expiry lands in that state routinely rather than as a narrow race.
Refutations I tested and eliminated: there is no flow control that would cap
in-flight chunks below 1024 (proto.OpSAck is an explicit no-op on the node,
node.go:694-695 "liveness only in v0"; the node pump at node.go:1028-1039 just
Sends), wire-level backpressure cannot retract chunks already buffered client-
side; connection loss does not unblock a channel send, and supervise
(client.go:151-169) will not even redial because Close deleted the session
from c.sessions before blocking; there is no drain goroutine and no other
reader of s.out anywhere in the repo (only ranging consumers in
cmd/remount/main.go:886, internal/sim/sim_test.go:303/355,
examples/agentloop/main.go:106, plus Run/Copy); and no documented "caller must
drain" contract exists on Chunks() (client.go:599) — the caller that violates
it is the library's own Run. Consequence: the whole Peer read loop is
permanently parked, so response correlation, pings, all other Sessions on that
Client, and every pending c.call hang; Run's Close is additionally passed
context.Background(), so it would be unbounded even if it got past the mutex.
One correction to the claim: no "send on closed channel" panic is possible,
because the single sender and both closers are serialized under s.mu — the
failure is purely the deadlock. Severity high rather than critical: it
permanently hangs an entire client connection and leaks goroutines, but
requires an abandoned/slow consumer with a saturated 1024-chunk buffer and
involves no data corruption or security impact.

### `internal/client/client.go:687` — reattach discards the s.attach error, so a session that fails to re-subscribe after a reconnect is never resumed and never reports failure

**Severity:** high

**Defect**

Connect fires `go s.reattach(...)` once per session per successful dial (line
142) and reattach throws the result away with `_ =`. There is no retry:
supervise has already returned once Connect succeeded (lines 161-163), and a
session that only produces output makes no further calls, so nothing re-drives
the attach. nodeCall retries only Conflict/Unreachable/Unauthorized, once
(line 385), and never CodeNotFound.

**How it triggers**

The connection drops while the workspace is mid-move or before the node re-
claims it after its own uplink reconnect. supervise reconnects, reattach's
s.attach returns CodeNotFound ("workspace is not on this node", node.go:573)
or exceeds its 30s ctx (line 684). The error is discarded; the Session stays
in c.sessions with attached=true, its out channel is never written or closed
again, and a caller doing `for ch := range s.Chunks()` (the documented
pattern, Copy at line 762) blocks forever with no error. The claim "a
reconnect re-attaches every live session" fails against the first slow node.

**Suggested fix**

Have reattach retry with backoff until it succeeds, the session closes, or the
connection generation changes, and propagate terminal failures to the caller
by closing out with a synthetic error chunk (or exposing s.Err()) rather than
leaving the cursor silently dead.

**Verification**

Confirmed. `go s.reattach(...)` at internal/client/client.go:142 is the sole
call site in the repo (verified by grep), and reattach discards its result at
line 687 with `_ =`, no logging, no state recorded on the Session. supervise
(151-169) returns once Connect succeeds and the new supervisor only fires on
the next peer death, so nothing re-drives a failed attach on a healthy
connection. nodeCall (377-392) returns a grant error immediately and retries
the node call at most once for Conflict/Unreachable/Unauthorized, both
attempts within milliseconds. The failure is not exotic: control.grant
(internal/control/control.go:1155) returns CodeConflict whenever the workspace
is not WSClaimed, which is exactly the state a node reconnect creates ("a
reconnect demoted them out of WSClaimed", internal/node/node.go:279), and
relay.go:172 returns CodeUnreachable while the node peer is absent. Backoffs
make the client lose the race deterministically after any shared outage such
as a relay restart: client backoff starts at 100ms (client.go:153) while node
backoff starts at 1s and doubles to 30s (node.go:214-228), and the node must
still resync and re-claim before a grant can succeed. The node cancels every
subscriber when its uplink drops (node.go:292-295), so client-side reattach is
the only recovery path. After the discarded error the Session stays in
c.sessions with attached=true and out is never written or closed again — it is
closed only by push() on an exit chunk (662-672) or Close() (707-718) — so
client.Copy's `for ch := range s.Chunks()` (762) blocks forever;
cmd/remount/main.go:684 uses exactly that for exec/run, giving a silent
permanent CLI hang with no error. The repo's own test acknowledges the gap:
internal/sim/sim_test.go:338 recovers by manually calling c.Attach(ctx, ws.ID,
s.ID, s.Next()) after a node flap. This contradicts the package doc
(client.go:5-8) promising an attached Session resumes so "the caller sees one
unbroken stream". No lock, earlier check, or caller convention makes it
unreachable, and the absence of any error surface or log rules out
"intentional best-effort".

### `internal/client/client.go:692` — Session input seq restarts at 0 for every new Session object, and the node silently drops it as a duplicate

**Severity:** high

**Defect**

Input derives its ISeq from s.iseq (zero-valued in newSession, client.go:596),
so every new Session object starts at 1. The node dedups with `if iseq <=
s.lastISeq { return nil }` (internal/session/session.go:92-96) against the
*server* session's high-water mark, shared by all cursors, and returns
success. Nothing in SOpenRes/SAttachReq/SessionStatus carries lastISeq back,
so a re-attaching client cannot resync.

**How it triggers**

Client A execs a shell and sends 20 inputs (iseq 1..20). A restarts (or any
second client, or the same process calling Attach(ctx, ws, sid, from) at
client.go:554) creates a fresh Session with iseq=0. Its first 20 keystrokes go
out as iseq 1..20, the node drops each one as a duplicate delivery, and Input
returns nil. The caller sees success and the shell receives nothing until the
21st input. The claim "input is idempotent" is in practice "input is silently
discarded after any re-attach".

**Suggested fix**

Seed s.iseq from the server on attach/open: have the node return lastISeq in
SOpenRes and set s.iseq.Store(res.LastISeq) in Attach/register, or make ISeq a
client-instance-scoped id (instance uuid + counter) that the node dedups per
(client-instance, session) rather than one global monotone counter.

**Verification**

Confirmed real and reachable. (1) internal/client/client.go:507 declares `iseq
atomic.Uint64` and line 692 is its only use (`s.iseq.Add(1)`) — there is no
Store or seeding anywhere, so every Session built by newSession (line 596)
restarts input numbering at 1. (2) internal/session/session.go:93-102 dedups
against a single per-server-session `lastISeq` with no client/attachment
identity and returns nil (success) on the drop, incrementing
metrics.InputsDropped. (3) No resync channel exists: proto.SOpenRes
(types.go:385) carries only {S, Next} where Next is the output log seq,
SessionStatus (types.go:432) likewise, and node.go's OpSAttach returns
SOpenRes{S, Log.Next()} while OpSInput (node.go:673) forwards req.ISeq with no
peer identity. Reachability is a shipped CLI path, not hypothetical:
cmd/remount/main.go:638 cmdAttach calls cl.Attach (fresh Session, iseq=0) then
drive(..., forwardStdin=true) unconditionally, and drive pumps os.Stdin
through s.Input. So `remount exec --pty`, N keystrokes, detach, `remount
attach WS SESSION` silently discards exactly the first N keystrokes; two
concurrent attach clients also mutually suppress each other in the shared
sequence space. Refutation attempts all fail: Session.reattach (line 673)
preserves iseq only for transport-level reconnects, not for Client.Attach
(line 554) which constructs a new Session and overwrites c.sessions[sid], nor
for a process restart; register's one-cursor-per-session dedup (line 578) is
in-process only and Attach bypasses it; the iseq==0 bypass at session.go:96 is
unreachable from the client since Add(1) is always >= 1; and multi-writer is
not out of contract — spec/PROTOCOL.md:248-250 and invariant 3 at line 343
describe iseq purely as retry suppression with no ownership model, while the
CLI's own attach command forwards stdin. The repo's own audit records the same
defect as source-confirmed at docs/engineering/codebase-deep-
dive-2026-09-02.md:820-826 (P1.7). Severity high rather than critical: silent
loss of user input on a supported workflow with a success return and no
caller-visible signal, but recoverable once the counter passes the watermark,
and with no security or persistent-corruption impact.

### `internal/control/control.go:694` — A lost release response silently discards the fresh snapshot the node already took

**Severity:** high

**Defect**

release() treats every error from send.Request as "node unreachable" and falls
back to ws.LastSnapshot. But the node's release handler (node.go:1305-1340)
removes the workspace from its map, snapshots, uploads, destroys the directory
and returns the new artifact id only in the response body — it never re-sends
ws.released. If the response is lost (connection drop, or the 10-minute
deadline elapsing after the node finished), the new artifact sits in the blob
store with nobody knowing its id and control moves on with a stale one.

**How it triggers**

ws_1 lives on n_A and has never been moved, so LastSnapshot=="". A client
calls ws.move. release() marks it WSReleased and requests ws.release; n_A
snapshots 2 GB, uploads it, destroys the local directory, and the uplink drops
before the response lands. control.go:694 returns "", wsMove sets
Spec.RestoreFrom="" and State=pending (726-727), and n_B materializes an empty
workspace. The workspace has no node and no snapshot despite having had data,
and n_A has nothing left to reclaim — resync will not mention it (deleted from
n.workspaces at node.go:1316) and reclaimLocal will not find the directory.

**Suggested fix**

Make the release acknowledgement idempotent and retriable rather than a single
request/response: have the node persist the snapshot id and re-send
OpWSReleased (which control already handles at control.go:889) on reconnect
until acknowledged, and have control treat a failed release as "holder state
unknown" — keep the workspace fenced instead of publishing a stale
RestoreFrom.

**Verification**

The claim holds against the current code. control.go:691-698 collapses every
Request error — relay CodeUnreachable, peer ErrClosed, and the 10-minute rctx
deadline — into one branch that returns ws.LastSnapshot with a nil error, so
wsMove (714-733) and wsSleep (757-769) proceed as if the release succeeded.
node.go:1303-1341 (Node.release) deletes the workspace from n.workspaces,
kills sessions, snapshots and uploads, then calls handle.Destroy, and reports
the new artifact id only in the response body; handleReq (547-558) ignores the
error from p.Respond and never sends a compensating ws.released (the only
node-initiated OpWSReleased is the materialize-failure path at node.go:1105).
Neither transport retries: Relay.Request (relay.go:203-229) and Peer.roundTrip
drop the pending entry on ctx.Done and a late response frame lands on an
unknown id. Recovery cannot find it either: resync (node.go:399-425) iterates
n.workspaces, from which the entry was deleted, and reclaimLocal (485-502)
scans DataDir/ws for directories that Destroy already removed. With
RestoreFrom=="" materialize (node.go:1119-1153) calls be.Create with a nil
restore reader, i.e. a fresh empty tree, exactly as the scenario states. The
scenario is reachable with no connection fault at all: the 10-minute deadline
is control-side only, the node's handler runs on the connection context, so a
slow large snapshot+upload finishes and destroys the source after control has
already given up and returned "". No lock or earlier check blocks it —
held(ws.State) at 679 only short-circuits when the node is not holding the
workspace. It is not intentional: docs/design.md:176 covers node death/lease
expiry, not control destroying the only copy and reporting success, and the
repo's own audit files this as a P0 (docs/engineering/code-
audit-2026-09-02.md:247-276, RM-007: "Control.release treats an
unreachable/rejected release as success and returns the previous
LastSnapshot", required fix: two-phase release), still unfixed. Only partial
mitigation: Node.snapshot emits EvWSSnapshot with the artifact id
(node.go:1276) best-effort to control's event log, so in the timeout case an
operator could recover the id from the log — but control's workspace state is
still reset to the stale/empty snapshot and ws.move returns success; on an
uplink drop that event is lost too.

### `internal/control/control.go:719` — wsMove and wsSleep resurrect a destroyed workspace

**Severity:** high

**Defect**

wsMove only checks existence via wsGet (711) and then unconditionally writes
State=WSPending, Node="" and RestoreFrom=snap at 726-729, with no WSDestroyed
guard — unlike wsWake, which does check at 780. release() is a no-op for a
destroyed workspace (679-683 returns LastSnapshot because held() is false), so
nothing stops the transition. wsSleep has the same hole at 762-767, where it
writes State=WSPaused over a destroyed record after having already created and
persisted a wake timer at 751-755.

**How it triggers**

A client destroys ws_1 (state destroyed, node cleared, timers cancelled) and
then a retry or a second automation calls ws.move on the same id. wsMove flips
it back to pending with RestoreFrom set to its last snapshot, emits ws.moved
and calls offerPending; a node claims it and restores the destroyed
principal's filesystem, re-leasing its secret bindings through bindingLease
(which only requires held state, 1124). The workspace reappears in ws.list.
ws.sleep on a destroyed workspace is the same defect with a timer that later
wakes it into pending.

**Suggested fix**

Reject WSDestroyed in wsMove and wsSleep the way wsWake already does
(control.go:780-783), and re-check the state after release() returns, since
release drops the lock for up to ten minutes.

**Verification**

Confirmed reachable and unguarded. wsGet (control.go:620-626) uses snapshotWS,
which returns a record in any state and errors only when nil, so it succeeds
on a destroyed workspace; wsDestroy (641-667) only marks State=WSDestroyed and
never removes the record, and nothing else in the file deletes from
c.workspaces (Tick 1212-1244 has no reaper), so the record persists
indefinitely in memory and in the DB. release (672-708) returns LastSnapshot
with a nil error for any non-held state (held = claimed||claiming, line 959),
so it cannot stop the flow. wsMove then writes Spec.RestoreFrom=snap,
State=WSPending, Node="", LeaseUntil=0 at 726-729 with no state check and
calls offerPending (971-991), which offers anything in WSPending; wsClaim
(797-841) gates only on State==WSPending plus eligibility, and the node
restores the filesystem from Spec.RestoreFrom
(internal/node/node.go:1125-1206). bindingLease (1119-1141) requires only
ws.Node==node && held(state) and returns b.Secret for every binding, with
Spec.Principal unchanged by move, so the destroyed principal's secret bindings
are re-leased to the claiming node. Refutation attempts all failed: dispatch
(459-470) applies no state or auth check for ws.move/ws.sleep; the c.idem
dedupe map is used only in wsCreate (573-574) and WSMoveReq.IdempotencyKey is
ignored by control while the client mints a fresh key per call
(client.go:282), so a retry is not deduped; client-side
MoveWorkspace/SleepWorkspace (client.go:279-296) have no state guard, and only
WaitClaimed knows about WSDestroyed; the scenario is sequential with c.mu
released between the two calls, so no lock or ordering makes it unreachable.
It is not intentional either: wsWake (780-783) rejects destroyed with
CodeConflict and grant rejects non-claimed, making move/sleep the outliers,
and the repo's own audits list it as a defect (docs/engineering/codebase-deep-
dive-2026-09-02.md:755 and docs/engineering/code-audit-2026-09-02.md:229-230).
The wsSleep variant is confirmed too and is worse: the wake timer is created
and persisted at 751-755 before any workspace check, then State=WSPaused is
written over the destroyed record at 761-767, so when the timer fires wsWake's
destroyed guard no longer applies and the workspace goes to pending. No test
covers move or sleep after destroy (sim_test.go:233-240 only checks that files
stop serving).

### `internal/control/control.go:875` — A node never learns it lost its lease, and wsRenew reports success for workspaces it no longer owns

**Severity:** high

**Defect**

Tick expires a lease (1218-1229) without notifying ws.Node and without bumping
Generation. The node has no lease bookkeeping of its own — grep for LeaseUntil
in node.go finds nothing; renewLoop only logs on error (node.go:363-365).
Worse, wsRenew skips a workspace whose Node no longer matches with a bare
`continue` and returns nil, so once another node has taken the workspace the
stale holder's renews succeed with no indication anything changed. Meanwhile
node.authorize compares the grant's generation against the node's own local
copy (node.go:586), so the stale node happily validates the stale generation,
and grants live an hour (control.go:1160) against a 30s lease with no
revocation.

**How it triggers**

ws_1 is claimed by n_A at gen 5; client c_1 holds a grant {gen 5, node n_A}.
Two consecutive renew round-trips exceed the 10s timeout (control-plane GC/DB
stall) while the relay still forwards client frames. Tick expires the lease:
pending, Node="", gen still 5. n_B claims (gen 6), restores from the stale
LastSnapshot, reports ready. n_A is never told: its next renew hits the
`continue` at 878 and returns success. c_1 keeps opening sessions on n_A
(authorize passes: 5 == n_A's local 5) while new clients drive n_B. Two nodes
serve ws_1 concurrently on divergent filesystems, and whichever releases last
overwrites LastSnapshot. This breaks both "exactly one node holds a workspace"
and "a stale generation cannot act".

**Suggested fix**

Make expiry explicit and fenceable: bump Generation in Tick on expiry, push a
ws.release/ws.lost event to the losing node, and have wsRenew return a per-
workspace rejection (not silent success) so the node can drop the workspace.
On the node, record LeaseUntil from WSClaimRes/renew and stop serving sessions
and grants for a workspace whose lease has lapsed.

**Verification**

Confirmed against the code, not refutable. Tick (control.go:1218-1229) returns
an expired workspace to pending without bumping Generation and without any
message to the losing node — OpWSRelease is only sent from wsDestroy (661) and
release (691). wsRenew (872-880) skips a workspace whose Node no longer
matches with a bare `continue` and the handler still returns success
(dispatch:494); node.renew passes nil for the response (node.go:361) and only
logs transport errors (363-365), so the node gets no per-workspace reject.
node.authorize (node.go:586) compares the grant generation to the node's own
local w.Generation, which nothing invalidates; the node's ws embeds
proto.Workspace but its LeaseUntil is only read by diag.go:59, never enforced
— there is no self-fencing and no local lease deadline. Grants live 1h
(control.go:1160) against a 30s lease (default at 289), and client.go:352-364
caches a grant until 60s before expiry, refreshing only on
Conflict/Unauthorized/Unreachable, none of which the stale node returns. The
re-offer reaches the stale holder too, but tryClaim (node.go:1067-1072)
returns silently when it already holds the workspace. Nothing kills the
connection either: PeerGone (363-392) deliberately lets leases run, and the
transport has no heartbeat or read deadline (only an unused Peer.Ping), so a
node stalled past the lease (suspended process, GC/DB stall, latency) keeps
serving clients through the relay while control reassigns. The disconnect path
IS fenced (resync -> ws.ready conflict -> dropWorkspace, node.go:410-421),
which is exactly why the connected-but-unrenewed path is the hole. The repo's
own audit documents this identically: docs/engineering/code-
audit-2026-09-02.md:156 "RM-004 - lease expiry does not fence the old node"
(same evidence bullets, same trigger, scheduled in Batch 1 "ownership and data
safety", line 1137) and codebase-deep-dive-2026-09-02.md:735 P0.6 — so it is a
known unfixed defect, contradicting ADR 0009 and spec/PROTOCOL.md:134, not
intended behaviour.

### `internal/node/node.go:1218` — materialize ignores a rejected ws.ready and keeps serving a workspace control no longer assigns to it

**Severity:** high

**Defect**

At the end of materialize a failed ws.ready is only logged with n.logger.Warn
and materialize returns nil, so the entry added to n.workspaces at 1202 stays,
with a live broker, a live directory, renew traffic (node.go:341-346) and
sessions that authorize will accept. resync handles exactly the same conflict
by dropping (node.go:418-421); this path does not. Control cannot correct it
either: wsDestroy's release request reaches the node while n.workspaces has no
entry yet and gets CodeNotFound, which control ignores (control.go:661), and
control's release() files that same error under "node unreachable"
(control.go:692).

**How it triggers**

n_A claims ws_1 at gen 5 and starts a slow restore. A client calls ws.destroy:
control sets State=destroyed, Node="" (649-650) and sends ws.release, which
n_A rejects with NotFound because materialize has not registered the entry
yet. materialize finishes, registers the workspace, starts the broker and
calls ws.ready, which control rejects with "stale ready" (853). n_A logs a
warning and keeps the destroyed workspace running indefinitely, renewing a
lease for it every 10s. The same shape occurs when ws.move races a mid-
materialize node, leaving the real data on n_A while n_B builds an empty
workspace from RestoreFrom=="".

**Suggested fix**

Treat CodeConflict/CodeNotFound from ws.ready the way resync does —
dropWorkspace (after snapshotting, per the resync finding) — and have
control's release()/wsDestroy distinguish "node says it does not have it" from
"node unreachable" instead of collapsing both into the stale-snapshot
fallback.

**Verification**

Not refutable. internal/node/node.go:1214-1221 calls ws.ready, logs only
n.logger.Warn on failure and returns nil, leaving the entry inserted at
1201-1203 with a live handle and running broker. resync (398-425) handles the
same rejection by calling n.dropWorkspace on CodeConflict/CodeNotFound, so the
divergence the claim describes is real code, not an artifact. No other
mechanism reconciles a connected node: resync and reclaimLocal run only from
the post-hello path (node.go:281-282), the node keeps no local lease deadline
(n.leaseSec is used only for the renew interval at 316), and control.wsRenew
silently continues past workspaces the node no longer owns
(control.go:870-882), so a rejected renewal never reaches the node. The race
is reachable: wsClaim returns at WSClaiming and materialize does the entire
create/restore, binding lease, broker start and env write before registering
the entry, so the window is the full restore. n.release (1300-1310) returns
CodeNotFound when n.workspaces has no entry, wsDestroy discards that error
(control.go:661) and Control.release files it under node-unreachable
(control.go:686-694) — both citations check out. The repo's own audit records
this as an open, source-confirmed defect requiring exactly the missing fence
(docs/engineering/code-audit-2026-09-02.md:188-211 RM-005; codebase-deep-
dive-2026-09-02.md:951). Two parts of the claim are overstated but do not save
it: in the destroy scenario sessions cannot attach, because Control.grant
requires State==WSClaimed (control.go:1148-1164) and the workspace never
reaches claimed at that generation, while Node.authorize rejects any older-
generation grant (node.go:583-586), and bindingLease denies refresh once the
workspace is not held so the broker fails closed; and the renew traffic is
ignored by control rather than extending any lease. The concrete residue is a
destroyed workspace's directory and broker left live on the node until
reconnect or restart, plus, on the move path, a ghost copy holding the real
data that can be reached with a grant minted in the narrow released->claimed
window left open by wsReady not checking state (control.go:844-860).

### `internal/node/node.go:1271` — Snapshot upload failure throws away a good local snapshot and destroys the workspace anyway

**Severity:** high

**Defect**

snapshot() has already computed and stored the artifact locally via
n.store.Put (1264) when the upload fails; it returns ("", 0, err), discarding
the id and skipping the EvWSSnapshot emit at 1276, so the artifact is orphaned
and unreferenced. release() then only logs the error (1329) and still calls
w.handle.Destroy at 1337 after having deleted the workspace from n.workspaces
at 1316. Control's release sees an empty res.Snapshot and keeps the old
LastSnapshot (control.go:700-702).

**How it triggers**

The artifact endpoint is down or returns 502 during ws.move of a workspace
whose LastSnapshot=="". n_A snapshots successfully to its local store, the PUT
fails, snapshot() returns "", release() logs "snapshot on release failed" and
destroys the directory, and control moves the workspace to another node with
RestoreFrom=="". All data is lost even though a complete snapshot exists in
n_A's local artifact store under an id nobody recorded.

**Suggested fix**

Return the local artifact id alongside the upload error so the caller can
decide, and refuse to destroy the workspace when req.Snapshot was requested
and the snapshot did not durably succeed — fail the release (control's caller
already handles an error) rather than deleting the only copy.

**Verification**

Confirmed; I could not refute it. All cited lines check out. node.go:1264
stores the artifact locally, then 1270-1272 returns ("", 0, err) on upload
failure, discarding the id and skipping the EvWSSnapshot emit at 1276, so the
id is recorded nowhere (not in the event, not in the response, not in the
error text). release() at 1316 has already removed the workspace from
n.workspaces, at 1328-1329 treats the snapshot error as non-fatal and only
logs "snapshot on release failed", then unconditionally closes the broker and
calls w.handle.Destroy at 1337, returning out with Snapshot=="" and a nil
error — a successful release. Control.release (control.go:700-702) only
overwrites LastSnapshot when res.Snapshot != "", so it silently keeps the
stale value and returns nil; wsMove (713-735) then sets Spec.RestoreFrom =
snap and re-offers the workspace, so with LastSnapshot=="" the target node
materializes an empty directory. wsSleep (757-769) has the same shape.
Refutation attempts all failed: no caller checks that snap advanced or is non-
empty; there is no upload retry or pending-upload queue; no quarantine of the
source; and the behaviour is not intentional — the repo's own audit documents
it as a defect needing a fix (docs/engineering/code-audit-2026-09-02.md RM-007
"failed checkpoint still destroys the source", and codebase-deep-
dive-2026-09-02.md:704 confirms it from source). The one correction: "all data
is lost" is slightly overstated — artifact.Store has no GC and exposes List(),
so the blob survives on the source node's disk and is manually recoverable by
an operator; it is unrecoverable through any product path, which is why this
is high rather than critical.

### `internal/relay/relay.go:161` — Relay mutates the recent map while holding only a read lock, then re-indexes it after dropping the lock

**Severity:** high

**Defect**

route() writes r.recent[f.From] = map[string]struct{}{} and r.recent[f.To] =
... while holding r.mu.RLock (relay.go:158-168), then re-acquires the write
lock and does r.recent[f.From][f.To] = struct{}{} (relay.go:176-179) after the
lock was released. Two peers' read loops route concurrently, and remove()
deletes r.recent[id] under the write lock (relay.go:130-131).

**How it triggers**

Two connections route their first frame to a peer at the same time: both hold
only RLock and both write to r.recent, which the Go runtime aborts with a non-
recoverable "fatal error: concurrent map writes", killing the whole server
(all tenants). Alternatively, a peer disconnects between the RUnlock at
relay.go:168 and the Lock at relay.go:176: remove() deleted r.recent[f.To], so
relay.go:178 assigns into a nil map and panics inside the peer read-loop
goroutine, which has no recover (peer.go:70-106). Either path is a remote
denial of service reachable by anyone holding the shared token, and the first
is reachable in normal fleet operation.

**Suggested fix**

Do all recent bookkeeping in a single write-locked critical section (take
r.mu.Lock once, look up peers[f.To], create the submaps and record both
directions, then unlock before sending).

**Verification**

Confirmed, not refuted, and reproduced. relay.go:161-166 performs map WRITES
into r.recent (r.recent[f.From] = ..., r.recent[f.To] = ...) while holding
only r.mu.RLock, which permits multiple concurrent holders. route() is called
from each connection's own transport readLoop goroutine (relay.go:98-101
handler -> transport/peer.go:57 `go p.readLoop()`), so N connected peers
execute this concurrently. r.recent has no other synchronization: its only
accesses are relay.go:130-131 (Lock, in remove) and 161-165 (RLock) / 177-178
(Lock) in route. Empirical proof on an unmodified copy of the tree: `go test
-race` reports "WARNING: DATA RACE ... write at relay.go:162" and
relay.go:165; without -race, a test where several goroutines route first-
frames concurrently (the ordinary fleet-reconnect shape, since the write only
fires on a peer's first frame) died with "fatal error: concurrent map read and
map write" in 8 of 8 runs. That abort is non-recoverable and takes down the
whole relay process, dropping every tenant. The second described path is also
real: remove() deletes r.recent[id] under the write lock (relay.go:131), and
if that lands in the gap between RUnlock (168) and Lock (176), relay.go:178
assigns into a nil map ("assignment to entry in nil map") on the peer read-
loop goroutine, which has no recover anywhere in readLoop
(transport/peer.go:70-103) — likewise fatal. No earlier check, lock, or caller
constraint makes either path unreachable, and nothing about writing a shared
map under a read lock reads as intentional. Fix would be to take r.mu.Lock()
once for the lookup-and-record, or do the lazy map creation inside the write-
locked section at 176-179. No files were edited; the repro lives in a
scratchpad copy.

### `internal/session/log.go:248` — A failed spill write deletes a seq range silently — no ErrEvicted, no gap

**Severity:** high

**Defect**

Violates "sequence numbers are contiguous" and "a replay from an evicted seq
produces an explicit gap rather than silence". evictLocked (log.go:211-213)
drops the chunk from memory and advances l.first BEFORE spillLocked runs. When
either WriteAt fails (log.go:247, log.go:250) spillLocked returns early
without advancing spillNext and without recording that the chunk is gone.
l.spillNext is now < l.first, but oldestLocked (log.go:186-188) still reports
spillFirst, so readLocked's eviction check at log.go:302-305 passes.
readSpillLocked then returns nothing for seqs >= spillNext (guard at
log.go:259) and readLocked line 313 unconditionally does `from = l.first`,
jumping the hole and returning err==nil. The index entry for the lost chunk
was already appended at log.go:240-242, so the index also points at an offset
that a later record overwrites.

**How it triggers**

Proven with a probe: MemBytes 20 / MaxChunk 10 / SpillBytes 1MiB; append 3
chunks, make spill writes fail while reads still work (an ENOSPC/EDQUOT/EIO
spill device — reopen O_RDONLY in the probe), append 6 more.
`l.Read(l.Oldest(), 0)` returns seqs [0 7 8] with err=nil — seqs 1..6 vanish
with no *ErrEvicted. A Cursor from 0 returns the same three chunks and
advances to 9. Downstream this wedges the client permanently:
node.go:1019-1044 only emits a StreamGap frame when Cursor.Next returns
*ErrEvicted, so the client's reorder buffer (client.go:642-645) buffers every
later chunk in s.pending waiting for seq 1 forever; push() is never reached,
so the StreamExit chunk is never delivered, s.out/s.exited never close, and
`remount exec` hangs for the life of the connection while pending grows
unbounded.

**Suggested fix**

Reconcile state on write failure instead of returning: on error, treat the
spill as having lost history from this chunk on — e.g. set l.spillFirst =
l.spillNext = c.Seq+1 (and optionally disable further spilling) so
oldestLocked falls through to l.first and readers get a truthful *ErrEvicted.
Also move the index append (log.go:240-242) after both WriteAt calls succeed.
Belt-and-braces: in readLocked, after the spill read, assert the returned
chunks end at l.first-1 and otherwise return &ErrEvicted{Requested: from,
Oldest: l.first} rather than silently setting from = l.first.

**Verification**

Confirmed by probe, not refuted. Running the described scenario in a
scratchpad copy of the repo (MemBytes 20 / MaxChunk 10 / SpillBytes 1MiB, 3
appends, spill handle swapped to a write-failing fd, 6 more appends) yields:
first=7, spillFirst=0, spillNext=1, and `l.Read(l.Oldest(), 0)` returns seqs
[0 7 8] with err=nil; a Cursor from 0 returns the same three chunks and
advances to 9. Mechanism is exactly as claimed: evictLocked (log.go:211-213)
drops the chunk and advances l.first before spillLocked; the early returns at
log.go:248/250 leave spillNext < first with no record of the loss;
oldestLocked (log.go:186-188) still returns spillFirst so readLocked's check
at log.go:303 passes; readSpillLocked's `from >= spillNext` guard (log.go:259)
returns nothing and log.go:313 unconditionally sets from = l.first, jumping
the hole. The index entry appended at log.go:240-242 for the unwritten chunk
points at an offset a later record overwrites. Refutation attempts all failed:
(1) not intentional — the package doc (log.go:6-10) promises a seq becomes
unavailable "only when the spill file itself is rotated … reported as an
Evicted error", and docs/adr/0003-session-is-a-log.md:25-42 makes truthful
gaps the core guarantee, explicitly rejecting silent truncation; (2) not
unreachable — production enables spill at internal/node/node.go:143
(SpillBytes 128MiB), and ENOSPC/EDQUOT/EIO on WriteAt is a realistic disk-full
condition; (3) no self-healing — rotation is gated on spillBytes growing,
which never happens while writes fail, and a reattach from the stuck seq still
passes the eviction check and returns the post-gap chunks; (4) the total-
failure case IS handled (first-ever spill write failing leaves spillNext ==
spillFirst so oldest falls back to l.first and ErrEvicted is raised) — only
the mixed partial-success case is broken. Downstream confirmed by reading
node.go:1019-1035 (StreamGap emitted only on *ErrEvicted) and
client.go:641-645 (any seq > next buffered in s.pending forever, so push()
never runs, StreamExit is never delivered, s.out/s.exited never close);
client.Copy (client.go:761-763) ranges over s.Chunks() with no context and
blocks indefinitely.

### `internal/session/log.go:272` — readSpillLocked ignores max and rescans the whole spill under l.mu — quadratic replay that stalls live output

**Severity:** high

**Defect**

readSpillLocked scans from the index offset to the end of the spill file (`for
off < l.spillBytes`) and materialises every record at or after `from`,
ignoring the caller's `max`; readLocked only truncates afterwards at
log.go:324-326. Because Read (log.go:293) and Cursor.Next (log.go:349-350)
hold l.mu across the whole scan, and node.go:1021 replays with `cur.Next(ctx,
64)`, a cold attach re-reads the entire remaining spill for every 64 chunks
delivered — two ReadAt syscalls per record per batch — while blocking every
Append.

**How it triggers**

Measured on a scratch copy (MemBytes 1KiB, MaxChunk 64, 40-byte chunks, 1GiB
spill): full cursor replay with max=64 takes 26ms for 2,000 chunks, 325ms for
8,000, and 2.03s for 20,000 — clean O(n^2). Production wires SpillBytes=128MiB
and MemBytes=2MiB (node.go:143), so a long pty session holds millions of small
chunks and a single `remount attach --from 0` never finishes. A second probe
measured the collateral damage: with only 20,000 spilled chunks, a concurrent
producer saw a worst-case single Append latency of 29.4ms and completed just
372 appends during the replay. Appends are the pty/pipe pump
(session.go:472-485), so the drain stalls, the kernel pipe fills, and the
child process blocks on write — one attaching client freezes the session for
everyone.

**Suggested fix**

Bound the scan: stop the loop once `max > 0 && len(out) == max`. Better, do
the file I/O without holding l.mu — snapshot (spillFirst, spillNext,
spillBytes, the index entry) under the lock, read outside it, and use a
bufio.Reader/ReadAt of a byte range instead of two syscalls per record.

**Verification**

Confirmed, not refuted. readSpillLocked (log.go:258-288) takes no max: the
spillIndex only selects the starting offset (263-269), while the scan loop
`for off < l.spillBytes` (272) materialises every record with seq >= from all
the way to EOF, two ReadAt calls each (273, 280). readLocked truncates only
afterwards at 324-326. Both Read (293-295) and Cursor.Next (349-357) hold l.mu
across the entire scan, and Append (117-119) contends on the same mutex.
node.go:1019-1021 replays with cur.Next(ctx, 64), and `from` is reachable as 0
via node.go:964/990 and as client-controlled req.From via node.go:662
(`remount attach WS SESSION --from N`, cmd/remount/main.go:642). Production
wires MemBytes 2MiB / SpillBytes 128MiB (node.go:143) with MaxChunk/MaxChunks
at defaults, and log.go:51's own comment notes PTYs emit tiny chunks, so the
spill routinely holds hundreds of thousands to millions of small records.
Measured with a `go test -overlay` probe (no repo files written): the claim's
config gives 2,000->22ms, 8,000->319ms, 20,000->2.02s, 40,000->8.07s (clean
O(n^2), matching the reported figures); production wiring with 40-byte pty
writes gives 25,000->0.47s, 50,000->6.0s, 100,000->39.6s for only ~4MB of
output, extrapolating to hours for a full 128MiB spill. A concurrent-producer
probe reproduced the collateral damage: only 144 appends completed during a
2.1s replay, worst-case single Append 33ms, which back-pressures the pty pump
(session.go:470-484) and blocks the child on write. Refutation attempts all
failed: the spillIndex fixes the seek but not the scan bound (an incomplete
optimization, not a mitigation); the ErrEvicted/Skip path resets the cursor to
spillFirst, i.e. the whole spill, making it worse; max=0 would scan once but
the only production caller passes 64; and nothing between the SAttach handler
and readLocked clamps `from` or throttles the subscriber goroutine.

### `internal/transport/peer.go:82` — Node's transport peer accepts any res frame with a matching ID, so a client can impersonate the control plane's replies

**Severity:** high

**Defect**

Peer.readLoop matches inbound responses purely on f.ID against p.pending
(peer.go:80-90); f.From is never compared with the peer the request was sent
to. node.dispatch only checks f.From == proto.PeerControl for *requests*
(node.go:594), so the res path has no equivalent guard. Node request IDs are a
per-connection counter starting at 1 (peer.go:158), so they are guessable.

**How it triggers**

Any authenticated client sends frames {T:"res", To:"n_victim", ID:1..50,
Body:...} through the relay (route() forwards any frame type to a connected
peer, relay.go:158-184). When the node has a control call outstanding, the
forged frame satisfies it: hijacking the ws.claim reply (node.go:1083) hands
the node an attacker-authored proto.Workspace, so Spec.RestoreFrom, Spec.Env,
Spec.Bindings, Spec.Principal and Generation are all attacker-chosen and are
materialized at node.go:1109-1222; hijacking the binding.lease reply
(node.go:1167 and node.go:383) installs attacker-chosen BindingLease entries
into the broker (node.go:388-391), controlling which placeholder is
substituted for which host.

**Suggested fix**

Carry the expected remote id with each pending request and drop responses
whose From does not match it (for node uplinks, require f.From ==
proto.PeerControl on every res).

**Verification**

Confirmed real and reachable, with a working end-to-end PoC. (1)
transport/peer.go:79-96 matches inbound res/pong purely on p.pending[f.ID];
From/To/Op are never compared, and roundTrip returns whatever frame lands in
the channel. (2) relay/relay.go:141-185 route() forwards every frame kind, res
included, to r.peers[f.To]; f.From=id at line 99 only stamps the true sender,
and nothing on the res path reads it. (3) The f.From == proto.PeerControl
guard at node.go:594 lives in dispatch, which node.handle (node.go:507-514)
reaches only for KindReq — a res whose ID matches a pending entry is consumed
in readLoop and never reaches any handler. (4) control.go:300-326
authenticates every peer with one shared token and lets any peer hello as
RoleClient. (5) IDs are a per-connection counter (peer.go:158); hello consumes
1, so requests start at 2 — trivially sprayable. PoC (run in a scratchpad copy
of the repo; no repo files modified): a plain RoleClient peer sprayed
{T:"res", To:<node id>, ID:2..40, Body:WSClaimRes{Workspace{ID:"ws_pwned",
Gen:1, Spec:{Env, Principal}}}} while a workspace was created. Output: "INFO
workspace claimed ws=ws_pwned gen=1 backend=process" followed by "ws.ready
failed err=not_found: workspace ws_pwned" — the node materialized an attacker-
authored workspace the control plane never issued, with attacker-chosen Spec
(Env, Principal, RestoreFrom) and Generation, winning on the first workspace
creation in ~0.4s. The binding.lease calls at node.go:383 and node.go:1167 use
the identical p.Call on the same peer and are hijackable the same way. Not
intentional: docs/adr/0010-trust-boundaries.md places client authority behind
control-signed grants (verified in node.authorize, node.go:568-590), which
this bypasses completely; the repo's own audit records the same finding as
unfixed P1.9 in docs/engineering/codebase-deep-dive-2026-09-02.md:837-841. No
lock, ordering, or caller constraint makes it unreachable. High rather than
critical only because the attacker needs the deployment's shared token and
must win a race — but the race was won on the first try, and that same token
already allows fake-node enrollment, so it is not a real barrier.

### `internal/fsops/fsops.go:84` — Resolve is a check-then-use race: the resolved path string is handed to symlink-following syscalls

**Severity:** high

**Defect**

Resolve returns a plain host path string after resolving symlinks in the
existing prefix. Every caller then re-walks that string with ordinary syscalls
that follow symlinks: os.Open (99), os.OpenFile (141), os.CreateTemp/os.Rename
(161, 183), os.MkdirAll (132, 232), os.Remove/os.RemoveAll (245, 247),
os.Rename (260), os.ReadDir (196), os.Lstat (218), os.ReadFile (351). Nothing
pins the resolved directory (no O_NOFOLLOW, no openat-relative walk, no
dirfd). The workspace is concurrently writable by the very process the product
is designed to run inside it (exec/shell sessions, and for the docker backend,
the container via the root:/work bind mount), so a component validated at line
64-77 can be swapped for an outward symlink before the syscall at line
99/141/183 runs.

**How it triggers**

Verified PoC: one goroutine repeatedly replaces <root>/d with a symlink to an
outside directory while the main goroutine calls FS.Write("d/victim", "PWNED",
...). The race was won after 253 attempts (~70 ms of wall time), and
<outside>/victim contained "PWNED". Rename is even easier: Resolve("src.txt")
and Resolve("dst/out.txt") both succeed, then <root>/dst is replaced by a
symlink out, and os.Rename at line 260 deposits the file outside root
(verified deterministically). For the docker backend this is a container-to-
host filesystem write: a process inside the container plants /work/d -> /etc
and a host-side fs.write follows it to the host's /etc.

**Suggested fix**

Do not return a string to be re-walked. Resolve to an *os.Root (Go 1.24) or a
dirfd and perform every operation with openat/renameat/unlinkat/mkdirat
relative to it, or use openat2(RESOLVE_IN_ROOT|RESOLVE_NO_MAGICLINKS) on
Linux. At minimum, open the final component with O_NOFOLLOW and re-verify with
fstat that the opened inode is the one Resolve validated.

**Verification**

Could not refute; reproduced it. Resolve (internal/fsops/fsops.go:58-85)
validates only the longest existing prefix with filepath.EvalSymlinks and
returns a plain string; every caller then re-walks that string with symlink-
following path syscalls (os.CreateTemp/os.Rename 161/183, os.MkdirAll 132/232,
os.OpenFile 141, os.Rename 260, os.RemoveAll 245, os.Open 99, os.ReadDir 196,
os.ReadFile 351). No O_NOFOLLOW, no openat/dirfd, and os.OpenRoot is used
nowhere in the repo. No lock: internal/node/node.go:750-860 dispatches fs.*
straight to handle.FS() with no serialization, and the concurrent mutator is a
workspace process outside the node's control. The scenario is reachable by
construction for the docker backend: Docker.Create bind-mounts the workspace
root at /work (internal/workspace/workspace.go:279) and Prepare execs into
that container as USER=root, so a container process can swap a path component
between Resolve and the syscall, giving a container-to-host arbitrary write as
the node supervisor user. Not intentional: ADR 0010 (docs/adr/0010-trust-
boundaries.md) states the workspace is trusted with nothing and the jail is
enforced at the supervisor, and its explicit known-gaps list covers only
process isolation:none and the missing microVM backend, not a jail TOCTOU; the
repo's own audit files this as an open defect requiring dirfd-relative no-
follow operations (docs/engineering/code-audit-2026-09-02.md:711-740, RM-028)
and docs/engineering/codebase-deep-dive-2026-09-02.md:380 says the resolver
"is not safe against a concurrently mutating hostile filesystem." The existing
test (internal/fsops/fsops_test.go:42-60) covers only static symlinks, not a
swap after resolution. Empirically confirmed in a scratch copy of the repo
(real working tree untouched, git status clean): a goroutine alternating
<root>/d between a directory and a symlink to an outside directory while
FS.Write("d/victim","PWNED",...) ran escaped after 287 attempts (~40 ms), with
<outside>/victim containing PWNED. Severity high rather than critical: under
the process backend (isolation:none, same uid) the escape grants no new
privilege, and the docker-backend escape requires winning a race against a
concurrent host-side fs operation rather than a single deterministic call —
though it is trivially repeatable and crosses the product's headline trust
boundary.

### `internal/fsops/fsops.go:245` — Remove follows the final symlink and deletes the link's target tree instead of the link

**Severity:** high

**Defect**

Resolve (line 239) fully resolves the final component via EvalSymlinks, so
Remove never sees the symlink itself. os.RemoveAll at line 245 (and os.Remove
at 247) therefore operates on the target. There is no way to delete a symlink
inside the workspace at all, and a request to delete a link silently destroys
whatever it points at. No race required; a single request does it.

**How it triggers**

Verified PoC: workspace contains <root>/real/keep.txt and <root>/link ->
<root>/real. FS.Remove("link", true) returned nil; <root>/link still exists as
a dangling symlink and <root>/real/keep.txt is gone. An agent that runs `ln -s
. snapshot` or a repo that ships a symlink to a source directory loses that
directory the first time anything removes the link. A symlink to the root
(<root>/self -> <root>) is undeletable: Resolve yields f.root and the guard at
line 241 refuses it forever.

**Suggested fix**

Lstat the path before resolving the last component; if it is a symlink, unlink
the link itself (resolve only the parent directory, then unlinkat the leaf
with no symlink following). Keep the root guard on the parent-resolved path.

**Verification**

Confirmed by execution, not refuted. Resolve (fsops.go:58-85) breaks its walk-
up loop as soon as os.Lstat succeeds, and Lstat succeeds on the symlink
itself, so for a path whose final component is a live symlink existing == host
and line 73's filepath.EvalSymlinks dereferences that final component; the
rest re-append at 80-83 is skipped, so Resolve returns the target. Remove
(236) passes that target to os.RemoveAll (245) / os.Remove (247). PoC run in-
tree (temp test since deleted, git tree clean): with <root>/real/keep.txt and
<root>/link -> <root>/real, Resolve("link") returned "<root>/real",
Remove("link", true) returned nil, <root>/link survived as a dangling symlink
and <root>/real/keep.txt was destroyed; non-recursive Remove("flink", false)
likewise deleted target.txt and left flink dangling; and with <root>/self ->
<root>, Remove("self", false) returned "denied: refusing to remove workspace
root", so a root symlink is permanently undeletable via the guard at line 241.
Refutation routes all fail: (1) not intentional -- List (196) uses
os.ReadDir/e.Info(), which is Lstat-based and reports IsLink:true, so the
protocol hands clients symlink entries and then provides no way to delete one;
following the final component is right for Read/Write/Edit but wrong for
Remove under POSIX unlink semantics, and the doc comment "followed only for
the existing prefix" describes prefix resolution, not final-component
dereference on delete. (2) Not unreachable -- internal/node/node.go:821
dispatches proto.OpFSRemove directly into FS().Remove(req.Path,
req.Recursive), with only authorize() (node.go:569) in front, which checks
grant client/ws/node/generation and inspects no paths; both processHandle and
dockerHandle construct the same host-side fsops.FS (workspace.go:171, 316).
(3) No earlier check or lock helps; the host == f.root guard at 241 is itself
the cause of the undeletable-root-symlink case. (4) Untested --
TestMkdirRemoveRename (fsops_test.go:121-143) covers only x/y, x, /, . and
TestJailBlocksEscapes covers escape, never symlink removal. (5) No race
required: single request, static filesystem, deterministic.


## Medium

### `internal/broker/broker.go:227` — Placeholder matching is substring-based, so a binding id that prefixes another causes false leak-blocks

**Severity:** medium

**Defect**

The match is `strings.Contains(v, ph)` with `ph` defaulting to `"ref:" + l.ID`
(line 159). Placeholders are not delimited, so binding `b_a` matches any
header containing `ref:b_ab`. The first matching lease decides the outcome at
line 230, and since the leak-block returns immediately (line 233), the wrong
binding's destination policy is applied to a request that never referenced it.

**How it triggers**

Bindings b_a (bound to api.other.com) and b_ab (bound to api.github.com).
Verified: a legitimate `X-Api-Key: ref:b_ab` request to api.github.com was
rejected 403 with `credential b_a is not bound to <host>` and logged as
DecisionLeakBlocked against b_a — a fabricated leak-attempt event pinned on
the workspace, plus a hard denial of a correctly-used credential. In the same-
host variant the substitution at line 242 rewrites the `ref:b_a` prefix inside
`ref:b_ab`, producing a mangled credential (`SECRET_SHORTb`). Operator-chosen
ids like b_gh / b_gh2 or b_openai / b_openai_admin trigger this.

**Suggested fix**

Sort leases longest-placeholder-first before the loop, or require delimited
matching (word/token boundary around the placeholder). Better: enforce at the
control plane (control.go:1140) that no lease placeholder is a prefix of
another in the same lease set, and reject the binding set otherwise.

**Verification**

Confirmed by direct repro against the real code (via `go test -overlay`, no
repo files modified; `git status` clean). With leases b_a (dest api.other.com)
and b_ab (dest upstream) and header `X-Api-Key: ref:b_ab`, the broker returned
`403 "remount broker: credential b_a is not bound to 127.0.0.1:60184"` and
emitted `decision=leak_blocked binding=b_a`. The same-host variant with b_a
later in the lease slice forwarded `key=SECRET_SHORTb` upstream with
`substituted` audits for both bindings. Refutation attempts all fail: (1)
Placeholder() at broker.go:155-160 returns a bare `"ref:"+l.ID` with no
delimiter and line 227 is a raw strings.Contains, so nothing anchors the id;
(2) binding ids are operator-authored free-form strings from a JSON file
(cmd/remount/main.go:242 loadBindings) stored verbatim at control.go:121, with
no format validation and no prefix-collision check — wsCreate (control.go:582)
only checks existence, and ids.New("b") (fixed-length, collision-free) is not
used for bindings, while docs use human names like b_openai/b_github, making
b_openai/b_openai_admin realistic; (3) lease ordering does not save it — I
expected the longer id sorting first to fix it, but the inner loop tests and
substitutes against the stale loop variable `v` rather than `vals[i]`, so
reversed order still produced the 403; (4) no lock or earlier check makes it
unreachable (leases are simply read under RLock at 217-219). Two corrections
that cap severity at medium rather than high/critical. First, the failure is
fail-closed on confidentiality: a secret is only substituted after hostMatches
passes for the lease that actually matched, so no secret ever reaches a host
outside its own binding's destinations. The real impact is a hard denial of a
legitimate credential use plus a fabricated leak_blocked audit event and
metrics.LeakBlocked increment — availability and audit-integrity, not
credential disclosure. Second, the claim's mangling detail is order-dependent,
not general: with b_a first in the slice, the later substitute call re-derives
from the original `v` and overwrites, so the upstream actually receives the
correct SECRET_LONG; SECRET_SHORTb only escapes when the shorter id sorts
after the longer one. It also requires a specific (though plausible) operator
configuration: two ids where one is a strict prefix of the other, both
attached to the same workspace. Adjacent bug found while verifying, same
stale-`v` root cause and worth filing separately: with two entirely distinct
placeholders in one header value, only the last-matching lease's substitution
survives — `ref:b_one|ref:b_two` was forwarded upstream as `ref:b_one|S2`,
sending a literal placeholder to the provider.

### `internal/broker/broker.go:242` — Substitution reads the stale loop variable, dropping earlier substitutions in the same header

**Severity:** medium

**Defect**

`vals[i] = substitute(v, ph, l.Secret)` substitutes into `v`, the value
captured by `for i, v := range vals` at line 224, not into the already-updated
`vals[i]`. When two leases match the same header value, the second write
overwrites the first, so the first binding's placeholder is transmitted
verbatim while `used[l.ID]` (line 243) still marks it substituted and
ModifyResponse (line 285) still emits a `substituted` audit event for it.

**How it triggers**

Git-over-HTTP with a split username/password binding: `Authorization: Basic
base64("ref:b_user:ref:b_pass")` with b_user and b_pass both bound to the same
host. Verified the upstream received `ref:b_user:REALPASS` — the b_user
placeholder leaked to the upstream unsubstituted and the request failed —
while the audit log recorded two `substituted` events (b_user and b_pass) and
metrics.CredUsed incremented twice. The audit trail asserts a credential was
used that never was, and vice versa the failure is invisible.

**Suggested fix**

Substitute against the running value: replace line 227's `v` reads and line
242 with a local `cur := vals[i]` refreshed each lease iteration, e.g.
`vals[i] = substitute(vals[i], ph, l.Secret)` and test containment against
`vals[i]`. Note that the leak-block check at line 227/230 must still run
against the pre-substitution text so a later placeholder is not masked by an
earlier secret.

**Verification**

Confirmed by execution, not just reading. At
/Users/you/Documents/remount/internal/broker/broker.go:224 the middle
loop is `for i, v := range vals`, so `v` is a copy taken once per header
value; the inner `for _, l := range leases` loop writes `vals[i] =
substitute(v, ph, l.Secret)` (line 242) from that stale `v` and never breaks.
Two matching leases therefore produce two independent writes to the same slot
and the last one wins, discarding the first substitution while `used[l.ID] =
true` (line 243) is set for both. I copied the repo to a scratch dir and added
a test using only the package's public API (two bindings for the same host, an
`Authorization: Basic base64("ref:b_user:ref:b_pass")` header). Result,
verbatim from the test log: upstream received `decoded="ref:b_user:REALPASS"`
and the audit recorder captured `substituted binding=b_user status=200` plus
`substituted binding=b_pass status=200`. So the b_user placeholder left the
machine unsubstituted, the request would fail auth upstream, and `b.emit`
(line 397) incremented `metrics.CredUsed` twice — exactly the claimed
behaviour. No repo files were modified. The refutation angles all fail: - No
lock or earlier check helps: `hostMatches` (line 230) passes because both
bindings name the same host, and the expiry check (236) passes;
`r.Header[name] = vals` (246) reassigns the same backing slice, so it changes
nothing. - `substitute` (line 452) cannot recover the lost write: for the
second lease it decodes the *original* `v`, replaces only its own placeholder,
and re-encodes, so the first placeholder is restored. - The scenario is
reachable as configured. `Control.bindingLease`
(internal/control/control.go:1130-1143) emits one lease per binding in
`ws.Spec.Bindings` with no constraint against two bindings sharing a
destination, and node.go:451 exports each as `REMOUNT_REF_<ID>`, which is
precisely how a workspace would compose a split user/password Basic header.
docs/harness-integration.md:161-164 advertises substitution "inside a Basic
credential", and the existing TestSubstitutesBearerForBoundHost already covers
the single-binding git-over-HTTP form (broker_test.go:128-133). - Nothing
marks the behaviour intentional; no comment or doc restricts a header value to
one placeholder. A related consequence of the same stale `v` is the default-
placeholder prefix case (`ref:b_gh` is a substring of `ref:b_gh_ro`), which
makes the double-write path reachable with only one placeholder in the header.
Fix shape: substitute against the current slot, e.g. `vals[i] =
substitute(vals[i], ph, l.Secret)` with the containment checks also reading
`vals[i]`.

### `internal/broker/broker.go:280` — Credential use is audited only after a successful response, so the workspace can suppress the cred_used event

**Severity:** medium

**Defect**

The `substituted` audit event is emitted inside ModifyResponse (line 280-294),
i.e. only when the upstream response headers come back. The secret was already
written to the wire at line 242 / rp.ServeHTTP (line 296). If the round trip
does not complete, ErrorHandler (line 275-279) fires instead and records
`Decision = DecisionDenied` with `audit.Binding` never set — so
metrics.CredUsed (line 398) never increments and node.go:1182 emits
EvEgressDenied instead of EvCredUsed. The package doc's 'emits an audit event
for every decision' (line 10) is false for exactly the decision that matters.

**How it triggers**

Workspace sends `GET /d/api.github.com/user` with `Authorization: Bearer
ref:b_gh` and aborts its own client connection ~250ms later (or picks any
endpoint slower than the abort). Verified against a slow upstream: the
upstream received `Authorization: Bearer ghp_REAL`, and the audit recorder
held zero `substituted` events and one `denied` event with reason `upstream:
context canceled` and an empty Binding. An agent can therefore exercise a
bound credential repeatedly (side effects land upstream) while the event log
shows only denials with no binding id.

**Suggested fix**

Emit the `substituted` audit event at the moment of substitution (right after
line 243, before rp.ServeHTTP), and use ModifyResponse only to attach the
upstream status as a follow-up/enrichment event. At minimum, carry the `used`
binding ids into ErrorHandler so an aborted request is logged as a credential
use, not a denial.

**Verification**

Reproduced, not refuted. An overlay test (no repo files modified) against a 2s
upstream with a client that aborts at 250ms produced exactly the claimed
state: the upstream received `Authorization: Bearer ghp_REAL`, and the
recorder held zero `substituted` events and one `denied` event with
`Binding=""` and reason `upstream: context canceled`. Every mechanical sub-
claim holds in broker.go: substitution at line 242 only sets `used[l.ID]=true`
and never `audit.Binding`; `DecisionSubstituted` is emitted solely from
`ModifyResponse` (line 280), which requires upstream response headers;
`ErrorHandler` (line 275) emits `DecisionDenied` with an empty Binding, so
`emit` (line 407) increments only `metrics.EgressDeny` and node.go:1184 maps
it to `proto.EvEgressDenied` instead of `EvCredUsed`. Reachability is trivial
and not adversarial-only: any client abort, upstream reset, TLS/dial failure
after the request is in flight, or the 5-minute ResponseHeaderTimeout. It is
worse than the claim in one respect: docs/operations.md:209 tells operators
that `egress.denied`/`denied` means "a host outside policy was requested,
usually a missing --allow", and broker.go:46 comments DecisionDenied as
"destination not permitted" — so the event affirmatively misleads an incident
responder into believing nothing reached upstream, when a real secret was in
fact released to a permitted host. Legitimate corrections that lower severity:
(1) the package doc's "audit event for every decision" is not literally false
— one event per request is still emitted carrying WS, principal, host, method,
path and reason, so an agent that aborts every request is misclassified rather
than invisible, and volume review still sees the traffic; (2) the current
placement is not arbitrary — ErrorHandler also covers DNS failure, the
private-address refusal in dial (line 373), dial failure and TLS handshake
failure, where the secret never left the node, so emitting `substituted` at
line 242 would over-report exposure, and ModifyResponse is where audit.Status
(shown in cred.used per docs/tutorial.md:371) becomes available; a correct fix
needs a separate attempted/completed pair, not a relocated emit; (3) no policy
is bypassed — the credential still only reaches a bound host, there is no leak
or unauthorized egress, and the aborting client can never read the response.
Net: a real, reachable audit-fidelity and metrics-undercount defect with a
misleading decision label, not a credential-containment or policy-enforcement
failure.

### `internal/broker/broker.go:313` — CONNECT never checks lease expiry, so an expired binding keeps granting egress

**Severity:** medium

**Defect**

handleConnect authorizes a destination from any lease whose Destinations match
(line 312-316) but omits the `l.ExpiresAt` check that the proxy path performs
at line 236. proto.BindingLease documents ExpiresAt as 'node must fail closed
after this' (types.go:243). Worse, refreshLeases keeps the previous lease set
when the control plane is unreachable (node.go:385-387, which logs 'broker
will fail closed at expiry'), so a partitioned or revoked node keeps
tunnelling indefinitely.

**How it triggers**

Broker configured with an empty Allow list and one lease for host H whose
ExpiresAt is an hour in the past. Raw `CONNECT H HTTP/1.1` returned `HTTP/1.1
200 OK` and the audit recorded `Decision: allowed`. The identical request
through the proxy path is correctly rejected with DecisionExpired
(broker_test.go:174). Revoking a binding at the control plane therefore does
not cut a workspace's tunnelled reachability to that host while the node
cannot refresh.

**Suggested fix**

Mirror the proxy-path expiry test inside the CONNECT lease loop: only set
`allowed = true` when `l.ExpiresAt == 0 || time.Now().UnixMilli() <=
l.ExpiresAt`, and emit DecisionExpired when a match existed but was stale.

**Verification**

Confirmed, not refuted. handleConnect (broker.go:311-316) sets allowed=true on
any lease whose Destinations match, with no ExpiresAt test, while proxy checks
it at line 236. Verified empirically with an overlay test (no repo files
modified, git status clean): Allow=nil plus one lease expired an hour ago, raw
`CONNECT <host> HTTP/1.1` returned "HTTP/1.1 200 OK" with audit
Decision=allowed, while the existing TestDeniesUnlistedHostAndExpiredLease
passed in the same run for the same host/lease on the proxy path. Every
refutation avenue fails: (1) not intentional — docs/engineering/openai-
huggingface-hardening-sdmr.md:330 says "CONNECT must check lease expiration"
and :633 states "An expired binding cannot authorize reverse proxy or CONNECT
traffic", and the repo's own audit RM-027 (docs/engineering/code-
audit-2026-09-02.md:695-713) lists this exact gap with required fix "apply
lease validity to all connection forms"; (2) reachable — EnvFor sets
HTTPS_PROXY to the broker so all workspace HTTPS uses CONNECT
(TestConnectTunnelAllowlisted); (3) no compensating check — SetLeases stores
leases verbatim, nothing prunes expired ones, node.go:348-355 only refreshes
within 60s of expiry, and refreshLeases (node.go:384-387) keeps the stale set
on control-plane failure while logging "broker will fail closed at expiry";
diag.go:67-73 likewise tells operators "the broker is failing closed", which
is false on this path. Severity corrected to medium rather than high: CONNECT
is never rewritten so no secret traverses it (the secret-blind guarantee
holds) — the loss is revocation of reachability plus a mislabelled audit
record — and internal/workspace/workspace.go:134,233 report
EgressEnforced:false for both exec and docker backends, so a non-cooperating
workspace could bypass HTTPS_PROXY and reach the host directly regardless.

### `internal/broker/broker.go:390` — isPrivate misses 100.64.0.0/10, leaving the Alibaba Cloud metadata endpoint reachable

**Severity:** medium

**Defect**

isPrivate relies on netip's IsLoopback/IsPrivate/IsLinkLocal* set, which for
IPv4 covers only 10/8, 172.16/12 and 192.168/16 plus 169.254/16. RFC 6598
carrier-grade NAT space 100.64.0.0/10 is not covered, and neither is
255.255.255.255 or NAT64 64:ff9b::/96. The Options doc at line 74-75 claims
this closes 'cloud metadata endpoints' generally, and broker_test.go:201 only
exercises 169.254.169.254.

**How it triggers**

Verified directly: isPrivate(100.100.100.200) == false and
isPrivate(100.64.0.1) == false. 100.100.100.200 is the Alibaba Cloud instance
metadata endpoint. On an Alibaba node with any `Allow` entry that resolves
there (or `Allow: ["*"]`, as in broker_test.go:193), a workspace can fetch
`http://127.0.0.1:PORT/http/100.100.100.200/latest/meta-data/ram/security-
credentials/` and lift the node's instance role credentials — exactly the
class of attack the private-address refusal exists to stop. On a NAT64
network, 64:ff9b::7f00:1 similarly reaches 127.0.0.1.

**Suggested fix**

Extend isPrivate with explicit prefix checks: 100.64.0.0/10, 192.0.0.0/24,
192.0.2.0/24, 198.18.0.0/15, 240.0.0.0/4, 255.255.255.255/32, and 64:ff9b::/96
(plus 64:ff9b:1::/48). Prefer an allowlist of globally-routable space
(netip.Addr.IsGlobalUnicast plus explicit reserved-prefix denies) over an ad-
hoc denylist.

**Verification**

Confirmed, not refutable. I compiled isPrivate (broker.go:390) standalone:
isPrivate(100.100.100.200)==false, isPrivate(100.64.0.1)==false,
isPrivate(100.127.255.255)==false, while 169.254.169.254/127.0.0.1/10.0.0.1
return true. Go's netip.Addr.IsPrivate is RFC 1918 + fc00::/7 only, so RFC
6598 shared address space (100.64.0.0/10) is genuinely uncovered, and
100.100.100.200 is the Alibaba Cloud instance metadata endpoint. Every
refutation avenue fails. (1) No earlier gate: proxy() at broker.go:246 permits
when len(used)>0 || hostMatches(host, opts.Allow), and hostMatches returns
true unconditionally for the "*" pattern; dial() at broker.go:359 is the sole
address-level filter; the net.Dialer is constructed with Control: nil
(broker.go:98) and grep shows no other IP policy in the tree. So
/http/100.100.100.200/latest/meta-data/... reaches the dialer un-refused. (2)
Not test-only: --allow is an operator flag (cmd/remount/main.go:301) and "*"
is a documented supported pattern (broker.go:69-72). The gap also does not
need a wildcard — any allowed hostname resolving into 100.64/10 (a
Tailscale/MagicDNS name, an internal service on a CGNAT network) is dialed
freely, defeating the "everything else on your network stays out of reach"
property. (3) Not intentional: grep -rn "100\.64|CGNAT|6598" over the repo
returns nothing; RFC 6598 is never considered, while broker.go:75,
docs/design.md:160, docs/adr/0010-trust-boundaries.md:38 and
docs/operations.md:150 all assert the general property "closes cloud metadata
endpoints". (4) No existing coverage: broker_test.go:190-205 exercises only
169.254.169.254, the one metadata address the current set does catch. The
claim overreaches on two limbs. 255.255.255.255 is not a meaningful TCP SSRF
target, and RFC 6052 forbids the 64:ff9b::/96 well-known prefix with non-
global IPv4, so a compliant NAT64 gateway drops 64:ff9b::7f00:1 rather than
translating it to loopback. The load-bearing part is 100.64.0.0/10. Severity
is medium rather than high: the default-deny allow list still stands in front,
default --allow is empty, and a workspace with no matching binding or allow
entry is 403'd at broker.go:248 before dial() runs. This is a defense-in-depth
gap in a control whose doc comment (broker.go:74-75) overstates what the code
delivers, exploitable only on a node in CGNAT-adjacent address space with a
permissive or unluckily-resolving allow entry.

### `internal/client/client.go:158` — supervise abandons reconnection if c.sessions is momentarily empty, and Exec has exactly such a window

**Severity:** medium

**Defect**

supervise returns permanently when it observes len(c.sessions) == 0 at the
moment the connection dies; reconnection is otherwise driven only by an
explicit c.call. Exec deletes its placeholder at line 528 and installs the
real session only at line 536 (register), so a client with one session in
flight looks session-less in between.

**How it triggers**

Single-session client. The relay drops the connection between client.go:528
and client.go:536. supervise wakes, reads n == 0, and returns. register then
installs a live, attached Session. There is now a live session, a dead peer,
and no supervisor: a caller that only ranges over Chunks() (Copy, line 762)
never makes another call, so nothing ever redials and the stream is dead
forever with no error. The `gen` field (declared line 49, incremented line
131) is written but read nowhere, suggesting the generation check intended for
exactly this window was never wired up.

**Suggested fix**

Keep the supervisor alive for the life of an open client instead of sampling a
session count (or re-arm it from register/Attach when a session is added while
no supervisor exists), and have Exec/register compare the connection
generation captured before the open call against c.gen, forcing a reattach
when it changed.

**Verification**

Could not refute; the defect is real and reachable. (1) The precondition
holds: transport/peer.go roundTrip receives the response from a buffered cap-1
channel that readLoop fills before fail() runs, so c.call can return nil error
on a peer that is dying at that instant, closing p.done and waking supervise.
(2) The window at client.go:529-536 is a genuine mutex race, not just an
instruction gap: supervise typically parks on c.mu while Exec holds it at
527-529, is woken by the Unlock at 529, and only has to beat Exec's later
register() Lock (behind an err check and an s.mu lock/unlock). It then reads
n==0 and returns. (3) Nothing else redials: `go c.supervise(p)` at line 144 is
the sole supervisor spawn in the repo, there is no ticker/keepalive/health
loop in the client, and Connect's dead-peer check only fires if someone makes
a call. (4) The stall is silent and terminal: s.out is closed only by push()
on an exit chunk (667) or explicit Close (714), never on peer death, so Copy
(762) and Run (736) block on range s.Chunks() forever with no error.
cmd/remount/main.go:684 (`remount exec` without --stdin), client.Run, and
examples/agentloop are exactly that caller shape; only a pty/--stdin session
incidentally heals via s.Input. (5) Not intentional: the comment at lines
138-140 states the supervisor exists precisely so a drop reconnects "even when
the caller is only ranging over chunks", which this defeats;
TestReconnectMidStreamIsLossless (internal/sim/sim_test.go:289) cuts only
after the session is registered, so the window is untested. (6) gen (declared
49, incremented 131) is confirmed written and never read anywhere in the
package, consistent with an unwired generation guard. Attach (554-571)
registers before its call and deletes on error, showing the correct pattern;
Exec and OpenPort (which registers no placeholder at all) do not follow it.

### `internal/client/client.go:177` — Orphan chunk buffer is never freed except by register, so its 256-session cap fills permanently and later sessions silently lose their first chunks

**Severity:** medium

**Defect**

c.orphans is deleted only at client.go:587 (register). Any session id that
gets buffered chunks but is never registered keeps its slice forever.
Session.Close deletes the session from c.sessions at line 709 *before* the
s.close round trip at line 717, and push deletes it at line 670, so chunks
still in flight land in orphans and stay there. Once 256 such ids accumulate,
`len(c.orphans) < 256` is false and every early chunk for every future session
is dropped with no gap frame, no error and no metric.

**How it triggers**

A long-lived client (CLI daemon, agent loop) opens and closes 256 chatty
sessions; each Close leaves at least one in-flight chunk under a now-dead id.
Session 257: the node starts streaming before the s.open response arrives (the
documented behaviour, client.go:44-45), so chunk seq 0 is dropped by the cap.
register delivers nothing, s.next stays 0, and every later chunk piles into
s.pending forever: Chunks() yields nothing, out is never closed, Copy/Run/Wait
hang forever. The 4096-per-session cap produces the same silent hole within a
single fast-producing session.

**Suggested fix**

Give orphans a lifetime: delete the entry when the open call fails or the
session is closed, evict oldest-first on overflow instead of refusing new
inserts, and emit a proto.StreamGap chunk when eviction actually discards data
so the loss is truthful rather than silent.

**Verification**

Could not refute; the mechanism is code-accurate and reachable.
delete(c.orphans, ...) appears only in register (client.go:587) — verified by
grep across the tree, with no sweep, no TTL, and no clear in Connect (which
resets grants at line 132 but not orphans), Session.Close, or push.
Session.Close deletes s.ID from c.sessions at line 709 before the s.close
round trip at 717, and push deletes it at 670; node.go:997 subscribe keeps
streaming until the node processes s.close and calls unsubscribe
(node.go:705), so frames in flight during that window hit handle with
c.sessions[f.S] == nil and are appended to orphans under an id that ids.New
guarantees is never issued again. Attach (554-572) inserts into c.sessions
without draining orphans, so it cannot recycle a stale key either. The
saturation consequence also holds: node.go:963 subscribes before returning
SOpenRes, and transport/peer.go:70-103 dispatches chunks synchronously in the
read loop while the res only wakes the Exec goroutine, so an early chunk can
be handled before register even when it arrives after the response —
MISTAKES.md section 2 records this as an observed hang, which is why the
orphan buffer exists. Once len(c.orphans) reaches 256 that chunk is dropped
with no gap frame, no error and no metric; Exec ignores res.Next, reattach
only runs on reconnect, and deliver parks every later chunk in s.pending while
s.next stays 0, so out is never closed and Copy/Run/Wait block forever. Not
intentional: the repo's own audit flags it as open P1 RM-023 ("orphan and out-
of-order buffers silently stop accepting data at hard-coded limits",
docs/engineering/code-audit-2026-09-02.md:619, with regression test "exceed
orphan bounds and receive a gap" at :631). Two parts of the claim overreach:
memory is bounded (256 x 4096 frames), so this is capacity exhaustion rather
than an unbounded leak; and the 4096-per-session cap is not reachable by a
fast producer within the one-round-trip pre-register window (the node batches
64 chunks per cursor read) — it needs the Exec/OpenPort error paths where the
node opened and subscribed a session the client never learns the id of.
Reachability of the 256-key exhaustion is likewise gated: the common end path
is the exit chunk via push, which leaves no trailing frames, so keys accrue
mainly from Close-on-live-session (cmd/remount/main.go:889 forward path,
client.go:750 cancel path), requiring a long-lived client with 256 such
closes.

### `internal/client/client.go:195` — TailEvents races its own cleanup goroutine: handle can send on the closed events channel (panic), and a second tail silently kills the first

**Severity:** medium

**Defect**

handle snapshots `ch := c.events` under c.mu, releases the lock at line 191,
then sends on ch at line 195. The TailEvents cleanup goroutine sets c.events =
nil and closes that same channel at lines 343-346 with no coordination with
in-flight senders. TailEvents also overwrites c.events unconditionally at line
335, and the older tail's cleanup goroutine then nils out the newer tail's
channel.

**How it triggers**

The CLI runs `remount events -f` (cmd/remount/main.go:958) and the user hits
Ctrl-C while an event frame is being handled: handle holds ch, the cleanup
goroutine closes ch, handle executes `ch <- e` -> "send on closed channel"
panic raised on the transport read-loop goroutine, killing the process.
Separately, with two concurrent TailEvents calls, the first's ctx ending sets
c.events = nil, so the second tail stops receiving events forever and its
channel is never closed, blocking its consumer indefinitely.

**Suggested fix**

Keep event delivery under the same mutex that nils the channel, or register
each tail in a map keyed by id, fan out to registered subscribers while
holding the lock, and close a channel only after removing it and ensuring no
sender still holds it.

**Verification**

The race is real and reachable as described. handle (client.go:189-199)
snapshots ch under c.mu, unlocks at 191, then sends at 195 with no protection;
the select's <-ctx.Done() arm is dead because transport.Peer.readLoop calls
the handler inline with context.Background() (peer.go:70-71, 97-101), so the
send is an unconditional blocking send on a 256-slot buffer. The TailEvents
cleanup goroutine (client.go:340-347) nils c.events and then close(ch) with no
coordination with a sender already holding the snapshot; closing a channel
with a parked sender panics that sender, and there is no recover() anywhere in
internal/ or cmd/, so the panic lands on the read-loop goroutine and kills the
process. Reachability is confirmed: main.go:52 uses
signal.NotifyContext(os.Interrupt, SIGTERM) so Ctrl-C during `remount events
-f` cancels exactly that ctx, the CLI consumer prints to stdout and can lag
behind the 256 buffer, and the control plane pushes event frames from an
independent goroutine (control.go:1033-1044) so a frame can be in handle at
cancellation time. Two details in the claim are wrong and lower the severity:
the second tail's channel is not leaked forever (each TailEvents closes its
own ch when its own ctx ends), and concurrent tails per client are already
unsupported by the protocol (control.go:1026-1030 cancels a peer's previous
tail), so that half is closer to unsupported usage than a defect. Blast radius
is limited: internal/client's TailEvents has exactly one caller
(cmd/remount/main.go:958) and the window is the Ctrl-C shutdown path, so the
outcome is a panic trace and exit 2 on a process that was exiting anyway - a
genuine crash bug, but medium rather than high.

### `internal/client/client.go:584` — Data race: register writes s.ID outside s.mu while reattach reads it under s.mu

**Severity:** medium

**Defect**

register assigns s.ID = id holding only c.mu, never s.mu. reattach reads s.ID
at line 678 under s.mu and again at line 687 under no lock, from a goroutine
Connect launched off a c.mu-protected snapshot taken earlier (lines 133-142).
There is no happens-before edge between that snapshot and the later write, so
this is a genuine race that -race will flag.

**How it triggers**

A reconnect lands while Exec is in flight. Connect snapshots c.sessions, which
contains the Exec placeholder (registered under "pending:"+idem at line 524,
the same *Session pointer), and spawns `go s.reattach`. That goroutine is
descheduled; Exec completes and register writes s.ID = "s_abc" at line 584;
the reattach goroutine then reads s.ID at lines 678/687. A torn or stale read
sends s.attach for the empty or half-written id, and the race detector aborts
any test exercising reconnect-during-open. Session.ID is additionally an
exported field read by callers with no synchronization.

**Suggested fix**

Take s.mu around the ID assignment in register, or make ID immutable by
constructing the Session only after the open response arrives and keying early
chunks solely through the orphans map.

**Verification**

Could not refute; reproduced with -race using the real code paths. Exec
publishes the *Session into c.sessions under "pending:"+idem at client.go:524
while s.ID is still "" and s.attached is false; s.attached=true is set at 534
under s.mu, and s.ID=id is written at 584 in register under c.mu ONLY. Connect
(133-142) snapshots c.sessions under c.mu and spawns `go s.reattach` for every
entry, including that placeholder. reattach reads s.ID at 678 under s.mu and
again at 687 under no lock. The write and the reads take different mutexes,
and the write at 584 happens strictly after the goroutine spawn, so the c.mu
snapshot orders only the constructor into the goroutine, not the later ID
write — no happens-before edge exists. The obvious refutation ("attached is
false the whole time the placeholder is in the map, so reattach short-circuits
before evaluating s.ID") fails: the goroutine holds a direct *Session pointer,
so the map delete at 528 is irrelevant, and it can be scheduled after 534
flips attached to true. Attach and OpenPort are genuinely safe (Attach sets ID
at construction before publishing; OpenPort writes ID and inserts into the map
inside one c.mu section) — only Exec's placeholder publishes a session before
its ID is written. Verified in a scratchpad copy (source tree untouched, git
status clean) with a helper replicating Connect lines 133-142 verbatim and NO
artificial delay in the spawned goroutine, only background CPU pressure of the
kind the sim suite generates under -race: "WARNING: DATA RACE / Read at ...
internal/client.(*Session).reattach() client.go:678 / Previous write at ...
internal/client.(*Client).register() client.go:584". Two claim details are
overstated but immaterial: an empty s.ID read returns early at 680 rather than
sending an attach for an empty id, and the "exported field read by callers"
point is unfounded (cmd/remount/main.go:634 reads s.ID in the same goroutine
after Exec returns, which is ordered). AGENTS.md mandates "make race" be green
and MISTAKES.md records several races here that surfaced only on the third or
fourth repeat run, so this is the class of narrow race this repo treats as a
bug. Severity medium rather than high because the functional consequence is
limited (at worst a skipped reattach); the primary harm is aborting -race runs
that hit the window.

### `internal/control/control.go:577` — An idempotency hit returns a destroyed workspace, and can panic the control plane

**Severity:** medium

**Defect**

The idem fast path dereferences c.workspaces[id] with no nil check and no
state check. wsDestroy never removes the idem row (641-667), so a repeated
create with the same key returns a WSDestroyed tombstone that will never be
offered (offerPending only walks pending workspaces, 980). And because the
idem row is written at 596 before saveWS at 598 — and saveWS only logs its
error (270) — a transient DB failure persists a key pointing at a workspace
that is absent after a restart's load(), making the deref a nil-pointer panic
inside the goroutine spawned by HandleFrame (399), which takes the whole
broker process down.

**How it triggers**

(a) A client creates with key "job-42", destroys the workspace, then retries
"job-42" after a crash; it gets back a workspace in state destroyed and blocks
forever in WaitClaimed. (b) The disk fills for one INSERT: the idem row
commits, the workspaces row does not. After the next broker restart, the
client's ordinary retry with "job-42" panics the control plane, and it panics
again on every retry.

**Suggested fix**

Check for nil and for a terminal state in the idem path: treat a missing or
destroyed target as a cache miss (deleting the stale row) and create a fresh
workspace. Also write the workspace row before the idem row, ideally in one
transaction, and clear the idem entry in wsDestroy.

**Verification**

Half the claim is refuted, half survives. REFUTED: scenario (a). WaitClaimed
explicitly handles the tombstone — client.go:246-263 has `case
proto.WSDestroyed: return nil, proto.Err(proto.CodeConflict, "workspace
destroyed")` — so it does not block forever; workspaces are never removed from
c.workspaces or the workspaces table (no delete(c.workspaces,…), no DELETE
FROM workspaces), so the destroyed entry is a live pointer; the SDK generates
a fresh random key per call (client.go:235 `ids.New("idem")`), reusing it only
for reconnect-retries of that same request, so no caller re-uses "job-42"
across a create/destroy/create cycle (the repo's own audit RM-025 confirms
keys are SDK-internal); and returning the same workspace for the same key,
with a destroyed workspace not re-offered, is the documented intent
(spec/PROTOCOL.md:170). NOT REFUTED: scenario (b). control.go:594-598 commits
the idem row before the workspace row as two separate autocommit statements,
saveWS only logs its error (267-272), load() restores idem rows with no
existence check (253-262: `if err := rows.Scan(&k,&w); err == nil { c.idem[k]
= w }`) and nothing prunes dangling keys (DBIntegrity is only PRAGMA
integrity_check), line 577 `cp := *c.workspaces[id]` has no nil guard even
though snapshotWS at 609-617 guards the identical lookup, HandleFrame
dispatches in a bare goroutine with no recover() anywhere in internal/, and
Client.call retries the identical body on transport.ErrClosed
(client.go:203-226) — i.e. the same-key retry after a broker crash is the
designed behaviour the idem table exists for. The mutex keeps in-memory state
consistent, so reaching nil requires a durability fault (death between the two
commits, SQLITE_FULL/IO error on the second, or WAL tail loss under
synchronous(NORMAL), eventlog.go:278) — a narrow, non-attacker-controlled
trigger that lowers severity, but the mechanism is real, unhandled, and self-
perpetuating once it occurs.

### `internal/control/control.go:764` — A sleep timer that fires during the release window leaves the workspace paused forever

**Severity:** medium

**Defect**

wsSleep creates and persists the wake timer (745-755) before calling
release(), which can hold the workspace in WSReleased for up to ten minutes
(689). If the timer fires in that window, fireTimer marks it Fired and calls
wsWake, which sees a state that is not WSPaused and returns the workspace
unchanged (784-788) without transitioning to pending. wsSleep then writes
WSPaused at 764 on top of an already-consumed timer.

**How it triggers**

ws.sleep is called with after_sec=60 (or at set in the past) on a workspace
with a 3-minute snapshot+upload. Tick fires the timer at t+60s: it is marked
Fired and saved, EvTimerFired is emitted, and wsWake is a no-op because the
state is WSReleased. At t+180s wsSleep sets State=WSPaused. The workspace is
now paused with its only timer already fired, so it is never offered again —
Tick's timer loop skips Fired timers (1232) and offerPending only considers
pending workspaces (980). The client's scheduled resume never happens.

**Suggested fix**

Set the timer after the release completes, or make wsWake tolerate
WSReleased/in-flight release (and re-check for a fired timer after wsSleep
installs WSPaused, promoting straight to pending when one has already fired).

**Verification**

Real, reachable, and undefended. wsSleep persists the timer at
control.go:745-755 and drops c.mu before calling release() at 757; release()
sets WSReleased (685), unlocks (687), and blocks on the node request under a
10-minute timeout (689), so the one-second Tick loop runs concurrently. Tick
(1229-1234) fires any due unfired timer, fireTimer (1105-1116) sets
Fired=true, saves, emits EvTimerFired, and calls wsWake, which at 784-788
returns unchanged for any state other than WSPaused (WSReleased included),
consuming the wake. wsSleep then unconditionally writes State=WSPaused at 764
without re-checking the timer. No recovery path: Tick skips Fired timers
(1232), offerPending only visits WSPending (980), load() repairs only held()
states on restart (208-213), and wsReleased promotes only when held(ws.State)
(904). WSPaused appears in exactly two places in the whole tree (764, 784), so
nothing auto-wakes it; only a manual `remount ws wake` recovers. Reachability
is easy: the CLI accepts any --after duration (cmd/remount/main.go:537-546)
and AtMillis has no future-time validation (748-750), so an `at` in the past
fires on the next tick inside any non-instant snapshot. The event variant is
deterministic rather than racy: release() itself emits EvWSReleased at 706
while the state is WSReleased, and emit -> fireEventTimers (1091-1103) ->
fireTimer -> wsWake no-ops, so `ws sleep --on ws.released` always loses its
wake. The repo's own audit lists this as an open, unfixed P1
(docs/engineering/code-audit-2026-09-02.md:487-504, RM-017) and repeats it at
docs/engineering/codebase-deep-dive-2026-09-02.md:759, so it is not intended
behaviour. Severity medium rather than high: the effect is a silently dropped
scheduled resume, not data loss or a security issue, and an operator can
recover with `remount ws wake`.

### `internal/session/session.go:152` — Signal on an exited exec session sends SIGKILL to a possibly-recycled process group

**Severity:** medium

**Defect**

Signal has no exited/reaped guard. For exec sessions cmd.SysProcAttr.Setpgid
is always true (session.go:381), so it takes the raw
`syscall.Kill(-s.cmd.Process.Pid, sig)` branch, which bypasses the os.Process
ErrProcessDone protection that the fallback at session.go:154 would give.
After cmd.Wait() reaps the child (session.go:410), that PID is free for the OS
to reuse, and the negated PID names whatever process group now owns it.

**How it triggers**

Sessions are retained after exit for ManagerOptions.Retention (24h default,
session.go:211-213) and stay addressable, and the OpSSignal handler
(node.go:684-693) calls s.Signal with no Exited() check. A client that signals
a session that just exited — or simply retries a signal frame — sends SIGKILL
to an unrelated process group on the node host once the PID has been recycled.
Two more routes into the same call: Manager.Remove's TOCTOU at
session.go:252-258 (Exited() is false, the process exits and is reaped, then
s.Kill() fires), and the timeout timer at session.go:353, which is armed after
the runner returns — if finish() (session.go:174-176) ran first, s.timeout was
still nil so nothing stops it, and it calls Signal("KILL") on the dead PID
when it expires.

**Suggested fix**

Return early from Signal when the process has already been reaped: check
`s.Exited()` / `s.exit != nil` under s.mu before signalling, and prefer
cmd.Process.Signal (which reports ErrProcessDone) with the process-group kill
applied only while the process is known live. Arm the timeout timer before
starting the runner, or have finish() set a flag that the AfterFunc re-checks.

**Verification**

Could not refute; the claim is accurate and reachable as described. (1)
Session.Signal (session.go:134-155) holds s.mu but never checks
s.exit/s.exited, and finish() (167-182) never clears s.cmd, so the s.cmd==nil
guard at 143 does not fire post-exit. (2) startExec sets Setpgid:true
unconditionally (381), so exec always takes the raw syscall.Kill(-pid,sig)
branch (152), bypassing the ErrProcessDone protection Go gives the fallback at
154 — the PTY path (which leaves Setpgid false and uses cmd.Process.Signal) is
guarded, exec is not; that asymmetry indicates a lost guard, not intent. (3)
The primary route needs no race: OpSSignal (node.go:684-693) resolves via
sessionFor (node.go:896-905), which checks only workspace authorization, and
exited sessions remain in m.sessions until scheduleReap fires at Retention
(368-370) — the production node (node.go:142-146) leaves Retention unset, so
it defaults to 24h (211-213). (4) SSignalReq carries no sequence number,
unlike SInputReq's ISeq dedupe at 96-102, so a retried signal frame is
genuinely re-executed. (5) The repo's own test at
internal/session/session_test.go:130-140 calls s.Signal after s.Wait returns,
escaping the syscall only because the name is "BOGUS" and the unknown-signal
check at 146-149 precedes the kill — direct proof the post-reap path is
reachable. (6) Both secondary routes hold: Open arms s.timeout at 351-357
after the runner returns with no s.exit check, while finish only stops a non-
nil timer (174-176), so a fast-exiting process leaves an unstoppable timer
that later calls Signal("KILL") on a reaped pgid; Remove's unlock-then-Kill at
252-258 is a real if narrow TOCTOU. (7) startExec uses plain exec.Command with
no namespace isolation, so a recycled pgid names another process group in the
node's own PID namespace. The only mitigation is probabilistic — harm needs
PID wraparound and the recycled PID to be a process-group leader, else
kill(-pid) returns ESRCH — which bounds likelihood, not correctness, and this
service makes every exec session a group leader by design.

### `internal/session/session.go:345` — The info chunk is appended after the runner starts, so seq 0 is not guaranteed to be it — and it can be dropped entirely

**Severity:** medium

**Defect**

Violates "seq zero is always the info chunk" — an invariant the package's own
test asserts (session_test.go:44-46). The runner is started first
(session.go:333-342): startExec spawns pump goroutines at session.go:406-407,
startPTY at session.go:441, startPort at session.go:464, all of which call
Log.Append with no happens-before relationship to the info Append on line 345.
Worse, the Append's error is discarded (`_, _ =`): if the session has already
finished, finish() has appended the exit chunk and called Log.Close
(session.go:179-180), so the info Append returns ErrLogClosed and the header
is silently lost.

**How it triggers**

Two outcomes from the same window. (a) The child writes before Open resumes —
seq 0 is a StreamStdout chunk and the info chunk lands at seq 1+, breaking
every replay-from-0 consumer that expects the header first. (b) The child
exits and is reaped before Open resumes — the log is already closed, the info
Append fails, and the log has no SessionInfo at all, so a cold replay cannot
reconstruct the session header. I could not force either in 2,000 port-banner
sessions and 900 exec sessions under scheduler pressure (the window between
the runner returning and the Append is ~1us against ~1ms of fork/exec
latency), so this is latent rather than routine — but nothing in the code
enforces the ordering, and Go's async preemption can deschedule the Open
goroutine for 10ms+ on a loaded node.

**Suggested fix**

Write the info chunk before the process can produce output: append a
placeholder as seq 0 before the runner switch, or have each runner do its
start work in two phases — create the process/conn and set Info.PID, append
the info chunk, then start the pump goroutines. At minimum stop discarding the
Append error at session.go:345.

**Verification**

Not refutable. (1) No synchronization exists: Log.Append assigns seq strictly
in call order under l.mu with no reservation for StreamInfo, and each runner
releases s.mu (session.go:403/439/462) before spawning the pump goroutines, so
nothing orders pumps against the info Append at session.go:345. (2) No caller
constraint helps — Node.sOpen (node.go:955) opens arbitrary user programs,
including instant-exit ones. (3) I confirmed reachability empirically: in a
scratch copy of the repo, inserting only a 50ms sleep at exactly that gap
(simulating a preemption, no other change) produced both outcomes — 'seq=0
stream=1 data="hi\n"' with info at seq=1, and 'INFO-APPEND seq=0 err=session:
log closed' causing the package's own test to fail with "no info chunk"
because the error is discarded via `_, _ =`. (4) The behavior is not
intentional-and-correct: seq 0 = info is a MUST-level protocol guarantee
(spec/PROTOCOL.md:233 and the :340 conformance list) and an AGENTS.md:68
invariant, and the repo's own audit already logs it as RM-019
(docs/engineering/code-audit-2026-09-02.md:528). Severity corrected down from
the claim's framing, not refuted: no in-tree consumer decodes StreamInfo
(internal/client/client.go:743,764 switches only on stdout/stderr/gap/exit),
and SessionStatus.Info (node.go:747) still serves the header from memory while
the session is retained, so the damage is to the published protocol guarantee
and cold log replay rather than to any shipped client, and the trigger needs a
multi-millisecond deschedule inside a ~microsecond window.

### `internal/session/session.go:348` — Sessions that fail to start never fire OnExit, so no s.exited event reaches the control plane

**Severity:** medium

**Defect**

On the start-failure path Open calls s.finish(...) and returns at
session.go:347-349 without ever starting the `go func() { <-s.exited;
m.opts.OnExit(...) }` watcher registered at session.go:358-364. OnExit is the
only caller of n.emit(proto.EvSExited, ...) (node.go:144-146).

**How it triggers**

Open a session with a program that does not exist, a bad Cwd, or a port
session whose dial is refused. finish() writes the StreamExit chunk and closes
the log, so a client tailing the log sees the failure, but the node never
emits EvSExited — the workspace event log and the control plane show the
session as opened and never exited. Anything driving off the event stream
(audit, reconciliation, session accounting) leaks that session forever.

**Suggested fix**

Call the exit notification on the failure path too — either invoke
m.opts.OnExit(s, *s.ExitInfo()) before the `return s, nil` at session.go:349,
or start the watcher goroutine before the runner switch so both paths funnel
through the same code.

**Verification**

The core defect is real and reachable, though two of the three scenarios given
are wrong and the impact is overstated. Confirmed statically and by running
the package: Open (session.go:346-350) calls finish() + scheduleReap() and
returns before installing the `go func(){ <-s.exited; OnExit(...) }` watcher
(session.go:358-364), and OnExit is the only caller of n.emit(proto.EvSExited,
...) (node.go:144-146). An out-of-module program using the real package showed
`Exited: true, ExitInfo: &{-1 exec: "…": executable file not found in $PATH}`
with "OnExit NOT called within 1.5s", while a normally-exiting session did
fire OnExit. Node-side, sOpen (node.go:958-962) emits EvSOpened
unconditionally and never checks s.Exited(), so an exec/PTY start failure
leaves s.opened with no matching s.exited in the workspace event log and
control plane. Partial refutations: (1) the port case is already handled —
portOpen (node.go:974-988) checks s.Exited(), calls sessions.Remove, and
returns CodeUnreachable, and it never emits EvSOpened, so a refused dial
produces no asymmetry; (2) a bad Cwd is rejected by processHandle.Prepare
(workspace.go:182-194) before Open is called, so no session and no event; (3)
nothing actually "leaks forever" — scheduleReap still runs, the log is closed
with a StreamExit chunk, Wait/Exited work, and metrics.SessionsExited is
incremented inside finish() regardless; no in-repo consumer does session
accounting off the event stream (only sim_test.go:219 asserts the event type
exists). What remains is an unpaired s.opened event for exec/PTY sessions
whose program cannot be started — an audit/observability correctness gap for
external consumers that pair opened/exited, also flagged as P1.33 in the
repo's own deep-dive doc, which argues against it being intentional.

### `internal/artifact/artifact.go:259` — Snapshot writes a tar header sized from a stale Lstat, so a file changing size corrupts or aborts the archive

**Severity:** medium

**Defect**

hdr comes from tar.FileInfoHeader(info, link) at line 239 using the size from
the Lstat at line 228, but the body is streamed with io.Copy at line 259 from
a file opened later at line 255. If the file grew, tar.Writer returns
"archive/tar: write too long"; if it shrank, the next WriteHeader fails with
"archive/tar: missed writing N bytes". Either way Snapshot returns an error
mid-stream.

**How it triggers**

Verified PoC: while Snapshot runs, a concurrent writer alternately grows a
file to 1 MiB and truncates it to 10 bytes. Snapshot fails with `archive/tar:
missed writing 4086 bytes` or `archive/tar: write too long` on every run. Any
agent writing a log or build output during a checkpoint reproduces this. Via
SnapshotToStore (line 354) the pipe is closed with the error so no artifact is
stored, turning a routine concurrent write into a hard snapshot failure.

**Suggested fix**

Stat the opened descriptor (f.Stat()) and build the header from that, then
copy exactly hdr.Size bytes with io.CopyN, padding or re-reading on short
reads; or open the file first and derive both header and body from the same
descriptor.

**Verification**

Could not refute; the defect is real and reachable. (1) Mechanism verified: I
reproduced lines 226-265 standalone (Lstat -> FileInfoHeader -> WriteHeader ->
Open -> io.Copy) and got exactly `archive/tar: write too long` when the file
grows and `archive/tar: missed writing 4086 bytes` when it shrinks. The shrink
error surfaces at the next WriteHeader's implicit Flush or at tw.Close() (line
266), so it escapes even for the final entry. (2) No mitigation exists in the
code: nothing between the Lstat at line 228 and the io.Copy at line 259
revalidates size, and there is no lock, retry, io.CopyN, or padding fallback.
(3) Reachable through a user-facing path with no quiescing:
cmd/remount/main.go:573 (`remount ws snapshot WS`) -> client.Snapshot
(internal/client/client.go:462) -> proto.OpWSSnapshot ->
internal/node/node.go:861-874 -> n.snapshot (line 1258) ->
processHandle.Snapshot -> artifact.Snapshot. That handler never stops
processes, while the release path deliberately does
(internal/node/node.go:1323: "// Stop processes first so the snapshot is
quiescent." then n.sessions.KillWorkspace) - the contrast shows mutation-
under-snapshot is understood to be unsafe and the explicit-snapshot path was
simply not covered. Sessions run real processes with cwd inside the workspace
root (internal/workspace/workspace.go:181), so an agent writing a log during a
checkpoint is the ordinary case. (4) Failure is terminal for the operation:
node.snapshot and SnapshotToStore (line 354) both propagate via
pw.CloseWithError, so Put aborts and no artifact is stored. (5) Not
intentional: ADR docs/adr/0006-snapshots-are-files.md says nothing about live
mutation, and the repo's own audit lists it as an open defect requiring a fix
(docs/engineering/code-audit-2026-09-02.md:865, RM-035, "or fail midway"). A
sibling TOCTOU at line 229 (file deleted between the WalkDir at line 202 and
the Lstat) returns ENOENT the same way. Severity is medium rather than higher
because it fails loudly instead of storing a corrupt artifact, is retryable,
and the load-bearing move/release path already quiesces - the exposure is
limited to the explicit live-checkpoint API.

### `internal/artifact/artifact.go:325` — Restore has no cap on entry count or extracted bytes

**Severity:** medium

**Defect**

io.Copy at line 325 is unbounded, the entry loop at 290-345 is unbounded, and
gzip.NewReader (line 279) is fed straight from the network. Nothing limits the
decompressed size of an artifact a node will restore.

**How it triggers**

A client PUTs a few-hundred-KB gzip of zeros as an artifact
(internal/server/server.go:186 accepts any body that hashes to the named id),
then creates a workspace with Spec.RestoreFrom pointing at it.
internal/node/node.go:1126 streams it to artifact.Restore, which expands it to
hundreds of GB in the node's data directory, filling the disk for every
workspace on that node.

**Suggested fix**

Wrap the gzip reader in an io.LimitedReader with a configured maximum, count
entries, and use io.CopyN bounded by hdr.Size; fail the restore and clean up
when the budget is exceeded.

**Verification**

Not refutable. The mechanism is exactly as described:
internal/artifact/artifact.go:279 wraps the caller's reader in gzip.NewReader,
the entry loop at 290-345 has no entry-count cap, and io.Copy(f, tr) at line
325 has no per-file or cumulative byte cap. The only in-loop validation is
path-escape rejection at 298-310. A repo-wide grep for
LimitReader/MaxBytesReader/quota finds limits only at
internal/server/server.go:232 (event JSON, 1<<20) and
internal/node/node.go:1298 (error body) - nothing on the artifact or restore
path. The chain is reachable: internal/server/server.go:186 passes r.Body
straight to Store.Put and only compares the resulting digest to the requested
id, so a gzip bomb (which hashes to its own id) is accepted;
internal/node/node.go:1126 fetches it and internal/workspace/workspace.go:148
(process) and :269 (docker) hand the stream to artifact.Restore. There is no
free-space precheck before materialize (diskSpace in internal/node/diag.go is
diagnostics only) and the docker backend passes only --cpus/--memory, never a
disk quota. Refutation attempts that failed: (1) Auth - the path requires the
shared bearer token (Server.authed; Control.Authenticate at control.go:301),
so it is token-holder-only and not reachable from an untrusted workspace, but
that limits the attacker set, it does not make the code correct. (2) Cleanup -
both Create sites do os.RemoveAll(root) when artifact.Restore errors, so an
ENOSPC failure does reclaim the partial tree; this makes the fill transient
rather than permanent but does not prevent it, and concurrent or repeated
restores keep the disk pressure up. (3) Intentional - the opposite: the
project's own audits confirm it. docs/engineering/code-
audit-2026-09-02.md:740-758 (RM-029) and docs/engineering/codebase-deep-
dive-2026-09-02.md:921-923 (P1.19) both mark it "Source-confirmed", state it
"permits disk exhaustion and decompression bombs from any token holder", and
require a gzip-bomb regression test.


## Low

### `internal/fsops/fsops.go:59` — A NUL byte in a request path yields an internal error instead of bad_request

**Severity:** low

**Defect**

Resolve never rejects NUL. path.Clean keeps it, the Lstat loop at 63-72 walks
past the NUL-bearing component up to a directory that does exist, and the
reconstruction at 80-83 re-attaches it, so a syntactically invalid path
reaches the syscall layer and fails with EINVAL, which mapErr's default branch
(line 414) reports as CodeInternal.

**How it triggers**

Verified PoC: FS.Read("a\x00b") returns `internal: open /.../ws/a b: invalid
argument`, and FS.Write("a\x00b", ...) returns `internal: rename
/.../ws/.remount-4041636957 /.../ws/a b: invalid argument`. Client-supplied
garbage is reported as a server fault, which will page an operator and pollute
error metrics; the write path also does the full temp-file dance before
failing.

**Suggested fix**

Reject any path containing \x00 at the top of Resolve with
proto.Err(proto.CodeBadRequest, "path contains NUL"), and add
ENAMETOOLONG/EINVAL to mapErr's bad_request branch.

**Verification**

The mechanism is real and I reproduced it verbatim: path.Clean keeps the NUL,
the Lstat loop at fsops.go:63-72 walks up to the existing root, EvalSymlinks
succeeds there, lines 80-83 re-attach the NUL suffix, and the syscall fails
EINVAL which mapErr's default (line 414) reports as CodeInternal.
Read/Write/Mkdir/Stat all return `internal: ...: invalid argument`, and it is
reachable — internal/node/node.go:750+ passes the decoded req.Path straight to
FS().Read with no validation anywhere upstream. So I cannot refute the
behavior. However the severity and framing are substantially overstated. (1)
NUL is not special: Read("f.txt/inner") returns `internal: ...: not a
directory` and a 400-char name returns `internal: ...: file name too long`, so
this is mapErr's general "unmapped errno -> internal" bucket
(fsops.go:406-414), not a NUL-specific bug at line 59; a NUL check there would
leave the same misclassification for far more common inputs. (2) The claimed
operational impact does not exist in this repo:
internal/metrics/metrics.go:165-200 is the complete metric list and contains
no error-code counter, and deploy/ has no alerting config, so "page an
operator and pollute error metrics" is unsupported. (3) The write path leaves
no residue — the defer at fsops.go:167-171 removes the temp file when the
rename fails (verified: directory contained only the pre-existing file
afterward). No security impact either: Resolve("../../etc\x00/passwd") still
normalizes inside the root, so the jail holds. Net: a cosmetic error-taxonomy
issue (client garbage reported as `internal` instead of `bad_request`) with no
security, data-loss, or availability consequence.

### `internal/fsops/fsops.go:99` — Read and Write on a FIFO block the request goroutine forever

**Severity:** low

**Defect**

os.Open at line 99 has no O_NONBLOCK and no file-type check before opening;
the IsDir check at line 108 happens after the open has already blocked.
Opening a FIFO with no writer blocks in open(2) indefinitely. The same applies
to os.OpenFile at line 141 (append) and os.ReadFile at line 351 (Edit). An
agent inside the workspace can create a FIFO with mkfifo, which is an ordinary
unprivileged operation.

**How it triggers**

Verified PoC: syscall.Mkfifo(<root>/pipe, 0644), then FS.Read("pipe", 0, 0)
had not returned after 2 s and would never return. In production this pins a
node request goroutine (internal/node/node.go:759) per request; a handful of
fs.read calls against planted FIFOs exhausts the node's ability to serve the
workspace. Device nodes have the same shape.

**Suggested fix**

Lstat the resolved path first and reject anything that is not a regular file
(or a directory, for the ops that want one) with bad_request; or open with
O_NONBLOCK|O_CLOEXEC, fstat the descriptor, and reject non-regular modes
before reading.

**Verification**

The mechanism is real and I reproduced it: fsops.go:99 opens with no
O_NONBLOCK and no type gate, and the IsDir check at :108 runs only after open
returns; a PoC (syscall.Mkfifo in the root, then FS.Read) was still blocked
after 2s and never returns. It is reachable — node.go:748 routes OpFSRead
straight to w.handle.FS().Read, and both backends serve the same host dir the
agent writes into (workspace.go:170 process root, :281 docker "-v
root:/work"), so an in-workspace mkfifo lands in the served tree. Lines 141
and 351 have the same shape. So I cannot refute the defect. But the claimed
impact is wrong. No lock is held across the open and handleReq runs each
request in its own goroutine (node.go:513); in the same PoC a Write plus Read
of a normal file in the same FS completed while the FIFO open stayed blocked,
so the node keeps serving the workspace. "A handful of fs.read calls exhausts
the node" is unsupported: each hung call costs one goroutine and one OS
thread, and Go's thread cap is 10,000, so exhaustion needs ~10k concurrent
hung reads. Blast radius is one workspace: every FS op passes n.authorize ->
control.VerifyGrant with client/ws/node/generation binding (node.go:569-592),
so a planted FIFO can only hang requests made against that same workspace by a
client already holding a signed grant for it — the workspace hangs its own
operator, which is the "workspace trusted with nothing / if compromised,
nothing worth having" row in docs/design.md:143-152. Search (fsops.go:293) and
Snapshot (artifact.go:254) already gate on IsRegular, and Destroy is
RemoveAll, so move/release/snapshot are unaffected and no fd leaks (the open
never succeeds). What is left is a genuine robustness bug — a FIFO, socket, or
device node, malicious or merely present in a repo, makes fs.read / append-
write / edit hang forever with no client-side deadline (client.go sets
timeouts only for hello and reattach) instead of erroring — fixed by an Lstat
or st.Mode().IsRegular() gate. That is low severity, not a node-level or
cross-tenant DoS.


## Method

The six dimensions were the credential broker, filesystem containment, the
session log, the claim queue and distributed correctness, authentication and
authorization, and the client and transport. Each reviewer was given the
specific claims the design makes and asked to defeat them, and was told to
report only defects it could attribute to a line.

Each candidate then went to a fresh agent whose instruction was to refute it:
a finding was to be dismissed if the code already handled it, if the scenario
was unreachable given how callers use the function, if a lock or earlier check
made it impossible, or if the behaviour was intentional. Uncertainty was to
resolve as refuted. Twenty-eight findings were dismissed on those grounds.

Several verifiers reproduced the defect rather than reasoning about it, either
by copying the function into a standalone program or by running the real code
under a Go overlay so no repository file was modified. Those reproductions are
quoted in the verification notes.

## Status

No fix has been applied. Every finding here is open. This document is a bug
register, and the line numbers are accurate as of commit `8c37a22`; re-locate
each symbol before acting on it.
