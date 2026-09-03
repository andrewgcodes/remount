# Plan B: repository-executable path to the agent-computer standard — 2026-09-03

**Status:** Proposed

**Scope:** implementation, tests, fixtures, simulations, packaging, and
documentation that can be completed from this repository

**Preferred reference path:** E2B whole-node provisioning, OpenCode, and OpenAI
through the Remount broker

## 1. Directive

This plan replaces the earlier Plan B requirement for design partners,
customers, vendor coordination, external reviewers, paid pilots, and public
release actions. Completion must not depend on another person or organization
doing anything.

The plan may exercise a public API when credentials are already available, but
every such lane is:

- fully automated;
- disposable and cleanup-verifying;
- secret-gated;
- visibly `unavailable` when its credential or service is unavailable; and
- supplementary to a deterministic repository-local contract.

No live credential is required for the code-only completion gate. E2B is the
preferred live compute lane and OpenAI is the preferred live model lane, but a
local fake E2B API and a local OpenAI-compatible endpoint are the required
deterministic paths.

## 2. Product boundary

Remount is the open protocol and reference implementation for the boundary
between an agent harness and the computer it acts through.

The harness owns models, prompts, reasoning, and harness-specific state.
Infrastructure systems own machines, networks, identities, reusable secrets,
and telemetry in their respective domains. Remount owns:

- portable workspace identity and generation;
- placement, claims, leases, readiness, and fencing;
- reconnectable cursor-based sessions with explicit gaps;
- authoritative filesystem checkpoints and safe movement;
- durable Agent identity, inbox, lifecycle, and approvals;
- secret-blind brokered capabilities; and
- the canonical event and evidence trail for those resources.

Every Plan B item must deepen one of these six primitives:

1. Workspace.
2. Agent.
3. Snapshot, move, or fork.
4. Broker.
5. Approval.
6. Event log.

Work that does not deepen one of them belongs in an example, an integration
fixture, or a later project.

## 3. Starting point

The current repository already contains the difficult semantic core:

- transactional lifecycle state and events;
- generations, leases, grants, authorization revisions, and controller epochs;
- `pending`, `claiming`, `claimed`, and authoritative `ws.ready`;
- cursor sessions, input deduplication, explicit replay gaps, and tiered logs;
- deterministic filesystem snapshots, chunked artifacts, and warm standby;
- secret-blind broker substitution, typed egress, budgets, and approvals;
- durable Agents, ACP, queues, children, schedules, previews, SDKs, and MCP;
- local process and Docker backends, a gVisor integration lane, and
  capability-honest Firecracker gates;
- provider-neutral pools with an E2B whole-node driver; and
- release, packaging, conformance, fuzz, simulation, and clean-build tooling.

The implementation closure and continuation handoff remain the authority for
what has actually been verified. This plan does not turn an existing
`externally gated` row green by restating it.

The most important remaining repository-executable work is not another broad
feature expansion. It is to make one reference path reproducible, hermetic,
failure-tested, externally exercisable when credentials exist, and useful as a
black-box conformance target.

## 4. Target end state

After this plan, a clean checkout can prove the following without a real cloud
or model credential:

1. A real OpenCode process can run inside a Remount workspace against a
   deterministic OpenAI-compatible test server through the credential broker.
2. The workspace contains a placeholder, never the reusable upstream value.
3. The harness can edit files, produce a structured ACP transcript, detach,
   reconnect, sleep, restore, move, and continue.
4. A fake E2B service proves the exact REST contract, retries, pagination,
   ambiguous-create recovery, tenant filtering, timeout handling, and cleanup.
5. The same candidate can optionally build an E2B template, provision whole
   nodes, run the reference workload, kill a node, recover, and verify teardown
   when `E2B_API_KEY` exists.
6. The same candidate can optionally replace the fake model with OpenAI when a
   server-side key exists, without putting that key in the workspace, test
   output, evidence bundle, or provider metadata.
7. A black-box conformance runner can test the reference binary or another
   implementation for Remount's authority, cursor, snapshot, broker, approval,
   and event semantics.
8. Repository-local deployment examples validate composition with Docker
   Compose, OpenTofu/Terraform, Helm/Kubernetes, private-network sidecars,
   external secret sources, S3-compatible storage, and Prometheus/event
   export, without claiming those systems are replaced by Remount.
9. Relay operation payloads can be protected from the relay through a
   negotiated end-to-end encryption mode, while routing metadata remains
   explicitly visible.
10. One aggregate command builds a candidate and emits a machine-readable
    evidence manifest that distinguishes `passed`, `failed`, and `unavailable`.

This is a code-completeness target, not a production-readiness declaration.

## 5. Hard constraints

### 5.1 No external actor in the critical path

Plan completion requires no:

- customer or design partner;
- Lakeside workload or repository;
- vendor support request, beta access, or roadmap commitment;
- human approval during a test;
- public package, image, or release publication;
- public governance process;
- external penetration test or architecture sign-off;
- production account migration; or
- sustained on-call operation.

Approvals are still tested as a Remount primitive, but a deterministic test
client decides them. No person waits at a prompt.

### 5.2 No secret in a workspace

The preferred live model flow is:

```text
OpenCode
  -> OPENAI_API_KEY=ref:b_openai
  -> OPENAI_BASE_URL=${REMOUNT_BROKER}/d/api.openai.com/v1
  -> node broker
  -> server-side OPENAI_API_KEY
  -> OpenAI
```

The key may exist only at the server or node secret boundary. It must never be
placed in:

- workspace files or harness state;
- `.remount/env`;
- snapshot or shared-volume artifacts;
- argv;
- events, diagnostics, metrics, transcripts, or test logs;
- E2B environment, metadata, template layers, or inventory; or
- committed fixtures.

Leak scans plant a synthetic credential-shaped canary. They never write the
real key merely to prove that scanning works.

### 5.3 Capability honesty

- A fake provider proves Remount's adapter logic, not the real provider.
- A live E2B Create/List/Destroy test proves that provider lifecycle only.
- E2B network controls do not upgrade a Remount backend capability.
- Process and Docker remain `local` or cooperative backends.
- An E2B node running process or Docker is not a hardened hostile-workspace
  boundary.
- A local OpenAI-compatible endpoint proves request shape and broker behavior,
  not the OpenAI service.
- A gVisor CI lane proves only the exact runner, `runsc`, rootfs, network, and
  candidate it exercised.
- A skipped lane is `unavailable`, never a pass.
- A one-time live pass is evidence for one commit, not a permanent capability.

### 5.4 Filesystem continuity only

Plan B does not claim migration of RAM, kernel state, arbitrary processes,
GPUs, or live TCP connections. Movement checkpoints the filesystem, advances
authority, and restarts or reopens harness state according to the declared
recipe capability.

## 6. Evidence model

Every acceptance scenario emits one JSON record:

```json
{
  "scenario": "B3",
  "candidate": "<git commit>",
  "layer": "sim",
  "status": "passed",
  "command": "go test ...",
  "started_at": "<UTC timestamp>",
  "duration_ms": 1234,
  "environment": {
    "os": "linux",
    "arch": "amd64",
    "backend": "docker",
    "provider": "fake-e2b"
  },
  "cleanup": "verified",
  "artifacts": ["test-report.json"]
}
```

Records may name environment variables, but never their values. Provider
object IDs may appear only in an ephemeral CI evidence artifact and only when
needed to prove cleanup. They are not committed as evergreen proof.

### 6.1 Evidence layers

| Layer | Meaning |
|---|---|
| `code` | unit, race, fuzz, static, generator, or deterministic fixture proof |
| `sim` | multi-peer composition under `internal/sim` fault injection |
| `host-ci` | exact kernel/runtime mechanism exercised on an identified CI host |
| `live-service` | exact candidate exercised against E2B or OpenAI with disposable state |
| `artifact` | reproducible binary, image, SDK package, SBOM, or install path built and verified locally |

`production` is deliberately not an evidence layer generated by this plan.
Production proof requires an operated deployment and belongs in a separate
ledger.

### 6.2 Evidence outcomes

- `passed`: the command ran and every postcondition, including cleanup, passed.
- `failed`: the command ran and a required postcondition failed.
- `unavailable`: the check could not run because a credential, daemon, host
  capability, or service was absent.

There is no `skipped-success` outcome. Aggregate reporting may complete with
optional rows unavailable, but it must print them prominently.

## 7. Reference matrices

### 7.1 Required hermetic matrix

| Layer | Reference choice |
|---|---|
| control | standalone single-writer SQLite |
| node | two in-process nodes for simulation |
| local execution | process for semantics; Docker for real OpenCode |
| isolated execution | gVisor on the existing exact-host CI lane |
| artifacts | local deterministic store and pinned MinIO |
| model | local OpenAI-compatible Responses and Chat Completions fake |
| harness | a pinned OpenCode package |
| provider | fake E2B HTTP service generated from checked-in contract fixtures |
| identity | signed local credentials and a pinned local OIDC fixture |
| reusable secrets | synthetic broker binding; local external-secret fixture |
| telemetry | canonical events, metrics, diagnostics, and local export receiver |
| packaging | static binaries, OCI archive, local SDK packages, SBOM, checksums |

Every row must run from a clean checkout without E2B or OpenAI credentials.

### 7.2 Optional live matrix

| Layer | Reference choice |
|---|---|
| coordinator | disposable E2B sandbox or another fully automated public test endpoint |
| worker capacity | E2B whole-node provider driver |
| harness | OpenCode |
| model | local fake first; OpenAI second when a key is available |
| credential path | Remount binding and broker only |
| artifacts | candidate-scoped S3-compatible or coordinator store |
| security profile | `local` unless the exact inner backend earns stronger capabilities |
| lifecycle | create, enroll, ready, run, checkpoint, replace, resume, destroy |

The E2B template is built from the exact candidate by checked-in tooling. The
lane must not depend on a manually maintained template, binary URL, enrollment
token, tenant, or pool variable. The test generates ephemeral values, records
only non-secret identifiers, destroys every sandbox and template it created,
and confirms they are absent.

If E2B cannot run the required inner gVisor contract, the lane remains a
provider and continuity proof under `local` security. It must not claim
`isolated` or `multi_tenant`.

## 8. Phase B0: regenerate current truth

### Work

1. Add a repository command that enumerates the acceptance scenarios, their
   owning test, required environment, and latest outcome.
2. Make every credential- or host-gated workflow emit an evidence record with
   `unavailable` rather than relying on a green GitHub job containing a skipped
   step.
3. Re-run the broad local gates against the starting commit.
4. Fix any current deterministic or race failure before adding scope.
5. Update the implementation closure with a generated Plan B starting ledger.
6. Check generated protocol, SDK, ACP, security-profile, and `llms.txt`
   artifacts for drift.

### Required gates

```sh
make lint
make test
make race
make conformance
make fuzz FUZZTIME=30s
make dist
go mod verify
go mod tidy -diff
```

### Exit

- The exact candidate and every command are recorded.
- Local failures are fixed or explicitly open.
- Every unrun external or host check is `unavailable`.
- No historical report is rewritten to imply a later pass.

## 9. Phase B1: deterministic OpenCode and broker lane

### Work

1. Add a bounded local OpenAI-compatible test server that implements only the
   Responses and Chat Completions behavior needed by the acceptance scenarios.
2. Script deterministic model responses that make OpenCode:
   - create a file;
   - read it back;
   - invoke a shell command;
   - request an approval;
   - finish with a known result; and
   - resume a previous session.
3. Run a pinned real OpenCode package in Docker through the standard
   `opencode` recipe.
4. Route the fake upstream through a normal Remount binding. The fake upstream
   requires a synthetic server-side bearer value, while OpenCode sees only
   `ref:b_openai`.
5. Make external model metadata discovery optional or point it at a local
   fixture so the deterministic lane does not require `models.dev`.
6. Scan the workspace tree, OpenCode state, snapshot, transcript, events,
   diagnostics, and evidence record for the synthetic reusable value.
7. Add an optional live OpenAI variant using the same scenario and broker path.
8. Pin the OpenCode version and package integrity in test tooling. Updating it
   is a visible dependency update with a compatibility run.

### Acceptance scenarios

| ID | Scenario | Evidence |
|---|---|---|
| B1 | OpenCode edits `GREETING.txt` through the local fake and exits zero | `code` |
| B2 | ACP transcript contains the expected tool call and terminal result | `code` |
| B3 | Client disconnect and reattach produce byte-identical session output | `sim` |
| B4 | Sleep, restore, and `session/load` retain the prior OpenCode conversation | `sim` |
| B5 | Automated approval commits before the approved tool result | `sim` |
| B6 | Synthetic reusable value occurs in zero workspace or durable evidence bytes | `code` |
| B7 | Live OpenAI run uses the broker and passes the same leak scan | optional `live-service` |

### Exit

The default OpenCode end-to-end lane is deterministic and keyless. A missing
OpenAI key affects only B7 and is reported as `unavailable`.

## 10. Phase B2: self-contained E2B provider lane

### Work

1. Check in an E2B template definition and candidate build tool using an
   official, pinned E2B CLI or SDK outside the Go runtime dependency graph.
2. Build the exact static Remount binary and OpenCode version into the
   candidate template. Do not fetch a mutable Remount binary during node
   startup.
3. Replace the live test's manually supplied template, binary URL, enrollment
   token, tenant, and pool with values created by the test harness.
4. Add a disposable coordinator mode for the live test:
   - start the control plane and artifact service;
   - expose only the authenticated endpoints required by nodes and clients;
   - generate single-use enrollment capabilities;
   - run the E2B driver from the test controller; and
   - shut down after provider inventory is empty.
5. Keep `E2B_API_KEY` only in the outer test controller. It must never enter
   an E2B sandbox, Remount event, template layer, or workspace.
6. Exercise create, list, pagination, duplicate create, ambiguous response,
   destroy, eventual deletion, max-lifetime replacement, and cleanup recovery.
7. Make the fake E2B server share the same request/response fixtures as the
   live lane so provider drift changes one visible contract.
8. Add a scheduled or manually dispatched live workflow that is
   `unavailable` when the key is absent and otherwise uploads the evidence
   manifest.

### Acceptance scenarios

| ID | Scenario | Evidence |
|---|---|---|
| B8 | Fake E2B contract covers create, paginated list, filter, and idempotent destroy | `code` |
| B9 | Ambiguous create is recovered by unique name without duplicate machines | `code` |
| B10 | Pool reconciliation never holds control authority while calling E2B | `sim` |
| B11 | Live E2B candidate enrolls and reaches `ws.ready` | optional `live-service` |
| B12 | Live E2B workspace runs the keyless OpenCode scenario through the broker | optional `live-service` |
| B13 | Every live sandbox and candidate template is absent after cleanup | optional `live-service` |

### Exit

The E2B adapter is fully testable without E2B. When the key exists, one command
builds, provisions, proves readiness, runs, and tears down the exact candidate.
No manually prepared vendor resource is a prerequisite.

## 11. Phase B3: continuity and authority failure matrix

### Work

1. Express every failure through the public client or protocol path, not a
   private state mutation.
2. Extend the deterministic E2B pool simulation and optional live lane with:
   - client disconnect during output;
   - node disconnect before and after checkpoint preparation;
   - control restart with claimed workspaces;
   - stale node reconnect after generation advance;
   - lease expiry during slow restore;
   - provider create accepted but response lost;
   - provider inventory temporarily unavailable;
   - worker loss during an OpenCode turn;
   - queue continuation on another node; and
   - artifact upload or verification failure.
3. Assert source retention, fencing, durable checkpoint ordering, current
   generation, exact session cursor behavior, and explicit incomplete state.
4. Add bounded repeated chaos runs in CI. Use deterministic seeds and store the
   failing seed.
5. Add an optional E2B replacement scenario: kill the worker sandbox through
   the provider API, let the pool replace it, and resume the same Agent from
   its authoritative filesystem and harness state.

### Acceptance scenarios

| ID | Scenario | Evidence |
|---|---|---|
| B14 | Cut client mid-command; reattach is byte-identical or has an explicit gap | `sim` |
| B15 | Lose node before checkpoint commit; source remains retained and fenced | `sim` |
| B16 | Lose node after commit; replacement claims only the new generation | `sim` |
| B17 | Restart control; stale grants, nodes, and ready messages are refused | `sim` |
| B18 | OpenCode queue resumes on another node without repeating a completed item | `sim` |
| B19 | Kill live E2B worker; replacement resumes the Agent and cleanup is complete | optional `live-service` |

### Exit

Every recovery claim names the authoritative row, generation, checkpoint
digest, session cursor, event, and cleanup result. Unknown outcomes remain
unknown rather than being retried as success.

## 12. Phase B4: black-box standard conformance

### Work

1. Extract the semantic core into a versioned conformance manifest:
   - protocol negotiation and stable errors;
   - workspace generation and readiness;
   - claims, leases, and stale-authority rejection;
   - cursor output, input deduplication, and gaps;
   - snapshot identity, restore, move, and source retention;
   - binding placeholders, destination scoping, and leak blocking;
   - approval durability and request fingerprinting;
   - Agent inbox, transcript, wake, and resume;
   - canonical event ordering and export cursor behavior.
2. Build a black-box runner that targets:
   - a freshly built local Remount binary;
   - an already running Remount endpoint; or
   - another implementation that declares the same protocol version.
3. Keep implementation-private tests in their packages. Conformance assertions
   may observe only public protocol, HTTP, CLI, SDK, event, and diagnostic
   surfaces.
4. Publish sanitized golden fixtures and deterministic failure seeds in the
   repository.
5. Version each conformance requirement and declare whether it is required,
   capability-gated, or extension-specific.
6. Make the runner output JUnit plus the evidence JSON schema from section 6.

### Acceptance scenarios

| ID | Scenario | Evidence |
|---|---|---|
| B20 | Reference binary passes the complete required conformance manifest | `artifact` |
| B21 | A deliberately broken test implementation fails each semantic category | `code` |
| B22 | Unknown extensions remain ignorable while unknown required capabilities fail | `code` |
| B23 | A protocol-version mismatch fails before any lifecycle mutation | `code` |

### Exit

The standard is testable without importing Remount internals. Passing means an
implementation satisfies the named semantic contract, not that it is secure or
production-qualified on every backend.

## 13. Phase B5: composability pack

This phase proves that Remount inserts into existing infrastructure rather
than replacing it.

### Work

1. Add a pinned Docker Compose reference deployment for:
   - control plane and relay;
   - two nodes;
   - MinIO;
   - a local OIDC issuer;
   - a local external-secret source;
   - a signed webhook receiver; and
   - a metrics/event export receiver.
2. Add OpenTofu/Terraform modules that provision only Remount infrastructure:
   network inputs, control service, artifact store, and node-pool capacity.
   Runtime workspaces, sessions, claims, moves, and checkpoints remain outside
   Terraform state.
3. Add a Helm chart or Kustomize reference that schedules Remount nodes and
   exposes capability labels without letting Kubernetes own Remount workspace
   lifecycle.
4. Add private-network examples for VPC-only, relay, and Tailscale-sidecar
   reachability. Static validation is required; a live Tailscale account is
   not.
5. Add secret-manager adapters or fixtures only through the existing
   secret-source interface. Local tests use a pinned local server or fake.
6. Add Prometheus scrape and event-export examples. OpenTelemetry collectors
   may consume these surfaces; Remount does not need to replace them.
7. Add policy tests that reject configurations with two owners for the same
   fact, such as Terraform attempting to move a live workspace or provider
   metadata carrying a reusable credential.

### Gates

- `docker compose config` and a clean Compose smoke;
- `tofu fmt -check` and `tofu validate` without cloud credentials;
- `helm lint` and golden `helm template` output;
- pinned-image digest checks;
- local OIDC, secret-source, webhook, MinIO, metrics, and export tests; and
- documentation examples compiled or executed by CI.

### Exit

The repository contains one validated way to compose with common infrastructure
categories. It does not claim that every AWS, Azure, Kubernetes, Terraform, or
Tailscale deployment has been operated.

## 14. Phase B6: close the relay confidentiality gap

The relay currently routes authenticated frames but may observe operation
payloads. This is a meaningful difference between a dumb router and a
confidential router.

### Work

1. Write a new ADR and protocol negotiation for optional end-to-end payload
   encryption between authenticated peers.
2. Use ephemeral key agreement authenticated by existing peer identities.
   Keep long-term identity, generation, grant, and authorization checks
   unchanged.
3. Bind ciphertext to protocol version, source, destination, request ID,
   direction, and monotonic sequence so replay or cross-peer substitution
   fails.
4. Encrypt operation bodies and responses while leaving only the minimum
   routing envelope visible.
5. Rekey on reconnect and bound key, replay-window, and orphan state.
6. Fail closed when a policy requires end-to-end encryption and a peer cannot
   negotiate it.
7. Add a malicious-relay test that records, reorders, drops, replays, mutates,
   and redirects frames.
8. Document residual metadata exposure: peer IDs needed for routing, frame
   length, timing, connection timing, and traffic volume.

### Acceptance scenarios

| ID | Scenario | Evidence |
|---|---|---|
| B24 | Relay capture contains no plaintext operation payload or secret-shaped canary | `sim` |
| B25 | Mutation, replay, and cross-destination substitution are rejected | `sim` |
| B26 | Reconnect rekeys without losing cursor or idempotency semantics | `sim` |
| B27 | Required encryption with an incompatible peer fails before mutation | `code` |

### Exit

The relay remains operationally useful but is no longer trusted with operation
contents when the negotiated mode is required. This does not conceal traffic
analysis metadata.

## 15. Phase B7: isolation, scale, and release-candidate artifacts

### Work

1. Keep process and Docker lanes for local semantics and real-harness
   compatibility.
2. Make the existing gVisor exact-host lane part of the Plan B aggregate
   report, including deny-first setup, raw socket and DNS/proxy exfiltration,
   synchronous revoke, cleanup, and candidate identity.
3. Keep Firecracker `unavailable` unless the test host has the exact KVM,
   jailer, guest, volume, and network prerequisites. Unit and simulation tests
   may not upgrade that status.
4. Run bounded scale scenarios with deterministic workloads:
   - 10,000 simultaneous reconnecting cursors;
   - provider pool bursts and drains;
   - session-log spill and artifact tiering;
   - snapshot chunk deduplication;
   - event/export backpressure; and
   - quota rejection and recovery.
5. Enforce memory, goroutine, descriptor, artifact, log, event, and retained
   state ceilings. Every dropped or rejected unit needs a metric and explicit
   result.
6. Build static binaries twice in clean containers and compare checksums.
7. Build the OCI archive, local Python wheel, local npm tarball, Go external
   module, SBOM, and checksum/signature test bundle without publishing them.
8. Install every artifact into a clean disposable environment and run the
   black-box conformance smoke.

### Acceptance scenarios

| ID | Scenario | Evidence |
|---|---|---|
| B28 | Exact gVisor candidate passes isolation and enforced-gateway conformance | `host-ci` |
| B29 | Firecracker reports either exact-host pass or `unavailable` with reason | `host-ci` |
| B30 | Reconnect and pool bursts stay within declared resource ceilings | `code` |
| B31 | Two clean builds produce the declared reproducible artifacts | `artifact` |
| B32 | Clean installs pass the black-box smoke without source-tree imports | `artifact` |

### Exit

The candidate has reproducible, installable artifacts and bounded scale
evidence. Nothing is publicly released by this phase.

## 16. Aggregate completion gate

Add one command, for example:

```sh
make plan-b
```

It must:

1. refuse a dirty tree unless explicitly run in developer mode;
2. record the exact commit;
3. run B0 through the required B1-B7 local, simulation, host-CI-compatible,
   conformance, and artifact checks;
4. detect optional E2B and OpenAI credentials by presence only;
5. run available live lanes without echoing secret values;
6. verify cleanup after every external resource;
7. generate JSON, JUnit, and Markdown summaries;
8. fail on a required failure, stale generated output, leaked canary, or
   cleanup failure; and
9. succeed with optional lanes unavailable only if the summary visibly lists
   each unavailable claim and its missing prerequisite.

The final committed closure entry links to the command and scenario definitions.
Runtime evidence remains a candidate-scoped CI artifact.

## 17. Ticket and merge order

Keep pull requests small and dependency ordered:

1. evidence schema, scenario registry, and aggregate reporter;
2. local OpenAI-compatible server and deterministic OpenCode lane;
3. OpenCode detach, approval, sleep, move, and resume scenarios;
4. E2B fixture contract and fake-service hardening;
5. candidate E2B template builder and self-contained live harness;
6. E2B replacement and cleanup scenarios;
7. black-box conformance manifest and runner;
8. Docker Compose local integration stack;
9. OpenTofu/Terraform and Helm validation;
10. relay end-to-end encryption ADR, negotiation, implementation, and tests;
11. bounded scale scenarios;
12. clean artifact build/install and final closure regeneration.

Protocol or security changes receive their own ADR and negotiation tests.
Provider work stays under `internal/provision/e2b` and tooling directories.
OpenCode-specific behavior stays in its recipe, fixtures, and integration tests
rather than entering the protocol core.

## 18. Explicit cuts

The following are not required by this Plan B:

- Lakeside;
- customer migration or a design partner;
- a broad agent-product adapter matrix;
- a broad cloud-provider matrix beyond E2B as the preferred live reference;
- winning on compute cost;
- a cost-based scheduler or compute marketplace;
- Terraform as a workspace lifecycle engine;
- a second agent reasoning loop;
- a new chat product, memory store, skills registry, or generic workflow engine;
- generic exactly-once external business effects;
- writable distributed shared filesystems;
- active-active global control-plane consensus;
- process-memory snapshots;
- transparent live migration;
- process-memory forks that duplicate grants, identity, sockets, or broker state;
- UFFD or demand-paged lazy restore;
- cells, registries, or extension marketplaces;
- a claim that E2B upgrades process or Docker isolation;
- mandatory WorkOS, Slack, GitHub App, Temporal Cloud, Vault Cloud, or public
  package registry tests;
- external pentesting or certification; or
- a public release, tag, image push, package publish, or signing-identity action.

An advanced capability may be proposed later only with its authority model,
generation behavior, durable commit point, failure semantics, bounds, and
conformance surface.

## 19. Definition of done

Plan B is complete when:

- every required B1-B32 scenario has a passing repository-executable proof;
- optional live rows are passing or explicitly `unavailable`;
- `make plan-b` works from a clean checkout;
- the black-box conformance runner passes the built reference binary;
- OpenCode completes the deterministic brokered model workflow and resumes
  after movement;
- the fake E2B contract and all provider failure paths pass under race;
- the live E2B lane, when available, creates all prerequisites automatically
  and verifies complete cleanup;
- no reusable secret or synthetic secret canary appears in prohibited state;
- relay payload confidentiality passes its malicious-relay suite;
- gVisor status comes only from the exact host lane;
- Firecracker remains fail-closed unless its exact host lane ran;
- reproducible local release artifacts install and pass a smoke;
- generated docs and evidence mappings are current; and
- the closure document lists residual architectural and external gates without
  calling them fixed.

## 20. What Remount becomes

Completing this plan makes Remount:

- a reproducible reference implementation of the agent-to-computer boundary;
- demonstrably compatible with a real preferred harness, OpenCode;
- demonstrably secret-blind for OpenAI-shaped model traffic;
- testable against a deterministic model and provider without credentials;
- automatically exercisable on E2B when a key exists;
- failure-tested across disconnect, movement, restart, replacement, and stale
  authority;
- independently testable through a black-box conformance runner;
- composable with the infrastructure categories users already operate;
- confidential from its relay at the operation-payload layer; and
- buildable into clean, reproducible, locally installable artifacts.

That is enough for Remount to be a serious open-standard candidate. It is not
enough to claim universal production readiness or ecosystem adoption.

## 21. What remains after code-only Plan B

These require evidence or coordination that a repository cannot manufacture:

1. Repeated qualification on named production hosts and exact backend
   versions, especially Firecracker and any combined E2B-plus-isolated path.
2. Sustained live E2B and OpenAI compatibility across provider API changes.
3. Independent security review and penetration testing.
4. Real customer production operation, incident history, SLOs, upgrades,
   rollback, backup, disaster recovery, and on-call evidence.
5. Independent implementations passing the conformance suite.
6. Neutral governance, extension allocation, compatibility policy, and a
   multi-party protocol evolution process.
7. Published packages and signed releases exercised through their public
   distribution systems.
8. Compliance mappings, attestations, contractual support, and managed-service
   operations.
9. Evidence that the standard reduces integration work for harness and compute
   implementers outside this repository.

Those are adoption and production-proof tracks. They must never block
repository engineering, and repository engineering must never pretend to have
completed them.
