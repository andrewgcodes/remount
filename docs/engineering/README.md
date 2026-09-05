# Engineering reviews and requests

This directory contains point-in-time engineering assessments and
implementation requests, alongside a maintained status index, verification
ledger and review playbook. Historical requests are planning inputs, not
statements that the described controls already exist.

## Documents

### [Current implementation status](./current-status.md) — start here

The maintained index separates implemented features, stock integration gaps,
host/provider prerequisites and dated verification. Use it with current code
and the verification ledger; the historical closure below is not a release-wide
feature inventory.

### [Codebase review and remediation handoff — 2026-09-04](./review-handoff-2026-09-04.md)

Report-only review with eleven reproduced failing invariants, a local-verification
coverage concern, exact source locations, self-contained diagnostic reproducers,
and remediation requirements. It distinguishes current findings from inherited
PR #7 issues and historical evidence. No bugs are fixed by this document; read
its finding register alongside the earlier implementation closure.

### [Hardening implementation closure — 2026-09-03](./implementation-closure-2026-09-03.md) — historical closure

Its `2026-09 build plan disposition` section records the Phase 2–6 build plan
at that implementation pass: every numbered item, every
acceptance scenario, the gates that were run, the defects found and fixed, and
the residual gaps with a named owner and exact next proof for each. Read the
two handoffs below for the requirements it dispositions.

### [Plan B evidence model and aggregate gate](./plan-b-evidence.md)

How `make plan-b` decides what a candidate has actually proven: the evidence
record schema, the scenario registry, the three outcomes (`passed`, `failed`,
`unavailable` — there is no `skipped-success`), and why a recorded outcome from
a document is provenance rather than a pass. `go run ./cmd/evidence list` shows
every registered scenario, its owning proof and what is still open.

### [Repository-executable Plan B — 2026-09-03](./plan-b-repository-executable-2026-09-03.md)

The proposed next implementation plan. It replaces people-, customer-, and
vendor-coordination gates with repository code, deterministic fixtures,
failure simulation, black-box conformance, and reproducible artifacts. E2B is
the preferred optional live compute lane; OpenCode with OpenAI through the
secret-blind broker is the preferred harness lane.

### [Codex continuation handoff — 2026-09-03](./handoff-2026-09-03-codex-wrap.md)

The continuation ledger for the Phase 2–6 implementation pass. It
records completed code and tests, integration/CI incidents, the exact remaining
local and external gates, and a safe ownership/verification order. Read it for
the requirements of the pass that followed the quota-bounded Codex session; its
current disposition is in the closure document above.

### [Build handoff — 2026-09-03](./handoff-2026-09-03.md)

The originating build plan: what Remount is, what the Phase 0 / 1 / 1A build
delivered (with commits, ADRs and the honest state of every acceptance
scenario), the reconnect fix merged in PR #6, and Phases 2–6 with their
verification lanes, secret-name inventory and safe parallel-work contracts. It
defines the numbered items; the closure document above records what each one's
status now is.

### [Plan B B1: the deterministic model lane](./plan-b-b1-model-lane.md)

How the keyless OpenCode lane works: a bounded local OpenAI-compatible server,
a real pinned harness in a docker workspace, and the broker path that keeps the
workspace holding a placeholder. Read it before changing the harness pin or the
model fake's script.

### Platform handoffs — [Linux](./handoff-linux-host-2026-09-03.md) · [Windows](./handoff-windows-host-2026-09-03.md)

What could not be proven on a macOS arm64 host, split by the platform that can
prove it. Each item names the exact missing prerequisite and the exact command,
so neither document needs re-derivation.

Linux carries gVisor, Firecracker KVM, the live E2B pool lanes, the
docker/gVisor scale matrix, and Terraform/Helm validation. Windows records the
prerequisites that were missing on that macOS host. Native Windows verification
and later Linux/KVM/gVisor work are now recorded in the monthly verification
ledger; those original unavailable results are historical, not claims that
the lanes have never run.

### [External and live verification ledger — 2026-09](./verification-2026-09.md)

The point-in-time record of external verification runs: the exact command, the
provider identifiers used, the observed result and the teardown. Every entry is
marked verified, verified but bounded, unavailable (externally gated), or not
attempted.

Read it before repeating a vendor run, and add to it whenever one is performed.
An entry that says unavailable means the proof could not run; it is never
evidence that the behavior works. Absence of an entry is not a pass.

### [Hardening lessons and review playbook](./hardening-lessons.md)

A living synthesis of the failure patterns behind the audits and fixes. It
explains authority and commit boundaries, post-lock revalidation, goroutine
joining, bounded-state design, truthful diagnostics, the test ladder, live
E2B/Modal validation, shared-worktree hygiene and the limits that must remain
explicit.

Use it when designing or reviewing a new change. The repo-scoped
[remount-hardening-review
skill](../../.agents/skills/remount-hardening-review/SKILL.md) turns the same
lessons into an executable agent workflow.

### [Hardening implementation closure — 2026-09-03](./implementation-closure-2026-09-03.md)

The dated disposition and verification ledger for the two reviews and two
implementation requests below. It maps every numbered audit finding, every
surviving adversarial finding, the incident-hardening requirements and the
production-readiness gates to implementation evidence or an explicit residual
external/architectural gate.

This entry is listed in full at the top of this file; the older documents
remain unchanged as point-in-time evidence of what was originally found and
requested.

### [Adversarial review — 2026-09-02](./adversarial-review-2026-09-02.md)

Six independent reviewers audited one dimension each, and every candidate
defect was then handed to a separate agent instructed to refute it. Thirty
seven findings survived that pass, three of them critical, several with
reproduced proofs of concept.

Read it to understand the original failure modes and proofs of concept. Its
findings were open when written; current dispositions are recorded in the
2026-09-03 implementation closure.

### [Code audit — 2026-09-02](./code-audit-2026-09-02.md)

A point-in-time audit of the current implementation, including reproduced test
and race failures, correctness and security findings, ruled-out suspects,
recommended fixes and regression tests.

Use it as the original bug register. Re-check each finding against the current
symbol when reviewing regressions because this codebase is changing quickly;
current dispositions are recorded in the 2026-09-03 implementation closure.

### [OpenAI / Hugging Face incident hardening SDMR](./openai-huggingface-hardening-sdmr.md)

A system design modification request derived from the July 2026 incident. It
defines the threat model, invariants, immediate correctness blockers, required
security architecture, protocol migration, rollout order and acceptance tests.

Use it as the normative threat-model input for isolation, egress, broker
identity, authorization, lease fencing, durable release, authoritative events
and fleet containment. Implementation status is in the closure document.

### [Production-readiness request](./production-readiness-request.md)

The consolidated “bring Remount to 10/10” engineering request. It contains the
point-in-time ratings, release gates, prioritized engineering work, test and
operations requirements, suggested ticket order and evidence required for
sign-off.

Use it as the original master readiness checklist. The security section
delegates its detailed design to the incident hardening SDMR above; the closure
document separates completed code work from external release gates.

## Status language

- **Proposed:** reviewed enough to estimate and split into tickets, but not yet
  accepted as an implementation plan.
- **Accepted:** technical lead has approved the requirements and recorded any
  exceptions.
- **In progress:** implementation has started; completed requirements should
  link to tests or pull requests.
- **Verified:** all acceptance criteria pass in the target deployment profile.
- **Superseded:** replaced by a newer dated document or ADR.

## How to use these documents

1. Confirm findings against the current code before implementation; line
   numbers and behavior may change.
2. Resolve or explicitly reject each P0 requirement.
3. Convert accepted sections into small, reviewable tickets.
4. Add the acceptance test with the implementation, not afterward.
5. Link completed tickets back to the requirement and evidence.
6. Update product documentation only after the relevant behavior is verified.

Architecture decisions that become stable should be recorded in `docs/adr/`.
User-facing behavior belongs in the README, tutorial, protocol specification or
operations guide. Keep this directory focused on assessments, requirements and
verification evidence.

The current-status index, verification ledger and hardening playbook are
maintained documents: update the index when implementation boundaries change,
append new proof to the ledger, and update the playbook when
a completed investigation reveals a reusable engineering method. Keep
point-in-time findings and their original severity unchanged, and record newly
accepted architecture in a new ADR.
