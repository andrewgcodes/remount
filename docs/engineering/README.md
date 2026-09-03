# Engineering reviews and requests

This directory contains point-in-time engineering assessments and
implementation requests. These documents are internal planning inputs, not
statements that the described controls already exist.

## Documents

### [Repository-executable Plan B — 2026-09-03](./plan-b-repository-executable-2026-09-03.md)

The proposed next implementation plan. It replaces people-, customer-, and
vendor-coordination gates with repository code, deterministic fixtures,
failure simulation, black-box conformance, and reproducible artifacts. E2B is
the preferred optional live compute lane; OpenCode with OpenAI through the
secret-blind broker is the preferred harness lane.

### [Codex continuation handoff — 2026-09-03](./handoff-2026-09-03-codex-wrap.md)

The current continuation ledger for the Phase 2–6 implementation pass. It
records completed code and tests, integration/CI incidents, the exact remaining
local and external gates, and a safe ownership/verification order. Start here
for work after the quota-bounded Codex implementation session, then consult the
original build handoff for the full requirements.

### [Build handoff — 2026-09-03](./handoff-2026-09-03.md)

The current status document for the build plan: what Remount is, what the
Phase 0 / 1 / 1A build delivered (with commits, ADRs and the honest state of
every acceptance scenario), the reconnect fix merged in PR #6, and the
remaining Phases 2–6, verification lanes and secret-name inventory, and safe
parallel-work contracts. Start here.

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

The current disposition and verification ledger for the two reviews and two
implementation requests below. It maps every numbered audit finding, every
surviving adversarial finding, the incident-hardening requirements and the
production-readiness gates to implementation evidence or an explicit residual
external/architectural gate.

Use this as the current status document. The older documents remain unchanged
as point-in-time evidence of what was originally found and requested.

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

The hardening playbook is the one intentional living exception: update it when
a completed investigation reveals a reusable engineering method. Keep
point-in-time findings and their original severity unchanged, and record newly
accepted architecture in a new ADR.
