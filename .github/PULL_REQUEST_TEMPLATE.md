## What changed

<!-- One paragraph. Name the boundary you touched (lifecycle, broker,
     backend, protocol, SDK, docs) and the user-visible effect. -->

## Why

<!-- The problem, or a link to the issue or ADR. -->

## Proof

<!-- Tests added and the exact command that runs them. For a lifecycle,
     credential, browser or isolation change, the live lane you ran and its
     entry in docs/engineering/verification-2026-09.md. An unavailable lane
     is stated as unavailable, never as a pass. -->

## Checklist

- [ ] `make verify` is green locally (or `make verify-fast` plus the named gates this change needs)
- [ ] Every state change emits an event; every mutating request has an idempotency key
- [ ] Wire changes are additive, `spec/PROTOCOL.md` is updated, `go run ./cmd/protogen --check` passes
- [ ] User-visible changes update `docs/using-remount.md` and the linked doc, then `make docs`
- [ ] New design decisions are a new ADR; existing ADRs are untouched
- [ ] No credential, host path or private endpoint in code, fixtures, logs or docs
