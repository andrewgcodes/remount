---
name: remount-use
description: Use when operating Remount as a user: running coding agents, retrieving their files, managing workspaces, deploying the Modal reference, or diagnosing a live deployment. Not for implementation changes.
---

# Use Remount

`docs/using-remount.md` is the canonical user journey. Read it before operating
Remount, then read the linked detailed guide for the selected workflow:

- `docs/tutorial.md` for workspace creation, files, sessions, push/pull,
  snapshots, moves, sleep, wake, and events;
- `docs/harness-integration.md` for recipes, durable Agents, ACP, provider
  bindings, handoff, queues, and custom harnesses;
- `docs/operations.md` for remote deployments, identity, nodes, backups,
  security profiles, pools, and incident response;
- `docs/api.md` for the Agent HTTP API.

Use `remount help` and `remount COMMAND --help` to confirm syntax against the
installed binary. Never guess a flag from memory.

## Operating rules

1. Set `REMOUNT_SERVER` and, for a remote deployment, `REMOUNT_TOKEN` in the
   environment. Never put a bearer in a command argument on a shared host.
2. Keep provider credentials in server/node secret storage. Workspaces receive
   binding references and broker URLs, not reusable keys.
3. Treat the process backend as unisolated and Docker egress as cooperative.
   Never claim a backend enforces a security profile it reports unsupported.
4. A successful launch or API response is not task completion. Verify the
   Agent state, transcript, resulting file or diff, command exit, and durable
   events.
5. Retrieve changes explicitly with `remount agent diff`, `remount fs read`, or
   `remount pull`. Agent changes are not automatically copied to the host.
6. For provider-backed nodes, verify readiness with an authenticated Remount
   operation and an online enrolled node. A provider credential check is not a
   Remount integration test.
7. Destroy disposable Agents and workspaces before tearing down their node or
   provider resources. Verify cleanup by listing both Remount and provider
   inventories.
8. Report unavailable prerequisites separately from passed and failed checks.

## Documentation maintenance

When a task changes a user-visible command, recipe, provider, deployment,
security behavior, file-transfer rule, or lifecycle outcome, update
`docs/using-remount.md` and its linked detailed guide in the same change. Run
`make docs`, then `make lint`; do not edit generated `llms.txt` or
`llms-full.txt` by hand.
