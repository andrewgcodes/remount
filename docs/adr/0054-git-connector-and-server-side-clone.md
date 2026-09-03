# ADR 0054: A typed git connector and a clone that happens before `ws.ready`

## Status

Accepted.

## Context

Most agent runs start from a repository. Until now the only way to reach one
from a workspace was the generic `/d/<host>/` reverse proxy with a
`url.<broker>/d/github.com/.insteadOf` rewrite. That works — smart HTTP is
plain GET/POST — but it is the wrong shape for three reasons:

- **It authorizes a host, not a repository.** A binding that covers
  `github.com` lets the workspace fetch or push any repository the token can
  see, call the REST API, download release assets and hit LFS.
- **The checkout happened inside the workspace.** A `git clone` run as the
  first session means the workspace was `claimed` while empty; a client that
  attached early saw no files, and a crash mid-clone left a half tree the
  next node would happily adopt.
- **The credential shape was wrong.** The recipe expected a long-lived PAT in
  a binding. GitHub Apps mint installation tokens that last an hour, which is
  what a broker should hold.

Related repositories were read for their approach and mostly do the opposite:
the sandbox providers surveyed clone with a token injected into the URL or
into `~/.git-credentials` inside the sandbox, and several rely on git's
credential helper to hand the secret to any process in the container.

## Decision

1. **`connector: "git"` is a typed capability** (`internal/connector/git.go`)
   reached only at `$REMOUNT_GIT_CONNECTOR/<host>/<owner>/<repo>[.git]/…`. The
   grammar is exactly smart HTTP: `GET info/refs?service=git-{upload,receive}-pack`
   and `POST git-{upload,receive}-pack` with an empty query. Dumb-HTTP object
   paths, `/api/`, `/raw/`, LFS and anything with userinfo, a traversal segment
   or an unexpected query are refused before the placeholder is substituted.
   Redirects are never followed. Request and response headers are allowlisted
   (`Cookie`, `X-Forwarded-For`, `Set-Cookie` do not cross). Request and
   response bodies are bounded by the rule's byte limits.
2. **A git rule scopes by `repos`** (`owner/name` or `owner/*`), never by
   `path_prefixes`, and by `push: bool`. Push is denied at the advertisement
   step, so `git push` fails before a packfile leaves the workspace. A rule
   that claims `immutable_read`, non-HTTPS, or a method other than GET/POST is
   rejected at policy validation.
3. **A workspace with `spec.repo` gets an implicit rule** covering exactly that
   repository (fetch and push) when the policy declares no typed git rule.
   Declaring a repository is declaring the intent to work on it; nothing else
   is opened.
4. **The node clones before `ws.ready`** (`internal/node/repo.go`). The clone
   runs through the workspace backend so it lands in the tree with the tree's
   ownership, but the credential path is the node's own broker with the
   workspace's binding placeholder: the token is substituted at the node edge.
   Git configuration is delivered as `GIT_CONFIG_*` environment (insteadOf
   routing, `credential.helper=`, `GIT_TERMINAL_PROMPT=0`,
   `protocol.allow=never` except HTTP/HTTPS) and regenerated on every
   materialization, because the broker address changes on every move. The
   clone verifies `git rev-parse --verify HEAD` and emits `repo.cloned{commit}`.
5. **A failed clone is not a workspace.** The node revokes the tree's network,
   destroys the fresh tree, and releases with `failed:true`. A tree whose
   completion marker (`.remount/repo`) is missing is discarded on adoption
   rather than served empty. Control holds a workspace out of placement after
   a failed materialization with a delay that doubles from 1s to 30s and
   resets on the next `ws.ready`; previously a clone that could never succeed
   was re-offered the instant it was released and burned hundreds of
   generations a second.
6. **Restores never re-clone.** `repo` is mutually exclusive with `base` and
   `restore_from`; a move or wake carries the checkout in the snapshot.
7. **The `/d/` stopgap stays documented** as what it is: the generic
   substitution path with none of the grammar above.

## Consequences

- `ws create --repo URL[@REF] [--repo-depth N]` and `run --repo` exist. The
  CLI refuses `--repo` alongside `--dir` or `--base`.
- `NodeInfo.connectors` must include `git` for a workspace with a git rule or
  a `spec.repo`; the scheduler fails closed otherwise.
- Every audit from the connector carries `connector: git`, `op: fetch|push`
  and `repo: owner/name`, so "which repositories did this agent push to" is
  one event query.
- The error a failed clone carries into `ws.released` has the broker
  capability URL redacted; git echoes the URL it dialed.
- `github-app://<app_id>/<installation_id>` as a binding *source* (an
  installation token minted by the broker, cached until expiry, never
  persisted) is the intended credential for GitHub. The connector, the
  placeholder path and the leak scans are shaped for it; the minting client is
  Phase 3.5 work and until then the binding holds a token an operator minted.
- Tests: connector grammar and budgets against an in-process `git
  http-backend` (`internal/connector/gittest`), clone and push through a real
  broker (`internal/broker/git_test.go`), and sim tests that clone at
  materialize, scan the tree, `.remount/env`, events and session environment
  for the token (proving the scan with a planted canary), fail a clone without
  ever reaching `ws.claimed`, retry it with backoff, and move a cloned
  workspace without re-cloning.
