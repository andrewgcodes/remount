# remount.dev vanity import site

The Go module path is `remount.dev/remount`. `go get` and `go install`
resolve it by fetching `https://remount.dev/remount?go-get=1` and reading the
`go-import` meta tag in this directory's `index.html`, which points at the
GitHub repository. `install.sh` is served from the same host so the documented
one-liner works.

This is a static site with no build step and no runtime. It is the only
piece of Remount that is hosted, and it hosts nothing but two meta tags and a
redirect.

## Deploy on Vercel

Deployed on 2026-09-05 as project `remount-vanity` under the Vercel scope
`andrew-gaos-projects-7ae0e473`, with `remount.dev` and `get.remount.dev`
attached and verified; `https://remount.dev/remount/cmd/remount?go-get=1`
answers 200 with the meta tag. The domain is registered through Vercel
Domains, so DNS is managed there. The `*.vercel.app` deployment URLs sit
behind Vercel's deployment protection and redirect to a login; only the
custom domains are public, which is what Go and `curl` use.

To redeploy after editing this directory:

```sh
npm i -g vercel            # once
cd deploy/vanity
vercel login
vercel link --yes --scope andrew-gaos-projects-7ae0e473 --project remount-vanity
vercel deploy --prod --yes --scope andrew-gaos-projects-7ae0e473
```

The first-time setup was `vercel domains add remount.dev remount-vanity` and
the same for `get.remount.dev`; both are already assigned.

Then verify from any machine:

```sh
curl -sS 'https://remount.dev/remount?go-get=1' | grep go-import
go install remount.dev/remount/cmd/remount@main    # works once the repository is public
```

`go install` also needs the repository to be reachable over `https` without
credentials, which means public, or a tagged release once one exists.

## What each rewrite is for

- `/remount` and `/remount/*`: Go requests the full import path of whatever
  package it is fetching (`/remount/cmd/remount?go-get=1`), so every subpath
  must answer with the same meta tag.
- `/install.sh`: proxies the installer from the repository's `main` branch.
  Point `get.remount.dev` at the same project so
  `curl -fsSL https://get.remount.dev/install.sh | sh` matches
  `docs/releases.md`. While the repository is private the raw URL answers
  404, so `get.remount.dev/install.sh` does too; once it is public the
  installer will run and, until a release is tagged, truthfully report that
  no release exists.
